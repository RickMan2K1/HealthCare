import os
import sys
import json
import io
import re
import uuid
import boto3

from datetime import datetime, timezone

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from pyspark.core.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# =====================================================================
# 1. INITIALIZE AWS GLUE
# =====================================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "secret_name",
        "aws_region",
        "target_folder_id",
        "bronze_bucket",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# =====================================================================
# 2. READ GOOGLE SERVICE ACCOUNT SECRET FROM AWS SECRETS MANAGER
# =====================================================================

def get_google_drive_secret(secret_name, region_name):
    """
    Retrieves the complete Google service-account JSON
    from AWS Secrets Manager.
    """

    secrets_client = boto3.client(
        "secretsmanager",
        region_name=region_name,
    )

    try:
        response = secrets_client.get_secret_value(
            SecretId=secret_name,
        )

        if "SecretString" not in response:
            raise RuntimeError(
                "The AWS secret does not contain a SecretString value."
            )

        secret_data = json.loads(response["SecretString"])

        required_keys = [
            "type",
            "project_id",
            "private_key",
            "client_email",
            "token_uri",
        ]

        missing_keys = [
            key for key in required_keys
            if key not in secret_data
        ]

        if missing_keys:
            raise KeyError(
                f"Service-account secret is missing keys: {missing_keys}"
            )

        return secret_data

    except Exception as error:
        print(
            f"Unable to retrieve Google credentials from secret "
            f"'{secret_name}': {error}"
        )
        raise


service_account_info = get_google_drive_secret(
    secret_name=args["secret_name"],
    region_name=args["aws_region"],
)


# =====================================================================
# 3. AUTHENTICATE WITH GOOGLE DRIVE
# =====================================================================

GOOGLE_DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly"
]

google_credentials = (
    service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=GOOGLE_DRIVE_SCOPES,
    )
)

drive_service = build(
    "drive",
    "v3",
    credentials=google_credentials,
    cache_discovery=False,
)


# =====================================================================
# 4. FIND CSV FILES INSIDE THE GOOGLE DRIVE FOLDER
# =====================================================================

def list_csv_files(drive_client, folder_id):
    """
    Returns all CSV files directly inside the given Google Drive folder.
    """

    csv_files = []
    page_token = None

    query = (
        f"'{folder_id}' in parents "
        "and mimeType = 'text/csv' "
        "and trashed = false"
    )

    while True:
        response = (
            drive_client.files()
            .list(
                q=query,
                fields=(
                    "nextPageToken,"
                    "files(id,name,mimeType,modifiedTime,size)"
                ),
                pageSize=1000,
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )

        csv_files.extend(response.get("files", []))

        page_token = response.get("nextPageToken")

        if not page_token:
            break

    return csv_files


target_folder_id = args["target_folder_id"]

google_drive_files = list_csv_files(
    drive_client=drive_service,
    folder_id=target_folder_id,
)

if not google_drive_files:
    raise RuntimeError(
        f"No CSV files were found in Google Drive folder "
        f"'{target_folder_id}'. Confirm that the folder is shared with "
        f"the service-account email."
    )

print(
    f"Found {len(google_drive_files)} CSV file(s) "
    f"in the Google Drive folder."
)


# =====================================================================
# 5. DOWNLOAD GOOGLE DRIVE FILES AND UPLOAD THEM TO TEMPORARY S3
# =====================================================================

def clean_file_name(file_name):
    """
    Removes characters that may create problematic S3 object keys.
    """

    cleaned_name = re.sub(
        r'[^A-Za-z0-9._-]',
        "_",
        file_name,
    )

    if not cleaned_name.lower().endswith(".csv"):
        cleaned_name = f"{cleaned_name}.csv"

    return cleaned_name


def download_google_file(drive_client, file_id):
    """
    Downloads a Google Drive file into memory.
    """

    request = drive_client.files().get_media(
        fileId=file_id,
        supportsAllDrives=True,
    )

    file_buffer = io.BytesIO()

    downloader = MediaIoBaseDownload(
        file_buffer,
        request,
        chunksize=10 * 1024 * 1024,
    )

    download_complete = False

    while not download_complete:
        _, download_complete = downloader.next_chunk()

    file_buffer.seek(0)

    return file_buffer


s3_client = boto3.client(
    "s3",
    region_name=args["aws_region"],
)

bronze_bucket = args["bronze_bucket"]

current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
run_id = str(uuid.uuid4())

temporary_prefix = (
    f"temporary/google-drive/"
    f"load_date={current_date}/"
    f"run_id={run_id}"
)

uploaded_s3_keys = []

for google_file in google_drive_files:
    file_id = google_file["id"]
    original_file_name = google_file["name"]
    safe_file_name = clean_file_name(original_file_name)

    print(f"Downloading Google Drive file: {original_file_name}")

    downloaded_file = download_google_file(
        drive_client=drive_service,
        file_id=file_id,
    )

    s3_key = f"{temporary_prefix}/{safe_file_name}"

    try:
        s3_client.upload_fileobj(
            Fileobj=downloaded_file,
            Bucket=bronze_bucket,
            Key=s3_key,
        )

        uploaded_s3_keys.append(s3_key)

        print(
            f"Uploaded '{original_file_name}' to "
            f"s3://{bronze_bucket}/{s3_key}"
        )

    except Exception as error:
        print(
            f"Failed to upload '{original_file_name}' to S3: {error}"
        )
        raise


if not uploaded_s3_keys:
    raise RuntimeError(
        "No files were uploaded to the temporary S3 location."
    )


# =====================================================================
# 6, 7, 8. PROCESS EACH CSV, ADD METADATA, AND WRITE TO PARQUET
# =====================================================================

from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit
)

print(f"Beginning processing for {len(uploaded_s3_keys)} files...")

total_records_processed = 0

# Iterate through the files one by one on the Driver node
for s3_key in uploaded_s3_keys:
    
    # --- A. Construct Paths & Names ---
    # Extract the file name (e.g., 'retail_sales.csv')
    file_name = os.path.basename(s3_key)
    
    # Strip the extension for the folder name (e.g., 'retail_sales')
    clean_table_name = os.path.splitext(file_name)[0]
    
    # Define exact source and target paths
    full_s3_source_path = f"s3://{bronze_bucket}/{s3_key}"
    bronze_output_path = f"s3://{bronze_bucket}/bronze/{clean_table_name}/"
    
    print(f"\n--- Processing: {clean_table_name} ---")
    
    # --- B. Read the Specific CSV ---
    raw_spark_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .load(full_s3_source_path)
    )

    if len(raw_spark_df.columns) == 0:
        print(f"Skipping '{file_name}': Spark could not identify any columns.")
        continue

    record_count = raw_spark_df.count()
    
    if record_count == 0:
        print(f"Skipping '{file_name}': File contains no data rows.")
        continue
        
    total_records_processed += record_count
    print(f"Read {record_count} records.")

    # --- C. Add Ingestion Metadata ---
    bronze_spark_df = (
        raw_spark_df
        .withColumn("source_file_name", input_file_name())
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", lit(current_date))
    )

    # --- D. Write to Dedicated Parquet Folder ---
    print(f"Writing to: {bronze_output_path}")
    
    (
        bronze_spark_df.write
        .format("parquet")
        .option("compression", "snappy")
        .mode("overwrite")
        .save(bronze_output_path)
    )

print(f"\nAll files processed. Total records written to Bronze: {total_records_processed}")# bronze_spark_df = (

# =====================================================================
# 9. DELETE TEMPORARY CSV FILES FROM S3
# =====================================================================

try:
    delete_objects = [
        {"Key": s3_key}
        for s3_key in uploaded_s3_keys
    ]

    s3_client.delete_objects(
        Bucket=bronze_bucket,
        Delete={
            "Objects": delete_objects,
            "Quiet": True,
        },
    )

    print("Temporary CSV files deleted from S3.")

except Exception as error:
    # Bronze processing already succeeded, so cleanup failure
    # should not fail the complete Glue job.
    print(
        f"Warning: Bronze load succeeded, but temporary files "
        f"could not be deleted: {error}"
    )


# =====================================================================
# 10. COMMIT GLUE JOB
# =====================================================================

job.commit()

print("Google Drive to S3 Bronze Glue job completed successfully.")