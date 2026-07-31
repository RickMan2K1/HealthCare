import sys
import awsglue.transforms as GTr
from awsglue.utils import getResolvedOptions
from pyspark.core.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

# Initialize Glue and Spark contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# -------------------------------------------------------------------
# 1. PATH CONFIGURATIONS
# -------------------------------------------------------------------
BRONZE_BASE = "s3://healthcare-rick/bronze/"
SILVER_BASE = "s3://healthcare-rick/silver/"
QUARANTINE_BASE = "s3://healthcare-rick/quarantine/"

# Create dedicated Data Catalog Databases for Silver and Quarantine
# spark.sql("CREATE DATABASE IF NOT EXISTS healthcare_silver")
# spark.sql("CREATE DATABASE IF NOT EXISTS healthcare_quarantine")

# Helper Function: Strip double quotes from all string columns in a DataFrame
def strip_quotes(df):
    string_cols = [field.name for field in df.schema.fields if field.dataType.typeName() == 'string']
    for col_name in string_cols:
        df = df.withColumn(col_name, F.regexp_replace(F.col(col_name), '"', ''))
        df = df.withColumn(col_name, F.trim(F.col(col_name)))
    return df

# -------------------------------------------------------------------
# 2. PROCESS DAILY NURSE STAFFING (FACT TABLE)
# -------------------------------------------------------------------
df_pbj_raw = spark.read.parquet(f"{BRONZE_BASE}PBJ_Daily_Nurse_Staffing_Q2_2024/")

# Data Quality Validation & Isolation Rules
df_pbj_validated = df_pbj_raw.withColumn(
    "quarantine_reason",
    F.when(
        F.col("PROVNUM").isNull() | (F.trim(F.col("PROVNUM")) == ""), 
        F.lit("MISSING_PROVIDER_NUM")
    ).when(
        F.to_date(F.col("WorkDate").cast("string"), "yyyyMMdd").isNull(),
        F.lit("INVALID_WORK_DATE")
    ).when(
        F.col("MDScensus").cast(DoubleType()).isNull() | (F.col("MDScensus") < 0),
        F.lit("INVALID_CENSUS_VALUE")
    ).otherwise(F.lit(None))
)

# Route Quarantine / Bad Records
df_pbj_quarantine = df_pbj_validated.filter(F.col("quarantine_reason").isNotNull())
(
    df_pbj_quarantine
    .withColumn("quarantined_at", F.current_timestamp())
    .write
    .mode("append")
    .partitionBy("quarantine_reason")
    .parquet(f"{QUARANTINE_BASE}daily_nurse_staffing_bad_records/")
)

# Process Valid Silver Daily Staffing Records
df_pbj_clean = (
    df_pbj_validated
    .filter(F.col("quarantine_reason").isNull())
    .drop("quarantine_reason")
    # Harmonize Key and Dates
    .withColumn("CCN", F.lpad(F.trim(F.col("PROVNUM").cast("string")), 6, "0"))
    .withColumn("WorkDate", F.to_date(F.col("WorkDate").cast("string"), "yyyyMMdd"))
    .withColumn("State", F.trim(F.col("STATE")))
    
    # Cast Numeric Fields & Handle Nulls
    .withColumn("MDScensus", F.col("MDScensus").cast(DoubleType()))
    .withColumn("Hrs_RN", F.coalesce(F.col("Hrs_RN").cast(DoubleType()), F.lit(0.0)))
    .withColumn("Hrs_LPN", F.coalesce(F.col("Hrs_LPN").cast(DoubleType()), F.lit(0.0)))
    .withColumn("Hrs_CNA", F.coalesce(F.col("Hrs_CNA").cast(DoubleType()), F.lit(0.0)))
    .withColumn("Hrs_RN_emp", F.coalesce(F.col("Hrs_RN_emp").cast(DoubleType()), F.lit(0.0)))
    .withColumn("Hrs_RN_ctr", F.coalesce(F.col("Hrs_RN_ctr").cast(DoubleType()), F.lit(0.0)))
    .withColumn("Hrs_LPN_emp", F.coalesce(F.col("Hrs_LPN_emp").cast(DoubleType()), F.lit(0.0)))
    .withColumn("Hrs_LPN_ctr", F.coalesce(F.col("Hrs_LPN_ctr").cast(DoubleType()), F.lit(0.0)))
    .withColumn("Hrs_CNA_emp", F.coalesce(F.col("Hrs_CNA_emp").cast(DoubleType()), F.lit(0.0)))
    .withColumn("Hrs_CNA_ctr", F.coalesce(F.col("Hrs_CNA_ctr").cast(DoubleType()), F.lit(0.0)))
    
    # Calculate Derived Silver Metrics
    .withColumn("Total_Nurse_Hours", F.col("Hrs_RN") + F.col("Hrs_LPN") + F.col("Hrs_CNA"))
    .withColumn("Total_Contract_Hours", F.col("Hrs_RN_ctr") + F.col("Hrs_LPN_ctr") + F.col("Hrs_CNA_ctr"))
    .withColumn("Nurse_Hours_Per_Resident_Day", 
                F.when(F.col("MDScensus") > 0, F.col("Total_Nurse_Hours") / F.col("MDScensus"))
                .otherwise(0.0))
    .dropDuplicates(["CCN", "WorkDate"])
)

# Write Silver Daily Staffing
(
    df_pbj_clean.write
    .mode("overwrite")
    .partitionBy("State")
    .parquet(f"{SILVER_BASE}silver_daily_nurse_staffing/")
)

# -------------------------------------------------------------------
# 3. PROCESS PROVIDER INFORMATION (DIMENSION TABLE)
# -------------------------------------------------------------------
df_provider_raw = spark.read.parquet(f"{BRONZE_BASE}NH_ProviderInfo_Oct2024/")
df_provider_stripped = strip_quotes(df_provider_raw)

df_dim_provider = (
    df_provider_stripped
    .withColumn("CCN", F.lpad(F.col("CMS Certification Number (CCN)"), 6, "0"))
    .select(
        F.col("CCN"),
        F.col("Provider Name").alias("Provider_Name"),
        F.col("City/Town").alias("City"),
        F.col("State"),
        F.lpad(F.col("ZIP Code"), 5, "0").alias("ZIP_Code"),
        F.col("County/Parish").alias("County"),
        F.col("Ownership Type").alias("Ownership_Type"),
        F.coalesce(F.col("Number of Certified Beds").cast(IntegerType()), F.lit(0)).alias("Certified_Beds"),
        F.col("Total nursing staff turnover").cast(DoubleType()).alias("Total_Nursing_Turnover_Pct"),
        F.col("Registered Nurse turnover").cast(DoubleType()).alias("RN_Turnover_Pct"),
        F.col("Latitude").cast(DoubleType()).alias("Latitude"),
        F.col("Longitude").cast(DoubleType()).alias("Longitude")
    )
    .filter(F.col("CCN").isNotNull() & (F.col("CCN") != ""))
    .dropDuplicates(["CCN"])
)

# Write Silver Provider Dimension
(
    df_dim_provider.write
    .mode("overwrite")
    .parquet(f"{SILVER_BASE}silver_dim_provider/")
)

# -------------------------------------------------------------------
# 4. PROCESS HEALTH CITATIONS (DIMENSION/EVENTS TABLE)
# -------------------------------------------------------------------
df_citations_raw = spark.read.parquet(f"{BRONZE_BASE}NH_HealthCitations_Oct2024/")
df_citations_stripped = strip_quotes(df_citations_raw)

df_citations_clean = (
    df_citations_stripped
    .withColumn("CCN", F.lpad(F.col("CMS Certification Number (CCN)"), 6, "0"))
    .select(
        F.col("CCN"),
        F.to_date(F.col("Survey Date"), "yyyy-MM-dd").alias("Survey_Date"),
        F.col("Deficiency Tag Number").cast(IntegerType()).alias("Deficiency_Tag_Number"),
        F.col("Scope Severity Code").alias("Scope_Severity_Code"),
        F.col("Deficiency Description").alias("Deficiency_Description"),
        F.col("Deficiency Category").alias("Deficiency_Category"),
        F.col("Inspection Cycle").cast(IntegerType()).alias("Inspection_Cycle"),
        F.col("Standard Deficiency").alias("Is_Standard_Deficiency"),
        F.col("Complaint Deficiency").alias("Is_Complaint_Deficiency")
    )
    .filter(F.col("CCN").isNotNull() & (F.col("CCN") != ""))
)

# Write Silver Health Citations
(
    df_citations_clean.write
    .mode("overwrite")
    .parquet(f"{SILVER_BASE}silver_health_citations/")
)

job.commit()