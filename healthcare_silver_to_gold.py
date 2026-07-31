import sys
from pyspark.core.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue and Spark contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# -------------------------------------------------------------------
# 1. PATH & CATALOG CONFIGURATIONS
# -------------------------------------------------------------------
GOLD_BASE = "s3://healthcare-rick/gold/"

# Create Data Catalog Database for Gold Layer
spark.sql("CREATE DATABASE IF NOT EXISTS healthcare_gold")

# -------------------------------------------------------------------
# 2. READ CLEAN SILVER TABLES
# -------------------------------------------------------------------
df_silver_staffing = spark.table("healthcare_silver.silver_daily_nurse_staffing")
df_silver_provider = spark.table("healthcare_silver.silver_dim_provider")

# -------------------------------------------------------------------
# 3. BUILD FACT_GOLD_MONTHLY_STAFFING
# -------------------------------------------------------------------
df_fact_monthly = (
    df_silver_staffing
    .withColumn("Year_Month", F.date_format(F.col("WorkDate"), "yyyy-MM"))
    .groupBy("CCN", "State", "Year_Month")
    .agg(
        F.round(F.avg("MDScensus"), 2).alias("Avg_Daily_Census"),
        F.round(F.sum("Total_Nurse_Hours"), 2).alias("Total_Nurse_Hours"),
        F.round(F.sum("Hrs_RN"), 2).alias("Total_RN_Hours"),
        F.round(F.sum("Hrs_LPN"), 2).alias("Total_LPN_Hours"),
        F.round(F.sum("Hrs_CNA"), 2).alias("Total_CNA_Hours"),
        F.round(F.sum("Total_Contract_Hours"), 2).alias("Total_Contract_Hours"),
        
        # Calculate Monthly HPRD (Nurse Hours Per Resident Day)
        F.round(
            F.when(F.sum("MDScensus") > 0, F.sum("Total_Nurse_Hours") / F.sum("MDScensus"))
            .otherwise(0.0), 2
        ).alias("Monthly_HPRD"),
        
        # Calculate Percentage of Hours Worked by Contractors
        F.round(
            F.when(F.sum("Total_Nurse_Hours") > 0, (F.sum("Total_Contract_Hours") / F.sum("Total_Nurse_Hours")) * 100)
            .otherwise(0.0), 2
        ).alias("Contractor_Hour_Pct")
    )
)

# Save Fact Table to S3 and Register in Glue Catalog
(
    df_fact_monthly.write
    .mode("overwrite")
    .option("path", f"{GOLD_BASE}fact_gold_monthly_staffing/")
    .partitionBy("State")
    .format("parquet")
    .saveAsTable("healthcare_gold.fact_gold_monthly_staffing")
)

# -------------------------------------------------------------------
# 4. BUILD DIM_GOLD_PROVIDER
# -------------------------------------------------------------------
df_dim_gold_provider = (
    df_silver_provider
    .select(
        "CCN",
        "Provider_Name",
        "City",
        "State",
        "ZIP_Code",
        "County",
        "Ownership_Type",
        "Certified_Beds",
        "Total_Nursing_Turnover_Pct",
        "RN_Turnover_Pct",
        "Latitude",
        "Longitude"
    )
    .dropDuplicates(["CCN"])
)

# Save Provider Dimension to S3 and Register in Glue Catalog
(
    df_dim_gold_provider.write
    .mode("overwrite")
    .option("path", f"{GOLD_BASE}dim_gold_provider/")
    .format("parquet")
    .saveAsTable("healthcare_gold.dim_gold_provider")
)

job.commit()