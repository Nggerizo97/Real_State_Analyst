# Databricks notebook source
# MAGIC %md
# MAGIC # Real Estate Data Processing (Bronze to Silver)
# MAGIC This code is geared towards Databricks Community Edition (Zero Cost).
# MAGIC It reads the raw JSON files from S3, cleans/deduplicates them, and prepares a Silver schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration

# COMMAND ----------

aws_bucket_name = "your-bronze-bucket-name"
s3_raw_path = f"s3a://{aws_bucket_name}/raw/fincaraiz/"
s3_silver_path = f"s3a://{aws_bucket_name}/silver/fincaraiz/"

# For standard Databricks with the Terraform Instance Profile, keys are not needed.
# For Community Edition, you might need to supply keys temporarily. Note: DO NOT hardcode in prod.
# spark.conf.set("fs.s3a.access.key", dbutils.secrets.get(scope = "aws", key = "aws-access-key"))
# spark.conf.set("fs.s3a.secret.key", dbutils.secrets.get(scope = "aws", key = "aws-secret-key"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read Bronze (Raw Data)

# COMMAND ----------

# Infer schema automatically from JSON structure uploaded by Playwright
try:
    df_bronze = spark.read.json(s3_raw_path)
    print(f"Loaded {df_bronze.count()} records from Bronze.")
    display(df_bronze)
except Exception as e:
    print("Bronze path might be empty or not reachable yet:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Transform & Deduplicate to Silver Layer

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import IntegerType

if 'df_bronze' in locals():
    # 2.1 Deduplication
    # Though deduplication happens at origin (S3 sync check), it is always safe to dedupe on Spark just in case 
    # of concurrent runs or schema issues.
    df_silver = df_bronze.dropDuplicates(["id_inmueble"])
    
    # 2.2 Data Cleaning
    # Filter out empty pricing
    df_silver = df_silver.filter(col("price") != "N/A")
    
    # Optional: Cast price to numeric (Requires parsing commas/symbols based on source string format)
    # Example format string "$ 250.000.000"
    from pyspark.sql.functions import regexp_replace
    df_silver = df_silver.withColumn("numeric_price", regexp_replace(col("price"), "[^\d]", "").cast("long"))
    
    # 2.3 Metadata appending
    df_silver = df_silver.withColumn("silver_processed_at", current_timestamp())
    
    print(f"Transformed to {df_silver.count()} clean records for Silver.")
    display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Load to Silver Target 
# MAGIC (Uncomment to execute if running on a cluster with write permissions to Bronze/Silver buckets)

# COMMAND ----------

# df_silver.write.mode("overwrite").parquet(s3_silver_path)
