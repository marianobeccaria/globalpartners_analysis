# ingestion_job.py
# GlobalPartners Business Insights - Bronze Layer Ingestion Job
# Purpose: Read source CSVs from S3 and write raw Parquet to Bronze layer,
#          partitioned by ingestion_date. Uses overwrite-by-partition strategy
#          so reruns are idempotent - only today's partition is overwritten.
#
# Layer:   Source (CSV) → Bronze (Parquet)
# Trigger: Glue Workflow Trigger 1 — runs daily at 2:00 AM UTC
#
# Input:   s3://<bucket>/source/*.csv
# Output:  s3://<bucket>/bronze/<table>/ingestion_date=YYYY-MM-DD/

import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, lower, regexp_replace


# ── Job Initialization ─────────────────────────────────────────────────────────
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",          # mybucket
        "S3_BUCKET",         # e.g. 
        "SOURCE_PREFIX",     # e.g. source
        "BRONZE_PREFIX",     # e.g. bronze
    ],
)

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Build S3 base paths from job args
bucket        = args["S3_BUCKET"]
source_prefix = args["SOURCE_PREFIX"]
bronze_prefix = args["BRONZE_PREFIX"]

SOURCE_BASE = f"s3://{bucket}/{source_prefix}"
BRONZE_BASE = f"s3://{bucket}/{bronze_prefix}"

# Today's ingestion date — used as the partition value
# Using UTC to stay consistent with creation_time_utc in the data
ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

print(f"{'='*60}")
print(f"ingestion_job started")
print(f"Ingestion date : {ingestion_date}")
print(f"Source         : {SOURCE_BASE}")
print(f"Bronze         : {BRONZE_BASE}")
print(f"{'='*60}")


# ── Helper: normalize column names to lowercase ────────────────────────────────
# SQL Server exports columns in uppercase. We normalize to lowercase here
# in Bronze so every downstream job works with consistent column names.
def normalize_columns(df):
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.lower())
    return df


# ── Helper: write to Bronze ────────────────────────────────────────────────────
# Adds ingestion_date partition column and writes Parquet.
# spark.conf overwrite-by-partition ensures only today's partition
# is replaced on reruns, preserving all historical partitions.
def write_to_bronze(df, table_name):
    output_path = f"{BRONZE_BASE}/{table_name}"

    # Add ingestion date as a partition column
    df = df.withColumn("ingestion_date", lit(ingestion_date))

    row_count = df.count()
    print(f"\n  Writing {table_name}:")
    print(f"    Rows          : {row_count:,}")
    print(f"    Output path   : {output_path}")
    print(f"    Partition     : ingestion_date={ingestion_date}")

    # Overwrite only the partition for today — not the entire table
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df.write \
        .mode("overwrite") \
        .partitionBy("ingestion_date") \
        .parquet(output_path)

    print(f"    {table_name} written successfully")


# ── Table 1: order_items ───────────────────────────────────────────────────────
# Core transactional table — customer orders, items, prices, loyalty flag.
# Expected: ~203,519 rows, 13 columns.
print("\n── Reading order_items ──")

df_items = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{SOURCE_BASE}/order_items.csv")

df_items = normalize_columns(df_items)

print(f"  Schema:")
df_items.printSchema()
print(f"  Row count: {df_items.count():,}")

write_to_bronze(df_items, "order_items")


# ── Table 2: order_item_options ────────────────────────────────────────────────
# Add-ons and customizations per line item.
# Note: Contains 2,299 exact duplicate rows and they will be dropped in Silver
print("\n── Reading order_item_options ──")

df_options = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{SOURCE_BASE}/order_item_options.csv")

df_options = normalize_columns(df_options)

print(f"  Schema:")
df_options.printSchema()
print(f"  Row count: {df_options.count():,}")

write_to_bronze(df_options, "order_item_options")


# ── Table 3: date_dim ──────────────────────────────────────────────────────────
# Calendar dimension table.
# Note: Provided date_dim only covers 2023 and uses DD-MM-YYYY format.
#       Both issues are remediated in Silver 
print("\n── Reading date_dim ──")

df_date = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{SOURCE_BASE}/date_dim.csv")

df_date = normalize_columns(df_date)

print(f"  Schema:")
df_date.printSchema()
print(f"  Row count: {df_date.count():,}")

write_to_bronze(df_date, "date_dim")


# ── Job Summary ────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"ingestion_job completed successfully")
print(f"Ingestion date : {ingestion_date}")
print(f"Tables written : order_items, order_item_options, date_dim")
print(f"Bronze path    : {BRONZE_BASE}")
print(f"{'='*60}")

job.commit()