# bronze_to_silver_job.py
# GlobalPartners Business Insights — Bronze to Silver Transformation Job
# Purpose: Read raw Bronze Parquet, apply all EDA-identified data quality fixes,
#          join the three tables into a single enriched table, compute
#          gross_revenue, and write clean Parquet to the Silver layer.
#
# Layer:   Bronze (raw Parquet) → Silver (cleaned & joined Parquet)
# Trigger: Glue Workflow Trigger 2 — runs on success of ingestion_job
#
# Input:   s3://<bucket>/bronze/order_items/
#          s3://<bucket>/bronze/order_item_options/
#          s3://<bucket>/bronze/date_dim/
# Output:  s3://<bucket>/silver/orders_enriched/year=YYYY/month=MM/

import sys
from datetime import date, timedelta

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    month,
    to_date,
    to_timestamp,
    when,
    year,
    date_format,
    dayofweek,
    weekofyear,
)
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# ── Job Initialization ─────────────────────────────────────────────────────────
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_BUCKET",         # e.g. mybucket
        "BRONZE_PREFIX",     # e.g. bronze
        "SILVER_PREFIX",     # e.g. silver
    ],
)

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket        = args["S3_BUCKET"]
bronze_prefix = args["BRONZE_PREFIX"]
silver_prefix = args["SILVER_PREFIX"]

BRONZE_BASE = f"s3://{bucket}/{bronze_prefix}"
SILVER_BASE = f"s3://{bucket}/{silver_prefix}"

print(f"{'='*60}")
print(f"bronze_to_silver_job started")
print(f"Bronze source  : {BRONZE_BASE}")
print(f"Silver output  : {SILVER_BASE}")
print(f"{'='*60}")


# ══════════════════════════════════════════════════════════════
# STEP 1 — READ BRONZE TABLES
# Read the latest partition from each Bronze table.
# Bronze uses ingestion_date partitioning so we read the full
# table — Spark will scan all partitions automatically.
# ══════════════════════════════════════════════════════════════
print("\n── STEP 1: Reading Bronze tables ──")

# STEP 1 — READ BRONZE TABLES
df_items   = spark.read.parquet(f"{BRONZE_BASE}/order_items/")
df_options = spark.read.parquet(f"{BRONZE_BASE}/order_item_options/")
df_date    = spark.read.parquet(f"{BRONZE_BASE}/date_dim/")

# Drop ingestion_date partition column that was added by the ingestion job
# for Bronze partitioning only and is not needed in Silver or Gold.
# Avoids duplicate column error when writing Silver.
df_items   = df_items.drop("ingestion_date")
df_options = df_options.drop("ingestion_date")
df_date    = df_date.drop("ingestion_date")

print(f"  order_items        : {df_items.count():,} rows")
print(f"  order_item_options : {df_options.count():,} rows")
print(f"  date_dim           : {df_date.count():,} rows")


# ══════════════════════════════════════════════════════════════
# STEP 2 — CLEAN order_item_options
# EDA Finding #3: 2,299 exact duplicate rows detected.
# Drop them here before any joins so they don't inflate revenue.
# EDA Finding #4: 51 rows with same option_name but different
# option_group_name are legitimate — kept by dropDuplicates()
# since the full row is not identical.
# ══════════════════════════════════════════════════════════════
print("\n── STEP 2: Deduplicating order_item_options ──")

before_count = df_options.count()
df_options = df_options.dropDuplicates()
after_count = df_options.count()
dropped = before_count - after_count

print(f"  Rows before dedup  : {before_count:,}")
print(f"  Rows after dedup   : {after_count:,}")
print(f"  Duplicate rows dropped : {dropped:,}")


# ══════════════════════════════════════════════════════════════
# STEP 3 — CLEAN order_items
# EDA Finding #8: 826 rows from 'Alltown Fresh - DEVELOPMENT'
# platform are test orders. Filter them out before Silver.
# EDA Finding #6: Parse creation_time_utc as ISO8601 timestamp.
# ══════════════════════════════════════════════════════════════
print("\n── STEP 3: Cleaning order_items ──")

# Filter out DEVELOPMENT test orders
before_count = df_items.count()
df_items = df_items.filter(
    col("app_name") != "Alltown Fresh - DEVELOPMENT"
)
after_count = df_items.count()
print(f"  DEVELOPMENT rows filtered : {before_count - after_count:,}")

# Parse ISO8601 timestamp. Format varies (some with ms, some without)
# Using to_timestamp with no format arg lets Spark handle ISO8601 variants
df_items = df_items.withColumn(
    "creation_time_utc",
    to_timestamp(col("creation_time_utc"))
)

# Extract order_date (date only) for joining with date_dim
df_items = df_items.withColumn(
    "order_date",
    to_date(col("creation_time_utc"))
)

print(f"  Timestamps parsed as ISO8601")
print(f"  order_date column extracted from creation_time_utc")
print(f"  Rows after cleaning : {df_items.count():,}")

# ── STEP 3b: Clean item_category names ────────────────────────────────────────
# Source data contains corrupted category names with URLs, typos,
# and trailing characters from data entry errors in the POS system.
# Standardize these before Silver so all downstream metrics use
# clean, consistent category names.
print("\n── STEP 3b: Cleaning item_category names ──")

from pyspark.sql.functions import regexp_replace, trim

# Step 1: Strip any URLs (anything from "https" to end of string)
df_items = df_items.withColumn(
    "item_category",
    regexp_replace(col("item_category"), r"https?://\S+", "")
)

# Step 2: Strip trailing non-alpha characters (digits, backticks, etc.)
df_items = df_items.withColumn(
    "item_category",
    regexp_replace(col("item_category"), r"[^a-zA-Z\s&'\-]+$", "")
)

# Step 3: Trim whitespace
df_items = df_items.withColumn(
    "item_category",
    trim(col("item_category"))
)

# Step 4: Fix known typos explicitly
category_corrections = {
    "Sqalads":    "Salads",
    "Sandwiches": "Sandwiches",  # catches Sandwiches`1 after step 2
}

for wrong, correct in category_corrections.items():
    df_items = df_items.withColumn(
        "item_category",
        when(col("item_category") == wrong, lit(correct))
        .otherwise(col("item_category"))
    )

# Verify results
print(f"  Unique categories after cleaning : {df_items.select('item_category').distinct().count()}")
df_items.groupBy("item_category").count().orderBy("count", ascending=False).show(35, truncate=False)


# ══════════════════════════════════════════════════════════════
# STEP 4 — REMEDIATE date_dim
# Strategy:
#   1. Generate full date spine 2020–2024 (fixes coverage gap)
#   2. Reformat source date_dim date_key DD-MM-YYYY → YYYY-MM-DD
#   3. Left join spine with source holidays so we preserve
#      is_holiday and holiday_name from the original CSV
# ══════════════════════════════════════════════════════════════
print("\n── STEP 4: Regenerating date_dim for 2020–2024 ──")

# Step 4a: Generate full date spine
start_date = date(2020, 1, 1)
end_date   = date(2024, 12, 31)
all_dates  = []

current = start_date
while current <= end_date:
    all_dates.append((current,))
    current += timedelta(days=1)

date_schema = StructType([
    StructField("date_key", DateType(), False),
])

df_date_generated = spark.createDataFrame(all_dates, schema=date_schema)

# Add calendar columns
df_date_full = df_date_generated \
    .withColumn("year",        year(col("date_key"))) \
    .withColumn("month",       date_format(col("date_key"), "MMMM")) \
    .withColumn("week",        weekofyear(col("date_key"))) \
    .withColumn("day_of_week", date_format(col("date_key"), "EEEE")) \
    .withColumn("is_weekend",
        when(dayofweek(col("date_key")).isin([1, 7]), lit(True))
        .otherwise(lit(False))
    )

# Step 4b: Reformat source date_dim date_key from DD-MM-YYYY → YYYY-MM-DD
# so it can join with our generated spine
df_date_source = df_date \
    .withColumn(
        "date_key_clean",
        to_date(col("date_key"), "dd-MM-yyyy")
    ) \
    .select(
        col("date_key_clean").alias("date_key"),
        col("is_holiday"),
        col("holiday_name"),
    )

print(f"  Source date_dim holidays : {df_date_source.filter(col('is_holiday') == True).count():,}")

# Step 4c: Left join spine with source holidays
# Dates in 2020–2022 and 2024 won't match (source only covers 2023)
# — those get is_holiday=False and holiday_name=null which is correct
df_date_full = df_date_full.join(
    df_date_source,
    on="date_key",
    how="left"
) \
.withColumn(
    "is_holiday",
    when(col("is_holiday").isNull(), lit(False))
    .otherwise(col("is_holiday").cast(BooleanType()))
) \
.withColumn(
    "holiday_name",
    col("holiday_name").cast(StringType())
)

holiday_count = df_date_full.filter(col("is_holiday") == True).count()
print(f"  Generated date_dim rows  : {df_date_full.count():,}")
print(f"  Holidays preserved       : {holiday_count:,}")
print(f"  Coverage                 : {start_date} → {end_date}")

# ══════════════════════════════════════════════════════════════
# STEP 5 — JOIN order_items ↔ order_item_options
# EDA Finding #5: 99.99% match rate (15 orphan options).
# Using INNER join — the 15 orphan option rows are dropped
# automatically since they have no matching parent in order_items.
# ══════════════════════════════════════════════════════════════
print("\n── STEP 5: Joining order_items - order_item_options ──")

df_joined = df_items.join(
    df_options,
    on=["order_id", "lineitem_id"],
    how="inner"
)

print(f"  Rows after join : {df_joined.count():,}")
print(f"  (15 orphan option rows dropped by inner join)")


# ══════════════════════════════════════════════════════════════
# STEP 6 — JOIN with date_dim
# Join the enriched order data with the regenerated date_dim
# to attach calendar attributes (week, month, is_weekend, etc.)
# to every order row for downstream trend analysis.
# ══════════════════════════════════════════════════════════════
print("\n── STEP 6: Joining with date_dim ──")

df_enriched = df_joined.join(
    df_date_full,
    on=df_joined["order_date"] == df_date_full["date_key"],
    how="left"
)

# Drop redundant date_key column after join
df_enriched = df_enriched.drop("date_key")

print(f"  Rows after date_dim join : {df_enriched.count():,}")


# ══════════════════════════════════════════════════════════════
# STEP 7 — COMPUTE gross_revenue
# Revenue per line item = item revenue + option revenue
#   item revenue   = item_price  × item_quantity
#   option revenue = option_price × option_quantity
# Note: No negative option_price found in EDA (no discounts).
#       Formula is correct as-is; will capture discounts if they
#       appear in future data as negative option_price values.
# ══════════════════════════════════════════════════════════════
print("\n── STEP 7: Computing gross_revenue ──")

df_enriched = df_enriched.withColumn(
    "gross_revenue",
    (col("item_price") * col("item_quantity")) +
    (col("option_price") * col("option_quantity"))
)

# Add year and month columns for Silver partitioning
df_enriched = df_enriched \
    .withColumn("year",  year(col("order_date"))) \
    .withColumn("month", month(col("order_date")))

# Quick sanity check on gross_revenue
revenue_stats = df_enriched.selectExpr(
    "min(gross_revenue) as min_revenue",
    "max(gross_revenue) as max_revenue",
    "avg(gross_revenue) as avg_revenue",
    "sum(gross_revenue) as total_revenue",
).collect()[0]

print(f"  gross_revenue stats:")
print(f"    Min   : ${revenue_stats['min_revenue']:,.4f}")
print(f"    Max   : ${revenue_stats['max_revenue']:,.4f}")
print(f"    Avg   : ${revenue_stats['avg_revenue']:,.4f}")
print(f"    Total : ${revenue_stats['total_revenue']:,.2f}")


# ══════════════════════════════════════════════════════════════
# STEP 8 — WRITE TO SILVER
# Write orders_enriched as Parquet partitioned by year/month.
# Uses dynamic partition overwrite so reruns only replace the
# partitions being written, not the entire Silver table.
# ══════════════════════════════════════════════════════════════
print("\n── STEP 8: Writing orders_enriched to Silver ──")

output_path = f"{SILVER_BASE}/orders_enriched"

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df_enriched.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(output_path)

print(f"  orders_enriched written to: {output_path}")
print(f"  Partitioned by: year / month")
print(f"  Total rows written: {df_enriched.count():,}")


# ── Job Summary ────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"bronze_to_silver_job completed successfully")
print(f"Output : {output_path}")
print(f"EDA fixes applied:")
print(f"  {dropped:,} duplicate rows dropped from order_item_options")
print(f"  DEVELOPMENT platform orders filtered out")
print(f"  ISO8601 timestamps parsed")
print(f"  date_dim regenerated for 2020–2024")
print(f"  15 orphan option rows dropped by inner join")
print(f"  gross_revenue computed per line item")
print(f"{'='*60}")

job.commit()