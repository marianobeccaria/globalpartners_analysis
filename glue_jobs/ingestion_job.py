# ingestion_job.py
# GlobalPartners Business Insights — Bronze Layer Ingestion Job
# Purpose: Read source CSVs from S3 and write raw Parquet to Bronze layer,
#          partitioned by ingestion_date. Uses overwrite-by-partition strategy
#          so reruns are idempotent — only today's partition is overwritten.
#
# Runtime: Python Shell (no Spark) — uses boto3 + pandas + pyarrow
# Layer:   Source (CSV) → Bronze (Parquet)
# Trigger: Glue Workflow Trigger 1 — runs daily at 2:00 AM UTC
#
# Input:   s3://<bucket>/source/*.csv
# Output:  s3://<bucket>/bronze/<table>/ingestion_date=YYYY-MM-DD/

import sys
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ── Read job parameters passed via --arguments ─────────────────────────────────
# In Python Shell, parameters are passed as --KEY VALUE in sys.argv
# We parse them manually since getResolvedOptions needs awsglue
def get_arg(key, default=None):
    key_flag = f"--{key}"
    if key_flag in sys.argv:
        return sys.argv[sys.argv.index(key_flag) + 1]
    return default

bucket        = get_arg("S3_BUCKET")
source_prefix = get_arg("SOURCE_PREFIX", "source")
bronze_prefix = get_arg("BRONZE_PREFIX", "bronze")

SOURCE_BASE = f"s3://{bucket}/{source_prefix}"
BRONZE_BASE = f"s3://{bucket}/{bronze_prefix}"

# Today's ingestion date — partition value
ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

print(f"{'='*60}")
print(f"ingestion_job started")
print(f"Ingestion date : {ingestion_date}")
print(f"Source         : {SOURCE_BASE}")
print(f"Bronze         : {BRONZE_BASE}")
print(f"{'='*60}")

# ── S3 client ──────────────────────────────────────────────────────────────────
s3 = boto3.client("s3")


# ── Helper: read CSV from S3 into pandas DataFrame ────────────────────────────
def read_csv_from_s3(table_name):
    key = f"{source_prefix}/{table_name}.csv"
    print(f"\n  Reading s3://{bucket}/{key}")
    response = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(response["Body"])
    # Normalize all column names to lowercase
    df.columns = df.columns.str.lower()
    print(f"    Rows   : {len(df):,}")
    print(f"    Cols   : {list(df.columns)}")
    return df


# ── Helper: write DataFrame to Bronze as Parquet ──────────────────────────────
def write_to_bronze(df, table_name):
    # Add ingestion_date partition column
    df["ingestion_date"] = ingestion_date

    # Write to a temp local file then upload to S3
    local_path = f"/tmp/{table_name}.parquet"
    s3_key     = f"{bronze_prefix}/{table_name}/ingestion_date={ingestion_date}/{table_name}.parquet"

    # Convert to PyArrow table and write Parquet
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, local_path)

    # Upload to S3
    s3.upload_file(local_path, bucket, s3_key)

    print(f"    Written to s3://{bucket}/{s3_key}")
    print(f"    Rows written : {len(df):,}")

    # Clean up temp file
    os.remove(local_path)


# ── Table 1: order_items ───────────────────────────────────────────────────────
print("\n── Reading order_items ──")
df_items = read_csv_from_s3("order_items")
write_to_bronze(df_items, "order_items")


# ── Table 2: order_item_options ────────────────────────────────────────────────
# Note: Contains 2,299 exact duplicate rows. Dropped in Silver, NOT here.
# Bronze is a faithful copy of the source.
print("\n── Reading order_item_options ──")
df_options = read_csv_from_s3("order_item_options")
write_to_bronze(df_options, "order_item_options")


# ── Table 3: date_dim ──────────────────────────────────────────────────────────
# Note: Provided date_dim only covers 2023 and uses DD-MM-YYYY format.
# Both issues are remediated in Silver — Bronze preserves source as-is.
print("\n── Reading date_dim ──")
df_date = read_csv_from_s3("date_dim")
write_to_bronze(df_date, "date_dim")


# ── Job Summary ────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"ingestion_job completed successfully")
print(f"Ingestion date : {ingestion_date}")
print(f"Tables written : order_items, order_item_options, date_dim")
print(f"Bronze path    : {BRONZE_BASE}")
print(f"{'='*60}")
