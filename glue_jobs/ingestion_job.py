# ingestion_job.py
# GlobalPartners Business Insights — Bronze Layer Ingestion Job
# Purpose: Read source data and write raw Parquet to Bronze layer,
#          partitioned by ingestion_date.
#
# Runtime: Glue Spark
# Layer:   Source (CSV or JDBC) → Bronze (Parquet)
# Trigger: Glue Workflow Trigger 1 — runs daily at 2:00 AM UTC
#
# Input:   CSV mode  : s3://<bucket>/source/*.csv
#          JDBC mode : SQL Server / RDS through Glue JDBC networking
# Output:  s3://<bucket>/bronze/<table>/ingestion_date=YYYY-MM-DD/

import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


# ── Job Initialization ─────────────────────────────────────────────────────────
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_BUCKET",
        "SOURCE_PREFIX",
        "BRONZE_PREFIX",
        "SOURCE_MODE",
    ],
)


def get_optional_arg(key, default=""):
    key_flag = f"--{key}"
    if key_flag not in sys.argv:
        return default

    value_index = sys.argv.index(key_flag) + 1
    if value_index >= len(sys.argv) or sys.argv[value_index].startswith("--"):
        return default

    return sys.argv[value_index]

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket          = args["S3_BUCKET"]
source_prefix   = args["SOURCE_PREFIX"]
bronze_prefix   = args["BRONZE_PREFIX"]
source_mode     = args["SOURCE_MODE"].lower()
jdbc_url        = get_optional_arg("JDBC_URL")
jdbc_tables_arg = get_optional_arg(
    "JDBC_TABLES",
    "order_items,order_item_options,date_dim",
)
jdbc_secret_arn = get_optional_arg("JDBC_SECRET_ARN")

SOURCE_BASE = f"s3://{bucket}/{source_prefix}"
BRONZE_BASE = f"s3://{bucket}/{bronze_prefix}"
TABLES = ["order_items", "order_item_options", "date_dim"]

# Today's ingestion date — partition value
ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

print(f"{'='*60}")
print(f"ingestion_job started")
print(f"Ingestion date : {ingestion_date}")
print(f"Source mode    : {source_mode}")
print(f"CSV source     : {SOURCE_BASE}")
print(f"JDBC source    : {jdbc_url if source_mode == 'jdbc' else 'not used'}")
print(f"Bronze         : {BRONZE_BASE}")
print(f"{'='*60}")


# ── Helpers ────────────────────────────────────────────────────────────────────
def get_secret_json(secret_arn):
    if not secret_arn:
        raise ValueError("JDBC_SECRET_ARN is required when SOURCE_MODE=jdbc")

    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_arn)

    import json
    return json.loads(response["SecretString"])


def normalize_columns(df):
    for column_name in df.columns:
        df = df.withColumnRenamed(column_name, column_name.lower())
    return df


def read_csv_table(table_name):
    path = f"{SOURCE_BASE}/{table_name}.csv"
    print(f"\n  Reading CSV table: {path}")
    df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    return normalize_columns(df)


def read_jdbc_table(table_name, credentials):
    jdbc_tables = {
        table.strip(): table.strip()
        for table in jdbc_tables_arg.split(",")
        if table.strip()
    }
    dbtable = jdbc_tables.get(table_name, table_name)

    print(f"\n  Reading JDBC table: {dbtable}")
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", dbtable) \
        .option("user", credentials["username"]) \
        .option("password", credentials["password"]) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    return normalize_columns(df)


def write_to_bronze(df, table_name):
    output_path = f"{BRONZE_BASE}/{table_name}/ingestion_date={ingestion_date}"

    row_count = df.count()
    print(f"    Rows   : {row_count:,}")
    print(f"    Cols   : {df.columns}")
    print(f"    Output : {output_path}")

    df.write.mode("overwrite").parquet(output_path)
    print(f"    {table_name} written successfully")


if source_mode not in ["csv", "jdbc"]:
    raise ValueError("SOURCE_MODE must be either 'csv' or 'jdbc'")

credentials = get_secret_json(jdbc_secret_arn) if source_mode == "jdbc" else None

for table_name in TABLES:
    if source_mode == "csv":
        df_source = read_csv_table(table_name)
    else:
        df_source = read_jdbc_table(table_name, credentials)

    write_to_bronze(df_source, table_name)


# ── Job Summary ────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"ingestion_job completed successfully")
print(f"Ingestion date : {ingestion_date}")
print(f"Source mode    : {source_mode}")
print(f"Tables written : order_items, order_item_options, date_dim")
print(f"Bronze path    : {BRONZE_BASE}")
print(f"{'='*60}")

job.commit()
