# load_sqlserver_from_s3_job.py
# GlobalPartners Business Insights — One-Time SQL Server Loader
# Purpose: Initialize or refresh SQL Server/RDS source tables from the
#          provided CSV files in S3. Run this before using ingestion_job.py
#          with SOURCE_MODE=jdbc.
#
# Runtime: Glue Spark
# Layer:   S3 CSV source files -> SQL Server/RDS source tables
#
# Input:   s3://<bucket>/<source_prefix>/order_items.csv
#          s3://<bucket>/<source_prefix>/order_item_options.csv
#          s3://<bucket>/<source_prefix>/date_dim.csv
# Output:  dbo.order_items
#          dbo.order_item_options
#          dbo.date_dim

import json
import re
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_BUCKET",
        "SOURCE_PREFIX",
        "JDBC_HOST",
        "JDBC_PORT",
        "JDBC_DATABASE",
        "JDBC_SECRET_ARN",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = args["S3_BUCKET"]
source_prefix = args["SOURCE_PREFIX"]
jdbc_host = args["JDBC_HOST"]
jdbc_port = args["JDBC_PORT"]
jdbc_database = args["JDBC_DATABASE"]
jdbc_secret_arn = args["JDBC_SECRET_ARN"]

SOURCE_BASE = f"s3://{bucket}/{source_prefix}"
JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

TABLE_SCHEMAS = {
    "order_items": {
        "app_name": StringType(),
        "restaurant_id": StringType(),
        "creation_time_utc": StringType(),
        "order_id": StringType(),
        "user_id": StringType(),
        "printed_card_number": StringType(),
        "is_loyalty": BooleanType(),
        "currency": StringType(),
        "lineitem_id": StringType(),
        "item_category": StringType(),
        "item_name": StringType(),
        "item_price": DoubleType(),
        "item_quantity": IntegerType(),
    },
    "order_item_options": {
        "order_id": StringType(),
        "lineitem_id": StringType(),
        "option_group_name": StringType(),
        "option_name": StringType(),
        "option_price": DoubleType(),
        "option_quantity": IntegerType(),
    },
    "date_dim": {
        "date_key": StringType(),
        "day_of_week": StringType(),
        "week": IntegerType(),
        "month": StringType(),
        "year": IntegerType(),
        "is_weekend": BooleanType(),
        "is_holiday": BooleanType(),
        "holiday_name": StringType(),
    },
}


def validate_sql_identifier(identifier, label):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"Invalid {label}: {identifier}")


def get_secret_json(secret_arn):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_arn)
    return json.loads(response["SecretString"])


def jdbc_url(database_name):
    return (
        f"jdbc:sqlserver://{jdbc_host}:{jdbc_port};"
        f"databaseName={database_name};"
        "encrypt=true;trustServerCertificate=true"
    )


def execute_sql(database_name, sql, credentials):
    jvm = spark.sparkContext._gateway.jvm
    jvm.java.lang.Class.forName(JDBC_DRIVER)

    conn = jvm.java.sql.DriverManager.getConnection(
        jdbc_url(database_name),
        credentials["username"],
        credentials["password"],
    )
    try:
        stmt = conn.createStatement()
        try:
            stmt.execute(sql)
        finally:
            stmt.close()
    finally:
        conn.close()


def ensure_database_exists(credentials):
    validate_sql_identifier(jdbc_database, "database name")
    sql = (
        f"IF DB_ID(N'{jdbc_database}') IS NULL "
        f"BEGIN CREATE DATABASE [{jdbc_database}] END"
    )
    print(f"Ensuring SQL Server database exists: {jdbc_database}")
    execute_sql("master", sql, credentials)


def normalize_columns(df):
    for column_name in df.columns:
        df = df.withColumnRenamed(column_name, column_name.lower())
    return df


def apply_target_schema(df, table_name):
    schema = TABLE_SCHEMAS[table_name]

    for column_name, target_type in schema.items():
        if column_name in df.columns:
            df = df.withColumn(column_name, col(column_name).cast(target_type))
        else:
            df = df.withColumn(column_name, lit(None).cast(target_type))

    return df.select(*schema.keys())


def read_source_csv(table_name):
    path = f"{SOURCE_BASE}/{table_name}.csv"
    print(f"\nReading source CSV: {path}")
    df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    df = normalize_columns(df)
    df = apply_target_schema(df, table_name)
    print(f"  Rows loaded from CSV : {df.count():,}")
    print(f"  Columns              : {df.columns}")
    return df


def write_sqlserver_table(df, table_name, credentials):
    dbtable = f"dbo.{table_name}"
    print(f"Writing SQL Server table: {dbtable}")

    df.write.format("jdbc") \
        .option("url", jdbc_url(jdbc_database)) \
        .option("dbtable", dbtable) \
        .option("user", credentials["username"]) \
        .option("password", credentials["password"]) \
        .option("driver", JDBC_DRIVER) \
        .mode("overwrite") \
        .save()

    print(f"  {dbtable} written successfully")


print("=" * 80)
print("load_sqlserver_from_s3_job started")
print(f"Source base     : {SOURCE_BASE}")
print(f"JDBC host       : {jdbc_host}")
print(f"JDBC database   : {jdbc_database}")
print("=" * 80)

credentials = get_secret_json(jdbc_secret_arn)
ensure_database_exists(credentials)

for table_name in TABLE_SCHEMAS:
    df_source = read_source_csv(table_name)
    write_sqlserver_table(df_source, table_name, credentials)

print("\n" + "=" * 80)
print("load_sqlserver_from_s3_job completed successfully")
print("Tables written: dbo.order_items, dbo.order_item_options, dbo.date_dim")
print("=" * 80)

job.commit()
