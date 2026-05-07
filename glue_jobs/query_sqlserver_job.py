# query_sqlserver_job.py
# GlobalPartners Business Insights — SQL Server Query Utility
# Purpose: Run a read-only SQL Server query through Glue JDBC and print
#          results to CloudWatch logs. Useful for validating RDS tables
#          without installing SQL client tools or opening SSH access.
#
# Runtime: Glue Spark
#
# Default query lists base tables in the configured database.

import json
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "JDBC_HOST",
        "JDBC_PORT",
        "JDBC_DATABASE",
        "JDBC_SECRET_ARN",
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


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

jdbc_host = args["JDBC_HOST"]
jdbc_port = args["JDBC_PORT"]
jdbc_database = args["JDBC_DATABASE"]
jdbc_secret_arn = args["JDBC_SECRET_ARN"]

DEFAULT_QUERY = """
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
"""

query = get_optional_arg("QUERY", DEFAULT_QUERY).strip()

blocked_tokens = [
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "DROP",
    "TRUNCATE",
    "ALTER",
    "CREATE",
    "EXEC",
    "EXECUTE",
]

query_upper = query.upper()
if not query_upper.startswith("SELECT"):
    raise ValueError("Only SELECT queries are allowed by query_sqlserver_job.py")

for token in blocked_tokens:
    if token in query_upper:
        raise ValueError(f"Blocked non-read-only SQL token detected: {token}")


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


credentials = get_secret_json(jdbc_secret_arn)

wrapped_query = f"({query}) query_result"

print("=" * 80)
print("query_sqlserver_job started")
print(f"JDBC host     : {jdbc_host}")
print(f"JDBC database : {jdbc_database}")
print("Query:")
print(query)
print("=" * 80)

df = spark.read.format("jdbc") \
    .option("url", jdbc_url(jdbc_database)) \
    .option("dbtable", wrapped_query) \
    .option("user", credentials["username"]) \
    .option("password", credentials["password"]) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

row_count = df.count()
print(f"Rows returned: {row_count:,}")
df.show(100, truncate=False)

print("=" * 80)
print("query_sqlserver_job completed successfully")
print("=" * 80)

job.commit()
