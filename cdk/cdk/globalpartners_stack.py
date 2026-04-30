import os
import tempfile
from dotenv import load_dotenv
from aws_cdk import (
    Stack,
    Duration,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_logs as logs,
)
from constructs import Construct

# Load environment variables from .env file
load_dotenv()


class GlobalPartnersStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── Config from .env ────────────────────────────────────────
        bucket_name    = os.getenv("S3_BUCKET_NAME")
        glue_role_name = os.getenv("GLUE_ROLE_NAME")

        # ── Resource 1: IAM Role for Glue ──────────────────────────
        # Grants all three Glue jobs permission to:
        #   - Read/write S3 (Bronze, Silver, Gold layers)
        #   - Execute as a Glue job
        #   - Write logs to CloudWatch
        glue_role = iam.Role(
            self,
            "GlobalPartnersGlueRole",
            role_name=glue_role_name,
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )

        # Scoped S3 policy — only our bucket, all three layers
        glue_role.add_to_policy(
            iam.PolicyStatement(
                sid="GlobalPartnersS3Access",
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                ],
                resources=[
                    f"arn:aws:s3:::{bucket_name}",
                    f"arn:aws:s3:::{bucket_name}/*",
                ],
            )
        )

        # CloudWatch Logs — write access for Glue job logs
        glue_role.add_to_policy(
            iam.PolicyStatement(
                sid="GlobalPartnersCloudWatchLogs",
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["arn:aws:logs:*:*:/aws-glue/*"],
            )
        )

# ── Resource 2: S3 Folder Structure ────────────────────────
        # Upload empty .keep files to establish Bronze / Silver / Gold prefixes in the bucket
        # and make the structure navigable in the AWS console and

        bucket = s3.Bucket.from_bucket_name(
            self,
            "GlobalPartnersBucket",
            bucket_name,
        )

        # Build folder structure locally using a temp directory
        tmp_dir = tempfile.mkdtemp()
        folders = [
            "bronze/order_items",
            "bronze/order_item_options",
            "bronze/date_dim",
            "silver/orders_enriched",
            "gold/customer_clv_daily",
            "gold/customer_rfm_segments",
            "gold/customer_churn_indicators",
            "gold/sales_trends",
            "gold/loyalty_comparison",
            "gold/location_performance",
            "gold/discount_effectiveness",
        ]

        for folder in folders:
            full_path = os.path.join(tmp_dir, folder)
            os.makedirs(full_path, exist_ok=True)
            with open(os.path.join(full_path, ".keep"), "w") as f:
                f.write("")

        s3deploy.BucketDeployment(
            self,
            "GlobalPartnersFolderStructure",
            sources=[s3deploy.Source.asset(tmp_dir)],
            destination_bucket=bucket,
        )

        # ── Resource 3: CloudWatch Log Groups ──────────────────────
        # One log group per Glue job.
        # Retention period is configurable via LOG_RETENTION_DAYS in .env
        # Log groups are pre-created so logs appear immediately on first
        # job run without waiting for auto-creation.

        log_retention_days = int(os.getenv("LOG_RETENTION_DAYS", "30"))
        
        # This maps the integer from .env (LOG_RETENTION_DAYS=30) to logs.RetentionDays.ONE_MONTH 
        retention_map = {
            1:   logs.RetentionDays.ONE_DAY,
            3:   logs.RetentionDays.THREE_DAYS,
            7:   logs.RetentionDays.ONE_WEEK,
            14:  logs.RetentionDays.TWO_WEEKS,
            30:  logs.RetentionDays.ONE_MONTH,
            60:  logs.RetentionDays.TWO_MONTHS,
            90:  logs.RetentionDays.THREE_MONTHS,
            180: logs.RetentionDays.SIX_MONTHS,
            365: logs.RetentionDays.ONE_YEAR,
        }
        retention = retention_map.get(log_retention_days, logs.RetentionDays.ONE_MONTH)

        ingestion_job_name      = os.getenv("INGESTION_JOB_NAME")
        bronze_to_silver_name   = os.getenv("BRONZE_TO_SILVER_JOB_NAME")
        silver_to_gold_name     = os.getenv("SILVER_TO_GOLD_JOB_NAME")

        logs.LogGroup(
            self,
            "IngestionJobLogGroup",
            log_group_name=f"/aws-glue/jobs/{ingestion_job_name}",
            retention=retention,
        )

        logs.LogGroup(
            self,
            "BronzeToSilverJobLogGroup",
            log_group_name=f"/aws-glue/jobs/{bronze_to_silver_name}",
            retention=retention,
        )

        logs.LogGroup(
            self,
            "SilverToGoldJobLogGroup",
            log_group_name=f"/aws-glue/jobs/{silver_to_gold_name}",
            retention=retention,
        )

