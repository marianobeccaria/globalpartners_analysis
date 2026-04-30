import os
import tempfile
from dotenv import load_dotenv
from aws_cdk import (
    Stack,
    aws_iam as iam,
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
