import os
import tempfile
from dotenv import load_dotenv
from aws_cdk import (
    Stack,
    CfnOutput,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_logs as logs,
    aws_glue as glue,
    aws_ec2 as ec2,
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
        dashboard_instance_type = os.getenv("DASHBOARD_INSTANCE_TYPE", "t3.small")
        dashboard_allowed_cidr  = os.getenv("DASHBOARD_ALLOWED_CIDR", "0.0.0.0/0")
        dashboard_port          = int(os.getenv("DASHBOARD_PORT", "8501"))


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
            prune=False,
        )

        # Upload local Glue job scripts to the S3 prefix used by the Glue jobs.
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        glue_scripts_path = os.path.join(repo_root, "glue_jobs")

        glue_scripts_deployment = s3deploy.BucketDeployment(
            self,
            "GlobalPartnersGlueScripts",
            sources=[
                s3deploy.Source.asset(
                    glue_scripts_path,
                    exclude=[
                        "__pycache__/*",
                        "*.pyc",
                    ],
                )
            ],
            destination_bucket=bucket,
            destination_key_prefix="glue_scripts",
            prune=False,
        )

        # Upload dashboard app to S3
        dashboard_app_path = os.path.join(repo_root, "dashboard")

        dashboard_app_deployment = s3deploy.BucketDeployment(
            self,
            "GlobalPartnersDashboardApp",
            sources=[
                s3deploy.Source.asset(
                    dashboard_app_path,
                    exclude=[
                        "__pycache__/*",
                        "**/__pycache__/*",
                        "*.pyc",
                        "**/*.pyc",
                        ".venv/*",
                        ".env",
                        ".env.*",
                    ],
                )
            ],
            destination_bucket=bucket,
            destination_key_prefix="dashboard_app",
            prune=True,
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

        # ── Resource 4: Glue Jobs ───────────────────────────────────
        # Glue jobs defined as infrastructure-as-code.
        # Each job references its PySpark script in S3 and receives

        glue_role_arn     = f"arn:aws:iam::{self.account}:role/{glue_role_name}"
        script_base       = f"s3://{bucket_name}/glue_scripts"
        ingestion_job_name      = os.getenv("INGESTION_JOB_NAME")
        bronze_to_silver_name   = os.getenv("BRONZE_TO_SILVER_JOB_NAME")
        silver_to_gold_name     = os.getenv("SILVER_TO_GOLD_JOB_NAME")
        churn_days        = os.getenv("CHURN_DAYS", "45")
        rfm_months        = os.getenv("RFM_MONTHS", "6")

        # ── Job 1: ingestion_job (Python Shell) ─────────────────────
        # Python Shell — no Spark cluster needed for simple CSV reads.
        # Lighter and cheaper than a Spark job for ingestion.
        ingestion_job = glue.CfnJob(
            self,
            "IngestionJob",
            name=ingestion_job_name,
            role=glue_role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="pythonshell",
                python_version="3.9",
                script_location=f"{script_base}/ingestion_job.py",
            ),
            default_arguments={
                "--job-language":                    "python",
                "--TempDir":                         f"s3://{bucket_name}/tmp/",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics":                  "true",
                "--additional-python-modules":       "pyarrow==11.0.0,pandas==2.0.0",
                "--S3_BUCKET":                       bucket_name,
                "--SOURCE_PREFIX":                   "source",
                "--BRONZE_PREFIX":                   "bronze",
            },
            max_capacity=0.0625,   # 1/16 DPU — minimum for Python Shell
            max_retries=1,
            timeout=30,            # minutes
            glue_version="3.0",
            description="GlobalPartners — ingest source CSVs to Bronze S3 layer",
        )
        ingestion_job.node.add_dependency(glue_scripts_deployment)

        # ── Job 2: bronze_to_silver_job (Spark) ─────────────────────
        # Spark job — handles joins, deduplication, and enrichment.
        # G.1X = 1 DPU worker (4 vCPU, 16GB RAM) — sufficient for 200K rows.
        bronze_to_silver_job = glue.CfnJob(
            self,
            "BronzeToSilverJob",
            name=bronze_to_silver_name,
            role=glue_role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"{script_base}/bronze_to_silver_job.py",
            ),
            default_arguments={
                "--job-language":           "python",
                "--TempDir":                f"s3://{bucket_name}/tmp/",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics":         "true",
                "--enable-spark-ui":        "true",
                "--spark-event-logs-path":  f"s3://{bucket_name}/spark-logs/",
                "--S3_BUCKET":              bucket_name,
                "--BRONZE_PREFIX":          "bronze",
                "--SILVER_PREFIX":          "silver",
            },
            worker_type="G.1X",
            number_of_workers=2,
            max_retries=1,
            timeout=60,            # minutes
            glue_version="4.0",
            description="GlobalPartners — Bronze to Silver transformation",
        )
        bronze_to_silver_job.node.add_dependency(glue_scripts_deployment)

        # ── Job 3: silver_to_gold_job (Spark) ───────────────────────
        # Spark job — computes all 7 business metrics from Silver.
        # G.1X with 2 workers handles window functions on 200K rows.
        silver_to_gold_job = glue.CfnJob(
            self,
            "SilverToGoldJob",
            name=silver_to_gold_name,
            role=glue_role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"{script_base}/silver_to_gold_job.py",
            ),
            default_arguments={
                "--job-language":           "python",
                "--TempDir":                f"s3://{bucket_name}/tmp/",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics":         "true",
                "--enable-spark-ui":        "true",
                "--spark-event-logs-path":  f"s3://{bucket_name}/spark-logs/",
                "--S3_BUCKET":              bucket_name,
                "--SILVER_PREFIX":          "silver",
                "--GOLD_PREFIX":            "gold",
                "--CHURN_DAYS":             churn_days,
                "--RFM_MONTHS":             rfm_months,
            },
            worker_type="G.1X",
            number_of_workers=2,
            max_retries=1,
            timeout=60,            # minutes
            glue_version="4.0",
            description="GlobalPartners — Silver to Gold metric computation",
        )
        silver_to_gold_job.node.add_dependency(glue_scripts_deployment)
        
        
        # ── Resource 5: Glue Workflow + Triggers ───────────────────
        # The Workflow groups the Glue jobs under one single pipeline.
        # Triggers chain them together so each job only starts when
        # the previous one succeeds.

        workflow_name = os.getenv("GLUE_WORKFLOW_NAME")

        # ── Workflow ────────────────────────────────────────────────
        workflow = glue.CfnWorkflow(
            self,
            "GlobalPartnersWorkflow",
            name=workflow_name,
            description="GlobalPartners daily pipeline — ingestion → bronze→silver → silver→gold → discount effectiveness",
        )

        # ── Trigger 1: Scheduled ────────────────────────────────────
        # Starts ingestion_job on a daily schedule at 2:00 AM UTC.
        # SCHEDULED type triggers fire independently. No predecessor.
        # Cron format: cron(minutes hours day-of-month month day-of-week year)
        pipeline_schedule = os.getenv("PIPELINE_SCHEDULE", "cron(0 2 * * ? *)")

        glue.CfnTrigger(
            self,
            "Trigger1Scheduled",
            name="globalpartners_trigger_1_scheduled",
            type="SCHEDULED",
            schedule=pipeline_schedule,
            workflow_name=workflow_name,
            start_on_creation=False,   # don't fire immediately on deploy
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=ingestion_job_name,
                )
            ],
        )

        # ── Trigger 2: On success of ingestion_job ──────────────────
        # CONDITIONAL type triggers watch for a predecessor job/crawler
        # to reach a specific state before firing.
        # Only fires if ingestion_job succeeds. Failure stops the chain.
        glue.CfnTrigger(
            self,
            "Trigger2BronzeToSilver",
            name="globalpartners_trigger_2_bronze_to_silver",
            type="CONDITIONAL",
            workflow_name=workflow_name,
            start_on_creation=False,
            predicate=glue.CfnTrigger.PredicateProperty(
                # AND = all conditions must be met (we only have one here)
                logical="AND",
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        job_name=ingestion_job_name,
                        logical_operator="EQUALS",
                        state="SUCCEEDED",
                    )
                ],
            ),
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=bronze_to_silver_name,
                )
            ],
        )

        # ── Trigger 3: On success of bronze_to_silver_job ──────────
        # Only fires if bronze_to_silver_job succeeds.
        # If bronze_to_silver_job fails, Gold is never touched
        glue.CfnTrigger(
            self,
            "Trigger3SilverToGold",
            name="globalpartners_trigger_3_silver_to_gold",
            type="CONDITIONAL",
            workflow_name=workflow_name,
            start_on_creation=False,
            predicate=glue.CfnTrigger.PredicateProperty(
                logical="AND",
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        job_name=bronze_to_silver_name,
                        logical_operator="EQUALS",
                        state="SUCCEEDED",
                    )
                ],
            ),
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=silver_to_gold_name,
                )
            ],
        )

        # ── Resource 6: Discount Effectiveness Glue Job ────────────────
        discount_job_name = os.getenv("DISCOUNT_JOB_NAME")

        # CloudWatch Log Group
        logs.LogGroup(
            self,
            "DiscountJobLogGroup",
            log_group_name=f"/aws-glue/jobs/{discount_job_name}",
            retention=retention,
        )

        # Glue Spark Job
        discount_effectiveness_job = glue.CfnJob(
            self,
            "DiscountEffectivenessJob",
            name=discount_job_name,
            role=glue_role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"{script_base}/discount_effectiveness_job.py",
            ),
            default_arguments={
                "--job-language":                    "python",
                "--TempDir":                         f"s3://{bucket_name}/tmp/",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics":                  "true",
                "--enable-spark-ui":                 "true",
                "--spark-event-logs-path":           f"s3://{bucket_name}/spark-logs/",
                "--S3_BUCKET":                       bucket_name,
                "--SILVER_PREFIX":                   "silver",
                "--GOLD_PREFIX":                     "gold",
            },
            worker_type="G.1X",
            number_of_workers=2,
            max_retries=1,
            timeout=60,
            glue_version="4.0",
            description="GlobalPartners — discount effectiveness from Silver",
        )
        discount_effectiveness_job.node.add_dependency(glue_scripts_deployment)

        # Trigger 4: On success of silver_to_gold_job
        glue.CfnTrigger(
            self,
            "Trigger4DiscountEffectiveness",
            name="globalpartners_trigger_4_discount_effectiveness",
            type="CONDITIONAL",
            workflow_name=workflow_name,
            start_on_creation=False,
            predicate=glue.CfnTrigger.PredicateProperty(
                logical="AND",
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        job_name=silver_to_gold_name,
                        logical_operator="EQUALS",
                        state="SUCCEEDED",
                    )
                ],
            ),
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=discount_job_name,
                )
            ],
        )

        # Resource 7: EC2 Streamlit Dashboard
        vpc = ec2.Vpc.from_lookup(
            self,
            "DefaultVpc",
            is_default=True,
        )

        dashboard_sg = ec2.SecurityGroup(
            self,
            "GlobalPartnersDashboardSecurityGroup",
            vpc=vpc,
            description="Allow browser access to the Streamlit dashboard",
            allow_all_outbound=True,
        )

        dashboard_sg.add_ingress_rule(
            peer=ec2.Peer.ipv4(dashboard_allowed_cidr),
            connection=ec2.Port.tcp(dashboard_port),
            description="Streamlit dashboard access",
        )

        dashboard_role = iam.Role(
            self,
            "GlobalPartnersDashboardEc2Role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMManagedInstanceCore"
                ),
            ],
        )

        dashboard_role.add_to_policy(
            iam.PolicyStatement(
                sid="ReadGoldAndDashboardAppObjects",
                actions=["s3:GetObject"],
                resources=[
                    f"arn:aws:s3:::{bucket_name}/gold/*",
                    f"arn:aws:s3:::{bucket_name}/dashboard_app/*",
                ],
            )
        )

        dashboard_role.add_to_policy(
            iam.PolicyStatement(
                sid="ListGoldAndDashboardAppPrefixes",
                actions=["s3:ListBucket", "s3:GetBucketLocation"],
                resources=[f"arn:aws:s3:::{bucket_name}"],
                conditions={
                    "StringLike": {
                        "s3:prefix": [
                            "gold/*",
                            "dashboard_app/*",
                        ]
                    }
                },
            )
        )

        dashboard_service_file = "\n".join([
            "cat > /etc/systemd/system/globalpartners-dashboard.service <<'EOF'",
            "[Unit]",
            "Description=GlobalPartners Streamlit Dashboard",
            "After=network-online.target",
            "Wants=network-online.target",
            "",
            "[Service]",
            "User=ec2-user",
            "Group=ec2-user",
            "WorkingDirectory=/opt/globalpartners/dashboard",
            f"Environment=S3_BUCKET_NAME={bucket_name}",
            f"Environment=AWS_DEFAULT_REGION={self.region}",
            f"ExecStart=/opt/globalpartners/venv/bin/streamlit run Home.py --server.address=0.0.0.0 --server.port={dashboard_port} --server.headless=true --browser.gatherUsageStats=false",
            "Restart=always",
            "RestartSec=10",
            "",
            "[Install]",
            "WantedBy=multi-user.target",
            "EOF",
        ])

        dashboard_user_data = ec2.UserData.for_linux()
        dashboard_user_data.add_commands(
            "set -euxo pipefail",
            "dnf update -y",
            "dnf install -y python3 python3-pip awscli",
            "mkdir -p /opt/globalpartners/dashboard",
            "aws s3 sync s3://{}/dashboard_app/ /opt/globalpartners/dashboard/ --delete".format(bucket_name),
            "python3 -m venv /opt/globalpartners/venv",
            "/opt/globalpartners/venv/bin/pip install --upgrade pip",
            "/opt/globalpartners/venv/bin/pip install -r /opt/globalpartners/dashboard/requirements.txt",
            "chown -R ec2-user:ec2-user /opt/globalpartners",
            dashboard_service_file,
            "systemctl daemon-reload",
            "systemctl enable --now globalpartners-dashboard",
        )

        dashboard_instance = ec2.Instance(
            self,
            "GlobalPartnersDashboardInstance",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            instance_type=ec2.InstanceType(dashboard_instance_type),
            machine_image=ec2.MachineImage.latest_amazon_linux2023(),
            security_group=dashboard_sg,
            role=dashboard_role,
            user_data=dashboard_user_data,
            associate_public_ip_address=True,
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/xvda",
                    volume=ec2.BlockDeviceVolume.ebs(
                        volume_size=20,
                        encrypted=True,
                        volume_type=ec2.EbsDeviceVolumeType.GP3,
                    ),
                )
            ],
        )

        dashboard_instance.node.add_dependency(dashboard_app_deployment)

        CfnOutput(
            self,
            "GlobalPartnersDashboardUrl",
            value=f"http://{dashboard_instance.instance_public_dns_name}:{dashboard_port}",
        )

        CfnOutput(
            self,
            "GlobalPartnersDashboardInstanceId",
            value=dashboard_instance.instance_id,
        )
