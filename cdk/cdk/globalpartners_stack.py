import os
import tempfile
from dotenv import load_dotenv
from aws_cdk import (
    Stack,
    CfnOutput,
    Duration,
    RemovalPolicy,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_logs as logs,
    aws_glue as glue,
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_secretsmanager as secretsmanager,
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

        enable_rds_source = os.getenv("ENABLE_RDS_SOURCE", "false").lower() == "true"
        source_mode = os.getenv("SOURCE_MODE", "csv").lower()
        sqlserver_db_name = os.getenv("SQLSERVER_DB", "globalpartners")
        sqlserver_port = int(os.getenv("SQLSERVER_PORT", "1433"))
        sqlserver_instance_id = os.getenv(
            "SQLSERVER_INSTANCE_ID",
            "globalpartners-sqlserver",
        )
        sqlserver_instance_type = os.getenv(
            "SQLSERVER_INSTANCE_TYPE",
            "t3.small",
        )
        sqlserver_allocated_storage = int(os.getenv("SQLSERVER_ALLOCATED_STORAGE", "20"))
        sqlserver_username = os.getenv("SQLSERVER_USER", "globalpartners_admin")
        sqlserver_jdbc_url = os.getenv("SQLSERVER_JDBC_URL", "")
        sqlserver_secret_arn = os.getenv("SQLSERVER_SECRET_ARN", "")
        glue_jdbc_connection_name = os.getenv(
            "GLUE_JDBC_CONNECTION_NAME",
            "globalpartners-sqlserver-jdbc",
        )
        jdbc_tables = os.getenv(
            "JDBC_TABLES",
            "order_items,order_item_options,date_dim",
        )

        github_repo = os.getenv("GITHUB_REPO", "marianobeccaria/globalpartners_analysis")
        github_branch = os.getenv("GITHUB_BRANCH", "main")
        github_actions_role_name = os.getenv(
            "GITHUB_ACTIONS_ROLE_NAME",
            "globalpartners-github-actions-role",
        )
        github_oidc_provider_arn = os.getenv("GITHUB_OIDC_PROVIDER_ARN")
        cdk_qualifier = os.getenv("CDK_QUALIFIER", "hnb659fds")

        vpc = ec2.Vpc.from_lookup(
            self,
            "DefaultVpc",
            is_default=True,
        )



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

        # ── Optional Resource: SQL Server RDS + Glue JDBC Connection ─────────
        # The current project build uses CSV files from S3. When ENABLE_RDS_SOURCE
        # is true, CDK provisions a SQL Server Express instance and a Glue JDBC
        # connection so ingestion_job.py can run in SOURCE_MODE=jdbc.
        jdbc_url = sqlserver_jdbc_url
        jdbc_secret_arn = sqlserver_secret_arn
        ingestion_job_connections = None

        if enable_rds_source:
            glue_jdbc_sg = ec2.SecurityGroup(
                self,
                "GlobalPartnersGlueJdbcSecurityGroup",
                vpc=vpc,
                description="Security group used by Glue JDBC jobs",
                allow_all_outbound=True,
            )

            sqlserver_sg = ec2.SecurityGroup(
                self,
                "GlobalPartnersSqlServerSecurityGroup",
                vpc=vpc,
                description="Allow SQL Server access from Glue JDBC jobs",
                allow_all_outbound=True,
            )

            sqlserver_sg.add_ingress_rule(
                peer=glue_jdbc_sg,
                connection=ec2.Port.tcp(sqlserver_port),
                description="SQL Server access from Glue JDBC connection",
            )

            sqlserver_secret = secretsmanager.Secret(
                self,
                "GlobalPartnersSqlServerSecret",
                secret_name=f"{sqlserver_instance_id}-credentials",
                generate_secret_string=secretsmanager.SecretStringGenerator(
                    secret_string_template=f'{{"username":"{sqlserver_username}"}}',
                    generate_string_key="password",
                    exclude_punctuation=True,
                    password_length=24,
                ),
            )

            sqlserver_instance = rds.DatabaseInstance(
                self,
                "GlobalPartnersSqlServerInstance",
                instance_identifier=sqlserver_instance_id,
                engine=rds.DatabaseInstanceEngine.sql_server_ex(
                    version=rds.SqlServerEngineVersion.VER_15,
                ),
                credentials=rds.Credentials.from_secret(sqlserver_secret),
                vpc=vpc,
                vpc_subnets=ec2.SubnetSelection(
                    subnet_type=ec2.SubnetType.PUBLIC,
                ),
                security_groups=[sqlserver_sg],
                instance_type=ec2.InstanceType(sqlserver_instance_type),
                allocated_storage=sqlserver_allocated_storage,
                max_allocated_storage=max(sqlserver_allocated_storage, 100),
                port=sqlserver_port,
                publicly_accessible=False,
                backup_retention=Duration.days(1),
                deletion_protection=False,
                removal_policy=RemovalPolicy.SNAPSHOT,
            )

            jdbc_url = (
                f"jdbc:sqlserver://{sqlserver_instance.db_instance_endpoint_address}:"
                f"{sqlserver_port};databaseName={sqlserver_db_name};"
                "encrypt=true;trustServerCertificate=true"
            )
            jdbc_secret_arn = sqlserver_secret.secret_arn

            glue_connection_subnet = vpc.public_subnets[0]
            glue_jdbc_connection = glue.CfnConnection(
                self,
                "GlobalPartnersGlueJdbcConnection",
                catalog_id=self.account,
                connection_input=glue.CfnConnection.ConnectionInputProperty(
                    name=glue_jdbc_connection_name,
                    connection_type="JDBC",
                    description="JDBC connection from AWS Glue to GlobalPartners SQL Server RDS",
                    connection_properties={
                        "JDBC_CONNECTION_URL": jdbc_url,
                        "SECRET_ID": sqlserver_secret.secret_arn,
                    },
                    physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                        availability_zone=glue_connection_subnet.availability_zone,
                        security_group_id_list=[glue_jdbc_sg.security_group_id],
                        subnet_id=glue_connection_subnet.subnet_id,
                    ),
                ),
            )
            glue_jdbc_connection.node.add_dependency(sqlserver_instance)

            sqlserver_secret.grant_read(glue_role)
            ingestion_job_connections = glue.CfnJob.ConnectionsListProperty(
                connections=[glue_jdbc_connection_name],
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

        # ── Job 1: ingestion_job (Spark) ────────────────────────────
        # Spark supports both current CSV ingestion and the target
        # production SQL Server/RDS JDBC ingestion path.
        ingestion_job = glue.CfnJob(
            self,
            "IngestionJob",
            name=ingestion_job_name,
            role=glue_role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"{script_base}/ingestion_job.py",
            ),
            connections=ingestion_job_connections,
            default_arguments={
                "--job-language":                     "python",
                "--TempDir":                          f"s3://{bucket_name}/tmp/",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics":                   "true",
                "--enable-spark-ui":                  "true",
                "--spark-event-logs-path":            f"s3://{bucket_name}/spark-logs/",
                "--S3_BUCKET":                        bucket_name,
                "--SOURCE_PREFIX":                    "source",
                "--BRONZE_PREFIX":                    "bronze",
                "--SOURCE_MODE":                      source_mode,
                "--JDBC_URL":                         jdbc_url or "unused",
                "--JDBC_TABLES":                      jdbc_tables,
                "--JDBC_SECRET_ARN":                  jdbc_secret_arn or "unused",
            },
            worker_type="G.1X",
            number_of_workers=2,
            max_retries=1,
            timeout=30,            # minutes
            glue_version="4.0",
            description="GlobalPartners — ingest source data to Bronze S3 layer",
        )
        ingestion_job.node.add_dependency(glue_scripts_deployment)
        if enable_rds_source:
            ingestion_job.node.add_dependency(glue_jdbc_connection)

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

        # Resource 8: GitHub Actions OIDC Role for CI/CD
        if github_oidc_provider_arn:
            github_oidc_provider = iam.OpenIdConnectProvider.from_open_id_connect_provider_arn(
                self,
                "GitHubOidcProvider",
                github_oidc_provider_arn,
            )
        else:
            github_oidc_provider = iam.OpenIdConnectProvider(
                self,
                "GitHubOidcProvider",
                url="https://token.actions.githubusercontent.com",
                client_ids=["sts.amazonaws.com"],
            )

        github_actions_role = iam.Role(
            self,
            "GlobalPartnersGitHubActionsRole",
            role_name=github_actions_role_name,
            assumed_by=iam.WebIdentityPrincipal(
                github_oidc_provider.open_id_connect_provider_arn,
                conditions={
                    "StringEquals": {
                        "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
                    },
                    "StringLike": {
                        "token.actions.githubusercontent.com:sub": (
                            f"repo:{github_repo}:ref:refs/heads/{github_branch}"
                        ),
                    },
                },
            ),
            description="OIDC role used by GitHub Actions to deploy GlobalPartners CDK stack",
        )

        github_actions_role.add_to_policy(
            iam.PolicyStatement(
                sid="AssumeCdkBootstrapRoles",
                actions=["sts:AssumeRole"],
                resources=[
                    f"arn:aws:iam::{self.account}:role/cdk-{cdk_qualifier}-deploy-role-{self.account}-{self.region}",
                    f"arn:aws:iam::{self.account}:role/cdk-{cdk_qualifier}-file-publishing-role-{self.account}-{self.region}",
                    f"arn:aws:iam::{self.account}:role/cdk-{cdk_qualifier}-image-publishing-role-{self.account}-{self.region}",
                    f"arn:aws:iam::{self.account}:role/cdk-{cdk_qualifier}-lookup-role-{self.account}-{self.region}",
                ],
            )
        )

        github_actions_role.add_to_policy(
            iam.PolicyStatement(
                sid="ReadStackOutputs",
                actions=[
                    "cloudformation:DescribeStacks",
                    "cloudformation:DescribeStackEvents",
                    "cloudformation:ListStackResources",
                    "cloudformation:GetTemplate",
                ],
                resources=[
                    f"arn:aws:cloudformation:{self.region}:{self.account}:stack/{self.stack_name}/*",
                ],
            )
        )

        github_actions_role.add_to_policy(
            iam.PolicyStatement(
                sid="RefreshDashboardViaSsm",
                actions=["ssm:SendCommand"],
                resources=[
                    f"arn:aws:ec2:{self.region}:{self.account}:instance/{dashboard_instance.instance_id}",
                    f"arn:aws:ssm:{self.region}::document/AWS-RunShellScript",
                ],
            )
        )

        github_actions_role.add_to_policy(
            iam.PolicyStatement(
                sid="ReadSsmCommandStatus",
                actions=[
                    "ssm:GetCommandInvocation",
                    "ssm:ListCommandInvocations",
                    "ssm:DescribeInstanceInformation",
                ],
                resources=["*"],
            )
        )

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
        
        CfnOutput(
            self,
            "GlobalPartnersGitHubActionsRoleArn",
            value=github_actions_role.role_arn,
        )

        if enable_rds_source:
            CfnOutput(
                self,
                "GlobalPartnersSqlServerEndpoint",
                value=sqlserver_instance.db_instance_endpoint_address,
            )

            CfnOutput(
                self,
                "GlobalPartnersGlueJdbcConnectionName",
                value=glue_jdbc_connection_name,
            )

            CfnOutput(
                self,
                "GlobalPartnersSqlServerSecretArn",
                value=jdbc_secret_arn,
            )
