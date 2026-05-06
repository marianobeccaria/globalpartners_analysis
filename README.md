# GlobalPartners Business Insights Pipeline

End-to-end analytics project for GlobalPartners restaurant order data. The project ingests source CSV files, builds Bronze/Silver/Gold datasets in S3 with AWS Glue, and serves seven business metric pages through a Streamlit dashboard.

The current build uses local CSV files as the source dataset. The intended production pattern is the same pipeline with SQL Server as the upstream source.

## Project Status

Completed:

- Exploratory data analysis
- Architecture design
- AWS CDK infrastructure
- Four Glue jobs
- Seven business metrics
- Streamlit dashboard with seven pages
- EC2 dashboard deployment
- GitHub Actions CI/CD using AWS OIDC
- Automated dashboard refresh on EC2 through SSM

Remaining:

- Final documentation polish
- SME review and assumption confirmation
- Record the project walkthrough / presentation

## Architecture Summary

```text
SQL Server / RDS
        |
        v
AWS Glue JDBC Connection
        |
        v
Glue ingestion_job.py
        |
        v
S3 Bronze
        |
        v
Glue bronze_to_silver_job.py
        |
        v
S3 Silver: orders_enriched
        |
        +----------------------------+
        |                            |
        v                            v
Glue silver_to_gold_job.py     Glue discount_effectiveness_job.py
        |                            |
        v                            v
S3 Gold business tables        S3 Gold discount_effectiveness
        |
        v
Streamlit Dashboard on EC2
```

The Glue workflow runs the jobs in sequence:

1. `globalpartners_ingestion_job`
2. `globalpartners_bronze_to_silver_job`
3. `globalpartners_silver_to_gold_job`
4. `globalpartners_discount_effectiveness_job`

## Repository Layout

```text
.
|-- .github/
|   `-- workflows/
|       `-- deploy.yml
|-- cdk/
|   |-- app.py
|   |-- cdk/globalpartners_stack.py
|   |-- cdk.json
|   |-- cdk.context.json
|   |-- .env.example
|   `-- requirements.txt
|-- dashboard/
|   |-- Home.py
|   |-- pages/
|   |   |-- 1_CLV.py
|   |   |-- 2_RFM_Segments.py
|   |   |-- 3_Churn_Risk.py
|   |   |-- 4_Sales_Trends.py
|   |   |-- 5_Loyalty_Impact.py
|   |   |-- 6_Restaurant_Performance.py
|   |   `-- 7_Discount_Effectiveness.py
|   |-- utils/data_loader.py
|   `-- requirements.txt
|-- glue_jobs/
|   |-- ingestion_job.py
|   |-- bronze_to_silver_job.py
|   |-- silver_to_gold_job.py
|   `-- discount_effectiveness_job.py
|-- docs/
|   |-- globalpartners_architecture.png
|   `-- solution_design_document.md
|-- src/
|   `-- exploratory_data_analysis.py
`-- data/
    |-- order_items.csv
    |-- order_item_options.csv
    `-- date_dim.csv
```

Notes:

- `data/` is ignored by git and should not be committed.
- `.env` files are ignored by git and should not be committed.
- CDK deploy uploads the Glue scripts from `glue_jobs/` to `s3://<bucket>/glue_scripts/`.
- CDK deploy uploads the Streamlit dashboard from `dashboard/` to `s3://<bucket>/dashboard_app/`.

## Source Data

Current source files:

- `order_items.csv`
- `order_item_options.csv`
- `date_dim.csv`

Known source data decisions reflected in the pipeline:

- The provided `date_dim.csv` only covers 2023, so Silver generates a full 2020-2024 date spine.
- `Alltown Fresh - DEVELOPMENT` rows are treated as test/development orders and filtered out of Silver.
- `order_item_options` duplicate rows are removed in Silver.
- Order items without options are preserved using a left join from items to aggregated options.
- Discounts are inferred using proxy signals because no explicit discount code or promotion table exists in the provided files.

## S3 Folder Structure

Expected bucket layout:

```text
s3://<bucket>/
|-- source/
|   |-- order_items.csv
|   |-- order_item_options.csv
|   `-- date_dim.csv
|-- glue_scripts/
|   |-- ingestion_job.py
|   |-- bronze_to_silver_job.py
|   |-- silver_to_gold_job.py
|   `-- discount_effectiveness_job.py
|-- dashboard_app/
|   |-- Home.py
|   |-- pages/
|   |-- utils/
|   `-- requirements.txt
|-- bronze/
|   |-- order_items/ingestion_date=YYYY-MM-DD/
|   |-- order_item_options/ingestion_date=YYYY-MM-DD/
|   `-- date_dim/ingestion_date=YYYY-MM-DD/
|-- silver/
|   `-- orders_enriched/year=YYYY/month=<1-12>/
|-- gold/
|   |-- customer_clv_daily/
|   |-- customer_rfm_segments/
|   |-- customer_churn_indicators/
|   |-- sales_trends/
|   |-- loyalty_comparison/
|   |-- location_performance/
|   `-- discount_effectiveness/
|-- tmp/
`-- spark-logs/
```

## Glue Jobs

| Job | Script | Runtime | Purpose | Output |
| --- | --- | --- | --- | --- |
| Ingestion | `glue_jobs/ingestion_job.py` | Python Shell | Read source CSVs from S3 and write raw Parquet to Bronze | `bronze/order_items`, `bronze/order_item_options`, `bronze/date_dim` |
| Bronze to Silver | `glue_jobs/bronze_to_silver_job.py` | Glue Spark | Clean, deduplicate, generate full date dimension, preserve optionless items, compute gross revenue | `silver/orders_enriched` |
| Silver to Gold | `glue_jobs/silver_to_gold_job.py` | Glue Spark | Build six core business metric tables | Six Gold metric tables |
| Discount Effectiveness | `glue_jobs/discount_effectiveness_job.py` | Glue Spark | Infer promotional orders from zero-price and below-category-price signals | `gold/discount_effectiveness` |

## Gold Tables

| Gold table | Dashboard usage |
| --- | --- |
| `customer_clv_daily` | Customer lifetime value page and Home KPIs |
| `customer_rfm_segments` | RFM segmentation page and Home distribution |
| `customer_churn_indicators` | Churn risk page and Home at-risk customer KPI |
| `sales_trends` | Sales trends page and monthly revenue trend |
| `loyalty_comparison` | Loyalty impact page |
| `location_performance` | Restaurant performance page and location KPIs |
| `discount_effectiveness` | Discount effectiveness page |

## Prerequisites

- Python 3.11 recommended
- AWS CLI configured with access to the target AWS account
- AWS CDK v2 installed
- Existing S3 bucket for the project data lake
- Permissions to create/update IAM roles, Glue jobs, Glue workflow/triggers, CloudWatch log groups, and S3 deployments

## Configure CDK

From the repository root:

```bash
cd cdk
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Update `cdk/.env` with your environment values. The current example uses:

```text
S3_BUCKET_NAME=mbeccaria-dea-globalpartners
GLUE_ROLE_NAME=globalpartners-glue-role
GLUE_DB_NAME=globalpartners_db
GLUE_WORKFLOW_NAME=globalpartners_daily_pipeline
INGESTION_JOB_NAME=globalpartners_ingestion_job
BRONZE_TO_SILVER_JOB_NAME=globalpartners_bronze_to_silver_job
SILVER_TO_GOLD_JOB_NAME=globalpartners_silver_to_gold_job
DISCOUNT_JOB_NAME=globalpartners_discount_effectiveness_job
PIPELINE_SCHEDULE=cron(0 2 * * ? *)
DASHBOARD_INSTANCE_TYPE=t3.small
DASHBOARD_ALLOWED_CIDR=0.0.0.0/0
DASHBOARD_PORT=8501
GITHUB_REPO=marianobeccaria/globalpartners_analysis
GITHUB_BRANCH=main
GITHUB_ACTIONS_ROLE_NAME=globalpartners-github-actions-role
CDK_QUALIFIER=hnb659fds
```

## Deploy Infrastructure

From `cdk/`:

```bash
cdk synth
cdk deploy
```

`cdk deploy` creates/updates:

- Glue IAM role and policies
- S3 folder placeholders
- Glue script uploads under `s3://<bucket>/glue_scripts/`
- Dashboard app upload under `s3://<bucket>/dashboard_app/`
- CloudWatch log groups
- Four Glue jobs
- Glue workflow and triggers
- EC2 instance for the Streamlit dashboard
- EC2 IAM role with read access to Gold and dashboard app S3 prefixes
- EC2 security group for dashboard browser access
- GitHub Actions OIDC role for CI/CD deployment
- CloudFormation outputs for dashboard URL, EC2 instance ID, and GitHub role ARN

## Upload Source CSV Files

If the source CSV files are local, upload them to the S3 source prefix before running the workflow:

```bash
aws s3 cp data/order_items.csv s3://<bucket>/source/order_items.csv
aws s3 cp data/order_item_options.csv s3://<bucket>/source/order_item_options.csv
aws s3 cp data/date_dim.csv s3://<bucket>/source/date_dim.csv
```

Replace `<bucket>` with the value from `S3_BUCKET_NAME`.

## Run the Glue Pipeline

Start the workflow manually:

```bash
aws glue start-workflow-run --name globalpartners_daily_pipeline
```

Check the latest run status for each job:

```bash
jobs=(
  globalpartners_ingestion_job
  globalpartners_bronze_to_silver_job
  globalpartners_silver_to_gold_job
  globalpartners_discount_effectiveness_job
)

for job_name in "${jobs[@]}"; do
  echo "Job Name: ${job_name}"

  aws glue get-job-runs \
    --job-name "${job_name}" \
    --max-results 1 \
    --query 'JobRuns[0].{Status:JobRunState,Error:ErrorMessage,Duration:ExecutionTime,Started:StartedOn}' \
    --output table
done
```

Expected result: all four jobs should finish with `SUCCEEDED`.

## Run the Dashboard Locally

From the repository root:

```bash
cd dashboard
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt python-dotenv
export S3_BUCKET_NAME=<bucket>
streamlit run Home.py
```

Local URL:

```text
http://localhost:8501
```

The dashboard reads the Gold Parquet tables from S3 using `dashboard/utils/data_loader.py`. Data is cached for one hour with `st.cache_data(ttl=3600)`.

## Dashboard Pages

- `Home.py`: executive overview and KPI summary
- `1_CLV.py`: customer lifetime value
- `2_RFM_Segments.py`: customer segmentation
- `3_Churn_Risk.py`: churn risk indicators
- `4_Sales_Trends.py`: revenue and order trends
- `5_Loyalty_Impact.py`: loyalty vs non-loyalty comparison
- `6_Restaurant_Performance.py`: location-level performance
- `7_Discount_Effectiveness.py`: inferred promotional order effectiveness

## EC2 Dashboard Deployment

The Streamlit dashboard is deployed to EC2 through CDK.

Current dashboard URL:

```text
http://ec2-13-219-230-252.compute-1.amazonaws.com:8501
```

Deployment details:

- Instance type: `t3.small`
- OS: Amazon Linux 2023
- Dashboard port: `8501`
- Streamlit entrypoint: `Home.py`
- Service manager: `systemd`
- Service name: `globalpartners-dashboard`
- App path on EC2: `/opt/globalpartners/dashboard`
- Virtual environment: `/opt/globalpartners/venv`
- Source app copy in S3: `s3://<bucket>/dashboard_app/`

The EC2 instance uses an IAM role with:

- S3 read access to `gold/*`
- S3 read access to `dashboard_app/*`
- SSM permissions through `AmazonSSMManagedInstanceCore`

Dashboard access is controlled by:

```text
DASHBOARD_ALLOWED_CIDR
```

For public demo access, this can be temporarily set to:

```text
0.0.0.0/0
```

For restricted access, use a specific public IP:

```text
x.x.x.x/32
```

Connect to the EC2 instance with SSM:

```bash
aws ssm start-session --target <instance-id> --region us-east-1
```

Useful troubleshooting commands on EC2:

```bash
sudo systemctl status globalpartners-dashboard --no-pager
sudo journalctl -u globalpartners-dashboard -n 100 --no-pager
sudo tail -n 100 /var/log/cloud-init-output.log
```

## CI/CD Deployment

GitHub Actions deploys the project from `main`.

Workflow file:

```text
.github/workflows/deploy.yml
```

Trigger behavior:

- Runs automatically on push to `main`
- Can also be run manually with `workflow_dispatch`

The workflow performs:

1. Checks out the repository
2. Assumes the AWS deploy role using GitHub OIDC
3. Installs Python and CDK dependencies
4. Runs `cdk synth`
5. Runs `cdk deploy GlobalPartnersStack --require-approval never`
6. Reads the EC2 instance ID from CDK outputs
7. Sends an SSM command to EC2 to refresh the dashboard
8. Restarts the `globalpartners-dashboard` service

GitHub repository variables required:

```text
AWS_ACCOUNT_ID=858477419022
DASHBOARD_ALLOWED_CIDR=0.0.0.0/0
```

The workflow uses AWS OIDC instead of long-lived AWS access keys.

OIDC role:

```text
globalpartners-github-actions-role
```

The role trust policy is scoped to:

```text
repo:marianobeccaria/globalpartners_analysis:ref:refs/heads/main
```

Dashboard refresh command run by CI/CD:

```bash
aws s3 sync s3://<bucket>/dashboard_app/ /opt/globalpartners/dashboard/ --delete
systemctl restart globalpartners-dashboard
```

## Validation Checklist

After CDK deploy and pipeline execution, validate:

- `s3://<bucket>/glue_scripts/` contains the four Glue scripts.
- `bronze/` contains Parquet outputs for all three source tables.
- `silver/orders_enriched/` contains year/month partitions.
- `gold/` contains all seven Gold tables.
- The Glue workflow run shows all four jobs as `SUCCEEDED`.
- Streamlit Home loads without errors.
- Home caption shows the current dynamic restaurant count.
- Restaurant Performance shows the expected locations after the Silver left join fix.
- Discount Effectiveness loads `gold/discount_effectiveness` and shows promotional vs full-price order metrics.
- EC2 dashboard URL opens in a browser.
- Streamlit charts render without Plotly keyword warnings.
- `globalpartners-dashboard` systemd service is active.
- GitHub Actions deploy workflow completes successfully from `main`.
- GitHub Actions refreshes the EC2 dashboard through SSM.
- `cdk/cdk.json` and `cdk/cdk.context.json` are committed so GitHub Actions can synthesize the stack.

## SME Assumptions To Confirm

These decisions are implemented or partially implemented and should be confirmed with the SME:

- Generate a full date dimension for 2020-2024 because the provided `date_dim.csv` only covers 2023.
- Treat `Alltown Fresh - DEVELOPMENT` as test data and exclude it from Silver.
- Use inferred discount signals because explicit discount fields/tables were not provided.
- Preserve order items that do not have modifiers/options.
- Decide whether very large bulk/catering-style orders should be included in CLV or handled separately.
- Confirm whether holiday enrichment should be expanded beyond the 2023 holidays provided in the source file.

## SME Confirmed Assumptions

- Generated date dimension
- Non-2023 holidays treated as false
- Proxy discount logic
- No profitability calculation due to missing cost data
- Bulk/catering orders included but flagged
- CSV ingestion is current build; SQL Server/RDS JDBC is target production design

## Security And Data Notes

- Do not commit raw CSV files, Parquet files, credentials, `.env` files, keys, or AWS credentials.
- `.gitignore` excludes local data, generated CDK output, virtual environments, and local testing artifacts.
- S3 bucket access should be scoped to the project prefixes where possible.
