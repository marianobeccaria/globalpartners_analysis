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

Remaining:

- Deploy the Streamlit dashboard to EC2 as a persistent browser-accessible service
- Add GitHub Actions CI/CD
- Merge `dev` into `main` after final validation
- Record the project walkthrough / presentation

## Architecture Summary

```text
Source data
  CSV files for current build
  SQL Server for intended production source
        |
        v
Glue Job 1: ingestion_job.py
        |
        v
S3 Bronze Layer
  Raw Parquet, partitioned by ingestion_date
        |
        v
Glue Job 2: bronze_to_silver_job.py
        |
        v
S3 Silver Layer
  orders_enriched, partitioned by year/month
        |
        +-------------------------------+
        |                               |
        v                               v
Glue Job 3: silver_to_gold_job.py   Glue Job 4: discount_effectiveness_job.py
        |                               |
        v                               v
S3 Gold Layer                       S3 Gold Layer
  6 core metric tables                discount_effectiveness
        |                               |
        +---------------+---------------+
                        |
                        v
              Streamlit Dashboard
```

The Glue workflow runs the jobs in sequence:

1. `globalpartners_ingestion_job`
2. `globalpartners_bronze_to_silver_job`
3. `globalpartners_silver_to_gold_job`
4. `globalpartners_discount_effectiveness_job`

## Repository Layout

```text
.
|-- cdk/
|   |-- app.py
|   |-- cdk/globalpartners_stack.py
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
- CloudWatch log groups
- Four Glue jobs
- Glue workflow and triggers

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

The dashboard currently runs locally. EC2 deployment is still pending.

Recommended deployment target:

- EC2 instance with Python 3.11
- IAM role or AWS credentials with read access to the Gold S3 prefixes
- Streamlit running as a `systemd` service
- Security group allowing inbound access to the dashboard port from approved IP ranges

After deployment, record the stakeholder URL here:

```text
Dashboard URL: TBD
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

## SME Assumptions To Confirm

These decisions are implemented or partially implemented and should be confirmed with the SME:

- Generate a full date dimension for 2020-2024 because the provided `date_dim.csv` only covers 2023.
- Treat `Alltown Fresh - DEVELOPMENT` as test data and exclude it from Silver.
- Use inferred discount signals because explicit discount fields/tables were not provided.
- Preserve order items that do not have modifiers/options.
- Decide whether very large bulk/catering-style orders should be included in CLV or handled separately.
- Confirm whether holiday enrichment should be expanded beyond the 2023 holidays provided in the source file.

## Security And Data Notes

- Do not commit raw CSV files, Parquet files, credentials, `.env` files, keys, or AWS credentials.
- `.gitignore` excludes local data, generated CDK output, virtual environments, and local testing artifacts.
- S3 bucket access should be scoped to the project prefixes where possible.
