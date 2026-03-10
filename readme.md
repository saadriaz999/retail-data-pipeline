# Retail Sales Analytics Pipeline

A full-stack retail data pipeline simulating real-world analytics architectures. It generates synthetic retail data, exposes it via a Flask API, ingests it into AWS (S3), transforms it with Glue ETL into a star schema, and enables querying via Snowflake data warehouse and dashboards via Streamlit.

---

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  data-fetcher   │     │  data-pipeline   │     │    analytics     │
│  (Flask API)    │────▶│  (AWS CDK)       │────▶│  (AWS CDK)       │
│  - /sales       │     │  - S3 Raw        │     │  - Glue Catalog  │
│  - /dimensions  │     │  - S3 Processed  │     │  - Snowflake     │
└─────────────────┘     │  - Lambda        │     │  - Streamlit     │
                         │  - Glue ETL      │     └─────────────────┘
                         └──────────────────┘
```

### Data Flow

1. **data-fetcher** – Flask API serving sales and dimension data from CSV
2. **Ingestion Lambda** – Fetches daily sales from API, uploads JSON to S3 (triggered by EventBridge daily)
3. **Raw S3** – `retail-raw-dataset` stores JSON at `year=YYYY/month=MM/day=DD/sales_*.json`
4. **S3 Event** – Each new file triggers the ETL Lambda
5. **Glue ETL** – Converts JSON to Parquet, writes to processed bucket with star schema
6. **Processed S3** – `retail-processed-dataset` with `fact_sales/` and `dimensions/`
7. **Snowflake** – Data warehouse with external tables over S3
8. **Streamlit** – Interactive dashboards and visualizations

---

## Project Structure

```
retail-data-pipeline/
├── data-fetcher/           # Flask API + synthetic data generator
│   ├── app.py              # API routes: /sales, /dimension_tables
│   ├── data_generator.py   # Generates star schema CSVs
│   └── data/               # CSV files (fact_sales, dim_*)
├── data-pipeline/          # AWS ingestion + ETL (CDK)
│   ├── app.py
│   ├── retail_stack/
│   │   └── data_pipeline_stack.py
│   ├── lambda/             # Ingestion + ETL trigger Lambdas
│   ├── lambda_dimension/   # Dimension loader (Custom Resource)
│   ├── glue/
│   │   └── glue_etl.py     # Spark ETL script
│   ├── backward_raw_data_filler.py    # Backfill raw JSON to S3
│   └── backward_processed_data_filler.py  # Run Glue once on all raw data
├── snowflake/              # Snowflake IaC + Streamlit config
│   ├── setup.py            # Creates stage + external tables (run once)
│   ├── .env                # Snowflake + AWS credentials (gitignored)
│   └── .env.example        # Template for env vars
├── streamlit/              # Streamlit in Snowflake
│   ├── streamlit_app.py    # App (runs inside Snowflake)
│   ├── deploy.py           # Deploy to Snowflake (Python IaC)
├── analytics/              # Athena + Glue Catalog (CDK, optional)
│   ├── app.py
│   └── analytics/
│       └── analytics_stack.py
└── readme.md
```

---

## Prerequisites

- Python 3.10+
- AWS CLI configured
- Node.js 20+ (for CDK)
- Flask API running (data-fetcher) for live ingestion

---

## Setup

### 1. Data Fetcher (Flask API)

```bash
cd data-fetcher
pip install flask pandas
python data_generator.py   # Creates data/ folder with CSVs
python app.py              # Starts API on port 5000
```

**API endpoints:**
- `GET /sales?date=20250115` – Sales for date (YYYYMMDD)
- `GET /dimension_tables` – All dimension tables

### 2. Data Pipeline (AWS)

```bash
cd data-pipeline
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cdk bootstrap               # First-time only
cdk deploy
```

Creates: S3 buckets, Lambda functions, Glue job, EventBridge rule, S3→Lambda trigger.

### 3. Analytics Stack (Athena + Glue Catalog)

```bash
cd analytics
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cdk deploy
```

Creates: Glue database `retail_db`, tables (fact_sales, dim_*), Athena workgroup, S3 for query results.

**Register partitions (once):**
```sql
MSCK REPAIR TABLE retail_db.fact_sales;
```

### 4. Athena Query Result Location

In Athena console → **Settings** → Set query result location to:
```
s3://retail-athena-results/queries/
```

### 5. Snowflake + Streamlit in Snowflake (Python IaC)

Creates stage and external tables in Snowflake, then deploys a Streamlit app **inside Snowflake** (runs in Snowsight).

**1. Configure credentials**

```bash
cp snowflake/.env.example snowflake/.env
# Edit: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
# and AWS keys (snowflake_access_key / snowflake_secret_key from IAM user snowflake-s3-reader)
```

**2. Run Snowflake IaC (stage + external tables)**

```bash
cd snowflake
pip install -r requirements.txt
python setup.py
```

**3. Deploy Streamlit to Snowflake**

```bash
cd streamlit
pip install snowflake-connector-python python-dotenv
python deploy.py
```

**4. View in Snowflake**

Open Snowsight → **Streamlit** → `retail_analytics`. The app runs inside Snowflake with revenue trends, top products, sales by country, and payment/device breakdown.

---

## Backfill Scripts

### Raw data backfill (entire 2025)

Uploads raw JSON for each day of 2025 to S3. Disables S3→Lambda trigger during upload to avoid 365 Glue runs.

```bash
cd data-pipeline
pip install requests boto3
python backward_raw_data_filler.py
```

### Processed data backfill (single Glue run)

Runs Glue once to process all raw data under `year=2025/` into Parquet.

```bash
python backward_processed_data_filler.py
```

---

## Schema Design

The project uses a **star schema**, standard in modern data warehouses.

---

### Fact Table

#### fact_sales

Stores transactional retail data. Partitioned by `year`, `month`.

| Column | Type | Description |
|--------|------|-------------|
| sale_id | bigint | Unique row identifier |
| order_id | bigint | Transaction ID |
| date_id | bigint | Foreign key to date dimension (YYYYMMDD) |
| product_id | bigint | Foreign key to product |
| customer_id | bigint | Foreign key to customer |
| store_id | bigint | Foreign key to store |
| quantity | bigint | Units sold |
| unit_price | double | Price at time of sale |
| discount | double | Discount applied |
| total_amount | double | Final revenue |
| payment_method | string | Payment type |
| device_type | string | Mobile or desktop |
| processed_timestamp | timestamp | ETL run time |
| date | date | Derived from date_id |
| year | int | Partition key |
| month | int | Partition key |

---

### Dimension Tables

#### dim_date

| Column | Type | Description |
|--------|------|-------------|
| date_id | bigint | YYYYMMDD key |
| full_date | timestamp | Actual date |
| day | bigint | Day of month |
| month | bigint | Month |
| year | bigint | Year |
| quarter | bigint | Quarter of year |
| day_of_week | bigint | Numeric weekday |
| is_weekend | boolean | Weekend flag |

#### dim_customer

| Column | Type | Description |
|--------|------|-------------|
| customer_id | bigint | Unique ID |
| country | string | Country |
| language | string | Language |

#### dim_product

| Column | Type | Description |
|--------|------|-------------|
| product_id | bigint | Unique ID |
| product_name | string | Furniture item name |
| category | string | Furniture |
| subcategory | string | Chair, table, etc. |
| brand | string | Brand |
| unit_cost | double | Cost of product |

#### dim_store

| Column | Type | Description |
|--------|------|-------------|
| store_id | bigint | Unique ID |
| store_name | string | Store name |
| city | string | City |
| state | string | State/region |
| country | string | Country |

---

## Example Athena Queries

```sql
-- Sales by month
SELECT year, month, SUM(total_amount) AS revenue
FROM retail_db.fact_sales
GROUP BY year, month
ORDER BY year, month;

-- Sales by country (join)
SELECT s.country, SUM(f.total_amount) AS total_sales
FROM retail_db.fact_sales f
JOIN retail_db.dim_store s ON f.store_id = s.store_id
GROUP BY s.country;

-- Top 10 products by revenue
SELECT p.product_name, SUM(f.total_amount) AS revenue
FROM retail_db.fact_sales f
JOIN retail_db.dim_product p ON f.product_id = p.product_id
GROUP BY p.product_name
ORDER BY revenue DESC
LIMIT 10;
```

---

## QuickSight

1. Subscribe to QuickSight (Standard ~$18/month)
2. Create data source → **Athena** → workgroup `retail-analytics`
3. Create dataset from `retail_db.fact_sales` or custom SQL
4. Build dashboards and charts

---

## AWS Resources

| Component | Resource |
|-----------|----------|
| Raw storage | `retail-raw-dataset` |
| Processed storage | `retail-processed-dataset` |
| Athena results | `retail-athena-results` |
| Glue job | `retail-sales-etl` |
| Glue database | `retail_db` |
| Athena workgroup | `retail-analytics` |
