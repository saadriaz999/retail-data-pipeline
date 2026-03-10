"""
Snowflake data setup: creates S3 stage and external tables over retail-processed-dataset.
Run once after data-pipeline is deployed. Requires .env with Snowflake + AWS credentials.
"""
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
for p in [Path(__file__).parent / ".env", Path(__file__).parent.parent / ".env"]:
    if p.exists():
        load_dotenv(p)
        break

import snowflake.connector


def get_config():
    cfg = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "RETAIL_DB"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        "aws_key": os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("snowflake_access_key"),
        "aws_secret": os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("snowflake_secret_key"),
    }
    missing = [k for k in ["account", "user", "password", "aws_key", "aws_secret"] if not cfg.get(k)]
    if missing:
        print(f"Missing: {missing}. Copy .env.example to .env and fill values.")
        sys.exit(1)
    return cfg


def run(conn, sql):
    for stmt in sql.strip().split(";"):
        if not (s := stmt.strip()):
            continue
        try:
            conn.cursor().execute(s)
        except Exception as e:
            if "already exists" not in str(e).lower():
                raise


def main():
    cfg = get_config()
    conn = snowflake.connector.connect(**{k: v for k, v in cfg.items() if k in ["account", "user", "password", "warehouse"]})
    aws_key = cfg["aws_key"].replace("'", "''")
    aws_secret = cfg["aws_secret"].replace("'", "''")

    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {cfg['database']}")
    cur.execute(f"USE DATABASE {cfg['database']}")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {cfg['schema']}")
    cur.execute(f"USE SCHEMA {cfg['schema']}")

    print("Creating stage...")
    run(conn, f"""
    CREATE STAGE IF NOT EXISTS retail_stage
        URL = 's3://retail-processed-dataset/'
        CREDENTIALS = (AWS_KEY_ID = '{aws_key}' AWS_SECRET_KEY = '{aws_secret}');
    """)

    print("Creating external tables...")
    run(conn, """
    CREATE OR REPLACE EXTERNAL TABLE fact_sales (
        customer_id INT AS (VALUE:customer_id::INT),
        device_type STRING AS (VALUE:device_type::STRING),
        discount FLOAT AS (VALUE:discount::FLOAT),
        order_id INT AS (VALUE:order_id::INT),
        payment_method STRING AS (VALUE:payment_method::STRING),
        product_id INT AS (VALUE:product_id::INT),
        quantity INT AS (VALUE:quantity::INT),
        sale_id INT AS (VALUE:sale_id::INT),
        store_id INT AS (VALUE:store_id::INT),
        total_amount FLOAT AS (VALUE:total_amount::FLOAT),
        unit_price FLOAT AS (VALUE:unit_price::FLOAT),
        processed_timestamp TIMESTAMP AS (VALUE:processed_timestamp::TIMESTAMP),
        date DATE AS (VALUE:date::DATE),
        year INT AS (SPLIT_PART(SPLIT_PART(METADATA$FILENAME,'year=',2),'/',1)::INT),
        month INT AS (SPLIT_PART(SPLIT_PART(METADATA$FILENAME,'month=',2),'/',1)::INT)
    )
    WITH LOCATION = @retail_stage/fact_sales/
    FILE_FORMAT = (TYPE = PARQUET)
    AUTO_REFRESH = FALSE;
    """)
    run(conn, """
    CREATE OR REPLACE EXTERNAL TABLE dim_customer (
        customer_id INT AS (VALUE:customer_id::INT),
        country STRING AS (VALUE:country::STRING),
        language STRING AS (VALUE:language::STRING)
    )
    WITH LOCATION = @retail_stage/dimensions/dim_customer/
    FILE_FORMAT = (TYPE = PARQUET)
    AUTO_REFRESH = FALSE;
    """)
    run(conn, """
    CREATE OR REPLACE EXTERNAL TABLE dim_product (
        product_id INT AS (VALUE:product_id::INT),
        product_name STRING AS (VALUE:product_name::STRING),
        category STRING AS (VALUE:category::STRING),
        subcategory STRING AS (VALUE:subcategory::STRING),
        brand STRING AS (VALUE:brand::STRING),
        unit_cost FLOAT AS (VALUE:unit_cost::FLOAT)
    )
    WITH LOCATION = @retail_stage/dimensions/dim_product/
    FILE_FORMAT = (TYPE = PARQUET)
    AUTO_REFRESH = FALSE;
    """)
    run(conn, """
    CREATE OR REPLACE EXTERNAL TABLE dim_store (
        store_id INT AS (VALUE:store_id::INT),
        store_name STRING AS (VALUE:store_name::STRING),
        city STRING AS (VALUE:city::STRING),
        state STRING AS (VALUE:state::STRING),
        country STRING AS (VALUE:country::STRING)
    )
    WITH LOCATION = @retail_stage/dimensions/dim_store/
    FILE_FORMAT = (TYPE = PARQUET)
    AUTO_REFRESH = FALSE;
    """)
    run(conn, """
    CREATE OR REPLACE EXTERNAL TABLE dim_date (
        date_id INT AS (VALUE:date_id::INT),
        full_date TIMESTAMP AS (VALUE:full_date::TIMESTAMP),
        day INT AS (VALUE:day::INT),
        month INT AS (VALUE:month::INT),
        year INT AS (VALUE:year::INT),
        quarter INT AS (VALUE:quarter::INT),
        day_of_week INT AS (VALUE:day_of_week::INT),
        is_weekend BOOLEAN AS (VALUE:is_weekend::BOOLEAN)
    )
    WITH LOCATION = @retail_stage/dimensions/dim_date/
    FILE_FORMAT = (TYPE = PARQUET)
    AUTO_REFRESH = FALSE;
    """)

    for tbl in ["fact_sales", "dim_customer", "dim_product", "dim_store", "dim_date"]:
        cur.execute(f"ALTER EXTERNAL TABLE {tbl} REFRESH")

    conn.close()
    print("Done. Run deploy.py to deploy Streamlit to Snowflake.")


if __name__ == "__main__":
    main()
