import requests
import pandas as pd
import boto3
import os
from io import BytesIO

s3 = boto3.client("s3")

RAW_BUCKET = os.environ["RAW_BUCKET"]
PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
API_URL = os.environ["API_URL"]


def lambda_handler(event, context):
    print("Fetching dimension tables")

    response = requests.get(f"{API_URL}/dimension_tables")
    data = response.json()

    for table, rows in data["data"].items():
        df = pd.DataFrame(rows)

        # ---------- RAW JSON ----------
        json_key = f"dimensions/{table}.json"
        s3.put_object(
            Bucket=RAW_BUCKET,
            Key=json_key,
            Body=df.to_json(orient="records")
        )

        # ---------- PROCESSED PARQUET ----------
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine='fastparquet')

        parquet_key = f"dimensions/{table}/{table}.parquet"

        s3.put_object(
            Bucket=PROCESSED_BUCKET,
            Key=parquet_key,
            Body=buffer.getvalue()
        )

    return {"status": "success"}
