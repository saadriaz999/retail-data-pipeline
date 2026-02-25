import json
import boto3
import requests
from datetime import datetime
import os

s3 = boto3.client("s3")
glue = boto3.client("glue")

BUCKET_NAME = os.environ["BUCKET"]

API_URL = "http://18.221.191.16:5000/sales"


def lambda_handler(event, context):
    try:
        now = datetime.utcnow()
        year = 2025
        month = now.strftime("%m")
        day = now.strftime("%d")

        date_param = f"{year}{month}{day}"
        response = requests.get(API_URL, params={"date": date_param}, timeout=10)
        response.raise_for_status()
        data = response.json()

        key = f"year={year}/month={month}/day={day}/sales_{year}-{month}-{day}.json"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(data)
        )

        return {
            "status": "success",
            "records": len(data)
        }

    except Exception as e:
        print("ERROR:", str(e))
        raise
