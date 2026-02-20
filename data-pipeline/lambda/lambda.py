import json
import boto3
import requests
from datetime import datetime

s3 = boto3.client("s3")

BUCKET_NAME = "retail-raw-dataset"
API_URL = "http://18.221.191.16:5000/sales/today"


def lambda_handler(event, context):
    try:
        # 1. Call retail API
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        # 2. Get current UTC date
        now = datetime.utcnow()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")

        # 3. Partitioned S3 path
        key = f"year={year}/month={month}/day={day}/sales_{year}-{month}-{day}.json"

        # 4. Save to S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(data)
        )

        return {
            "status": "success",
            "s3_key": key,
            "records": len(data)
        }

    except Exception as e:
        print("ERROR:", str(e))
        raise
