import json
import requests
import boto3
from datetime import datetime, timedelta

BUCKET = "retail-raw-dataset"
API_URL = "http://18.221.191.16:5000/sales"

s3 = boto3.client("s3")

def disable_glue_trigger():
    config = s3.get_bucket_notification_configuration(Bucket=BUCKET)
    s3.put_bucket_notification_configuration(
        Bucket=BUCKET,
        NotificationConfiguration={
            "LambdaFunctionConfigurations": [],
            "QueueConfigurations": config.get("QueueConfigurations", []),
            "TopicConfigurations": config.get("TopicConfigurations", []),
        },
    )

def enable_glue_trigger(saved_config):
    s3.put_bucket_notification_configuration(
        Bucket=BUCKET,
        NotificationConfiguration={
            "LambdaFunctionConfigurations": saved_config.get("LambdaFunctionConfigurations", []),
            "QueueConfigurations": saved_config.get("QueueConfigurations", []),
            "TopicConfigurations": saved_config.get("TopicConfigurations", []),
        },
    )

saved_config = s3.get_bucket_notification_configuration(Bucket=BUCKET)
disable_glue_trigger()

start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 12, 31)
current = start_date

while current <= end_date:
    date_str = current.strftime("%Y-%m-%d")
    date_param = current.strftime("%Y%m%d")
    print("Processing:", date_str)
    response = requests.get(API_URL, params={"date": date_param})
    data = response.json()

    year, month, day = date_str.split("-")
    key = f"year={year}/month={month}/day={day}/sales_{date_str}.json"
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(data))
    current += timedelta(days=1)

enable_glue_trigger(saved_config)
print("Backfill completed")