import boto3
from datetime import datetime

BUCKET = "retail-raw-dataset"
GLUE_JOB = "retail-sales-etl"

# Path to all 2025 raw data (or use "s3://retail-raw-dataset/" for everything)
INPUT_PREFIX = "s3://retail-raw-dataset/year=2025/"

glue = boto3.client("glue")

print("Starting Glue job to process all raw data...")
response = glue.start_job_run(
    JobName=GLUE_JOB,
    Arguments={"--input_path": INPUT_PREFIX}
)
job_run_id = response["JobRunId"]
print(f"Glue job started: {job_run_id}")

print("Glue job started:", job_run_id)
print("Check AWS Glue console for status. Parquet will be written to s3://retail-processed-dataset/fact_sales/")