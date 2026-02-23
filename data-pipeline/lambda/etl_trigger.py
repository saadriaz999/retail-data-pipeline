import boto3
import os

glue = boto3.client("glue")

def lambda_handler(event, context):
    job_name = os.environ["GLUE_JOB"]

    response = glue.start_job_run(JobName=job_name)

    return response
