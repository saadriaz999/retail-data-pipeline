import boto3

glue = boto3.client("glue")

def lambda_handler(event, context):

    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        file_path = f"s3://{bucket}/{key}"

        glue.start_job_run(
            JobName="retail-sales-etl",
            Arguments={
                "--input_path": file_path
            }
        )

    return {"status": "Glue started"}
