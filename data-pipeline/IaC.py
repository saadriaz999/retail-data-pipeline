# ============================================================
# FULL PYTHON IaC USING AWS CDK
# Glue + Lambda + S3 + requests package + Glue ETL script
# ============================================================
# This is a COMPLETE reproducible setup.
# You can recreate your pipeline anytime.

# ============================================================
# PROJECT STRUCTURE
# ============================================================
# retail-pipeline/
# │
# ├── app.py
# ├── retail_stack.py
# ├── requirements.txt
# │
# ├── lambda/
# │   ├── lambda_function.py
# │   └── requirements.txt
# │
# └── glue/
#     └── glue_etl.py


# ============================================================
# STEP 1: INSTALL CDK
# ============================================================
# pip install aws-cdk-lib constructs
# npm install -g aws-cdk
# cdk init app --language python


# ============================================================
# STEP 2: requirements.txt (ROOT)
# ============================================================
# aws-cdk-lib
# constructs


# ============================================================
# STEP 3: app.py
# ============================================================
from aws_cdk import App
from retail_stack import RetailPipelineStack

app = App()
RetailPipelineStack(app, "RetailPipelineStack")
app.synth()


# ============================================================
# STEP 4: retail_stack.py
# ============================================================
from aws_cdk import (
    Stack,
    Duration,
    aws_s3 as s3,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_s3_notifications as s3n,
    aws_glue as glue,
)
from constructs import Construct


class RetailPipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # =====================================================
        # S3 BUCKETS
        # =====================================================
        raw_bucket = s3.Bucket(self, "RawRetailBucket")

        processed_bucket = s3.Bucket(self, "ProcessedRetailBucket")

        # =====================================================
        # GLUE ROLE
        # =====================================================
        glue_role = iam.Role(
            self,
            "GlueRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        )

        glue_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSGlueServiceRole"
            )
        )

        raw_bucket.grant_read(glue_role)
        processed_bucket.grant_read_write(glue_role)

        # =====================================================
        # GLUE JOB
        # =====================================================
        glue_job = glue.CfnJob(
            self,
            "RetailGlueJob",
            name="retail-sales-etl",
            role=glue_role.role_arn,
            glue_version="4.0",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{processed_bucket.bucket_name}/scripts/glue_etl.py",
                python_version="3",
            ),
            default_arguments={
                "--TempDir": f"s3://{processed_bucket.bucket_name}/temp/",
            },
            max_retries=1,
            timeout=60,
        )

        # =====================================================
        # LAMBDA ROLE
        # =====================================================
        lambda_role = iam.Role(
            self,
            "LambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )

        lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaBasicExecutionRole"
            )
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:StartJobRun"],
                resources=["*"],
            )
        )

        # =====================================================
        # LAMBDA FUNCTION
        # =====================================================
        lambda_function = _lambda.Function(
            self,
            "TriggerGlueLambda",
            runtime=_lambda.Runtime.PYTHON_3_10,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.seconds(60),
            role=lambda_role,
        )

        # =====================================================
        # S3 EVENT TRIGGER
        # =====================================================
        raw_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(lambda_function),
        )


# ============================================================
# STEP 5: lambda/requirements.txt
# ============================================================
# requests


# ============================================================
# STEP 6: lambda/lambda_function.py
# ============================================================
import json
import boto3
import requests


def lambda_handler(event, context):

    # Example external API call
    # (shows requests package is working)
    try:
        requests.get("https://example.com", timeout=3)
    except Exception:
        pass

    glue = boto3.client("glue")

    response = glue.start_job_run(
        JobName="retail-sales-etl"
    )

    return {
        "statusCode": 200,
        "body": json.dumps(response)
    }


# ============================================================
# STEP 7: glue/glue_etl.py
# ============================================================
# This is your Glue PySpark job

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, [])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# =====================================================
# READ RAW DATA
# =====================================================
raw_df = spark.read.option("header", True).csv(
    "s3://YOUR_RAW_BUCKET/"
)

# =====================================================
# SIMPLE TRANSFORM
# =====================================================
clean_df = raw_df.dropDuplicates()

# Example transformation
clean_df = clean_df.withColumn("processed_timestamp", F.current_timestamp())

# =====================================================
# WRITE TO PROCESSED
# =====================================================
clean_df.write.mode("overwrite").parquet(
    "s3://YOUR_PROCESSED_BUCKET/fact_sales/"
)


# ============================================================
# STEP 8: DEPLOY
# ============================================================
# cdk bootstrap
# cdk deploy


# ============================================================
# STEP 9: UPLOAD GLUE SCRIPT
# ============================================================
# Upload glue/glue_etl.py to:
# processed bucket -> scripts/glue_etl.py


# ============================================================
# HOW IT WORKS
# ============================================================
# 1. Upload file to raw bucket
# 2. S3 triggers Lambda
# 3. Lambda calls external API (requests)
# 4. Lambda starts Glue job
# 5. Glue processes raw -> fact table


# ============================================================
# OPTIONAL IMPROVEMENTS
# ============================================================
# - Pass S3 file path dynamically
# - Add Glue crawler
# - Partitioned fact table
# - Monitoring and alerts
# - Dead-letter queue
# - Retry policies

# Ask if you want a production version.
