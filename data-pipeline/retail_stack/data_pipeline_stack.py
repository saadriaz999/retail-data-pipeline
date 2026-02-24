from aws_cdk import (
    Stack,
    Duration,
    aws_s3 as s3,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_glue as glue,
    aws_events as events,
    aws_events_targets as targets,
    aws_s3_assets as s3_assets,
    aws_s3_notifications as s3_notifications
)
from constructs import Construct
import os


class RetailPipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # =====================================================
        # S3 BUCKETS
        # =====================================================
        raw_bucket = s3.Bucket(self, "RawRetailBucket",
                               bucket_name="retail-raw-dataset")

        processed_bucket = s3.Bucket(self, "ProcessedRetailBucket",
                                     bucket_name="retail-processed-dataset")

        # =====================================================
        # UPLOAD GLUE SCRIPT AUTOMATICALLY
        # =====================================================
        glue_asset = s3_assets.Asset(
            self,
            "GlueScript",
            path=os.path.join("glue", "glue_etl.py")
        )

        # =====================================================
        # GLUE ROLE
        # =====================================================
        glue_role = iam.Role(
            self,
            "GlueRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com")
        )

        glue_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSGlueServiceRole"
            )
        )

        raw_bucket.grant_read(glue_role)
        processed_bucket.grant_read_write(glue_role)
        glue_asset.grant_read(glue_role)

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
                script_location=f"s3://{glue_asset.s3_bucket_name}/{glue_asset.s3_object_key}",
                python_version="3"
            ),
            default_arguments={
                "--TempDir": f"s3://{processed_bucket.bucket_name}/temp/"
            },
            max_retries=1,
            timeout=60
        )

        # =====================================================
        # LAMBDA ROLE
        # =====================================================
        lambda_role = iam.Role(
            self,
            "LambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )

        lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaBasicExecutionRole"
            )
        )

        raw_bucket.grant_write(lambda_role)

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:StartJobRun"],
                resources=["*"]
            )
        )

        # =====================================================
        # INGESTION LAMBDA FUNCTION
        # =====================================================
        lambda_function = _lambda.Function(
            self,
            "RetailIngestionLambda",
            runtime=_lambda.Runtime.PYTHON_3_10,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                "lambda",
                bundling={
                    "image": _lambda.Runtime.PYTHON_3_10.bundling_image,
                    "command": [
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && cp -au . /asset-output"
                    ],
                },
            ),
            timeout=Duration.seconds(60),
            role=lambda_role,
            environment={
                "BUCKET": raw_bucket.bucket_name,
                "GLUE_JOB": "retail-sales-etl"
            }
        )

        # =====================================================
        # DIMENSION TABLE LAMBDA FUNCTION
        # =====================================================

        # =====================================================
        # ETL TRIGGER LAMBDA
        # =====================================================
        etl_lambda = _lambda.Function(
            self,
            "RetailGlueTriggerLambda",
            runtime=_lambda.Runtime.PYTHON_3_10,
            handler="etl_trigger.lambda_handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.seconds(60),
            role=lambda_role,
            environment={
                "GLUE_JOB": "retail-sales-etl"
            }
        )

        # =====================================================
        # DAILY SCHEDULE (AUTOMATIC INGESTION)
        # =====================================================
        rule = events.Rule(
            self,
            "DailyIngestion",
            schedule=events.Schedule.rate(Duration.days(1))
        )

        rule.add_target(targets.LambdaFunction(lambda_function))


        # =====================================================
        # S3 EVENT → GLUE TRIGGER
        # =====================================================
        raw_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3_notifications.LambdaDestination(etl_lambda)
        )
