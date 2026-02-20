import boto3
import json

# 1️⃣ S3
s3 = boto3.client('s3')
s3.create_bucket(Bucket='retail-raw-dataset')
s3.put_bucket_versioning(Bucket='retail-raw-dataset',
                         VersioningConfiguration={'Status': 'Enabled'})

# 2️⃣ IAM role for Lambda
iam = boto3.client('iam')
role_name = 'retail-lambda-role'
assume_role_policy = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "lambda.amazonaws.com"},
        "Action": "sts:AssumeRole"
    }]
}
role = iam.create_role(RoleName=role_name,
                       AssumeRolePolicyDocument=json.dumps(assume_role_policy))

# 3️⃣ Attach policy for S3 PutObject
policy = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": "s3:PutObject",
        "Resource": "arn:aws:s3:::retail-raw-dataset/*"
    }]
}
iam.put_role_policy(RoleName=role_name,
                    PolicyName='S3PutObjectPolicy',
                    PolicyDocument=json.dumps(policy))

# 4️⃣ Lambda (upload code, set handler)
lambda_client = boto3.client('lambda')
with open('lambda_function.zip', 'rb') as f:
    zipped_code = f.read()

lambda_client.create_function(
    FunctionName='retail-sales-ingestion',
    Runtime='python3.11',
    Role=role['Role']['Arn'],
    Handler='lambda_function.lambda_handler',
    Code={'ZipFile': zipped_code},
    Timeout=30,
    MemorySize=128
)

# 5️⃣ EventBridge Scheduler
scheduler = boto3.client('scheduler')
scheduler.create_schedule(
    Name='retail-daily-sales-ingestion',
    ScheduleExpression='rate(1 day)',
    Target={
        'Arn': 'arn:aws:lambda:us-east-2:157680547037:function:retail-sales-ingestion',
        'RoleArn': role['Role']['Arn']
    }
)
