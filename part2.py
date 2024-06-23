import boto3
import json
import logging
import zipfile
import io
import time
import sys
from botocore.exceptions import ClientError

# Set the AWS region
region = 'us-west-2'

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb', region_name=region)
iam = boto3.client('iam')
lambda_client = boto3.client('lambda', region_name=region)

# Define the DynamoDB table name
DYNAMODB_TABLE_NAME = 'S3-object-size-history'

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def progress_bar(seconds):
    """Shows a simple progress bar in the command window."""
    for _ in range(seconds):
        time.sleep(1)
        print(".", end="")
        sys.stdout.flush()
    print()

def create_iam_role_for_lambda(iam_role_name):
    """
    Creates an IAM role that grants the Lambda function basic permissions. If a
    role with the specified name already exists, it is used.

    :param iam_role_name: The name of the role to create.
    :return: The role ARN.
    """

    lambda_assume_role_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }

    # Attach AWS managed policies for Lambda basic execution, S3 full access, and DynamoDB full access
    policies = [
        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
        "arn:aws:iam::aws:policy/AmazonS3FullAccess",
        "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess"
    ]

    try:
        role = iam.create_role(
            RoleName=iam_role_name,
            AssumeRolePolicyDocument=json.dumps(lambda_assume_role_policy),
        )
        logger.info("Created role %s.", iam_role_name)
    except ClientError as error:
        if error.response["Error"]["Code"] == "EntityAlreadyExists":
            role = iam.get_role(RoleName=iam_role_name)
            logger.warning("The role %s already exists. Using it.", iam_role_name)
        else:
            logger.exception(
                "Couldn't create role %s.",
                iam_role_name,
            )
            raise

    for policy_arn in policies:
        try:
            iam.attach_role_policy(
                RoleName=iam_role_name,
                PolicyArn=policy_arn
            )
            logger.info("Attached policy %s to role %s.", policy_arn, iam_role_name)
        except ClientError:
            logger.exception("Couldn't attach policy %s to role %s.", policy_arn, iam_role_name)
            raise

    return role['Role']['Arn']

def create_deployment_package(source_file):
    """
    Creates a Lambda deployment package in .zip format in an in-memory buffer. This
    buffer can be passed directly to Lambda when creating the function.

    :param source_file: The name of the file that contains the Lambda handler
                        function.
    :return: The deployment package.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zipped:
        zipped.write(source_file)
    buffer.seek(0)
    return buffer.read()

def create_function(function_name, handler_name, iam_role_arn, deployment_package):
    """
    Deploys a Lambda function.

    :param function_name: The name of the Lambda function.
    :param handler_name: The fully qualified name of the handler function. This
                         must include the file name and the function name.
    :param iam_role_arn: The IAM role to use for the function.
    :param deployment_package: The deployment package that contains the function
                               code in .zip format.
    :return: The Amazon Resource Name (ARN) of the newly created function.
    """
    try:
        response = lambda_client.create_function(
            FunctionName=function_name,
            Description="Process S3 events and update DynamoDB",
            Runtime="python3.12",
            Role=iam_role_arn,
            Handler=handler_name,
            Code={"ZipFile": deployment_package},
            Environment={
                'Variables': {
                    'BUCKET_NAME': 'lecture2-yaqundeng',
                    'TABLE_NAME': DYNAMODB_TABLE_NAME
                }
            },
            Timeout=30,
            MemorySize=128,
            Publish=True,
        )
        function_arn = response["FunctionArn"]
        waiter = lambda_client.get_waiter("function_active")
        waiter.wait(FunctionName=function_name)
        logger.info(
            "Created function '%s' with ARN: '%s'.",
            function_name,
            response["FunctionArn"],
        )
    except ClientError:
        logger.error("Couldn't create function %s.", function_name)
        raise
    else:
        return function_arn

def configure_s3_trigger(function_arn, bucket_name):
    """
    Configure an S3 bucket to trigger a Lambda function.

    :param function_arn: The ARN of the Lambda function to trigger.
    :param bucket_name: The name of the S3 bucket.
    """
    try:
        lambda_client.add_permission(
            FunctionName=function_arn,
            StatementId='s3-trigger',
            Action='lambda:InvokeFunction',
            Principal='s3.amazonaws.com',
            SourceArn=f'arn:aws:s3:::{bucket_name}',
            SourceAccount='726117619475'
        )
        s3_client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={
                'LambdaFunctionConfigurations': [
                    {
                        'LambdaFunctionArn': function_arn,
                        'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*', 's3:ObjectRestore:*']
                    }
                ]
            }
        )
        logger.info(f"S3 trigger configured for Lambda function '{function_arn}'.")
    except ClientError:
        logger.exception(f"Couldn't configure S3 trigger for function {function_arn}.")
        raise

def main():
    # IAM Role
    role_name = 'lambda-s3-dynamodb-role'
    role_arn = create_iam_role_for_lambda(role_name)

    print("Creating lambda role", end="")
    progress_bar(5)

    # Lambda Function
    function_name = 'S3ObjectSizeTracker'
    source_file = 'lambda_handler_size.py'
    zip_file = create_deployment_package(source_file)

    print("Creating deployment packages", end="")
    progress_bar(5)

    handler_name = 'lambda_handler_size.lambda_handler'
    function_arn = create_function(function_name, handler_name, role_arn, zip_file)

    print("Creating {function_name} Lambda function", end="")
    progress_bar(5)

    # S3 Trigger Configuration
    bucket_name = 'lecture2-yaqundeng'
    configure_s3_trigger(function_arn, bucket_name)

if __name__ == '__main__':
    main()