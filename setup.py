import boto3
import json
import logging
import sys
import time
from botocore.exceptions import ClientError

# Initialize the logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Initialize a session using Amazon IAM
iam = boto3.client('iam')

def progress_bar(seconds):
    """Shows a simple progress bar in the command window."""
    for _ in range(seconds):
        time.sleep(1)
        print(".", end="")
        sys.stdout.flush()
    print()

def create_user(user_name):
    try:
        user = iam.create_user(UserName=user_name)
        logger.info("Created user %s.", user_name)
    except ClientError:
        logger.exception("Couldn't create user %s.", user_name)
        raise
    else:
        return user

def create_access_key(user_name):
    try:
        access_key = iam.create_access_key(UserName=user_name)
        logger.info("Created access key for user %s.", user_name)
        return access_key['AccessKey']['AccessKeyId'], access_key['AccessKey']['SecretAccessKey']
    except ClientError:
        logger.exception("Couldn't create access key for user %s.", user_name)
        raise

def create_role(role_name, user_arn):
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": user_arn},
                "Action": "sts:AssumeRole",
            }
        ],
    }

    try:
        role = iam.create_role(
            RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust_policy)
        )
        logger.info("Created role %s.", role['Role']['RoleName'])
    except ClientError:
        logger.exception("Couldn't create role %s.", role_name)
        raise
    else:
        return role

def attach_inline_policy(role_name, policy_name, policy_document):
    try:
        iam.put_role_policy(
            RoleName=role_name,
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document)
        )
        logger.info("Attached inline policy %s to role %s.", policy_name, role_name)
    except ClientError:
        logger.exception("Couldn't attach inline policy to role %s.", role_name)
        raise

def create_inline_policy_for_user(user_name, role_arn):
    try:
        policy_name = "allow_assume_role"
        policy_document = json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "sts:AssumeRole",
                    "Resource": role_arn,
                }
            ],
        })
        
        iam.put_user_policy(
            UserName=user_name,
            PolicyName=policy_name,
            PolicyDocument=policy_document
        )
        logger.info(f"Created an inline policy for {user_name} that lets the user assume the role.")
    except ClientError as error:
        logger.exception(
            f"Couldn't create an inline policy for user {user_name}. Here's why: "
            f"{error.response['Error']['Message']}"
        )
        raise

def assume_role(role_arn, session_name, access_key_id, secret_access_key):
    sts_client = boto3.client(
        "sts", aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key
    )
    try:
        response = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=session_name
        )
        logger.info("Assumed role %s.", role_arn)
        return response['Credentials']
    except ClientError:
        logger.exception("Couldn't assume role %s.", role_arn)
        raise

def create_bucket(bucket_name, region, credentials):
    s3_client = boto3.client(
        's3',
        region_name=region,
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
    try:
        if region == 'us-east-1':
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        logger.info("Created bucket %s in region %s.", bucket_name, region)
    except ClientError:
        logger.exception("Couldn't create bucket %s in region %s.", bucket_name, region)
        raise

def create_dynamodb_table(table_name, region, credentials):
    dynamodb_client = boto3.client(
        'dynamodb',
        region_name=region,
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
    try:
        table = dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'S3ObjectKey',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'Timestamp',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'S3ObjectKey',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'Timestamp',
                    'AttributeType': 'N'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        logger.info("Created DynamoDB table %s.", table_name)
    except ClientError:
        logger.exception("Couldn't create DynamoDB table %s.", table_name)
        raise

def main():
    user_name = 'ExampleUser'
    dev_role_name = 'Dev'
    region = 'us-west-2'

    user = create_user(user_name)
    user_arn = user['User']['Arn']

    if user:
        access_key_id, secret_access_key = create_access_key(user_name)
        print(f"Access Key ID: {access_key_id}")
        print(f"Secret Access Key: {secret_access_key}")

    print(f"Wait for user to be ready.", end="")
    progress_bar(10)

    # Define the policy for full access to S3
    dev_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "s3:*",
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": "dynamodb:*",
                "Resource": "*"
            }
        ]
    }

    # Create the Dev role
    dev_role = create_role(dev_role_name, user_arn)
    if dev_role:
        attach_inline_policy(dev_role_name, 'DevS3FullAccess', dev_policy)
        create_inline_policy_for_user(user_name, dev_role["Role"]["Arn"])

    print("Creating dev role", end="")
    progress_bar(10)

    print("Give AWS time to propagate these new resources and connections.", end="")
    progress_bar(5)

    # Assume the dev role
    dev_credentials = assume_role(dev_role["Role"]["Arn"], 'ExampleDevSession', access_key_id, secret_access_key)

    # Create the S3 bucket
    bucket_name = 'lecture2-yaqundeng'
    create_bucket(bucket_name, region, dev_credentials)

    print("Creating bucket", end="")
    progress_bar(5)

    # Create the DynamoDB table
    table_name = 'S3-object-size-history'
    create_dynamodb_table(table_name, region, dev_credentials)

    print("Creating DynamoDB table S3-object-size-history", end="")
    progress_bar(5)

if __name__ == '__main__':
    main()