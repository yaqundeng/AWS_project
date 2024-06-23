import boto3
import logging
import time
from datetime import datetime
from botocore.exceptions import ClientError

# Initialize the logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Define the DynamoDB table name
DYNAMODB_TABLE_NAME = 'S3-object-size-history'

def lambda_handler(event, context):
    try:
        logger.info(f"Event: {event}")
        # Extract the bucket name from the event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        
        # Calculate the total size of all objects in the bucket
        total_size, object_count = calculate_total_size(bucket_name)
        
        logger.info(f"total_size, object_count: {total_size}, {object_count}")
        
        # Get the current timestamp
        timestamp = int(datetime.utcnow().timestamp())
        
        # Write to the DynamoDB table
        write_to_dynamodb(bucket_name, timestamp, object_count, total_size)
        
        logger.info(f"Successfully processed event for bucket {bucket_name}.")
        
        # return {
        #     'statusCode': 200,
        #     'body': json.dumps('Hello from Lambda!')
        # }
    except Exception as e:
        logger.error(f"Error processing event: {str(e)}")
        raise

def calculate_total_size(bucket_name):
    """
    Calculate the total size of all objects in the given S3 bucket.

    :param bucket_name: The name of the S3 bucket.
    :return: A tuple containing the total size in bytes and the number of objects.
    """
    total_size = 0
    object_count = 0

    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name):
            for obj in page.get('Contents', []):
                total_size += obj['Size']
                object_count += 1
        
        logger.info(f"Bucket {bucket_name} contains {object_count} objects with a total size of {total_size} bytes.")
    except ClientError as e:
        logger.error(f"Error calculating total size of bucket {bucket_name}: {str(e)}")
        raise

    return total_size, object_count

def write_to_dynamodb(bucket_name, timestamp, object_count, total_size):
    """
    Write the provided information to the DynamoDB table.

    :param bucket_name: The name of the S3 bucket.
    :param timestamp: The current timestamp.
    :param object_count: The number of objects in the bucket.
    :param total_size: The total size of the objects in the bucket in bytes.
    """
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)

    try:
        table.put_item(
            Item={
                'S3ObjectKey': bucket_name,  # Use bucket name as the partition key
                'Timestamp': timestamp,  # Use timestamp as the sort key
                'object_count': object_count,
                'total_size': total_size
            }
        )
        logger.info(f"Successfully wrote to DynamoDB: {bucket_name}, {timestamp}, {object_count}, {total_size}.")
    except ClientError as e:
        logger.error(f"Error writing to DynamoDB: {str(e)}")
        raise