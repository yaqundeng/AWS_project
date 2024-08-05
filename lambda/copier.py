import boto3
import os
import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
sns = boto3.client('sns')

def handler(event, context):
    source_bucket = os.environ['SOURCE_BUCKET']
    destination_bucket = os.environ['DESTINATION_BUCKET']
    logger.info(f"Copier triggered for source bucket: {source_bucket} and destination bucket: {destination_bucket}")
    

    try:
        # Log the event details
        logger.info(f"Event received: {event}")
        
        # Process S3 event records
        for record in event['Records']:
            message = json.loads(record['body'])
            source_bucket = message['Records'][0]['s3']['bucket']['name']
            key = message['Records'][0]['s3']['object']['key']

            # Log the source and destination keys
            logger.info(f"Copying from source key: {key} to destination key: {key}")

            # Copy the object
            copy_source = {'Bucket': source_bucket, 'Key': key}
            s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=key)
            logger.info(f"Successfully copied {key} to {key}")

    except Exception as e:
        logger.error(f"Error processing bucket {source_bucket}: {str(e)}")
