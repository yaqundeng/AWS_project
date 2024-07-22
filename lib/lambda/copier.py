import boto3
import os
import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

def handler(event, context):
    source_bucket = os.environ['SOURCE_BUCKET']
    destination_bucket = os.environ['DESTINATION_BUCKET']
    logger.info(f"Copier triggered for source bucket: {source_bucket} and destination bucket: {destination_bucket}")

    try:
        # Log the event details
        logger.info(f"Event received: {event}")

        # Process S3 event records
        for record in event['Records']:
            source_key = record['s3']['object']['key']
            destination_key = source_key

            # Log the source and destination keys
            logger.info(f"Copying from source key: {source_key} to destination key: {destination_key}")

            # Copy the object
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)
            logger.info(f"Successfully copied {source_key} to {destination_key}")
            size = s3.head_object(Bucket=source_bucket, Key=source_key)['ContentLength']

            is_temporary = 'temp' in source_key

            print(json.dumps({
                'action': 'copy',
                'object_key': source_key,
                'size': size,
                'is_temporary': is_temporary,
                'status': 'success'
            }))

    except Exception as e:
        logger.error(f"Error processing bucket {source_bucket}: {str(e)}")
