import boto3
import os
import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')

def get_total_temp_size(bucket_name):
    total_temp_size = 0
    try:
        # List objects in the destination bucket
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            for obj in response['Contents']:
                if 'temp' in obj['Key'].lower():
                    # Accumulate size of temporary objects
                    total_temp_size += obj['Size']
    except Exception as e:
        logger.error(f"Error listing objects in bucket {bucket_name}: {str(e)}")
    return total_temp_size

def handler(event, context):
    source_bucket = os.environ['SOURCE_BUCKET']
    destination_bucket = os.environ['DESTINATION_BUCKET']
    logger.info(f"Copier triggered for source bucket: {source_bucket} and destination bucket: {destination_bucket}")
    

    try:
        # Log the event details
        logger.info(f"Event received: {event}")

        # Calculate total size of temporary objects in the destination bucket
        total_temp_size = get_total_temp_size(destination_bucket)
        
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

            is_temporary = 'temp' in source_key.lower()

            if is_temporary:
                total_temp_size += size
            
            print(json.dumps({
                'action': 'copy',
                'object_key': source_key,
                'size': size,
                'is_temporary': is_temporary,
                'status': 'success',
                'total_temp_size': total_temp_size
            }))

    except Exception as e:
        logger.error(f"Error processing bucket {source_bucket}: {str(e)}")
