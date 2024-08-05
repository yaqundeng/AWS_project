import logging
import json
import boto3
import os
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

log_client = boto3.client('logs')
s3 = boto3.client('s3')
cloudwatch_client = boto3.client('cloudwatch')

def get_total_temp_size(bucket_name):
    total_temp_size = 0
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            for obj in response['Contents']:
                if 'temp' in obj['Key'].lower():
                    total_temp_size += obj['Size']
    except Exception as e:
        logger.error(f"Error listing objects in bucket {bucket_name}: {str(e)}")
    return total_temp_size

def handler(event, context):
    destination_bucket = os.environ['DESTINATION_BUCKET']
    logger.info(f"Logger triggered for destination bucket: {destination_bucket}")

    try:
        for record in event['Records']:
            time.sleep(5)
            total_temp_size = get_total_temp_size(destination_bucket)
            
            cloudwatch_client.put_metric_data(
                Namespace='CustomNamespace',
                MetricData=[
                    {
                        'MetricName': 'TempObjectSize',
                        'Dimensions': [
                            {
                                'Name': 'BucketName',
                                'Value': destination_bucket
                            },
                        ],
                        'Value': total_temp_size,
                        'Unit': 'Bytes'
                    },
                ]
            )
            
            logger.info(json.dumps({
                'action': 'log',
                'total_temp_size': total_temp_size,
                'status': 'success'
            }))

    except Exception as e:
        logger.error(f"Error processing log for bucket {destination_bucket}: {str(e)}")
