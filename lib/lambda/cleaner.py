import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
cloudwatch_client = boto3.client('cloudwatch')

def handler(event, context):
    bucket_name = os.environ['DESTINATION_BUCKET']
    alarm_name = 'TemporaryObjectsSizeAlarm'
    logger.info(f"Cleaner triggered for bucket: {bucket_name}")
    logger.info(f"Event received: {event}")

    try:
        # List objects in the bucket
        response = s3.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' not in response:
            logger.info("No objects found in the bucket.")
            return
        
        # Filter objects containing the substring "temp" and sort by LastModified
        temp_objects = [obj for obj in response['Contents'] if 'temp' in obj['Key']]
        if not temp_objects:
            logger.info("No temporary objects found in the bucket.")
            return

        oldest_object = min(temp_objects, key=lambda x: x['LastModified'])
        oldest_key = oldest_object['Key']

        # Delete the oldest temporary object
        s3.delete_object(Bucket=bucket_name, Key=oldest_key)
        logger.info(f"Deleted oldest temporary object: {oldest_key}")

        # # Reset the alarm state to OK
        # cloudwatch_client.set_alarm_state(
        #     AlarmName=alarm_name,
        #     StateValue='OK',
        #     StateReason='Cleaner lambda executed and object deleted'
        # )
        # logger.info(f"Alarm {alarm_name} state set to OK after cleaning.")

    except Exception as e:
        logger.error(f"Error processing bucket {bucket_name}: {str(e)}")
