import boto3
import logging
import time

# Set the AWS region
region = 'us-west-2'

# Initialize AWS clients
s3_client = boto3.client('s3', region_name=region)

# Initialize the logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constants
S3_BUCKET_NAME = 'lecture2-yaqundeng'
OBJECT_KEY_1 = 'assignment1.txt'
OBJECT_KEY_2 = 'assignment2.txt'

def create_object(key, content):
    """
    Create an object in S3 with the given key and content.

    :param key: The key of the object.
    :param content: The content to be written to the object.
    """
    try:
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=content)
        logger.info("Created object %s with content: %s", key, content)
    except Exception as e:
        logger.error("Error creating object %s: %s", key, e)
        raise

def update_object(key, content):
    """
    Update an object in S3 with the given key and new content.

    :param key: The key of the object.
    :param content: The new content to be written to the object.
    """
    try:
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=content)
        logger.info("Updated object %s with new content: %s", key, content)
    except Exception as e:
        logger.error("Error updating object %s: %s", key, e)
        raise

def delete_object(key):
    """
    Delete an object in S3 with the given key.

    :param key: The key of the object to be deleted.
    """
    try:
        s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=key)
        logger.info("Deleted object %s", key)
    except Exception as e:
        logger.error("Error deleting object %s: %s", key, e)
        raise

def main():
    try:
        # Step 1: Create object assignment1.txt
        create_object(OBJECT_KEY_1, "Empty Assignment 1")
        time.sleep(10)  # Sleep for 10 second

        # Step 2: Update object assignment1.txt
        update_object(OBJECT_KEY_1, "Empty Assignment 1222222222")
        time.sleep(10)  # Sleep for 10 second

        # Step 3: Delete object assignment1.txt
        delete_object(OBJECT_KEY_1)
        time.sleep(10)  # Sleep for 10 second

        # Step 4: Create object assignment2.txt
        create_object(OBJECT_KEY_2, "Empty Assignment 2")
        time.sleep(10)  # Sleep for 10 second

    except Exception as e:
        logger.error("Error in main execution: %s", e)

if __name__ == "__main__":
    main()
