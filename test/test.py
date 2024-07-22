import boto3
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Initialize the S3 client
s3_client = boto3.client('s3')

# Define the bucket name and objects to upload
source_bucket_name = 'bucketstack-sourceadfc1803-7dmt8khaylbt'

objects_to_upload = [
    ('project.txt', 1024),  # 1KB
    ('temp.txt', 1024),  # 1KB
    ('project_new.txt', 1024),  # 1KB
    ('temporary_data.txt', 2560),  # 2.5KB
    ('project_new_new.txt', 1024),  # 1KB
    ('real_temporary_data.txt', 2048)  # 2KB
]

def create_dummy_file(file_name, size_in_bytes):
    """
    Create a dummy file with the specified name and size.
    
    :param file_name: The name of the file to create.
    :param size_in_bytes: The size of the file in bytes.
    """
    with open(file_name, 'wb') as f:
        f.write(b'0' * size_in_bytes)
    logger.info(f'Created dummy file: {file_name} of size {size_in_bytes} bytes')

def upload_object(file_name):
    """
    Upload the specified file to the source bucket.
    
    :param file_name: The name of the file to upload.
    """
    try:
        s3_client.upload_file(file_name, source_bucket_name, file_name)
        logger.info(f'Uploaded {file_name} to bucket {source_bucket_name}')
    except Exception as e:
        logger.error(f'Failed to upload {file_name} to bucket {source_bucket_name}: {e}')

def main():
    """
    Main function to create and upload objects to the source bucket.
    """
    for file_name, size in objects_to_upload:
        create_dummy_file(file_name, size)
        upload_object(file_name)
        time.sleep(10)  
    
    # upload_object('project.txt')
    # time.sleep(5)
    # upload_object('temp.txt')
    # time.sleep(60)
    # upload_object('project_new.txt')
    # time.sleep(5)
    # upload_object('temporary_data.txt')
    # time.sleep(80)
    # upload_object('project_new_new.txt')
    # time.sleep(5)
    # upload_object('real_temporary_data.txt')
    # time.sleep(60)

if __name__ == '__main__':
    main()