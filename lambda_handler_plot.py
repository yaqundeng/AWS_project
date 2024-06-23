import boto3
import json
import logging
import matplotlib.pyplot as plt
from io import BytesIO
from botocore.exceptions import ClientError

# Set the AWS region
region = 'us-west-2'

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb', region_name=region)

# Initialize the logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constants
DYNAMODB_TABLE_NAME = 'S3-object-size-history'
S3_BUCKET_NAME = 'lecture2-yaqundeng'
PLOT_KEY = 'plot.png'

def lambda_handler(event, context):
    try:
        logger.info("Received event: %s", json.dumps(event))
        # Fetch items from DynamoDB
        items = fetch_dynamodb_items()

        if not items:
            logger.warning("No data found in DynamoDB table.")
            return {
                'statusCode': 404,
                'body': 'No data found in DynamoDB table'
            }

        # Generate plot
        plot_url = generate_plot(items)
        logger.info("Plot generated and saved to S3: %s", plot_url)

        return {
            'statusCode': 200,
            'body': json.dumps({'plot_url': plot_url})
        }

    except ClientError as e:
        logger.error("ClientError: %s", e)
        return {
            'statusCode': 500,
            'body': json.dumps('Internal Server Error')
        }
    except Exception as e:
        logger.error("Exception: %s", e)
        return {
            'statusCode': 500,
            'body': json.dumps('Internal Server Error')
        }

def fetch_dynamodb_items():
    """
    Retrieve all items from DynamoDB table and sort them by timestamp.

    :return: List of items from DynamoDB.
    """
    try:
        table = dynamodb.Table(DYNAMODB_TABLE_NAME)
        response = table.scan()
        items = response['Items']

        logger.info("Fetched %d items from DynamoDB.", len(items))

        # Paginate through results if necessary
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
            logger.info("Fetched additional items from DynamoDB, total count: %d.", len(items))

        # Sort items by timestamp
        items.sort(key=lambda x: x['Timestamp'])
        logger.info("Sorted items by timestamp.")

        return items
    except ClientError as e:
        logger.error("Error fetching items from DynamoDB: %s", e)
        raise e

def generate_plot(items):
    """
    Generate a line chart plot based on DynamoDB items and save it to S3.

    :param items: List of items fetched from DynamoDB.
    :return: URL of the generated plot in S3.
    """
    try:
        timestamps = [item['Timestamp'] for item in items]
        total_sizes = [item['total_size'] for item in items]

        # Create line chart plot
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps, total_sizes, marker='o', linestyle='-', color='b')
        plt.xlabel('Timestamp')
        plt.ylabel('Total Object Size')
        plt.title('Total Object Size Over Time')
        plt.grid(True)

        # Save plot to a buffer
        buffer = BytesIO()
        plt.savefig(buffer, format='png')
        buffer.seek(0)

        logger.info("Generated plot and saved to buffer.")

        # Upload plot to S3 bucket
        upload_plot_to_s3(buffer)

        # Return URL to the generated plot
        plot_url = f"s3://{S3_BUCKET_NAME}/{PLOT_KEY}"
        logger.info("Plot uploaded to S3: %s", plot_url)

        return plot_url
    except Exception as e:
        logger.error("Error generating plot: %s", e)
        raise e

def upload_plot_to_s3(buffer):
    """
    Upload the plot image to S3 bucket.

    :param buffer: BytesIO object containing the plot image.
    """
    try:
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=PLOT_KEY, Body=buffer, ContentType='image/png')
        logger.info("Plot image saved to S3 bucket: s3://%s/%s", S3_BUCKET_NAME, PLOT_KEY)
    except ClientError as e:
        logger.error("Error uploading plot to S3: %s", e)
        raise e