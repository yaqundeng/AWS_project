import boto3
import time
import sys
import logging
from botocore.exceptions import ClientError
from part2 import create_function, create_deployment_package

# Set the AWS regioncd
region = 'us-west-2'

# Initialize AWS clients
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda', region_name=region)
apigateway_client = boto3.client('apigateway', region_name=region)

# Initialize the logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def progress_bar(seconds):
    """Shows a simple progress bar in the command window."""
    for _ in range(seconds):
        time.sleep(1)
        print(".", end="")
        sys.stdout.flush()
    print()

def create_api_gateway(lambda_arn, LAMBDA_FUNCTION_NAME, API_NAME, STAGE_NAME):
    try:
        # Get Lambda function ARN
        logger.info(f"Lambda ARN: {lambda_arn}")

        # Create API
        api_response = apigateway_client.create_rest_api(
            name=API_NAME,
            description='API to trigger Lambda function that plots S3 object size history',
            endpointConfiguration={'types': ['REGIONAL']}
        )
        api_id = api_response['id']
        logger.info(f"Created API Gateway with ID: {api_id}")

        # Get Root Resource ID
        resources = apigateway_client.get_resources(restApiId=api_id)
        root_id = [resource['id'] for resource in resources['items'] if resource['path'] == '/'][0]

        # Create Resource
        resource_response = apigateway_client.create_resource(
            restApiId=api_id,
            parentId=root_id,
            pathPart='plot'
        )
        resource_id = resource_response['id']
        logger.info(f"Created resource 'plot' with ID: {resource_id}")

        # Create Method
        apigateway_client.put_method(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod='GET',
            authorizationType='NONE'
        )
        logger.info(f"Created GET method for resource 'plot'")

        # Link Lambda Function
        apigateway_client.put_integration(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod='GET',
            type='AWS_PROXY',
            integrationHttpMethod='POST',
            uri=f'arn:aws:apigateway:{region}:lambda:path/2015-03-31/functions/{lambda_arn}/invocations'
        )
        logger.info(f"Integrated Lambda function '{LAMBDA_FUNCTION_NAME}' with GET method on resource 'plot'")

        # Grant Permission to API Gateway
        lambda_client.add_permission(
            FunctionName=lambda_arn,
            StatementId='apigateway-invoke',
            Action='lambda:InvokeFunction',
            Principal='apigateway.amazonaws.com',
            SourceArn=f'arn:aws:execute-api:{region}:{boto3.client("sts").get_caller_identity()["Account"]}:{api_id}/*/GET/plot'
        )
        logger.info(f"Granted API Gateway permission to invoke Lambda function '{LAMBDA_FUNCTION_NAME}'")

        # Deploy API
        apigateway_client.create_deployment(
            restApiId=api_id,
            stageName=STAGE_NAME,
            description='Deployment for S3ObjectSizeHistoryAPI'
        )
        logger.info(f"Deployed API Gateway to stage '{STAGE_NAME}'")

        return f"https://{api_id}.execute-api.{region}.amazonaws.com/{STAGE_NAME}/plot"
    except Exception as e:
        logger.error(f"Error creating API Gateway: {e}")
        raise

def main():
    # Constants
    LAMBDA_FUNCTION_NAME = 'S3ObjectSizePlotter'
    API_NAME = 'S3ObjectSizePlotAPI'
    ROLE_ARN = 'arn:aws:iam::726117619475:role/lambda-s3-dynamodb-role'
    STAGE_NAME = 'prod'

    # Lambda Function
    source_file = 'lambda_handler_plot.py'

    # Create deployment package
    zip_file = create_deployment_package(source_file)

    print("Creating deployment packages", end="")
    progress_bar(5)

    # Create Lambda function
    function_arn = create_function(LAMBDA_FUNCTION_NAME, 'lambda_handler_plot.lambda_handler', ROLE_ARN, zip_file)

    print(f"Creating {LAMBDA_FUNCTION_NAME} Lambda function {function_arn}", end="")
    progress_bar(10)

    # Create API Gateway
    api_url = create_api_gateway(function_arn, LAMBDA_FUNCTION_NAME, API_NAME, STAGE_NAME)
    print(f"API Gateway URL: {api_url}")

if __name__ == "__main__":
    main()
