import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';
import { RemovalPolicy } from 'aws-cdk-lib';

export class BucketStack extends cdk.Stack {
  public readonly sourceBucketName: string;
  public readonly destinationBucketName: string;
  public readonly copierLambda: lambda.Function;
  public readonly copierFunctionArn: string;
  public readonly copierLogGroupArn: string;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create the source bucket
    const sourceBucket = new s3.Bucket(this, 'source', { 
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });
    this.sourceBucketName = sourceBucket.bucketName;

    // Create the backup bucket
    const destinationBucket = new s3.Bucket(this, 'destination', { 
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });
    this.destinationBucketName = destinationBucket.bucketName;

    // Create a log group for the copier Lambda function
    const copierLogGroup = new logs.LogGroup(this, 'CopierLogs', {
      logGroupName: `/aws/lambda/copier`,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    this.copierLogGroupArn = copierLogGroup.logGroupArn

    // Create the copier Lambda function
    const bucket = s3.Bucket.fromBucketName(this, "s3bucket", "testbucket-yaqun")
    const copierFunction = new lambda.Function(this, 'CopierFunction', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'copier.handler',
      code: lambda.Code.fromBucket(bucket, "copier.zip"),
      environment: {
        SOURCE_BUCKET: sourceBucket.bucketName,
        DESTINATION_BUCKET: destinationBucket.bucketName,
        LOG_GROUP_NAME: copierLogGroup.logGroupName
      },
      logGroup: copierLogGroup,
    });

    this.copierFunctionArn = copierFunction.functionArn;

    // Add permissions to the Lambda execution role using addToRolePolicy
    copierFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: ['s3:*'],
      effect: iam.Effect.ALLOW,
      resources: [
        sourceBucket.bucketArn,
        `${sourceBucket.bucketArn}/*`,
        destinationBucket.bucketArn,
        `${destinationBucket.bucketArn}/*`
      ]
    }));

    // Grant necessary permissions to the copier function
    sourceBucket.grantRead(copierFunction);
    destinationBucket.grantWrite(copierFunction);

    // Add event notification to the source bucket to trigger the copier Lambda on object creation
    sourceBucket.addEventNotification(s3.EventType.OBJECT_CREATED, new cdk.aws_s3_notifications.LambdaDestination(copierFunction));
  }

}
