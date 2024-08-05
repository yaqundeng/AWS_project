import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as sns_subscriptions from 'aws-cdk-lib/aws-sns-subscriptions';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda_event_sources from 'aws-cdk-lib/aws-lambda-event-sources';
import { Construct } from 'constructs';
import { RemovalPolicy } from 'aws-cdk-lib';

export class BucketStack extends cdk.Stack {
  public readonly sourceBucketName: string;
  public readonly destinationBucketName: string;
  public readonly copierLambda: lambda.Function;
  public readonly copierFunctionArn: string;
  public readonly loggerFunctionArn: string;
  public readonly copierLogGroupArn: string;
  public readonly snsTopicArn: string;

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

    // Create the SNS topic
    const snsTopic = new sns.Topic(this, 'SNSTopic');
    this.snsTopicArn = snsTopic.topicArn;

    // Create an SQS queue for the copier Lambda function
    const copierQueue = new sqs.Queue(this, 'CopierQueue', {
      deadLetterQueue: {
        maxReceiveCount: 1,
        queue: new sqs.Queue(this, 'CopierDLQ', {
          retentionPeriod: cdk.Duration.days(7)
        })
      },
      visibilityTimeout: cdk.Duration.seconds(180)
    });

    // Create an SQS queue for the logger Lambda function
    const loggerQueue = new sqs.Queue(this, 'LoggerQueue', {
      deadLetterQueue: {
        maxReceiveCount: 1,
        queue: new sqs.Queue(this, 'LoggerDLQ', {
          retentionPeriod: cdk.Duration.days(7)
        })
      },
      visibilityTimeout: cdk.Duration.seconds(180)
    });

    // Subscribe the SQS queues to the SNS topic
    snsTopic.addSubscription(new sns_subscriptions.SqsSubscription(copierQueue, { rawMessageDelivery: true }));
    snsTopic.addSubscription(new sns_subscriptions.SqsSubscription(loggerQueue, { rawMessageDelivery: true }));

    // Grant SNS permission to send messages to SQS queues
    copierQueue.grantConsumeMessages(new iam.ServicePrincipal('sns.amazonaws.com'));
    loggerQueue.grantConsumeMessages(new iam.ServicePrincipal('sns.amazonaws.com'));
    
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
        SNS_TOPIC_ARN: snsTopic.topicArn,
      },
      timeout: cdk.Duration.seconds(10),
      events: [new lambda_event_sources.SqsEventSource(copierQueue)],
    });

    this.copierFunctionArn = copierFunction.functionArn;

    // Create the logger Lambda function
    const loggerFunction = new lambda.Function(this, 'LoggerFunction', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'logger.handler',
      code: lambda.Code.fromBucket(bucket, "logger.zip"),
      environment: {
        DESTINATION_BUCKET: destinationBucket.bucketName,
        LOG_GROUP_NAME: copierLogGroup.logGroupName,
      },
      logGroup: copierLogGroup,
      timeout: cdk.Duration.seconds(10),
      events: [new lambda_event_sources.SqsEventSource(loggerQueue)],
    });
    this.loggerFunctionArn = loggerFunction.functionArn;

// Add permissions for the copier function
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

    // Add permissions for the logger function
    loggerFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: ['logs:CreateLogStream', 'logs:PutLogEvents', 'logs:CreateLogGroup'],
      effect: iam.Effect.ALLOW,
      resources: [`${copierLogGroup.logGroupArn}:*`],
    }));

    // Add permission to put metrics data
    loggerFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: ['cloudwatch:PutMetricData'],
      effect: iam.Effect.ALLOW,
      resources: ['*']
    }));

    // Grant necessary permissions to the Lambda functions
    sourceBucket.grantRead(copierFunction);
    destinationBucket.grantReadWrite(copierFunction);
    destinationBucket.grantRead(loggerFunction);

    // Allow SNS topic to send messages to the SQS queues
    copierQueue.addToResourcePolicy(new iam.PolicyStatement({
      actions: ['sqs:SendMessage'],
      effect: iam.Effect.ALLOW,
      principals: [new iam.ServicePrincipal('sns.amazonaws.com')],
      resources: [copierQueue.queueArn],
      conditions: {
        ArnEquals: {
          'aws:SourceArn': snsTopic.topicArn,
        }
      }
    }));

    loggerQueue.addToResourcePolicy(new iam.PolicyStatement({
      actions: ['sqs:SendMessage'],
      effect: iam.Effect.ALLOW,
      principals: [new iam.ServicePrincipal('sns.amazonaws.com')],
      resources: [loggerQueue.queueArn],
      conditions: {
        ArnEquals: {
          'aws:SourceArn': snsTopic.topicArn,
        }
      }
    }));

    // Add event notification to the source bucket to trigger the copier Lambda on object creation
    sourceBucket.addEventNotification(s3.EventType.OBJECT_CREATED, new cdk.aws_s3_notifications.SnsDestination(snsTopic));
  }
}
