import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as sns_subscriptions from 'aws-cdk-lib/aws-sns-subscriptions';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as sqs_event_sources from 'aws-cdk-lib/aws-lambda-event-sources';
import * as actions from 'aws-cdk-lib/aws-cloudwatch-actions';
import { Construct } from 'constructs';

export interface CleanerStackProps extends cdk.StackProps {
  destinationBucketName: string;
  copierLogGroupArn: string;
}

export class CleanerStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: CleanerStackProps) {
    super(scope, id, props);

    const destinationBucket = s3.Bucket.fromBucketName(this, 'DestinationBucket', props.destinationBucketName);

    // Create a metric filter for the copier log group
    // const logGroup = logs.LogGroup.fromLogGroupArn(this, 'CopierLogs', props.copierLogGroupArn);
    const logGroup = logs.LogGroup.fromLogGroupName(this, 'LogGroup', '/aws/lambda/copier');

    // Create a metric filter for total_temp_size
    const metricFilter = new logs.MetricFilter(this, 'MetricFilter', {
      logGroup,
      metricNamespace: 'Copier',
      metricName: 'TotalTempSize',
      filterPattern: logs.FilterPattern.exists('$.total_temp_size'),
      metricValue: '$.total_temp_size',
    });

    // Create an SNS topic
    const snsTopic = new sns.Topic(this, 'AlarmSNSTopic');

    // Create an SQS queue
    const sqsQueue = new sqs.Queue(this, 'AlarmSQSQueue');

    // Subscribe the SQS queue to the SNS topic
    snsTopic.addSubscription(new sns_subscriptions.SqsSubscription(sqsQueue));

    // Create an alarm based on the metric
    const alarm = new cloudwatch.Alarm(this, 'Alarm', {
      metric: metricFilter.metric({
        statistic: 'sum',
        period: cdk.Duration.seconds(10),
      }),
      threshold: 3000,
      evaluationPeriods: 1,
      alarmName: 'TemporaryObjectsSizeAlarm',
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });

    // Create the cleaner Lambda function
    const bucket = s3.Bucket.fromBucketName(this, "s3bucket", "testbucket-yaqun")
    const cleanerFunction = new lambda.Function(this, 'CleanerFunction', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'cleaner.handler',
      code: lambda.Code.fromBucket(bucket, "cleaner.zip"),
      environment: {
        DESTINATION_BUCKET: destinationBucket.bucketName
      }
    });

    // Add permissions to the Lambda execution role using addToRolePolicy
    cleanerFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: ['s3:*', 'cloudwatch:SetAlarmState',
        'cloudwatch:DescribeAlarms',],
      effect: iam.Effect.ALLOW,
      resources: [
        `arn:aws:s3:::${props.destinationBucketName}`,
        `arn:aws:s3:::${props.destinationBucketName}/*`,
        alarm.alarmArn
      ]
    }));

    // Grant necessary permissions to the cleaner function
    destinationBucket.grantReadWrite(cleanerFunction);

    // Grant CloudWatch permission to invoke the cleaner function
    cleanerFunction.addPermission('CloudWatchInvoke', {
      principal: new cdk.aws_iam.ServicePrincipal('cloudwatch.amazonaws.com'),
      action: 'lambda:InvokeFunction',
      sourceArn: alarm.alarmArn,
    });

    // // Add permissions to the Lambda execution role
    // cleanerFunction.addToRolePolicy(new iam.PolicyStatement({
    //   actions: ['cloudwatch:SetAlarmState'],
    //   effect: iam.Effect.ALLOW,
    //   resources: [alarm.alarmArn],
    // }));

    // Set up the alarm action to trigger the cleaner function
    alarm.addAlarmAction(new actions.SnsAction(snsTopic));

    // Configure the cleaner Lambda function to be triggered by the SQS queue
    cleanerFunction.addEventSource(new sqs_event_sources.SqsEventSource(sqsQueue));
  }

}