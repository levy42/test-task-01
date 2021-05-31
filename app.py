from datetime import datetime
import json

import boto3
from chalice import Chalice, Rate

# this could be moved to conf file in production
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-2:469214050417:email-test-topic'
SQS_QUEUE = 'test-queue-01'
BUCKET_NAME = 'test-task-bucket-01'

app = Chalice(app_name='test-task')

sqs = boto3.client('sqs')
sns = boto3.client('sns')
s3 = boto3.resource('s3')
queue_url = sqs.get_queue_url(QueueName=SQS_QUEUE).get('QueueUrl')


@app.schedule(Rate(3, unit=Rate.MINUTES), description="Function that run's every 3 mins")
def periodic_function(event, context=None):
    timestamp = str(datetime.now())
    message = json.dumps({'timestamp': timestamp})
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=message
    )


@app.on_sqs_message(queue=SQS_QUEUE)
def triggered_function(event, context=None):
    for record in event:
        # there should be actually one record in our case
        input_message = json.loads(record.body)
        timestamp_str = input_message['timestamp']

        s3.Object(BUCKET_NAME, f"{timestamp_str.replace(' ', '_')}.txt").put(Body=timestamp_str)

        timestamp = datetime.fromisoformat(timestamp_str)
        if timestamp.minute == 0:
            sns.publish(
                TargetArn=SNS_TOPIC_ARN,
                Message=f'Received a timestamp with exact hour. The hour is {timestamp.hour}'
            )
        if 30 > timestamp.minute > 40:
            sns.publish(
                TargetArn=SNS_TOPIC_ARN,
                Message=f'Received a timestamp with 15 > minutes > 20. The minute is {timestamp.minute}'
            )
