import boto3
import json
import os
import write_to_db
localstack_host = os.getenv('LOCALSTACK_HOST')
queue_url = os.getenv('QUEUE_URL')
aws_key = os.getenv('AWS_KEY')
aws_secret_key = os.getenv('AWS_SECRET_KEY')


def read_message():
    region_name = 'ap-south-1'
    aws_access_key_id = aws_key
    aws_secret_access_key = aws_secret_key

    sqs = boto3.client(
        'sqs',
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=f'http://{localstack_host}:4566'
    )

    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20
        )

        if 'Messages' in response:
            msg_batch = []
            receipt_handles = []

            for message in response['Messages']:
                handle = message['ReceiptHandle']
                if (message is not None and handle is not None):
                    msg = json.loads(message['Body'])
                    msg_batch.append(msg)
                    receipt_handles.append(handle)

            if msg_batch:  # if the list is not empty
                write_to_db.main_entrypoint(msg_batch, receipt_handles, sqs)
        else:
            print('No messages in the queue')


read_message()

