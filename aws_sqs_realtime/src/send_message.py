import boto3
import json
import time
import requests
import threading
import uuid
from botocore.exceptions import ClientError
import os
localstack_host = os.getenv('LOCALSTACK_HOST')

def fail(err):
    if err:
        print(f"Error: {err}")
        exit(-1)

class SQSSender:
    def __init__(self, queue_url):
        self.queue_url = queue_url
        self.client = boto3.client('sqs', region_name="ap-south-1",
                                   aws_access_key_id="AKID",
                                   aws_secret_access_key="SECRET_KEY",
                                   aws_session_token="TOKEN",
                                   endpoint_url=f"http://{localstack_host}:4566")
        self.lock = threading.Lock()

    def send_messages(self, messages, id):
        chunks = [messages[i:i + 10] for i in range(0, len(messages), 10)]
        for chunk in chunks:
            entries = []
            for message in chunk:
                time.sleep(5)
                msg = json.dumps(message)
                entries.append({
                    'Id': str(uuid.uuid4()),
                    'MessageBody': msg,
                    'DelaySeconds': 1
                })

            try:
                response = self.client.send_message_batch(QueueUrl=self.queue_url, Entries=entries)
            except ClientError as e:
                fail(e)

            if 'Failed' in response and response['Failed']:
                fail(f"{len(response['Failed'])} messages failed to be sent")
            print(".", end="", flush=True)


    def start_fetching(self, u):
        try:
            resp = requests.get(u)
            resp.raise_for_status()
            message = resp.json()
            # print(message)
        except requests.RequestException as e:
            fail(f"Error while getting data {e}")
        t = threading.Thread(target=self.send_messages, args=(message, u))
        t.start()


def main():

    client = boto3.client('sqs', region_name="ap-south-1",
                          aws_access_key_id="AKID",
                          aws_secret_access_key="SECRET_KEY",
                          aws_session_token="TOKEN",
                          endpoint_url=f"http://{localstack_host}:4566")

    queue_name = "test-queue"

    print(f"Creating SQS queue [{queue_name}]")
    try:
        response = client.create_queue(QueueName=queue_name)
        queue_url = response['QueueUrl']
        print(f"Queue created, url: {queue_url}")
    except ClientError as e:
        fail(e)

    sender = SQSSender(queue_url)

    print("Getting data index...")
    try:
        resp = requests.get("https://interview-data-bucket.s3.ap-south-1.amazonaws.com/data-eng-interview-data/index.json")
        resp.raise_for_status()
        urls = resp.json()
    except requests.RequestException as e:
        fail(f"Error while getting data from s3 {e}")

    print("Sending messages to queue...")
    threads = []
    for url in urls:
        t = threading.Thread(target=sender.start_fetching, args=(url['url'],))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
