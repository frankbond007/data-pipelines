import aiohttp
import asyncio
import json
import boto3
import time
import uuid
import os
localstack_host = os.getenv('LOCALSTACK_HOST')
queue_url = os.getenv('QUEUE_URL')
region = os.getenv('AWS_REGION')
aws_key = os.getenv('AWS_KEY')
aws_secret_key = os.getenv('AWS_SECRET_KEY')
sample_data_url = os.getenv('SAMPLE_DATA_URL')


class URLDownloader:
    def __init__(self, queue_url):
        self.queue_url = queue_url
        self.region = 'ap-south-1'
        self.client = boto3.client('sqs', region_name=region,
                                   aws_access_key_id=aws_key,
                                   aws_secret_access_key=aws_secret_key,
                                   aws_session_token="TOKEN",
                                   endpoint_url=f"http://{localstack_host}:4566")

    async def download_content(self, url):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        return await response.text()
                    else:
                        return None
        except aiohttp.ClientError:
            return None

    async def download_and_send_to_queue(self, url_list):
        tasks = [self.download_content(url['url']) for url in url_list]
        downloaded_content = await asyncio.gather(*tasks)

        messages = [content for content in downloaded_content if content is not None]
        await self.send_messages(messages)

    async def send_messages(self, messages):
        chunks = [messages[i:i + 10] for i in range(0, len(messages), 10)]
        send_tasks = []

        for chunk in chunks:
            entries = []
            for message in chunk:
                #time.sleep(2)
                msg = json.dumps(message)
                entries.append({
                    'Id': str(uuid.uuid4()),
                    'MessageBody': msg,
                    'DelaySeconds': 1
                })

            send_task = asyncio.create_task(self.send_batch(entries))
            send_tasks.append(send_task)

        await asyncio.gather(*send_tasks)

    async def send_batch(self, entries):
        try:
            self.client.send_message_batch(QueueUrl=self.queue_url, Entries=entries)
        except ClientError as e:
            pass

async def main():
    async with aiohttp.ClientSession() as session:
        async with session.get(sample_data_url) as response:
            if response.status == 200:
                url_list = await response.json()
                downloader = URLDownloader(queue_url)
                await downloader.download_and_send_to_queue(url_list)
            else:
                print(f"Failed to fetch URL list: Status code {response.status}")

if __name__ == "__main__":
    asyncio.run(main())
