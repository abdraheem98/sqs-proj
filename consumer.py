# consumer.py — Polls SQS and processes orders one by one
# Run: python consumer.py
# Install: pip install boto3

import boto3
import json
import time
import random
import os

sqs = boto3.client("sqs", region_name="us-east-1")

QUEUE_URL = os.environ["SQS_QUEUE_URL"]


def process_order(order):
    """Simulates DB write + payment. Fails 10% of the time for DLQ demo."""
    print(f"\n⚙️  Processing order: {order['orderId']}")
    print(f"   Product  : {order['product']} x{order['quantity']}")
    print(f"   User     : {order['userId']}")

    time.sleep(random.uniform(0.5, 1.5))

    # Simulate 10% failure rate — increase to 0.5 to demo DLQ
    if random.random() < 0.1:
        raise Exception(f"Payment gateway timeout for order {order['orderId']}")

    print(f"✅ Order {order['orderId']} fulfilled!")


def poll_queue():
    print("🔄 Consumer started. Polling SQS queue...\n")

    while True:
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=5,
            WaitTimeSeconds=20,
            VisibilityTimeout=30,
        )

        messages = response.get("Messages", [])

        if not messages:
            print("📭 Queue empty. Waiting for orders...")
            continue

        print(f"📬 Received {len(messages)} message(s)")

        for message in messages:
            order = json.loads(message["Body"])

            try:
                process_order(order)

                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=message["ReceiptHandle"],
                )
                print("🗑️  Message deleted from queue")

            except Exception as e:
                # DO NOT delete — SQS retries after VisibilityTimeout
                # After maxReceiveCount (3) retries → moves to DLQ
                print(f"❌ Failed: {e}")
                print("   Order will be retried by SQS...")


if __name__ == "__main__":
    poll_queue()
