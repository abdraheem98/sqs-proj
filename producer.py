# producer.py — Places a single order into SQS
# Run: python producer.py
# Install: pip install boto3

import boto3
import json
import random
import os
from datetime import datetime, timezone

sqs = boto3.client("sqs", region_name="us-east-1")

QUEUE_URL = os.environ["SQS_QUEUE_URL"]

PRODUCTS = [
    "Wireless Headphones",
    "Mechanical Keyboard",
    "USB-C Hub",
    "Webcam 4K",
    "Laptop Stand",
]


def generate_order():
    return {
        "orderId": f"ORD-{random.randint(100000, 999999)}",
        "product": random.choice(PRODUCTS),
        "quantity": random.randint(1, 3),
        "userId": f"USR-{random.randint(1000, 9999)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def place_order():
    order = generate_order()

    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(order),
        MessageAttributes={
            "Source": {
                "DataType": "String",
                "StringValue": "shopq-frontend",
            }
        },
    )

    print("✅ Order placed successfully!")
    print(f"   Order ID  : {order['orderId']}")
    print(f"   Product   : {order['product']}")
    print(f"   Quantity  : {order['quantity']}")
    print(f"   MessageId : {response['MessageId']}")
    print(f"   Queued at : {order['timestamp']}")


if __name__ == "__main__":
    place_order()
