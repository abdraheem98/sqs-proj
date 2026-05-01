# surge.py — Simulates a flash sale: sends N orders in rapid succession
# Run: python surge.py 50
# Install: pip install boto3

import boto3
import json
import random
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

sqs = boto3.client("sqs", region_name="ap-south-1")

QUEUE_URL = os.environ["SQS_QUEUE_URL"]

PRODUCTS = [
    "Wireless Headphones",
    "Mechanical Keyboard",
    "USB-C Hub",
    "Webcam 4K",
    "Laptop Stand",
]


def generate_order(i):
    return {
        "orderId": f"ORD-{100000 + i}",
        "product": PRODUCTS[i % len(PRODUCTS)],
        "quantity": (i % 3) + 1,
        "userId": f"USR-{1000 + i}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def send_batch(orders, batch_number):
    entries = [
        {"Id": str(idx), "MessageBody": json.dumps(order)}
        for idx, order in enumerate(orders)
    ]

    response = sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=entries)

    success = len(response.get("Successful", []))
    failed = len(response.get("Failed", []))
    print(f"  Batch {batch_number}: ✅ {success} sent  ❌ {failed} failed")
    return success, failed


def run_surge():
    total = int(sys.argv[1]) if len(sys.argv) > 1 else 30

    print(f"\n🚀 FLASH SALE STARTED!")
    print(f"   Sending {total} orders to SQS...\n")

    start = time.time()

    all_orders = [generate_order(i) for i in range(total)]
    batches = [all_orders[i: i + 10] for i in range(0, total, 10)]

    total_success = 0
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(send_batch, batch, i + 1): i
            for i, batch in enumerate(batches)
        }
        for future in as_completed(futures):
            success, _ = future.result()
            total_success += success

    elapsed = time.time() - start

    print(f"\n✅ Surge complete!")
    print(f"   {total_success}/{total} orders queued in {elapsed:.2f}s")
    print(f"   Consumer will now process them at its own pace")
    print(f"   Check AWS Console → SQS → shopq-orders → Monitoring\n")


if __name__ == "__main__":
    run_surge()