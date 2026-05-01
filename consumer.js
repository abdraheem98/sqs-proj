// consumer.js — Polls SQS and processes orders one by one
// Run: node consumer.js

const {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
} = require("@aws-sdk/client-sqs");

const client = new SQSClient({ region: "us-east-1" });

const QUEUE_URL = process.env.SQS_QUEUE_URL;

// Simulates DB write / payment processing
// Randomly fails 10% of the time to demo DLQ behavior
async function processOrder(order) {
  console.log(`\n⚙️  Processing order: ${order.orderId}`);
  console.log(`   Product  : ${order.product} x${order.quantity}`);
  console.log(`   User     : ${order.userId}`);

  // Simulate processing time (0.5s – 1.5s)
  await new Promise((res) => setTimeout(res, 500 + Math.random() * 1000));

  // Simulate 10% failure rate — increase to 0.5 to demo DLQ
  if (Math.random() < 0.1) {
    throw new Error(`Payment gateway timeout for order ${order.orderId}`);
  }

  console.log(`✅ Order ${order.orderId} fulfilled!`);
}

async function pollQueue() {
  console.log("🔄 Consumer started. Polling SQS queue...\n");

  while (true) {
    const response = await client.send(
      new ReceiveMessageCommand({
        QueueUrl: QUEUE_URL,
        MaxNumberOfMessages: 5,    // batch up to 5 at a time
        WaitTimeSeconds: 20,       // long polling — saves API cost
        VisibilityTimeout: 30,     // hide msg for 30s while processing
      })
    );

    const messages = response.Messages || [];

    if (messages.length === 0) {
      console.log("📭 Queue empty. Waiting for orders...");
      continue;
    }

    console.log(`📬 Received ${messages.length} message(s)`);

    for (const message of messages) {
      const order = JSON.parse(message.Body);

      try {
        await processOrder(order);

        // Delete ONLY after successful processing
        await client.send(
          new DeleteMessageCommand({
            QueueUrl: QUEUE_URL,
            ReceiptHandle: message.ReceiptHandle,
          })
        );

        console.log(`🗑️  Message deleted from queue`);
      } catch (err) {
        // DO NOT delete — SQS will retry after VisibilityTimeout expires
        // After maxReceiveCount (3) retries, it moves to DLQ automatically
        console.error(`❌ Failed: ${err.message}`);
        console.error(`   Order will be retried by SQS...`);
      }
    }
  }
}

pollQueue().catch(console.error);
