// surge.js — Simulates a flash sale: sends N orders in rapid succession
// Run: node surge.js 50

const { SQSClient, SendMessageBatchCommand } = require("@aws-sdk/client-sqs");

const client = new SQSClient({ region: "us-east-1" });

const QUEUE_URL = process.env.SQS_QUEUE_URL;

const PRODUCTS = [
  "Wireless Headphones",
  "Mechanical Keyboard",
  "USB-C Hub",
  "Webcam 4K",
  "Laptop Stand",
];

function generateOrder(i) {
  return {
    orderId: "ORD-" + (100000 + i),
    product: PRODUCTS[i % PRODUCTS.length],
    quantity: (i % 3) + 1,
    userId: "USR-" + (1000 + i),
    timestamp: new Date().toISOString(),
  };
}

async function sendBatch(orders, batchNumber) {
  const entries = orders.map((order, idx) => ({
    Id: String(idx),
    MessageBody: JSON.stringify(order),
  }));

  const response = await client.send(
    new SendMessageBatchCommand({
      QueueUrl: QUEUE_URL,
      Entries: entries,
    })
  );

  const successCount = response.Successful?.length || 0;
  const failCount = response.Failed?.length || 0;
  console.log(`  Batch ${batchNumber}: ✅ ${successCount} sent  ❌ ${failCount} failed`);
}

async function runSurge() {
  const total = parseInt(process.argv[2]) || 30;

  console.log(`\n🚀 FLASH SALE STARTED!`);
  console.log(`   Sending ${total} orders to SQS...\n`);

  const startTime = Date.now();

  const allOrders = Array.from({ length: total }, (_, i) => generateOrder(i));
  const batches = [];
  for (let i = 0; i < total; i += 10) {
    batches.push(allOrders.slice(i, i + 10));
  }

  await Promise.all(batches.map((batch, i) => sendBatch(batch, i + 1)));

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);

  console.log(`\n✅ Surge complete!`);
  console.log(`   ${total} orders queued in ${elapsed}s`);
  console.log(`   Consumer will now process them at its own pace`);
  console.log(`   Check AWS Console → SQS → shopq-orders → Monitoring\n`);
}

runSurge().catch(console.error);
