// producer.js — Places a single order into SQS
// Run: node producer.js

const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs");

const client = new SQSClient({ region: "ap-south-1" });

const QUEUE_URL = process.env.SQS_QUEUE_URL; // export SQS_QUEUE_URL=<your-queue-url>

const PRODUCTS = [
  "Wireless Headphones",
  "Mechanical Keyboard",
  "USB-C Hub",
  "Webcam 4K",
  "Laptop Stand",
];

function generateOrder() {
  return {
    orderId: "ORD-" + Math.floor(Math.random() * 900000 + 100000),
    product: PRODUCTS[Math.floor(Math.random() * PRODUCTS.length)],
    quantity: Math.floor(Math.random() * 3 + 1),
    userId: "USR-" + Math.floor(Math.random() * 9000 + 1000),
    timestamp: new Date().toISOString(),
  };
}

async function placeOrder() {
  const order = generateOrder();

  const command = new SendMessageCommand({
    QueueUrl: QUEUE_URL,
    MessageBody: JSON.stringify(order),
    MessageAttributes: {
      Source: {
        DataType: "String",
        StringValue: "shopq-frontend",
      },
    },
  });

  const response = await client.send(command);

  console.log("✅ Order placed successfully!");
  console.log("   Order ID  :", order.orderId);
  console.log("   Product   :", order.product);
  console.log("   Quantity  :", order.quantity);
  console.log("   MessageId :", response.MessageId);
  console.log("   Queued at :", order.timestamp);
}

placeOrder().catch(console.error);