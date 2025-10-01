/*  This is an AMQP publisher. It publishes static data to a AMQP server.
    Environment variables are read from the shell.
*/

//Library imports.
require('dotenv').config();
const amqp = require('amqplib');
const crypto = require('crypto');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const QUEUE_NAME = process.env.QUEUE_NAME || 'default_queue';

//Function which dumps related ENV variables.
function printEnvs() {
  console.log("ENV: ", process.env.RABBITMQ_URL);
  console.log("ENV: ", process.env.QUEUE_NAME);
}

//Function which publishes to the AMPQ server. Handles reconnecting and closing.
async function publish(message) {
  try {
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();

    await channel.assertQueue(QUEUE_NAME, { durable: true });

    channel.sendToQueue(QUEUE_NAME, Buffer.from(message), { persistent: true });

    console.log("[x] Sent: ", message);

    await channel.close();
    await connection.close();
  } catch (err) {
    console.error("Publish error:", err.message);
  }
}

//Generate a random string.
function generateRandomString(length) {
  return crypto.randomBytes(12).toString('base64').slice(0, length);
}

printEnvs();

//Timer function.
setInterval(() => {
  let message = generateRandomString(process.env.RANDOM_LENGTH)
  //console.log("Generate message: ", message);
  publish(message);
}, process.env.PUBLISH_INTERVAL);


// Example: send message from CLI args
//const msg = process.argv.slice(2).join(" ") || "Hello! This is a message from the backend";


