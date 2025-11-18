/*  This is an AMQP publisher. It publishes static data to a AMQP server.
    Environment variables are read from the shell.
*/

//Library imports.
require('dotenv').config();
const amqp = require('amqplib');
const crypto = require('crypto');

const RABBITMQ_HOST = process.env.RABBITMQ_HOST;
const RABBITMQ_PORT = process.env.RABBITMQ_PORT;
const RABBITMQ_USERNAME = process.env.RABBITMQ_USERNAME;
const RABBITMQ_PASSWORD = process.env.RABBITMQ_PASSWORD;
const QUEUE_NAME = process.env.QUEUE_NAME || 'default_queue';
const RANDOM_LENGTH = process.env.RANDOM_LENGTH;
const PUBLISH_INTERVAL = process.env.PUBLISH_INTERVAL;
var RABBITMQ_URL = `amqp://${RABBITMQ_USERNAME}:${RABBITMQ_PASSWORD}@${RABBITMQ_HOST}:${RABBITMQ_PORT}`;

//Function which dumps related ENV variables.
function printEnvs() {
  console.log("ENV: ", RABBITMQ_HOST);
  console.log("ENV: ", RABBITMQ_PORT);
  console.log("ENV: ", RABBITMQ_USERNAME);
  console.log("ENV: ", RABBITMQ_PASSWORD);
  console.log("ENV: ", QUEUE_NAME);
  console.log("ENV: ", RANDOM_LENGTH);
  console.log("ENV: ", PUBLISH_INTERVAL);
  console.log("ENV: ", RABBITMQ_URL);
}

//Function which publishes to the AMPQ server. Handles reconnecting and closing.
async function publish(message) {
  try {
    console.log("Attempting to establish a connection with the AMPQ server...");
    const connection = await amqp.connect(RABBITMQ_URL);

    console.log("Attempting to create an AMPQ channel...");
    const channel = await connection.createChannel();

    console.log("Attempting to create an AMPQ queue...");
    await channel.assertQueue(QUEUE_NAME, { durable: true });

    console.log("Attempting to publish message...");
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


