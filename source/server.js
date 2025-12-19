/*  This is an AMQP publisher. It publishes static data to a AMQP server.
    Environment variables are read from the shell.
*/

//Library imports.
require('dotenv').config();
const AMQP = require('amqplib');
const crypto = require('crypto');

const RABBITMQ_HOST = process.env.RABBITMQ_HOST;
const RABBITMQ_PORT = process.env.RABBITMQ_PORT;
const RABBITMQ_USERNAME = process.env.RABBITMQ_USERNAME;
const RABBITMQ_PASSWORD = process.env.RABBITMQ_PASSWORD;
const QUEUE_NAME = process.env.QUEUE_NAME || 'QBA_QUEUE_1';
const RANDOM_LENGTH = process.env.RANDOM_LENGTH;
const PUBLISH_INTERVAL = process.env.PUBLISH_INTERVAL || '3000';

let AMQP_URL = `amqp://${RABBITMQ_USERNAME}:${RABBITMQ_PASSWORD}@${RABBITMQ_HOST}:${RABBITMQ_PORT}`;
let AMQP_CONNECTION, AMQP_CHANNEL;
let AMQP_STATUS_OK = false

//Function which dumps related ENV variables.
function printEnvs() {
  console.log("ENV: ", RABBITMQ_HOST);
  console.log("ENV: ", RABBITMQ_PORT);
  console.log("ENV: ", RABBITMQ_USERNAME);
  console.log("ENV: ", RABBITMQ_PASSWORD);
  console.log("ENV: ", QUEUE_NAME);
  console.log("ENV: ", RANDOM_LENGTH);
  console.log("ENV: ", PUBLISH_INTERVAL);
  console.log("ENV: ", AMQP_URL);
}

async function createAmqpConnection(amqpUrl){
  try{
    console.log("Attempting to connect with the AMPQ server...");
    AMQP_CONNECTION = await AMQP.connect(amqpUrl);
    console.log("Connected to the AMPQ server");
  } catch (err) {
    console.error("Could not connect to the AMQP server! : ", err.message);
    //Retry after a wait interval.
    await new Promise(resolve => setTimeout(resolve, 2000));
    await createAmqpConnection(amqpUrl);
  }
}

async function createAmqpChannel(amqpConnection){
  try{
    console.log("Attempting to create an AMPQ channel...");
    AMQP_CHANNEL = await amqpConnection.createChannel();
    console.log("Created an AMPQ channel");
  } catch (err) {
    console.error("Could not create an AMPQ channel! : ", err.message);
    //Retry after a wait interval.
    await new Promise(resolve => setTimeout(resolve, 2000));
    createAmqpChannel(amqpConnection);
  }
}

async function createAmqpQueue(amqpChannel, queueName){
  try{
    console.log("Attempting to create an AMPQ queue...");
    /*
    Durable: Queue survives a broker or connection restart but messages are lost. Queue is written to disk (without messages).
    Persistent: Messages in a queue survives a broker restart. Messages are written to disk. Needs a durable queue.
    Exclusive: Accessible only by the connection which created the queue. If that connection fails, the queue is destroyed.
    Auto delete: Delete the queue if the last subscriber disconnects.

    Duplicate creation does not create an exception if all properties are the same.
    */
    await amqpChannel.assertQueue(queueName, { durable: true, persistent: false, exclusive: false, autoDelete: false });
    console.log("Created an AMPQ queue");
  } catch (err) {
    console.error("Could not create an AMPQ queue! : ", err.message);
    //Retry after a wait interval.
    await new Promise(resolve => setTimeout(resolve, 2000));
    createAmqpQueue(amqpChannel, queueName);
  }
}

//Set a flag if the AMQP setup is successful.
async function amqpStatus() {
  AMQP_STATUS_OK = true
}

async function amqpSetup() {
  await createAmqpConnection(AMQP_URL);
  await createAmqpChannel(AMQP_CONNECTION);
  await createAmqpQueue(AMQP_CHANNEL, QUEUE_NAME);
  //Wait for all phases to be successful to set the status flag.
  await amqpStatus();
}

//Function which publishes to the AMPQ server. Handles reconnecting and closing.
async function publishAmqpMessage(message, channel, queue) {
  //Skip execution if the connection is being self-healed.
  if (!AMQP_STATUS_OK) return;

  try {
    console.log("Attempting to publish message...");
    channel.sendToQueue(queue, Buffer.from(message));
    console.log("[x] Sent: ", message);
  } catch (err) {
    console.error("Publish error! :", err.message);
    //Retry AMPQ broker setup.
    await amqpSetup();
  }
}

//Generate a random string.
function generateRandomString(length) {
  return crypto.randomBytes(12).toString('base64').slice(0, length);
}

//Print environment variables.
printEnvs();

//Set up a connection.
amqpSetup();

//Timer function.
setInterval(() => {
  let message = generateRandomString(process.env.RANDOM_LENGTH)
    publishAmqpMessage(message, AMQP_CHANNEL, QUEUE_NAME);
}, process.env.PUBLISH_INTERVAL);


