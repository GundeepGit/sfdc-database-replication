const amqp = require('amqplib');
const fs = require('fs');

const configFilePath = 'config.json';
const mockEventsFilePath = 'mockCDCEvents.json';

// Read the RabbitMQ configuration from the JSON file
const readConfig = () => {
    try {
        const rawData = fs.readFileSync(configFilePath, 'utf-8');
        return JSON.parse(rawData);
    } catch (error) {
        console.error(`Error reading config file: ${error}`);
        process.exit(1);
    }
};

// Read the mock events from the JSON file
const readMockEvents = () => {
    try {
        const rawData = fs.readFileSync(mockEventsFilePath, 'utf-8');
        return JSON.parse(rawData);
    } catch (error) {
        console.error(`Error reading mock events file: ${error}`);
        process.exit(1);
    }
};

const config = readConfig();
const mockEvents = readMockEvents();

// Prepare TLS options
const caCertFile = fs.readFileSync('trustid-x3-root.pem');

const tlsOptions = {
    ca: [caCertFile]
  };

// Function to split array into chunks (batches)
const chunkArray = (array, chunkSize) => {
    let chunks = [];
    for (let i = 0; i < array.length; i += chunkSize) {
        chunks.push(array.slice(i, i + chunkSize));
    }
    return chunks;
};

// Push the mock events to the RabbitMQ queue in batches
const pushEventsToQueue = async () => {
    const connection = await amqp.connect(config.rabbitmq.url, { tls: tlsOptions });
    const channel = await connection.createChannel();

    await channel.assertQueue(config.rabbitmq.batchQueue, {
        durable: true
    });

    const eventBatches = chunkArray(mockEvents, config.batchSize);

    for (const batch of eventBatches) {
        for (const event of batch) {
            channel.sendToQueue(config.rabbitmq.batchQueue, Buffer.from(JSON.stringify(event)));
        }
        console.log(`Pushed a batch of ${batch.length} events to ${config.rabbitmq.batchQueue} queue`);
    }

    // Close the channel and connection after pushing all events
    await channel.close();
    await connection.close();
};

pushEventsToQueue().catch(err => {
    console.error('Error pushing events:', err);
});
