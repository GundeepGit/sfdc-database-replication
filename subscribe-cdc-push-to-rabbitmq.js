const fs = require('fs');
const jsforce = require('jsforce');
const amqp = require('amqplib');
const winston = require('winston');
const DailyRotateFile = require('winston-daily-rotate-file');
const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json()
    ),
    defaultMeta: { service: 'SubscribeCDCEventsPushToRabbitMQ' },
    transports: [
        new DailyRotateFile({
            filename: 'logs/app_%DATE%.log',
            datePattern: 'YYYY-MM-DD-HH',
            zippedArchive: true,
            maxSize: '20m', // 20 MB
            maxFiles: '14d' // keep logs for 14 days
        }),
        new winston.transports.Console({
            format: winston.format.combine(
                winston.format.colorize(),
                winston.format.simple()
            )
        })
    ]
});

const caCertFile = fs.readFileSync('trustid-x3-root.pem');

const tlsOptions = {
    ca: [caCertFile]
  };

class SubscribeCDCEventsPushToRabbitMQApp {
    constructor(configPath) {
        this.config = JSON.parse(fs.readFileSync(configPath));
        this.sfConn = new jsforce.Connection({
            loginUrl: this.config.salesforce.loginUrl
        });
        this.retryDelay = 10000; // 10 seconds
        this.maxRetries = 5;
        this.retryCount = 0;
        this.entityTypeFieldMappings = {};
        this.eventMappings = {};
    }

    async initialize() {
        try {
            await this.connectToRabbitMQ();
            await this.connectToSalesforce();
        } catch (error) {
            logger.error('Initialization error:', error);
            process.exit(1);
        }
    }

    async connectToRabbitMQ() {
        try {
            this.rabbitMQConnection = await amqp.connect(this.config.rabbitmq.url, { tls: tlsOptions });
            this.rabbitMQChannel = await this.rabbitMQConnection.createChannel();
            await this.rabbitMQChannel.assertQueue(this.config.rabbitmq.queueName, { durable: true });
            logger.info('Connected to RabbitMQ');
        } catch (error) {
            logger.error('RabbitMQ connection failed:', error);
            process.exit(1);
        }
    }

    async connectToSalesforce() {
        try {
            await this.sfConn.login(this.config.salesforce.username, this.config.salesforce.password + this.config.salesforce.securityToken);
            logger.info('Connected to Salesforce');
            this.subscribeToStreamingApi();
        } catch (error) {
            logger.error('Salesforce login error:', error);
            if (error.errorCode === 'LOGIN_MUST_USE_SECURITY_TOKEN') {
                logger.info(`Retrying in ${this.retryDelay / 1000} seconds...`);
                setTimeout(() => this.retrySalesforceConnection(), this.retryDelay);
            } else {
                process.exit(1);
            }
        }
    }

    retrySalesforceConnection() {
        this.retryCount++;
        if (this.retryCount <= this.maxRetries) {
            logger.info(`Retrying Salesforce connection (attempt ${this.retryCount})...`);
            this.connectToSalesforce();
        } else {
            logger.error('Max retry attempts reached. Exiting...');
            process.exit(1);
        }
    }

    subscribeToStreamingApi() {
        const streamingApi = this.sfConn.streaming.createClient();
        streamingApi.subscribe('/data/ChangeEvents', (message) => {
            //logger.info(message);
            //logger.info(this.config.rabbitmq.queueName);
            const event = message;
            this.rabbitMQChannel.sendToQueue(this.config.rabbitmq.queueName, Buffer.from(JSON.stringify(event)));
        });
        logger.info('Subscribed to Salesforce Streaming API');
    }

}

const app = new SubscribeCDCEventsPushToRabbitMQApp('config.json');
app.initialize();