const fs = require('fs');
const jsforce = require('jsforce');
const amqp = require('amqplib');
const mysql = require('mysql2/promise');
const winston = require('winston');
const DailyRotateFile = require('winston-daily-rotate-file');
const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json()
    ),
    defaultMeta: { service: 'SalesforceStreamingApp' },
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

class CircuitBreaker {
    constructor() {
        this.states = {
            CLOSED: 'CLOSED',
            OPEN: 'OPEN',
            HALF_OPEN: 'HALF_OPEN'
        };

        this.currentState = this.states.CLOSED;
        this.failureCount = 0;
        this.failureThreshold = 3;
        this.successCount = 0;
        this.successThreshold = 3;
        this.halfOpenStateTimeout = 60000; // 1 minute
    }

    call(fn) {
        if (this.currentState === this.states.OPEN) {
            throw new Error('Circuit is currently open. Can\'t process the request');
        }

        try {
            const result = fn();
            this.onSuccess();
            return result;
        } catch (error) {
            this.onFailure(error);
            throw error;
        }
    }

    onSuccess() {
        if (this.currentState === this.states.HALF_OPEN) {
            this.successCount++;
            if (this.successCount >= this.successThreshold) {
                this.reset();
            }
        } else {
            this.reset();
        }
    }

    onFailure(error) {
        this.failureCount++;
        this.successCount = 0; // Reset the success count
        if (this.failureCount >= this.failureThreshold) {
            this.open();
        }
    }

    open() {
        this.currentState = this.states.OPEN;
        console.log('Circuit is now OPEN');

        // After the predefined duration, switch to HALF_OPEN state
        setTimeout(() => {
            this.currentState = this.states.HALF_OPEN;
            this.successCount = 0; // Reset the success count
            console.log('Circuit is now HALF_OPEN');
        }, this.halfOpenStateTimeout);
    }

    reset() {
        this.failureCount = 0;
        this.successCount = 0; // Reset the success count
        if (this.currentState !== this.states.CLOSED) {
            this.currentState = this.states.CLOSED;
            console.log('Circuit is now CLOSED');
        }
    }
}

const sfCircuitBreaker = new CircuitBreaker();
const rabbitMQCircuitBreaker = new CircuitBreaker();
const dbCircuitBreaker = new CircuitBreaker();

class SalesforceStreamingApp {
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
            await this.connectToDatabase();
            await this.connectToRabbitMQ();
            await this.connectToSalesforce();
        } catch (error) {
            logger.error('Initialization error:', error);
            process.exit(1);
        }
    }

    async connectToDatabase() {
        try {
            await dbCircuitBreaker.call(async () => {
                this.dbConn = await mysql.createConnection({
                    host: this.config.mysql.host,
                    user: this.config.mysql.user,
                    password: this.config.mysql.password,
                    database: this.config.mysql.database
                });
                logger.info('Connected to MySQL database');
            });
        } catch (error) {
            logger.error('MySQL connection failed:', error);
            
            if (error.message.includes('Circuit is currently open')) {
                logger.warn('Circuit breaker is open for MySQL. Waiting before trying again...');
                setTimeout(() => this.connectToDatabase(), this.retryDelay);
            } else {
                process.exit(1);
            }
        }
    }    

    async connectToRabbitMQ() {
        try {
            await rabbitMQCircuitBreaker.call(async () => {
                this.rabbitMQConnection = await amqp.connect(this.config.rabbitmq.url, { tls: tlsOptions });
                this.rabbitMQChannel = await this.rabbitMQConnection.createChannel();
                await this.rabbitMQChannel.assertQueue(this.config.rabbitmq.queueName, { durable: true });
                logger.info('Connected to RabbitMQ');
            });
        } catch (error) {
            logger.error('RabbitMQ connection failed:', error);
    
            if (error.message.includes('Circuit is currently open')) {
                logger.warn('Circuit breaker is open for RabbitMQ. Waiting before trying again...');
                setTimeout(() => this.connectToRabbitMQ(), this.retryDelay);
            } else {
                process.exit(1);
            }
        }
    }
    

    async connectToSalesforce() {
        try {
            await sfCircuitBreaker.call(async () => {
                await this.sfConn.login(this.config.salesforce.username, this.config.salesforce.password + this.config.salesforce.securityToken);
                logger.info('Connected to Salesforce');
            });
        } catch (error) {
            logger.error('Salesforce login error:', error);

            if (error.errorCode === 'LOGIN_MUST_USE_SECURITY_TOKEN') {
                logger.info(`Retrying in ${this.retryDelay / 1000} seconds...`);
                setTimeout(() => this.retrySalesforceConnection(), this.retryDelay);
            } else if (error.message.includes('Circuit is currently open')) {
                logger.warn('Circuit breaker is open. Waiting before trying again...');
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

    getUpdatedFields(event) {
        const changeType = event.payload.ChangeEventHeader.changeType;
        var changedFields = [];

        if (changeType == 'CREATE') {
            return Object.keys(event.payload);
        }

        changedFields = event.payload.ChangeEventHeader.changedFields;
        logger.info(changedFields.length);
        if (changedFields.length > 1) {
            return changedFields;
        }
        return [];
    }

    async getObjectMetadata(objectName) {
        const metadata = await this.sfConn.sobject(objectName).describe();
        return metadata.fields.map(field => ({
            name: field.name,
            type: this.mapSalesforceTypeToSqlType(field.type)
        }));
    }

    mapSalesforceTypeToSqlType(salesforceType) {
        switch (salesforceType) {
            case 'string':
                return 'TEXT';
            case 'number':
                return 'INT';
            default:
                return 'TEXT';
        }
    }

    async createRecord(table, event) {

        try {
            const fields = await this.getObjectMetadata(table);
            //logger.info(fields);
            await this.ensureTable(table, fields);
            //await this.ensureColumns(table, event);

            // Remove unwanted properties like the event header
            const data = { ...event };
            const Id = event.payload.ChangeEventHeader.recordIds[0];
            data.payload['Id'] = Id;
            delete data.payload.ChangeEventHeader;

            // Construct query dynamically
            logger.info(data);
            const columns = Object.keys(data.payload).join(', ');
            const placeholders = Object.keys(data.payload).fill('?').join(', ');
            const values = Object.values(data.payload);

            const sql = `INSERT INTO ${table} (${columns}) VALUES (${placeholders})`;
            logger.info(sql);
            logger.info(values);
            const [results] = await this.dbConn.query(sql, values);
            logger.info(`Record inserted: ${results.affectedRows}`);
            dbCircuitBreaker.onSuccess();
        } catch (error) {
            dbCircuitBreaker.onFailure(error);
            logger.error('Error ensuring columns:', error);
        }
    }

    async updateRecord(table, id, event) {
        try {
            const fields = await this.getObjectMetadata(table);
            await this.ensureTable(table, fields);
            // Remove unwanted properties like the event header
            const data = { ...event };
            delete data.payload.ChangeEventHeader;

            // Construct query dynamically
            const columnsValues = Object.keys(data.payload).map(key => `${key} = ?`).join(', ');
            const values = [...Object.values(data.payload), id];

            const sql = `UPDATE ${table} SET ${columnsValues} WHERE id = ?`;
            logger.info(sql);
            const [results] = await this.dbConn.query(sql, values);
            logger.info(`Record updated: ${results.affectedRows}`);
            dbCircuitBreaker.onSuccess();
        } catch (error) {
            dbCircuitBreaker.onFailure(error);
            logger.error('Error while updating:', error);
        }
    }

    async deleteRecord(table, Ids) {
        try {
            const sql = `DELETE FROM ${table} WHERE id in (?)`;
            const [results] = await this.dbConn.query(sql, [Ids]);
            logger.info(`Record deleted: ${results.affectedRows}`);
            dbCircuitBreaker.onSuccess();
        } catch (error) {
            dbCircuitBreaker.onFailure(error);
            logger.error('Error while deleting:', error);
        }
    }

    async ensureTable(table, fields) {
        const sql = `CREATE TABLE IF NOT EXISTS ${table} (primary_key INT PRIMARY KEY NOT NULL AUTO_INCREMENT, ${fields.map(field => `${field.name} ${field.type}`).join(', ')})`;
        return await this.dbConn.query(sql);
    }

    async ensureColumns(table, event, fields) {
        // Remove unwanted properties like the event header
        const data = { ...event };
        delete data.payload.ChangeEventHeader;

        // Get columns from database
        const [rows] = await this.dbConn.query(`SHOW COLUMNS FROM ${table}`);
        const existingColumns = rows.map(row => row.Field);

        
        // Check for missing columns
        const missingColumns = Object.keys(data).filter(column => !existingColumns.includes(column));
        const extraneousColumns = existingColumns.filter(column => !fields.find(field => field.name === column) && column !== 'id');

        let queries = [];

        // If there are missing columns, create them
        if (missingColumns.length > 0) {
            queries = [...queries, ...missingColumns.map(column => `ADD ${column} TEXT`)]; // add logic to consider different datatypes other than string
        }

        // If there are extraneous columns, drop them
        if (extraneousColumns.length > 0) {
            queries = [...queries, ...extraneousColumns.map(column => `DROP ${column}`)];
        }

        if (queries.length > 0) {
            const sql = `ALTER TABLE ${table} ${queries.join(', ')}`;
            return this.dbConn.query(sql);
        }
    }

    async processEvent(event) {
        try {
            const table = event.payload.ChangeEventHeader.entityName;
            const operation = event.payload.ChangeEventHeader.changeType;

            logger.info(JSON.stringify(event, null, 2));
            logger.info(event.payload.ChangeEventHeader.recordIds);

            switch (operation) {
                case 'CREATE':
                    await this.createRecord(table, event);
                    break;
                case 'UPDATE':
                    await this.updateRecord(table, event.payload.ChangeEventHeader.recordIds[0], event);
                    break;
                case 'DELETE':
                    await this.deleteRecord(table, event.payload.ChangeEventHeader.recordIds);
                    break;
                default:
                    logger.info('Unknown operation:', operation);
            }
        } catch (error) {
            this.connectToDatabase();
            logger.error('Event processing error:', error);
        }
    }

    async consumeEventsFromQueue() {
        try {
            await this.connectToRabbitMQ(); // Ensure to connect to RabbitMQ first
            logger.info('Consuming events from the queue...');

            const queueChannel = await this.rabbitMQConnection.createChannel(); // Create a new channel
            await queueChannel.assertQueue(this.config.rabbitmq.queueName, { durable: true });

            queueChannel.consume(
                this.config.rabbitmq.queueName,
                async (msg) => {
                    console.log(dbCircuitBreaker.currentState);
                    if (dbCircuitBreaker.currentState === dbCircuitBreaker.states.CLOSED || dbCircuitBreaker.currentState === dbCircuitBreaker.states.HALF_OPEN) {
                        const event = JSON.parse(msg.content.toString());
                        await this.processEvent(event);
                        queueChannel.ack(msg); // Acknowledge the message
                    } else {
                        logger.warn('Circuit breaker is OPEN. Delaying event processing.');
                        setTimeout(() => {
                            queueChannel.nack(msg, false, true); // nack and requeue the message.
                            this.connectToDatabase();
                        }, this.retryDelay);
                    }
                },
                { noAck: false } // Set noAck to false for manual acknowledgement
            );
        } catch (error) {
            rabbitMQCircuitBreaker.onFailure(err);
            logger.error('Error while consuming events from the queue:', error);
        }
    }

}

const app = new SalesforceStreamingApp('config.json');
app.initialize();
app.consumeEventsFromQueue();