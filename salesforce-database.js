const fs = require('fs');
const jsforce = require('jsforce');
const amqp = require('amqplib');
const mysql = require('mysql2/promise');
const CircuitBreaker = require('opossum');
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
const options = {
    timeout: 40000, // If our function takes longer than 40 seconds, trigger a failure
    errorThresholdPercentage: 50, // When 50% of requests fail, trip the circuit
    resetTimeout: 30000 // After 30 seconds, try again.
  };


class SalesforceStreamingApp {
    constructor(configPath) {
        this.config = JSON.parse(fs.readFileSync(configPath));
        this.sfConn = new jsforce.Connection({
            loginUrl: this.config.salesforce.loginUrl
        });
        this.retryDelay = 10000; // 10 seconds
        this.maxRetries = 3;
        this.retryCount = 0;
        this.createRetryCount =0;
        this.entityTypeFieldMappings = {};
        this.eventMappings = {};
    }

    
    async fetchObjectFields(objectName) {      
                const objectDescription = await this.sfConn.describe(objectName);
        return objectDescription.fields
            .filter((field) => field.createable && field.updateable)
            .map((field) => field.name);
    }

    async initialize() {
        try {
            await this.connectToDatabase();
            await this.connectToRabbitMQ();
            await this.connectToDLQ();
            await this.connectToSalesforce();
        } catch (error) {
            logger.error('Initialization error:', error);
            process.exit(1);
        }
    }

    async connectToDatabase() {
        try {
            this.dbConn = await mysql.createConnection({
                host: this.config.mysql.host,
                user: this.config.mysql.user,
                password: this.config.mysql.password,
                database: this.config.mysql.database
            });
            logger.info('Connected to MySQL database');
        } catch (error) {
            logger.error('MySQL connection failed:', error);
            process.exit(1);
        }
    }

    async connectToRabbitMQ() {
        try {
            console.log("inside MQ call");
            this.rabbitMQConnection = await amqp.connect(this.config.rabbitmq.url);
            this.rabbitMQChannel = await this.rabbitMQConnection.createChannel();
            await this.rabbitMQChannel.assertQueue(this.config.rabbitmq.queueName, { durable: true });
            logger.info('Connected to RabbitMQ');
        } catch (error) {
            logger.error('RabbitMQ connection failed:', error);
            process.exit(1);
        }
    }

    async connectToDLQ() {
        try {
            this.rabbitMQConnection = await amqp.connect(this.config.rabbitmq.url);
            this.rabbitMQChannel = await this.rabbitMQConnection.createChannel();
            await this.rabbitMQChannel.assertQueue(this.config.rabbitmq.dlqName, { durable: true });
            logger.info('Connected to DLQ');

        } catch (error) {
            logger.error('DLQ connection failed:', error);
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

    // async retryConnection(table, event) {
    //     this.createRetryCount++;
    //     if (this.createRetryCount <= this.maxRetries) {
    //         logger.info(`Retrying DB connection (attempt ${this.createRetryCount})...`);
    //         this.createRecord(table, event);
    //     } else {
    //         logger.error('Max retry attempts reached. Pushing to DLQ...');
    //         this.rabbitMQChannel.sendToQueue(this.config.rabbitmq.dlqName, Buffer.from(JSON.stringify(event)));
    //         this.createRetryCount = 0;
    //     }
    // }
    
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

    getUpdatedFields(event) {
        const changeType = event.payload.ChangeEventHeader.changeType;
        var changedFields = [];

        if (changeType == 'CREATE') {
            return Object.keys(event.payload);
        }

        changedFields = event.payload.ChangeEventHeader.changedFields;
        //logger.info(changedFields.length);
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
        let count = 0;
        let success = true;
        do{
            
            try {
                const fields = await this.getObjectMetadata(table);
                //logger.info(fields);
                await this.ensureTable(table, fields);
               // await this.ensureColumns(table, event);
    
                // Remove unwanted properties like the event header
                const data = { ...event };
                
                data.payload['Id'] = Id;
                delete data.payload.ChangeEventHeader;    
                // Construct query dynamically
                logger.info(data);
                const Id = event.payload.ChangeEventHeader.recordIds[0];
                const columns = Object.keys(data.payload).join(', ');
                const placeholders = Object.keys(data.payload).fill('?').join(', ');
                const values = Object.values(data.payload);
                const sql = `INSERT INTO ${table} (${columns}) VALUES (${placeholders})`;
                logger.info(sql);
                logger.info(values);
                const [results] = await this.dbConn.query(sql, values); //sql ,
                logger.info(`Record inserted: ${results.affectedRows}`);
                success = true;
            } catch (err) {
                if(count <3 ){ 
                logger.error('Error creating records:', err);
                logger.info(`Retrying in ${this.retryDelay / 1000} seconds...`);
                await this.returnNewPromiseWithTimeout();
                count++;
                success =false;
                }
                else{
                logger.error('Max retry attempts reached. Pushing to DLQ...');
                this.rabbitMQChannel.sendToQueue(this.config.rabbitmq.dlqName, Buffer.from(JSON.stringify(event)));
                count++;
                }
            }
        
        }while(count<=3 && !success);
       
    }

    async returnNewPromiseWithTimeout () {
        return new Promise((resolve) => {
            setTimeout(()=> {resolve()}, this.retryDelay);
        });
    }

    async updateRecord(table, id, event) {
        let count = 0;
        let success = true;
        do{
        try {
            // Remove unwanted properties like the event header
            const data = { ...event };
            delete data.payload.ChangeEventHeader;

            // Construct query dynamically
            const columnsValues = Object.keys(data.payload).map(key => `${key} = ?`).join(', ');
            const values = [...Object.values(data.payload), id];

            const sql = `UPDATE ${table} SET ${columnsValues} WHERE id = ?`;
            logger.info(sql);
            const [results] = await this.dbConn.query( values); //sql,
            logger.info(`Record updated: ${results.affectedRows}`);
        } catch (error) {
            if(count <3 ){ 
            logger.error('Error while updating records:', error);
            logger.info(`Retrying in ${this.retryDelay / 1000} seconds...`);
            await this.returnNewPromiseWithTimeout();
            count++;
            success =false;
            }
            else{
                logger.error('Max retry attempts reached. Pushing to DLQ...');
                this.rabbitMQChannel.sendToQueue(this.config.rabbitmq.dlqName, Buffer.from(JSON.stringify(event)));
                count++;
                }
        }
    }while(count<=3 && !success);
    }

    async deleteRecord(table, Ids,event) {
        let count = 0;
        let success = true;
        do{
        try {
            const sql = `DELETE FROM ${table} WHERE id in (?)`;
            const [results] = await this.dbConn.query(sql, [Ids]);
            logger.info(`Record deleted: ${results.affectedRows}`);
        } catch (error) {
            if(count <3 ){ 
                logger.error('Error while deleting records:', error);
                logger.info(`Retrying in ${this.retryDelay / 1000} seconds...`);
                await this.returnNewPromiseWithTimeout();
                count++;
                success =false;
            }
            else{
                logger.error('Max retry attempts reached. Pushing to DLQ...');
                this.rabbitMQChannel.sendToQueue(this.config.rabbitmq.dlqName, Buffer.from(JSON.stringify(event)));
                count++;
                }
        }
    }while(count<=3 && !success);
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
            queries = [...queries, ...missingColumns.map(column => `ADD ${column} TEXT`)];
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
                    this.createRetryCount=0;
                    await this.createRecord(table, event);
                    break;
                case 'UPDATE':
                    await this.updateRecord(table, event.payload.ChangeEventHeader.recordIds[0], event);
                    break;
                case 'DELETE':
                    await this.deleteRecord(table, event.payload.ChangeEventHeader.recordIds,event);
                    break;
                default:
                    logger.info('Unknown operation:', operation);
            }
        } catch (error) {
            logger.error('Event processing error:', error);
        }
    }

    getQueryValues(event, entityType, eventType) {
        const fieldMappings = this.entityTypeFieldMappings[entityType]; // Define field mappings
        const updatedFields = this.getUpdatedFields(event);
        const values = [];

        logger.info(JSON.stringify(event, null, 2));
        //logger.info(fieldMappings[eventType]);
        logger.info(updatedFields);
        if (fieldMappings && updatedFields.length > 0) {
            //logger.info(fieldMappings[eventType]);
            for (const field of fieldMappings[eventType]) {
                //logger.info(updatedFields.includes(field));
                logger.info(event.payload[field]);
                if (updatedFields.includes(field)) {
                    values.push(event.payload[field]);
                }
            }
        }
        values.push(event.payload.ChangeEventHeader.recordIds[0]);

        return values;
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
                    const event = JSON.parse(msg.content.toString());
                    await this.processEvent(event);
                    queueChannel.ack(msg); // Acknowledge the message
                },
                { noAck: false } // Set noAck to false for manual acknowledgement
            );
        } catch (error) {
            logger.error('Error while consuming events from the queue:', error);
        }
    }

}

const app = new SalesforceStreamingApp('config.json');
app.initialize();
const breaker = new CircuitBreaker(app.consumeEventsFromQueue, options);
breaker.fire()
breaker.fallback(() => console.log('Sorry, out of service right now'));
