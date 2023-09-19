const CircuitBreaker = require('opossum');
const fs = require('fs');
const mysql = require('mysql2/promise');
const configFile = fs.readFileSync('config.json');
const config = JSON.parse(configFile);

const options = {
    timeout: 4000, // If our function takes longer than 40 seconds, trigger a failure
    errorThresholdPercentage: 50, // When 50% of requests fail, trip the circuit
    resetTimeout: 30000 // After 30 seconds, try again.
  };
 
  
async function connectToDatabase() {
    try {
        const dbConn = await mysql.createConnection({
            host: config.mysql.host,
            user: config.mysql.user,
            password: config.mysql.password,
            database: config.mysql.database
        });
        console.log('Connected to MySQL database');
    } catch (error) {
        //console.log('MySQL connection failed:', error);
        throw error;
    }
}

const breaker = new CircuitBreaker(connectToDatabase, options);
breaker.fire()
breaker.fallback(() => console.log('Sorry, out of service right now'));
