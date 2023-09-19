const fs = require('fs');
const jsforce = require('jsforce');

// Read configuration from config.json
const configFile = fs.readFileSync('config.json');
const config = JSON.parse(configFile);

// Initialize Salesforce connection
const conn = new jsforce.Connection();

// Log in to Salesforce
conn.login(config.salesforce.username, config.salesforce.password + config.salesforce.securityToken, function(err, userInfo) {
  if (err) {
    return console.error(err);
  }

  console.log('Logged in as: ' + userInfo.username);

  // Generate random number of new Account records
  const numberOfNewAccounts = 10;
  const newAccounts = [];
 

  for (let i = 0; i < numberOfNewAccounts; i++) {
    newAccounts.push({
      Name: generateRandomName(),
      Description: generateRandomName()
    });
  }

  conn.sobject('Account').create(newAccounts,{ allowRecursive: true }, function(err, rets) {
    if (err) {
      return console.error(err);
    }

    for (let i = 0; i < rets.length; i++) {
      if (rets[i].success) {
        console.log('Account created with ID: ' + rets[i].id);
      } else {
        console.error('Failed to create account: ' + JSON.stringify(rets[i].errors));
      }
    }
  });
});

// Custom function to generate random account names
function generateRandomName() {
  const adjectives = ['Red', 'Blue', 'Green', 'Bright', 'Sunny', 'Clever', 'Golden', 'Silver', 'Crystal'];
  const nouns = ['Mountain', 'Valley', 'River', 'Forest', 'Meadow', 'Island', 'Desert', 'Star', 'Planet'];

  const randomAdjective = adjectives[Math.floor(Math.random() * adjectives.length)];
  const randomNoun = nouns[Math.floor(Math.random() * nouns.length)];

  return `${randomAdjective} ${randomNoun} Corporation`;
}
