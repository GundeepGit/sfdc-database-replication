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

  // Query Accounts to delete
  conn.query("SELECT Id FROM Account", function(err, accounts) {
    if (err) {
      return console.error(err);
    }

    // Delete accounts
    const accountIds = accounts.records.map(account => account.Id);

    conn.sobject("Account").del(accountIds, { allowRecursive: true }, function(err, rets) {
      if (err) {
        return console.error(err);
      }

      for (let i = 0; i < rets.length; i++) {
        if (rets[i].success) {
          console.log('Account with ID ' + rets[i].id + ' deleted successfully');
        } else {
          console.error('Failed to delete account with ID ' + rets[i].id, rets[i].errors);
        }
      }
    });
  });
});
