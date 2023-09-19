const fs = require('fs');
const jsforce = require('jsforce');

// Read configuration from config.json
const configFile = fs.readFileSync('config.json');
const config = JSON.parse(configFile);

// Initialize Salesforce connection
const conn = new jsforce.Connection({
    loginUrl: config.salesforce.loginUrl
});

// Log in to Salesforce
conn.login(config.salesforce.username, config.salesforce.password + config.salesforce.securityToken, function (err, userInfo) {
    if (err) {
        return console.error(err);
    }

    console.log('Logged in as: ' + userInfo.username);

    // Query all accounts and contacts
    conn.query("SELECT Id FROM Account", function (err, accounts) {
        if (err) {
            return console.error(err);
        }

        console.log(accounts);
        // Update accounts
        accounts.records.forEach(account => {
            const accountFields = {
                Id: account.Id,
                Phone: config.commonPhoneNumber,
                Name: "Sample UPDATE " + account.Id
            };
            conn.sobject("Account").update(accountFields, function (err, ret) {
                if (err) {
                    console.error('Error updating account with ID ' + account.Id, err);
                } else if (!ret.success) {
                    console.error('Error updating account with ID ' + account.Id, ret.errors);
                } else {
                    console.log('Account with ID ' + account.Id + ' updated successfully');
                }
            });
        });
    });
});
