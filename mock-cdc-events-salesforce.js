const fs = require('fs');

// Number of CDC events to generate
const numberOfEvents = 1000000;

const generateRandomCDCEvent = () => {
    return {
        schema: "ABCDEFGH123456",
        payload: {
            ChangeEventHeader: {
                changeOrigin: "com/salesforce/api/rest/update",
                changeType: "UPDATE",
                transactionKey: Math.random().toString(36).substr(2),
                entityName: "Account",
                recordIds: [Math.random().toString(36).substr(2, 15)]
            },
            Name: "Demo Account " + Math.floor(Math.random() * 10000),
            Phone: `+1-${Math.floor(Math.random() * 10000)}-${Math.floor(Math.random() * 10000)}`,
            AnnualRevenue: Math.floor(Math.random() * 100000)
        }
    };
};

const generateMockEvents = () => {
    let mockEvents = [];
    for (let i = 0; i < numberOfEvents; i++) {
        mockEvents.push(generateRandomCDCEvent());
    }
    return mockEvents;
};

const saveEventsToFile = (events) => {
    fs.writeFileSync('mockCDCEvents.json', JSON.stringify(events, null, 2));
};

const mockEvents = generateMockEvents();
saveEventsToFile(mockEvents);

console.log(`Generated ${numberOfEvents} mock CDC events and saved to mockCDCEvents.json`);
