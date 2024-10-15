const { MongoClient } = require('mongodb');

module.exports = function (RED) {
    function MongoOnChangeStreamNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;

        // MongoDB Configuration from node properties
        const mongoUrl = 'mongodb+srv://loop:loop2024@cluster0.vor3mxd.mongodb.net/';  // MongoDB connection URI
        const dbName = 'ble';      // Database name
        const collectionName = 'deviceauths';  // Watching 'deviceauths' collection
        
        let client, changeStream;

        // Connect to MongoDB and start watching the 'deviceauths' collection
        async function startChangeStream() {
            try {
                node.status({ fill: "yellow", shape: "dot", text: "connecting..." });
                
                // Connect to MongoDB
                client = await MongoClient.connect(mongoUrl, { useNewUrlParser: true, useUnifiedTopology: true });
                const db = client.db(dbName);
                const collection = db.collection(collectionName);
                
                node.status({ fill: "green", shape: "dot", text: "connected" });

                // Start watching the change stream
                changeStream = collection.watch();

                // Listen for change events
                changeStream.on('change', (change) => {
                    // Send the change event as payload
                    node.send({ payload: change });
                });

                node.status({ fill: "green", shape: "dot", text: "watching for changes..." });

            } catch (err) {
                node.error("MongoDB connection error: " + err.message);
                node.status({ fill: "red", shape: "dot", text: "error" });
            }
        }

        // Stop the change stream when the node is closed
        node.on('close', async function (done) {
            if (changeStream) {
                await changeStream.close();
            }
            if (client) {
                await client.close();
            }
            done();
        });

        // Start the change stream when the node is initialized
        startChangeStream();
    }

    RED.nodes.registerType("mongo-onchangestream", MongoOnChangeStreamNode, {
        credentials: {
            mongoUrl: { type: "text" },
            dbName: { type: "text" }
        }
    });
};
