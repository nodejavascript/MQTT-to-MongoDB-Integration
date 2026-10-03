# MQTT-to-MongoDB-Integration

A device that reports a reading is only useful if the reading is kept. This subscribes to every topic on an MQTT broker and writes each message into MongoDB, deciding the collection and the document from the topic itself — so a new device streaming to the broker needs no new code here.

It connects with the broker URL, username, password and client ID given in the environment, and the screenshots in this repository (`devices.png`, `collections.png`, `analog.png`) show messages arriving and the documents they became.

---

This application parses the topic and message to determine the appropriate MongoDB collection and document to create or update on the fly

Here's a brief overview of how it works:

The application connects to the MQTT broker using the provided URL, username, password, and client ID.

It subscribes to all topics on the MQTT broker.

Before saving the message, it checks if a device with the same deviceId already exists in the database. If not, it creates a new device document.

When a message is received, it parses the topic to determine the collection name. If the message is a JSON object, it iterates over the keys in the object. For each key, it creates a new Mongoose model with the key as the collection name and saves the message as a document in that collection. References deviceId.

If the message is not a JSON object, it creates a new Mongoose model with the collection name derived from the topic and saves the message as a document in that collection.References deviceId.

#

I'm using Tasmota (mostly), and this is an example of the tables:

_Devices_

![Devices](./devices.png)

_Collections_

![Collections](./collections.png)

_Analog Data_

![Analog](./analog.png)

### Next steps:

Implement GraphQL to watch real-time node visualizations. Stay tuned
