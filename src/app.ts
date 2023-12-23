import mongoose from 'mongoose'
import mqtt, { MqttClient } from 'mqtt'
import 'dotenv/config'

const {
  NODE_ENV,
  MQTT_URL,
  MQTT_USERNAME,
  MQTT_PASSWORD,
  MQTT_CLIENT,
  MONGODB_URL,
  MONGODB_DB_DATABASE,
  MONGODB_DB_MAX_CONNECT,
  MONGODB_HOST_LOCAL,
  MONGODB_HOST_PROD
} = process.env

interface ErrorWithCode {
  code: number
}

const serverSelectionTimeoutMS: number = MONGODB_DB_MAX_CONNECT ? parseInt(MONGODB_DB_MAX_CONNECT, 10) : 50000

const mongodbURL = (MONGODB_URL || 'default_url').replace(
  '{HOST}',
  NODE_ENV === 'local' ? MONGODB_HOST_LOCAL || 'localhost' : MONGODB_HOST_PROD || 'productionhost'
)
mongoose.connect(mongodbURL as string, {
  dbName: MONGODB_DB_DATABASE,
  serverSelectionTimeoutMS
})

mongoose.pluralize(function (name) {
  return name
})

const messageSchema = new mongoose.Schema(
  {
    topic: String,
    levels: [String],
    message: mongoose.Schema.Types.Mixed,
    deviceId: { type: mongoose.Schema.Types.ObjectId, ref: 'Device' }
  },
  {
    timestamps: true, // This will add createdAt and updatedAt fields
    pluralize: null // This will prevent Mongoose from pluralizing the model name
  }
)

messageSchema.index({ deviceId: 1 })

const deviceSchema = new mongoose.Schema(
  {
    deviceName: { type: String, unique: true }
  },
  {
    timestamps: true, // This will add createdAt and updatedAt fields
    pluralize: null // This will prevent Mongoose from pluralizing the model name
  }
)

const Device = mongoose.model('Device', deviceSchema, 'device')

interface MessageObject {
  [key: string]: MessageObject
}

const connectMqtt = (): Promise<MqttClient> => {
  return new Promise((resolve, reject) => {
    const mqttClient: MqttClient = mqtt.connect(MQTT_URL as string, {
      clean: true,
      username: MQTT_USERNAME,
      password: MQTT_PASSWORD,
      clientId: MQTT_CLIENT
    })

    mqttClient.on('connect', () => {
      resolve(mqttClient)
    })

    mqttClient.on('error', (err) => {
      reject(err)
    })
  })
}

const main = async () => {
  const mqttClient = await connectMqtt()

  mqttClient.subscribe('#')

  mqttClient.on('message', async (topic, message) => {
    const collectionName = topic.split('/')[0]
    const levels = topic.split('/')

    let messageToSave: string | MessageObject
    try {
      messageToSave = JSON.parse(message.toString())
    } catch (error) {
      messageToSave = message.toString()
    }

    let device = await Device.findOne({ deviceName: collectionName })

    if (!device) {
      device = new Device({
        deviceName: collectionName,
        levels
      })

      try {
        await device.save()
      } catch (error) {
        if ((error as ErrorWithCode).code === 11000) {
          // Check if the error is a duplicate key error
          console.log('Duplicate deviceId. Message not saved.')
          return
        }
        console.error('Error inserting device:', error)
      }
    }

    if (typeof messageToSave === 'object') {
      for (const key in messageToSave) {
        const Message = mongoose.model(key, messageSchema)

        const newMessage = new Message({
          topic: `${collectionName}/${key}`,
          message: (messageToSave as MessageObject)[key], // Type assertion
          deviceId: device._id
        })

        newMessage
          .save()
          .then(() => {
            NODE_ENV === 'local' && console.log('Message inserted successfully')
          })
          .catch((err) => {
            console.error('Error inserting message:', err)
          })
      }
    } else {
      const Message = mongoose.model(collectionName, messageSchema)

      const newMessage = new Message({
        topic: topic.toString(),
        levels,
        message: messageToSave,
        deviceId: device._id
      })

      newMessage
        .save()
        .then(() => {
          NODE_ENV === 'local' && console.log('Message inserted successfully')
        })
        .catch((err) => {
          console.error('Error inserting message:', err)
        })
    }
  })
}

main().catch((err) => {
  console.error('Error in main:', err)
})
