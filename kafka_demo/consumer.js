const { Kafka } = require('kafkajs')

const kafka = new Kafka({
    clientId: 'my-consumer',
    brokers: ['localhost:9093', 'localhost:9094', 'localhost:9095']

})

const consumer = kafka.consumer({ groupId: 'test-group' })
const topic = 'animal'

const run = async () => {

    // Consuming
    await consumer.connect()
    await consumer.subscribe({topic})
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                partition,
                offset: message.offset,
                value: message.value.toString(),
            })
        },
    })
}

run().catch(console.error)