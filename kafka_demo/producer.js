const { Kafka, Partitioners} = require('kafkajs')
const Chance = require('chance')
const chance = new Chance()

const kafka = new Kafka({
    // clientId: 'my-app',
    // brokers: ['kafka1:9092', 'kafka2:9092']
    clientId: 'my-producer',
    brokers: ['localhost:9093', 'localhost:9094', 'localhost:9095']
})

const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner })
const topic = 'animal'

const producerMessage = async () => {
    try {
        const value = chance.animal();
        console.log(value);
        await  producer.send({
            topic,
            messages: [
                {value},
            ],
        })
    } catch (error) {
        console.log(error);
    }
}

const run = async () => {
    // Producing
    await producer.connect()
    setInterval(producerMessage, 1000)
}

run().catch(console.error)