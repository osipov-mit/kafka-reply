import { Kafka } from 'kafkajs';
import { KafkaAdmin } from './admin';
import config from './config';
import { KafkaProducer } from './producer';
import { listen, setRoutes } from './server';

const main = async () => {
  const kafka = new Kafka({
    clientId: config.kafka.clientId,
    brokers: config.kafka.brokers,
    sasl: {
      mechanism: 'plain',
      username: config.kafka.sasl.username,
      password: config.kafka.sasl.password,
    },
  });

  const topics = ['test.reply', 'reply.test'];
  const admin = new KafkaAdmin(kafka);
  await admin.createTopic(topics);

  const producer = new KafkaProducer(kafka);
  await producer.run(topics);

  setRoutes(producer);
  listen();
};

main().catch((error) => {
  console.log(error);
  process.exit(1);
});
