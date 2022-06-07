import { Kafka } from 'kafkajs';
import config from './config';
import { KafkaConsumer } from './consumer';

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

  const consumer = new KafkaConsumer(kafka);
  await consumer.run(topics);
};

main().catch((error) => {
  console.log(error);
  process.exit(1);
});
