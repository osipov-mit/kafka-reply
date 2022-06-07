import { Consumer, Kafka, KafkaMessage, Producer } from 'kafkajs';

export class KafkaConsumer {
  kafka: Kafka;
  producer: Producer;
  consumer: Consumer;

  constructor(kafka: Kafka) {
    this.kafka = kafka;
    this.producer = this.kafka.producer();
    this.consumer = this.kafka.consumer({ groupId: 'test-reply-consumer' });
  }

  async run(topics: string[]) {
    await this.consumer.connect();
    await this.producer.connect();
    this.consumer.on('consumer.connect', () => console.log('consumer.connected'));
    console.log('*** Consumer is connected ***');
    this.consumer.subscribe({ topics, fromBeginning: false });
    this.consumer.run({
      eachMessage: async ({ message }) => {
        this.sendReply(message);
      },
    });
  }

  async sendReply({ value, headers }: KafkaMessage) {
    await this.producer.send({
      topic: headers?.kafka_replyTopic?.toString() as string,
      messages: [
        {
          value: `REPLY: ${value?.toString()}`,
          headers: {
            kafka_correlationId: headers?.kafka_correlationId,
          },
        },
      ],
    });
  }
}
