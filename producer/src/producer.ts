import { Consumer, Kafka, Producer } from 'kafkajs';

export class KafkaProducer {
  kafka: Kafka;
  producer: Producer;
  consumer: Consumer;
  requests: Map<string, any>;

  constructor(kafka: Kafka) {
    this.kafka = kafka;
    this.producer = this.kafka.producer();
    this.consumer = this.kafka.consumer({ groupId: 'test-reply-producer' });
    this.requests = new Map();
  }

  async run(topics: string[]) {
    await this.producer.connect();
    await this.consumer.connect();
    console.log('*** Producer is connected ***');
    this.consumer.subscribe({ topics: topics.map((topic) => `${topic}.reply`), fromBeginning: false });
    this.consumer.run({
      eachMessage: async ({ message }) => {
        const replyResolve = this.requests.get(message?.headers?.kafka_correlationId?.toString() as string);
        if (replyResolve) replyResolve(message.value?.toString());
      },
    });
  }

  async send(topic: string, message: string) {
    const correlationId = Math.random().toString(36);
    await this.producer.send({
      topic,
      messages: [
        {
          value: message,
          headers: {
            kafka_correlationId: correlationId.toString(),
            kafka_replyTopic: `${topic}.reply`,
          },
        },
      ],
    });

    let resolvePromise;
    const replyPromise = new Promise((resolve) => (resolvePromise = resolve));
    this.requests.set(correlationId, resolvePromise);
    return replyPromise;
  }
}
