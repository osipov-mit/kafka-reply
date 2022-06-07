import { Kafka } from 'kafkajs';

export class KafkaAdmin {
  kafka: Kafka;

  constructor(kafka: Kafka) {
    this.kafka = kafka;
  }

  async createTopic(topics: string[]) {
    const admin = this.kafka.admin();
    try {
      await admin.connect();
      console.log('*** Admin is connected ***');
    } catch (error) {
      console.log(error);
      console.log('Admin is not connected');
      throw error;
    }
    try {
      const createdTopics = await admin.listTopics();
      topics.push(...topics.map((topic) => `${topic}.reply`));
      for (let topic of topics) {
        if (createdTopics.includes(topic)) {
          continue;
        }
        await admin.createTopics({
          waitForLeaders: true,
          topics: [
            {
              topic,
            },
          ],
        });
        console.log(`Topic <${topic}> created`);
      }
    } catch (error) {
      console.log(error);
      await admin.disconnect();
      console.log('Admin is disconnected');
      throw error;
    }
    await admin.disconnect();
    console.log('Admin is disconnected');
  }
}
