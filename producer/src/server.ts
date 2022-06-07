import express, { json } from 'express';
import { KafkaProducer } from './producer';

const app = express();
app.use(json());

export function setRoutes(producer: KafkaProducer) {
  app.post('/', async (req, res) => {
    res.json(await producer.send(req.body.topic, req.body.message));
  });
}

export function listen() {
  app.listen(3000, () => console.log(`Server is running on port 3000`));
}
