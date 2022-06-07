import { config } from 'dotenv';
config();

import { strict as assert } from 'assert';

const checkEnv = (envName: string): string => {
  const env = process.env[envName];
  assert.notStrictEqual(env, undefined, `${envName} is not specified`);
  return env as string;
};

export default {
  kafka: {
    clientId: checkEnv('KAFKA_CLIENT_ID'),
    brokers: checkEnv('KAFKA_BROKERS').split(','),
    sasl: {
      username: checkEnv('KAFKA_SASL_USERNAME'),
      password: checkEnv('KAFKA_SASL_PASSWORD'),
    },
  },
};
