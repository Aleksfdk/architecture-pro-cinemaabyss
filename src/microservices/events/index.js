import http from 'node:http';
import { Kafka, logLevel } from 'kafkajs';

const port = Number(process.env.PORT || 8082);
let kafkaReady = false;
const brokers = (process.env.KAFKA_BROKERS || 'kafka:9092')
  .split(',')
  .map((broker) => broker.trim())
  .filter(Boolean);

const TOPICS = {
  movie: 'movie-events',
  user: 'user-events',
  payment: 'payment-events',
};

const kafka = new Kafka({
  clientId: 'events-service',
  brokers,
  logLevel: logLevel.WARN,
  retry: {
    retries: 8,
    initialRetryTime: 300,
  },
});

const producer = kafka.producer();
const consumers = [];

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

function sendJson(res, status, payload) {
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload));
}

function requireFields(body, fields) {
  const missing = fields.filter((field) => body[field] === undefined || body[field] === null || body[field] === '');
  if (missing.length > 0) {
    const error = new Error(`Missing fields: ${missing.join(', ')}`);
    error.statusCode = 400;
    throw error;
  }
}

function buildEvent(type, payload) {
  const entityId = payload.movie_id ?? payload.user_id ?? payload.payment_id ?? 'unknown';
  return {
    id: `${type}-${entityId}-${payload.action || payload.status || 'event'}-${Date.now()}`,
    type,
    timestamp: payload.timestamp || new Date().toISOString(),
    payload,
  };
}

async function publish(type, payload) {
  const topic = TOPICS[type];
  const event = buildEvent(type, payload);
  const key = String(payload.movie_id ?? payload.user_id ?? payload.payment_id ?? event.id);

  const record = await producer.send({
    topic,
    messages: [{ key, value: JSON.stringify(event) }],
  });

  const meta = record[0];
  console.log(`[events] produced ${type} event topic=${topic} partition=${meta.partition} offset=${meta.baseOffset} id=${event.id}`);

  return {
    status: 'success',
    partition: meta.partition,
    offset: Number(meta.baseOffset),
    event,
  };
}

async function handleEvent(req, res, type, requiredFields) {
  const raw = await readBody(req);
  let body;
  try {
    body = raw ? JSON.parse(raw) : {};
  } catch {
    sendJson(res, 400, { error: 'Invalid JSON' });
    return;
  }

  requireFields(body, requiredFields);
  const result = await publish(type, body);
  sendJson(res, 201, result);
}

async function startConsumer(type, topic) {
  const consumer = kafka.consumer({ groupId: `events-service-${type}` });
  consumers.push(consumer);
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const value = message.value ? message.value.toString('utf8') : '';
      console.log(`[events] consumed ${type} event topic=${topic} partition=${partition} offset=${message.offset} payload=${value}`);
    },
  });
  console.log(`[events] consumer subscribed to ${topic}`);
}

async function connectKafka() {
  for (;;) {
    try {
      await producer.connect();
      await startConsumer('movie', TOPICS.movie);
      await startConsumer('user', TOPICS.user);
      await startConsumer('payment', TOPICS.payment);
      console.log(`[events] kafka ready, brokers=${brokers.join(',')}`);
      return;
    } catch (error) {
      console.error(`[events] kafka is not ready: ${error.message}. retry in 3s`);
      await Promise.all(consumers.splice(0).map(async (consumer) => {
        try {
          await consumer.disconnect();
        } catch {
          // The broker may already have dropped the connection.
        }
      }));
      try {
        await producer.disconnect();
      } catch {
        // Producer was not connected.
      }
      await new Promise((resolve) => setTimeout(resolve, 3000));
    }
  }
}

const server = http.createServer(async (req, res) => {
  try {
    const pathname = new URL(req.url, 'http://events').pathname;

    if (req.method === 'GET' && pathname === '/api/events/health') {
      if (!kafkaReady) {
        sendJson(res, 503, { status: false });
        return;
      }
      sendJson(res, 200, { status: true });
      return;
    }

    if (!kafkaReady && req.method === 'POST' && pathname.startsWith('/api/events/')) {
      sendJson(res, 503, { error: 'Kafka is not ready' });
      return;
    }

    if (req.method === 'POST' && pathname === '/api/events/movie') {
      await handleEvent(req, res, 'movie', ['movie_id', 'title', 'action']);
      return;
    }

    if (req.method === 'POST' && pathname === '/api/events/user') {
      await handleEvent(req, res, 'user', ['user_id', 'action', 'timestamp']);
      return;
    }

    if (req.method === 'POST' && pathname === '/api/events/payment') {
      await handleEvent(req, res, 'payment', ['payment_id', 'user_id', 'amount', 'status', 'timestamp']);
      return;
    }

    sendJson(res, 404, { error: 'Not found' });
  } catch (error) {
    console.error('[events] request failed', error);
    if (!res.headersSent) {
      sendJson(res, error.statusCode || 500, { error: error.message || 'Internal Server Error' });
    }
  }
});

server.listen(port, () => {
  console.log(`[events] listening on ${port}`);
});

connectKafka().then(() => {
  kafkaReady = true;
  console.log('[events] kafka consumers are ready');
});
