import http from 'node:http';

const port = Number(process.env.PORT || 8000);
const monolithUrl = process.env.MONOLITH_URL || 'http://monolith:8080';
const moviesUrl = process.env.MOVIES_SERVICE_URL || 'http://movies-service:8081';
const eventsUrl = process.env.EVENTS_SERVICE_URL || 'http://events-service:8082';

function migrationPercent() {
  const value = Number(process.env.MOVIES_MIGRATION_PERCENT ?? 0);
  if (Number.isNaN(value)) {
    return 0;
  }
  return Math.min(100, Math.max(0, value));
}

function gradualMigrationEnabled() {
  return String(process.env.GRADUAL_MIGRATION).toLowerCase() === 'true';
}

// Strangler Fig: while the flag is on, only a percentage of movie traffic
// leaves the monolith. When the flag is off, the movies domain is fully cut over.
function routeMovies() {
  if (!gradualMigrationEnabled()) {
    return { name: 'movies-service', base: moviesUrl, reason: 'flag off, full cutover' };
  }

  const percent = migrationPercent();
  const roll = Math.random() * 100;
  if (roll < percent) {
    return { name: 'movies-service', base: moviesUrl, reason: `roll ${roll.toFixed(1)} < ${percent}` };
  }
  return { name: 'monolith', base: monolithUrl, reason: `roll ${roll.toFixed(1)} >= ${percent}` };
}

function resolveUpstream(pathname) {
  if (pathname === '/api/movies/health') {
    return { name: 'movies-service', base: moviesUrl, reason: 'movies health' };
  }

  if (pathname === '/api/movies' || pathname.startsWith('/api/movies/')) {
    return routeMovies();
  }

  if (pathname === '/api/events' || pathname.startsWith('/api/events/')) {
    return { name: 'events-service', base: eventsUrl, reason: 'events domain' };
  }

  if (pathname.startsWith('/api/')) {
    return { name: 'monolith', base: monolithUrl, reason: 'not migrated' };
  }

  return null;
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

async function forward(req, res, upstream) {
  const target = new URL(req.url, upstream.base);
  const hasBody = req.method !== 'GET' && req.method !== 'HEAD';
  const body = hasBody ? await readBody(req) : undefined;
  const headers = {};
  if (req.headers['content-type']) {
    headers['content-type'] = req.headers['content-type'];
  }
  if (req.headers.accept) {
    headers.accept = req.headers.accept;
  }

  console.log(`[proxy] ${req.method} ${req.url} -> ${upstream.name} (${upstream.reason})`);

  const upstreamResponse = await fetch(target, {
    method: req.method,
    headers,
    body: body && body.length > 0 ? body : undefined,
  });

  const payload = Buffer.from(await upstreamResponse.arrayBuffer());
  const responseHeaders = {
    'x-upstream': upstream.name,
  };
  const contentType = upstreamResponse.headers.get('content-type');
  if (contentType) {
    responseHeaders['content-type'] = contentType;
  }

  res.writeHead(upstreamResponse.status, responseHeaders);
  res.end(payload);
}

const server = http.createServer(async (req, res) => {
  try {
    const pathname = new URL(req.url, 'http://proxy').pathname;

    if (req.method === 'GET' && pathname === '/health') {
      res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
      res.end('Strangler Fig Proxy is healthy');
      return;
    }

    const upstream = resolveUpstream(pathname);
    if (!upstream) {
      res.writeHead(404, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: 'Not found' }));
      return;
    }

    await forward(req, res, upstream);
  } catch (error) {
    console.error('[proxy] upstream error', error);
    if (!res.headersSent) {
      res.writeHead(502, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: 'Bad gateway' }));
    }
  }
});

server.listen(port, () => {
  console.log(
    `[proxy] listening on ${port}; gradual=${gradualMigrationEnabled()}; movies=${migrationPercent()}%`,
  );
});
