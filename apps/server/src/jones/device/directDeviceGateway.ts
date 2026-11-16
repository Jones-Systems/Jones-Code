import {
  DEVICE_HUB_POLICY,
  deviceHubRoutePolicy,
  deviceHubForwardHeaders,
  stripDeviceHubQuery,
  validDirectClientOrigin,
} from "./directMediaPolicy.ts";

export interface DirectDeviceGatewayConfig {
  readonly hostId: string;
  readonly owner: string;
  readonly generation: string;
  readonly hubPort: number;
  readonly hubEntry: string;
  readonly admissionPort: number;
}

/** Runs on the device host with only opaque device grants; the upstream and callback are fixed loopback ports. */
export const directDeviceGatewaySource =
  `
const policy = ${JSON.stringify(DEVICE_HUB_POLICY)};
const routePolicy = ${deviceHubRoutePolicy.toString()};
const forwardHeaders = ${deviceHubForwardHeaders.toString()};
const stripQuery = ${stripDeviceHubQuery.toString()};
const validOrigin = ${validDirectClientOrigin.toString()};
` +
  String.raw`
const http = require('node:http');
const { createRequire } = require('node:module');
const { WebSocket, WebSocketServer } = createRequire(config.hubEntry)('ws');
const hubOrigin = 'http://127.0.0.1:' + config.hubPort;
const callback = 'http://127.0.0.1:' + config.admissionPort + '/api/device-hub/direct-admission';
const prefix = '/api/device-hub';
const wss = new WebSocketServer({ noServer: true, perMessageDeflate: false, maxPayload: 1024 * 1024 });
const active = new Set();
const pending = new Set();
const closing = new Set();
const sockets = new Set();
let shuttingDown = false;
function reject(socket, status, origin) {
  const cors = origin ? 'Access-Control-Allow-Origin: ' + origin + '\r\nVary: Origin\r\n' : '';
  socket.end('HTTP/1.1 ' + status + ' Denied\r\n' + cors + 'Cache-Control: no-store\r\nConnection: close\r\nContent-Length: 0\r\n\r\n');
}
function parse(req, upgrade) {
  if (typeof req.url !== 'string' || req.url.length > 8192) return null;
  const url = new URL(req.url, 'http://127.0.0.1');
  if (!url.pathname.startsWith(prefix + '/')) return null;
  const hints = url.searchParams.getAll('clientOrigin');
  if (hints.length > 1) return null;
  const origin = req.headers.origin ?? hints[0];
  if (typeof origin !== 'string' || !validOrigin(origin) || (hints.length && hints[0] !== origin)) return null;
  const path = url.pathname.slice(prefix.length);
  const grant = url.searchParams.get('grant');
  if (!grant || grant.length > 256 || url.searchParams.getAll('grant').length !== 1) return { status: 403, origin };
  const method = req.method === 'OPTIONS' ? req.headers['access-control-request-method'] : req.method;
  if (typeof method !== 'string') return { status: 403, origin };
  const rule = path === '/readyz' && method === 'GET' && !upgrade ? 'read' : routePolicy(policy, path, method, upgrade);
  if (typeof rule === 'number') return { status: rule, origin };
  return { grant, hostId: config.hostId, generation: config.generation, path, search: url.search, method, upgrade, origin };
}
async function admit(input, signal) {
  const controller = new AbortController();
  const abort = () => controller.abort();
  signal.addEventListener('abort', abort, { once: true });
  if (signal.aborted || shuttingDown) controller.abort();
  pending.add(controller);
  const timeout = setTimeout(abort, 5000);
  try {
    const response = await fetch(callback, { method: 'POST', redirect: 'error', headers: { 'content-type': 'application/json' },
      body: JSON.stringify(input), signal: controller.signal });
    if (response.status === 403) { await response.body?.cancel(); return { kind: 'Denied' }; }
    if (response.status !== 200 || !response.body || !/^application\/json(?:\s*;.*)?$/i.test(response.headers.get('content-type') ?? '')) {
      await response.body?.cancel(); return { kind: 'Unavailable' };
    }
    const reader = response.body.getReader();
    let size = 0;
    const chunks = [];
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        size += value.length;
        if (size > 2048) { await reader.cancel(); return { kind: 'Unavailable' }; }
        chunks.push(Buffer.from(value));
      }
    } finally { reader.releaseLock(); }
    const verdict = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    if (verdict.allowed !== true || verdict.owner !== config.owner || verdict.generation !== config.generation ||
      verdict.origin !== input.origin || !Number.isSafeInteger(verdict.expiresAt)) return { kind: 'Unavailable' };
    if (verdict.expiresAt <= Date.now()) return { kind: 'Denied' };
    return { kind: 'Allowed', expiresAt: verdict.expiresAt };
  } catch { return { kind: 'Unavailable' }; }
  finally { clearTimeout(timeout); pending.delete(controller); signal.removeEventListener('abort', abort); }
}
function monitor(input, verdict, close) {
  let stopped = false;
  let checking = false;
  const controller = new AbortController();
  const cleanup = () => {
    if (stopped) return;
    stopped = true;
    controller.abort();
    clearInterval(interval);
    clearTimeout(expiration);
    active.delete(cancel);
  };
  const cancel = kind => { if (stopped) return; cleanup(); close(kind ?? 'Unavailable'); };
  const interval = setInterval(async () => {
    if (stopped || checking) return;
    checking = true;
    const valid = await admit(input, controller.signal);
    checking = false;
    if (!stopped && valid.kind !== 'Allowed') cancel(valid.kind);
  }, 30000);
  const expiration = setTimeout(() => cancel('Denied'), Math.max(0, verdict.expiresAt - Date.now()));
  active.add(cancel);
  return cleanup;
}
function cors(response, origin) {
  if (origin) { response.setHeader('access-control-allow-origin', origin); response.setHeader('vary', 'Origin'); }
  response.setHeader('cache-control', 'no-store, no-transform');
}
function closeClient(client, code) {
  if (!client) return;
  const terminate = () => { clearTimeout(deadline); closing.delete(terminate); client.terminate(); };
  const deadline = setTimeout(terminate, 1000);
  closing.add(terminate);
  client.once('close', () => { clearTimeout(deadline); closing.delete(terminate); });
  client.once('error', terminate);
  try { client.close(code); } catch { terminate(); }
}
const server = http.createServer(async (req, res) => {
  if (shuttingDown) { res.writeHead(503); return res.end(); }
  if (req.url === '/readyz' && req.method === 'GET') {
    res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    return res.end(JSON.stringify({ owner: config.owner, generation: config.generation }));
  }
  let input;
  try { input = parse(req, false); } catch {}
  cors(res, input?.origin);
  if (!input || input.status) { res.writeHead(input?.status ?? 403); return res.end(); }
  const controller = new AbortController();
  const abort = () => controller.abort();
  res.once('close', abort);
  req.once('aborted', abort);
  const verdict = await admit(input, controller.signal);
  if (shuttingDown || controller.signal.aborted || res.destroyed) return;
  if (verdict.kind !== 'Allowed') { res.writeHead(verdict.kind === 'Denied' ? 403 : 503); return res.end(); }
  if (req.method === 'OPTIONS') {
    const requestedHeaders = String(req.headers['access-control-request-headers'] ?? '').toLowerCase().split(',').map(value => value.trim()).filter(Boolean);
    if (requestedHeaders.some(name => name !== 'content-type')) { res.writeHead(403); return res.end(); }
    res.setHeader('access-control-allow-methods', input.method);
    if (requestedHeaders.length) res.setHeader('access-control-allow-headers', 'content-type');
    if (req.headers['access-control-request-private-network'] === 'true') res.setHeader('access-control-allow-private-network', 'true');
    res.writeHead(204); return res.end();
  }
  if (input.path === '/readyz') {
    res.setHeader('content-type', 'application/json');
    return res.end(JSON.stringify({ owner: config.owner, generation: config.generation }));
  }
  const upstream = http.request(hubOrigin + input.path + stripQuery(input.search), {
    method: input.method, headers: forwardHeaders(policy, req.headers, hubOrigin),
  });
  let body;
  const cleanup = monitor(input, verdict, () => { upstream.destroy(); body?.destroy(); res.destroy(); });
  res.once('close', () => { cleanup(); upstream.destroy(); body?.destroy(); });
  req.once('error', () => { cleanup(); upstream.destroy(); res.destroy(); });
  upstream.once('response', response => {
    if (res.destroyed || shuttingDown) return response.destroy();
    body = response;
    const connection = String(response.headers.connection ?? '').toLowerCase().split(',').map(name => name.trim());
    for (const [name, value] of Object.entries(response.headers)) {
      if (value === undefined || name.startsWith('access-control-') || connection.includes(name) ||
        ['set-cookie', 'location', 'connection', 'transfer-encoding', 'content-encoding', 'keep-alive', 'trailer', 'upgrade'].includes(name)) continue;
      res.setHeader(name, value);
    }
    cors(res, input.origin);
    res.writeHead(response.statusCode ?? 502);
    response.once('error', () => res.destroy());
    response.pipe(res);
  });
  upstream.once('error', () => { cleanup(); if (res.destroyed) return; if (!res.headersSent) res.writeHead(502); res.end(); });
  req.pipe(upstream);
});
server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
server.on('upgrade', async (req, socket, head) => {
  let input;
  try { input = parse(req, true); } catch {}
  if (!input || input.status) return reject(socket, input?.status ?? 403, input?.origin);
  const controller = new AbortController();
  socket.once('close', () => controller.abort());
  const verdict = await admit(input, controller.signal);
  if (shuttingDown || controller.signal.aborted || socket.destroyed) return;
  if (verdict.kind === 'Unavailable') return reject(socket, 503, input.origin);
  if (verdict.kind === 'Denied') return wss.handleUpgrade(req, socket, head, client => closeClient(client, 1008));
  const upstream = new WebSocket(hubOrigin.replace(/^http/, 'ws') + input.path + stripQuery(input.search), {
    headers: forwardHeaders(policy, req.headers, hubOrigin), handshakeTimeout: 10000, perMessageDeflate: false, maxPayload: 16 * 1024 * 1024,
  });
  let client;
  let relaying = true;
  const close = kind => {
    if (!relaying) return;
    relaying = false;
    upstream.terminate();
    if (client) closeClient(client, kind === 'Denied' ? 1008 : 1013);
    else if (kind === 'Denied' && !socket.destroyed && !shuttingDown) wss.handleUpgrade(req, socket, head, connected => closeClient(connected, 1008));
    else if (!socket.destroyed) reject(socket, 503, input.origin);
  };
  const cleanup = monitor(input, verdict, close);
  socket.once('close', () => { relaying = false; cleanup(); upstream.terminate(); });
  upstream.once('error', () => { cleanup(); close('Unavailable'); });
  upstream.once('close', () => { cleanup(); close('Unavailable'); });
  upstream.once('open', () => {
    if (!relaying || socket.destroyed || shuttingDown) { cleanup(); return upstream.terminate(); }
    wss.handleUpgrade(req, socket, head, connected => {
      client = connected;
      client.once('close', () => { relaying = false; cleanup(); upstream.terminate(); });
      client.once('error', () => { cleanup(); close('Unavailable'); });
      // Stop relaying before the bounded close handshake; queued input is never replayed.
      const relay = (target, data, binary) => {
        if (!relaying) return;
        if (target.readyState !== WebSocket.OPEN || target.bufferedAmount > 16 * 1024 * 1024) { cleanup(); return close('Unavailable'); }
        target.send(data, { binary }, error => { if (error) { cleanup(); close('Unavailable'); } });
      };
      upstream.on('message', (data, binary) => relay(client, data, binary));
      client.on('message', (data, binary) => relay(upstream, data, binary));
    });
  });
});
function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  for (const controller of pending) controller.abort();
  for (const close of [...active]) close('Unavailable');
  for (const terminate of [...closing]) terminate();
  for (const socket of sockets) socket.destroy();
  wss.close();
  server.close(() => process.exit(0));
}
process.once('SIGTERM', shutdown);
process.once('SIGINT', shutdown);
server.listen(0, '127.0.0.1', () => {
  process.send?.({ port: server.address().port });
  process.disconnect?.();
});
`;

export const directDeviceGatewayScript = (config: DirectDeviceGatewayConfig) =>
  `const config = ${JSON.stringify(config)};\n${directDeviceGatewaySource}`;
