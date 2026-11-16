/**
 * Built-ins-only CJS source deliberately embedded for source, bundle, Electron and SEA.
 * This Worker alone owns FD3. It never loads fff, selects another PID, or relies on the native main loop.
 */
export const guardianSource = String.raw`
'use strict';
const { workerData: boot, parentPort } = require('node:worker_threads');
const { Socket } = require('node:net');
const die = () => { process.kill(process.pid, 'SIGKILL'); };
process.on('uncaughtException', die);
process.on('unhandledRejection', die);
const pipe = new Socket({ fd: 3, readable: true, writable: true });
let buffer = '', blocked = false, queued = 0, go = false, armed = null, highRss = 0;
const startup = setTimeout(die, 10000);
const ownKeys = (value, names) => value && typeof value === 'object' && !Array.isArray(value) &&
  Object.keys(value).length === names.length && names.every(k => Object.hasOwn(value, k));
const send = value => {
  if (++queued > 32) return die();
  const line = JSON.stringify({ ...boot, ...value }) + '\n';
  if (Buffer.byteLength(line) > 4096) return die();
  blocked = !pipe.write(line, error => { queued--; if (error) die(); });
};
const ack = (type, requestId) => send({ type, requestId });
pipe.on('error', die); pipe.on('end', die); pipe.on('close', die);
parentPort.on('close', die);
parentPort.on('message', msg => {
  if (!go || !ownKeys(msg, ['type', 'heapLimit', 'availableParallelism']) || msg.type !== 'nativeReady' ||
      !Number.isSafeInteger(msg.heapLimit) || msg.heapLimit < 268435456 || msg.heapLimit > 419430400 ||
      !Number.isSafeInteger(msg.availableParallelism) || msg.availableParallelism < 1) return die();
  send(msg);
});
pipe.on('data', chunk => {
  buffer += chunk.toString('utf8');
  if (Buffer.byteLength(buffer) > 16384) return die();
  let end;
  while ((end = buffer.indexOf('\n')) >= 0) {
    const line = buffer.slice(0, end); buffer = buffer.slice(end + 1);
    if (Buffer.byteLength(line) > 4096) return die();
    let msg; try { msg = JSON.parse(line); } catch { return die(); }
    if (!msg || !Object.keys(boot).every(k => msg[k] === boot[k])) return die();
    const fields = Object.keys(boot);
    if (msg.type === 'go') {
      if (go || !ownKeys(msg, [...fields, 'type'])) return die();
      go = true; clearTimeout(startup);
      // Covers native import/initialization before the first data request.
      armed = { id: 0, deadline: performance.now() + 10000 };
      parentPort.postMessage({ type: 'go' }); send({ type: 'go' });
    } else if (msg.type === 'loaded') {
      if (!go || !armed || armed.id !== 0 || !ownKeys(msg, [...fields, 'type'])) return die();
      armed = null; send({ type: 'loaded' });
    } else if (msg.type === 'arm') {
      if (!go || armed || !ownKeys(msg, [...fields, 'type', 'requestId', 'deadline']) ||
          !Number.isSafeInteger(msg.requestId) || msg.requestId < 1 ||
          !Number.isSafeInteger(msg.deadline) || msg.deadline <= Date.now() || msg.deadline > Date.now() + 30000) return die();
      armed = { id: msg.requestId, deadline: performance.now() + msg.deadline - Date.now() }; ack('armed', msg.requestId);
    } else if (msg.type === 'disarm') {
      if (!armed || armed.id !== msg.requestId || !ownKeys(msg, [...fields, 'type', 'requestId'])) return die();
      armed = null; ack('disarmed', msg.requestId);
    } else if (msg.type === 'credit' || msg.type === 'cancel') {
      if (!armed || armed.id !== msg.requestId || !ownKeys(msg, [...fields, 'type', 'requestId'])) return die();
      parentPort.postMessage({ type: msg.type, requestId: msg.requestId });
    } else if (msg.type === 'terminate') {
      if (!ownKeys(msg, [...fields, 'type'])) return die();
      die();
    } else return die();
  }
});
setInterval(() => {
  const now = performance.now();
  if (armed && now >= armed.deadline) die();
}, 20);
// This is an observed retirement watermark, not a hard RSS/native allocation cap.
setInterval(() => {
  const rss = process.memoryUsage.rss();
  highRss = rss > 512 * 1024 * 1024 ? highRss + 1 : 0;
  if (highRss >= 2) { send({ type: 'overBudget' }); die(); }
  if (!blocked) send({ type: 'resources', rss });
}, 1000);
pipe.on('drain', () => { blocked = false; });
send({ type: 'ready', pid: process.pid });
`;
