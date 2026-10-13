/** Emitted definitions only. A cold permit issuer and native identity qualification are not installed by this module. */
export const jonesColdBackendCustodySource = String.raw`
import * as childProcess from 'node:child_process';
import * as crypto from 'node:crypto';

const coldMessageTypes = new Set(['READY', 'DISPATCH_INTENT', 'BACKEND_CAPTURED', 'BACKEND_SETTLED', 'STOP_REQUEST', 'STOP_RESULT', 'TERMINAL_CLOSED']);
const coldBindingKeys = ['transactionId', 'transferId', 'handoffSha256', 'sourceSha', 'sourceTree', 'artifactSha256', 'bootSessionUuid'];
const coldPermits = new WeakMap();
const coldLimit = 16384;
const coldOutputLimit = 1024 * 1024;

function coldRequire(condition) {
  if (!condition) throw new Error('Cold custody binding or lifetime requires reconciliation.');
}
function coldObject(value, keys) {
  coldRequire(value !== null && typeof value === 'object' && !Array.isArray(value));
  coldRequire(Object.keys(value).sort().join(',') === [...keys].sort().join(','));
}
function coldBinding(value) {
  coldObject(value, coldBindingKeys);
  for (const key of coldBindingKeys) {
    const length = key === 'sourceSha' || key === 'sourceTree' ? 40 : 64;
    coldRequire(typeof value[key] === 'string');
    coldRequire(key === 'bootSessionUuid' ? /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value[key]) : new RegExp('^[0-9a-f]{' + length + '}$').test(value[key]));
  }
  coldRequire(value.transactionId !== value.transferId);
  return Object.freeze(Object.fromEntries(coldBindingKeys.map(key => [key, value[key]])));
}
function coldIdentity(value) {
  coldObject(value, ['pid', 'parentPid', 'uniqueId', 'parentUniqueId', 'pidVersion', 'uid']);
  for (const key of ['pid', 'parentPid', 'pidVersion', 'uid']) coldRequire(Number.isSafeInteger(value[key]) && value[key] >= (key === 'uid' ? 0 : 1));
  for (const key of ['uniqueId', 'parentUniqueId']) coldRequire(typeof value[key] === 'string' && /^[1-9][0-9]{0,19}$/.test(value[key]) && BigInt(value[key]) <= 18446744073709551615n);
  return Object.freeze({...value});
}

class ColdChannel {
  #stream; #binding; #sent = 0; #received = 0; #buffer = Buffer.alloc(0); #queue = []; #waiter; #failure; #ended = false; #reading = false;
  constructor(stream, binding) {
    this.#stream = stream;
    this.#binding = coldBinding(binding);
    stream.on('data', chunk => {
      if (this.#failure) return;
      try {
        coldRequire(!this.#ended && Buffer.isBuffer(chunk));
        this.#buffer = Buffer.concat([this.#buffer, chunk]);
        coldRequire(this.#buffer.length <= coldLimit);
        let newline;
        while ((newline = this.#buffer.indexOf(10)) >= 0) {
          const line = this.#buffer.subarray(0, newline);
          this.#buffer = this.#buffer.subarray(newline + 1);
          const value = JSON.parse(line.toString('utf8'));
          coldObject(value, ['protocol', 'binding', 'sequence', 'type', 'identity']);
          coldRequire(value.protocol === 1 && Number.isSafeInteger(value.sequence) && value.sequence === this.#received + 1);
          coldRequire(coldMessageTypes.has(value.type));
          const binding = coldBinding(value.binding);
          coldRequire(coldBindingKeys.every(key => binding[key] === this.#binding[key]));
          if (value.type === 'READY' || value.type === 'BACKEND_CAPTURED') coldIdentity(value.identity);
          else coldRequire(value.identity === null);
          this.#received = value.sequence;
          coldRequire(this.#queue.length < 16);
          this.#queue.push(Object.freeze(value));
        }
      } catch (cause) { this.#fail(cause); }
      this.#wake();
    });
    stream.on('error', cause => this.#fail(cause));
    stream.on('end', () => { this.#ended = true; if (this.#buffer.length) this.#fail(new Error('Truncated cold channel.')); this.#wake(); });
    stream.on('close', () => { this.#ended = true; this.#wake(); });
  }
  #fail(cause) { this.#failure ??= cause; this.#wake(); }
  #wake() { const wake = this.#waiter; this.#waiter = undefined; wake?.(); }
  async receive(expected, timeoutMs = 5000) {
    coldRequire(Array.isArray(expected) && expected.length > 0 && expected.every(type => coldMessageTypes.has(type)));
    coldRequire(Number.isSafeInteger(timeoutMs) && timeoutMs > 0 && timeoutMs <= 30000 && !this.#reading);
    this.#reading = true;
    const deadline = Date.now() + timeoutMs;
    try {
      while (!this.#queue.length && !this.#failure && !this.#ended) {
        const remaining = deadline - Date.now();
        coldRequire(remaining > 0);
        await new Promise(resolve => {
          const timer = setTimeout(() => { this.#waiter = undefined; resolve(); }, remaining);
          this.#waiter = () => { clearTimeout(timer); resolve(); };
        });
      }
      if (this.#failure) throw this.#failure;
      coldRequire(this.#queue.length > 0);
      const frame = this.#queue.shift();
      coldRequire(expected.includes(frame.type));
      return frame;
    } catch (cause) { this.#fail(cause); throw cause; }
    finally { this.#reading = false; }
  }
  async send(type, identity = null) {
    try {
      coldRequire(!this.#failure && !this.#ended && coldMessageTypes.has(type));
      if (type === 'READY' || type === 'BACKEND_CAPTURED') coldIdentity(identity);
      else coldRequire(identity === null);
      coldRequire(this.#sent < Number.MAX_SAFE_INTEGER);
      const bytes = Buffer.from(JSON.stringify({protocol: 1, binding: this.#binding, sequence: ++this.#sent, type, identity}) + '\n');
      coldRequire(bytes.length <= coldLimit);
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('Cold channel write deadline expired.')), 5000);
        this.#stream.write(bytes, error => { clearTimeout(timer); error ? reject(error) : resolve(); });
      });
    } catch (cause) { this.#fail(cause); throw cause; }
  }
  get failed() { return this.#failure !== undefined; }
}

class CapturedColdBackend {
  #child; #streams = []; #exit; #closed = false; #spawned = false; #spawnUnknown = true; #failure;
  #channel; #settling; #outputBytes = 0; #events = []; #abortSignal; #abortListener; #readySeen = false; #stopRequestedAt;
  constructor(command, binding, signal) {
    this.binding = coldBinding(binding);
    this.dispatchId = crypto.randomUUID();
    this.#events.push('DISPATCH_INTENT');
    coldRequire(!signal?.aborted);
    // Only the internal permit resolver may provide a production command. No spawner or PID can be supplied.
    try {
      this.#child = childProcess.spawn(command.executable, command.args, {
        cwd: command.cwd, env: command.env, detached: false,
        stdio: ['ignore', 'pipe', 'pipe', 'pipe', 'pipe', 'pipe', 'pipe', 'pipe', 'pipe'],
      });
    } catch (cause) {
      this.#failure = cause;
      return;
    }
    const child = this.#child;
    child.once('spawn', () => { this.#spawned = true; this.#spawnUnknown = false; this.#events.push('BACKEND_CAPTURED'); });
    child.on('error', cause => { this.#failure ??= cause; });
    child.once('exit', (code, signal) => { this.#exit = {code, signal}; this.#events.push('EXIT_OBSERVED'); });
    child.once('close', () => { this.#closed = true; this.#events.push('CLOSE_OBSERVED'); this.#detachAbort(); });
    for (let fd = 1; fd <= 8; fd++) {
      const stream = child.stdio[fd];
      if (stream === null || typeof stream?.on !== 'function') { this.#failure ??= new Error('Cold child pipe was not captured.'); this.#signal(); continue; }
      const owned = {fd, stream, closed: false};
      this.#streams.push(owned);
      stream.once('close', () => { owned.closed = true; });
      stream.on('error', cause => { this.#failure ??= cause; });
      if (fd !== 8) stream.on('data', chunk => {
        this.#outputBytes += chunk.length;
        if (this.#outputBytes > coldOutputLimit) { this.#failure ??= new Error('Cold child output bound exceeded.'); this.#signal(); }
      });
    }
    // Reserve the desktop FD3–7 slots; this held unit does not install their bootstrap/telemetry/browser consumers. FD8 is custody only.
    if (child.stdio[8]) this.#channel = new ColdChannel(child.stdio[8], binding);
    if (signal) {
      this.#abortSignal = signal;
      this.#abortListener = () => { this.#failure ??= new Error('Cold child cancelled.'); this.#stopRequestedAt ??= Date.now(); this.#signal(); };
      signal.addEventListener('abort', this.#abortListener, {once: true});
      if (signal.aborted) this.#abortListener();
    }
  }
  #detachAbort() { if (this.#abortListener) this.#abortSignal.removeEventListener('abort', this.#abortListener); }
  #signal(kind = 'SIGTERM') {
    if (this.#child && this.#exit === undefined) {
      try { if (!this.#child.kill(kind)) this.#failure ??= new Error('Captured cold child stop was not confirmed.'); }
      catch (cause) { this.#failure ??= cause; }
    }
  }
  async ready() {
    try {
      coldRequire(this.#child !== undefined && this.#channel !== undefined && !this.#failure && !this.#readySeen);
      const ready = await this.#channel.receive(['READY']);
      coldRequire(this.#spawned && ready.identity.pid === this.#child.pid && ready.identity.parentPid === process.pid);
      // Identity decoding does not qualify native identity. Unit2 must compare a native observer issued in this live scope.
      this.#readySeen = true;
      return ready;
    } catch (cause) { this.#failure ??= cause; throw cause; }
  }
  async stop() {
    this.#events.push('STOP_REQUEST');
    this.#stopRequestedAt ??= Date.now();
    this.#signal();
    return this.wait();
  }
  wait() {
    this.#settling ??= this.#finish();
    return this.#settling;
  }
  async #finish() {
    const deadline = Date.now() + 3000;
    while (!this.#closed && this.#child && Date.now() < deadline && (this.#stopRequestedAt === undefined || Date.now() < this.#stopRequestedAt + 100)) await new Promise(resolve => setTimeout(resolve, 5));
    if (!this.#closed && this.#child) {
      this.#failure ??= new Error('Cold child closure was not observed.');
      // Stop only this captured object. A timeout never becomes closure evidence.
      this.#signal();
      const grace = Date.now() + 100;
      while (!this.#closed && Date.now() < grace) await new Promise(resolve => setTimeout(resolve, 5));
      if (!this.#closed) this.#signal('SIGKILL');
      const drain = Date.now() + 1000;
      while (!this.#closed && Date.now() < drain) await new Promise(resolve => setTimeout(resolve, 5));
    }
    const pipesClosed = this.#streams.length === 8 && this.#streams.every(item => item.closed);
    const exitStatus = this.#exit === undefined ? null : Object.freeze({...this.#exit});
    const abnormalExit = exitStatus === null || exitStatus.code !== 0 || exitStatus.signal !== null;
    const settled = this.#spawned && !this.#spawnUnknown && this.#exit !== undefined && this.#closed && pipesClosed;
    if (settled) this.#events.push('BACKEND_SETTLED', 'TERMINAL_CLOSED');
    return Object.freeze({dispatchId: this.dispatchId, spawned: this.#spawned, spawnUnknown: this.#spawnUnknown,
      exitStatus, abnormalExit, stopRequested: this.#stopRequestedAt !== undefined,
      exitObserved: this.#exit !== undefined, closeObserved: this.#closed, pipesClosed, settled,
      held: true, lifecycleFailed: !settled || abnormalExit || !this.#readySeen || this.#failure !== undefined || this.#channel?.failed === true,
      events: Object.freeze([...this.#events]), nativeIdentityQualified: false, chromiumWritersQualified: false,
      stateAdmissionAllowed: false, activationAllowed: false, restorationAllowed: false});
  }
}

function startJonesColdBackend(permit, signal) {
  // Unit2 must add the private, source-validated issuer. Serialized objects and qualification reports cannot issue permits.
  const command = permit !== null && typeof permit === 'object' ? coldPermits.get(permit) : undefined;
  coldRequire(command !== undefined);
  coldPermits.delete(permit);
  return new CapturedColdBackend(command, command.binding, signal);
}
`;
