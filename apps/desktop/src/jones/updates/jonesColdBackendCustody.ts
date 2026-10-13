// @effect-diagnostics nodeBuiltinImport:off -- This Jones-owned boundary retains the actual Node spawn object and its pipes.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeProcess from "node:process";
import * as NodeStream from "node:stream";
import * as NodeTimers from "node:timers";
import * as NodePerfHooks from "node:perf_hooks";

const coldPermitBrand: unique symbol = Symbol("JonesColdBackendPermit");
export interface JonesColdBackendPermit {
  readonly [coldPermitBrand]: true;
}
type ColdMessageType =
  | "READY"
  | "DISPATCH_INTENT"
  | "BACKEND_CAPTURED"
  | "BACKEND_SETTLED"
  | "STOP_REQUEST"
  | "STOP_RESULT"
  | "TERMINAL_CLOSED";
type ColdBindingKey =
  | "transactionId"
  | "transferId"
  | "handoffSha256"
  | "sourceSha"
  | "sourceTree"
  | "artifactSha256"
  | "bootSessionUuid";
type ColdBinding = Readonly<Record<ColdBindingKey, string>>;
interface ColdIdentity {
  readonly pid: number;
  readonly parentPid: number;
  readonly uniqueId: string;
  readonly parentUniqueId: string;
  readonly pidVersion: number;
  readonly uid: number;
}
interface ColdFrame {
  readonly protocol: 1;
  readonly binding: ColdBinding;
  readonly sequence: number;
  readonly type: ColdMessageType;
  readonly identity: ColdIdentity | null;
}
interface ColdCommand {
  readonly executable: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string;
  readonly env: Record<string, string | undefined>;
  readonly binding: ColdBinding;
}
interface ColdExitStatus {
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
}
export interface JonesColdBackendResult {
  readonly dispatchId: string;
  readonly spawned: boolean;
  readonly spawnUnknown: boolean;
  readonly exitStatus: ColdExitStatus | null;
  readonly abnormalExit: boolean;
  readonly stopRequested: boolean;
  readonly exitObserved: boolean;
  readonly closeObserved: boolean;
  readonly pipesClosed: boolean;
  readonly settled: boolean;
  readonly held: true;
  readonly lifecycleFailed: boolean;
  readonly events: ReadonlyArray<string>;
  readonly nativeIdentityQualified: false;
  readonly chromiumWritersQualified: false;
  readonly stateAdmissionAllowed: false;
  readonly activationAllowed: false;
  readonly restorationAllowed: false;
}
export interface JonesColdBackendOwner {
  readonly pid: number;
  readonly input: (fd: number) => NodeStream.Writable;
  readonly output: (fd: number) => NodeStream.Readable;
  readonly ready: () => Promise<ColdFrame>;
  readonly exit: () => Promise<ColdExitStatus>;
  readonly stop: () => Promise<JonesColdBackendResult>;
  readonly wait: () => Promise<JonesColdBackendResult>;
}
let coldStartupHeld = false;

export function holdJonesColdStartup(env: Readonly<Record<string, string | undefined>>): void {
  // Cold startup cannot reach profile configuration until the private native issuer and fence exist.
  coldStartupHeld ||=
    env.T3CODE_JONES_COLD_CHANNEL_FD !== undefined ||
    env.T3CODE_JONES_COLD_TRIAL_DESCRIPTOR !== undefined;
  coldRequire(!coldStartupHeld);
}

export function resolveJonesColdBackendPermit(): JonesColdBackendPermit | undefined {
  coldRequire(!coldStartupHeld);
  // Unit2B must issue the source-bound permit inside this single packaged module.
  return undefined;
}

const coldMessageTypes = new Set<ColdMessageType>([
  "READY",
  "DISPATCH_INTENT",
  "BACKEND_CAPTURED",
  "BACKEND_SETTLED",
  "STOP_REQUEST",
  "STOP_RESULT",
  "TERMINAL_CLOSED",
]);
const coldBindingKeys: ReadonlyArray<ColdBindingKey> = [
  "transactionId",
  "transferId",
  "handoffSha256",
  "sourceSha",
  "sourceTree",
  "artifactSha256",
  "bootSessionUuid",
];
const coldPermits = new WeakMap<JonesColdBackendPermit, ColdCommand>();
const coldLimit = 16384;
const coldOutputLimit = 1024 * 1024;

function coldRequire(condition: unknown): asserts condition {
  if (!condition) throw new Error("Cold custody binding or lifetime requires reconciliation.");
}
function coldObject(
  value: unknown,
  keys: ReadonlyArray<string>,
): asserts value is Record<string, unknown> {
  coldRequire(value !== null && typeof value === "object" && !Array.isArray(value));
  coldRequire(Object.keys(value).sort().join(",") === [...keys].sort().join(","));
}
function coldBinding(value: unknown): ColdBinding {
  coldObject(value, coldBindingKeys);
  for (const key of coldBindingKeys) {
    const length = key === "sourceSha" || key === "sourceTree" ? 40 : 64;
    const item = value[key];
    coldRequire(typeof item === "string");
    coldRequire(
      key === "bootSessionUuid"
        ? /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(item)
        : new RegExp("^[0-9a-f]{" + length + "}$").test(item),
    );
  }
  coldRequire(value.transactionId !== value.transferId);
  return Object.freeze(
    Object.fromEntries(coldBindingKeys.map((key) => [key, value[key]])),
  ) as ColdBinding;
}
function coldIdentity(value: unknown): ColdIdentity {
  coldObject(value, ["pid", "parentPid", "uniqueId", "parentUniqueId", "pidVersion", "uid"]);
  const number = (key: string, minimum: number): number => {
    const item = value[key];
    coldRequire(typeof item === "number" && Number.isSafeInteger(item) && item >= minimum);
    return item;
  };
  const decimal = (key: string): string => {
    const item = value[key];
    coldRequire(
      typeof item === "string" &&
        /^[1-9][0-9]{0,19}$/.test(item) &&
        BigInt(item) <= 18446744073709551615n,
    );
    return item;
  };
  return Object.freeze({
    pid: number("pid", 1),
    parentPid: number("parentPid", 1),
    uniqueId: decimal("uniqueId"),
    parentUniqueId: decimal("parentUniqueId"),
    pidVersion: number("pidVersion", 1),
    uid: number("uid", 0),
  });
}
function coldMessageType(value: unknown): value is ColdMessageType {
  return typeof value === "string" && Array.from(coldMessageTypes).some((type) => type === value);
}

class ColdChannel {
  #stream: NodeStream.Duplex;
  #binding: ColdBinding;
  #sent = 0;
  #received = 0;
  #buffer = Buffer.alloc(0);
  #queue: ColdFrame[] = [];
  #waiter: (() => void) | undefined;
  #failure: unknown;
  #ended = false;
  #reading = false;
  constructor(stream: NodeStream.Duplex, binding: ColdBinding) {
    this.#stream = stream;
    this.#binding = coldBinding(binding);
    stream.on("data", (chunk) => {
      if (this.#failure) return;
      try {
        coldRequire(!this.#ended && Buffer.isBuffer(chunk));
        this.#buffer = Buffer.concat([this.#buffer, chunk]);
        coldRequire(this.#buffer.length <= coldLimit);
        let newline;
        while ((newline = this.#buffer.indexOf(10)) >= 0) {
          const line = this.#buffer.subarray(0, newline);
          this.#buffer = this.#buffer.subarray(newline + 1);
          const value: unknown = JSON.parse(line.toString("utf8"));
          coldObject(value, ["protocol", "binding", "sequence", "type", "identity"]);
          coldRequire(
            value.protocol === 1 &&
              typeof value.sequence === "number" &&
              Number.isSafeInteger(value.sequence) &&
              value.sequence === this.#received + 1,
          );
          coldRequire(coldMessageType(value.type));
          const binding = coldBinding(value.binding);
          coldRequire(coldBindingKeys.every((key) => binding[key] === this.#binding[key]));
          const identity =
            value.type === "READY" || value.type === "BACKEND_CAPTURED"
              ? coldIdentity(value.identity)
              : null;
          if (identity === null) coldRequire(value.identity === null);
          this.#received = value.sequence;
          coldRequire(this.#queue.length < 16);
          this.#queue.push(
            Object.freeze({
              protocol: 1,
              binding,
              sequence: value.sequence,
              type: value.type,
              identity,
            }),
          );
        }
      } catch (cause) {
        this.#fail(cause);
      }
      this.#wake();
    });
    stream.on("error", (cause) => this.#fail(cause));
    stream.on("end", () => {
      this.#ended = true;
      if (this.#buffer.length) this.#fail(new Error("Truncated cold channel."));
      this.#wake();
    });
    stream.on("close", () => {
      this.#ended = true;
      this.#wake();
    });
  }
  #fail(cause: unknown) {
    this.#failure ??= cause;
    this.#wake();
  }
  #wake() {
    const wake = this.#waiter;
    this.#waiter = undefined;
    wake?.();
  }
  async receive(expected: ReadonlyArray<ColdMessageType>, timeoutMs = 5000): Promise<ColdFrame> {
    coldRequire(
      Array.isArray(expected) &&
        expected.length > 0 &&
        expected.every((type) => coldMessageTypes.has(type)),
    );
    coldRequire(
      Number.isSafeInteger(timeoutMs) && timeoutMs > 0 && timeoutMs <= 30000 && !this.#reading,
    );
    this.#reading = true;
    const deadline = NodePerfHooks.performance.now() + timeoutMs;
    try {
      while (!this.#queue.length && !this.#failure && !this.#ended) {
        const remaining = deadline - NodePerfHooks.performance.now();
        coldRequire(remaining > 0);
        await new Promise<void>((resolve) => {
          const timer = NodeTimers.setTimeout(() => {
            this.#waiter = undefined;
            resolve();
          }, remaining);
          this.#waiter = () => {
            NodeTimers.clearTimeout(timer);
            resolve();
          };
        });
      }
      if (this.#failure) throw this.#failure;
      coldRequire(this.#queue.length > 0);
      const frame = this.#queue.shift();
      coldRequire(frame !== undefined && expected.includes(frame.type));
      return frame;
    } catch (cause) {
      this.#fail(cause);
      throw cause;
    } finally {
      this.#reading = false;
    }
  }
  async send(type: ColdMessageType, identity: ColdIdentity | null = null): Promise<void> {
    try {
      coldRequire(!this.#failure && !this.#ended && coldMessageTypes.has(type));
      if (type === "READY" || type === "BACKEND_CAPTURED") coldIdentity(identity);
      else coldRequire(identity === null);
      coldRequire(this.#sent < Number.MAX_SAFE_INTEGER);
      const bytes = Buffer.from(
        JSON.stringify({
          protocol: 1,
          binding: this.#binding,
          sequence: ++this.#sent,
          type,
          identity,
        }) + "\n",
      );
      coldRequire(bytes.length <= coldLimit);
      await new Promise<void>((resolve, reject) => {
        const timer = NodeTimers.setTimeout(
          () => reject(new Error("Cold channel write deadline expired.")),
          5000,
        );
        this.#stream.write(bytes, (error) => {
          NodeTimers.clearTimeout(timer);
          if (error) reject(error);
          else resolve();
        });
      });
    } catch (cause) {
      this.#fail(cause);
      throw cause;
    }
  }
  get failed() {
    return this.#failure !== undefined;
  }
}

class CapturedColdBackend implements JonesColdBackendOwner {
  readonly binding: ColdBinding;
  readonly dispatchId: string;
  #exitCompletion = Promise.withResolvers<ColdExitStatus | Error>();
  #child: NodeChildProcess.ChildProcess | undefined;
  #streams: Array<{ fd: number; stream: NodeStream.Duplex; closed: boolean }> = [];
  #exit: ColdExitStatus | undefined;
  #closed = false;
  #spawned = false;
  #spawnUnknown = true;
  #failure: unknown;
  #channel: ColdChannel | undefined;
  #settling: Promise<JonesColdBackendResult> | undefined;
  #outputBytes = 0;
  #events: string[] = [];
  #abortSignal: AbortSignal | undefined;
  #abortListener: (() => void) | undefined;
  #readySeen = false;
  #stopRequestedAt: number | undefined;
  constructor(command: ColdCommand, binding: ColdBinding, signal?: AbortSignal) {
    this.binding = coldBinding(binding);
    this.dispatchId = NodeCrypto.randomUUID();
    this.#events.push("DISPATCH_INTENT");
    coldRequire(!signal?.aborted);
    // Only the internal permit resolver may provide a production command. No spawner or PID can be supplied.
    try {
      this.#child = NodeChildProcess.spawn(command.executable, command.args, {
        cwd: command.cwd,
        env: command.env,
        detached: false,
        stdio: ["ignore", "pipe", "pipe", "pipe", "pipe", "pipe", "pipe", "pipe", "pipe"],
      });
    } catch (cause) {
      this.#failure = cause;
      this.#exitCompletion.resolve(new Error("Captured cold child spawn failed."));
      return;
    }
    const child = this.#child;
    child.once("spawn", () => {
      this.#spawned = true;
      this.#spawnUnknown = false;
      this.#events.push("BACKEND_CAPTURED");
    });
    child.on("error", (cause) => {
      this.#failure ??= cause;
      this.#exitCompletion.resolve(cause);
    });
    child.once("exit", (code, signal) => {
      this.#exit = { code, signal };
      this.#exitCompletion.resolve(this.#exit);
      this.#events.push("EXIT_OBSERVED");
    });
    child.once("close", () => {
      this.#closed = true;
      this.#events.push("CLOSE_OBSERVED");
      this.#detachAbort();
    });
    for (let fd = 1; fd <= 8; fd++) {
      const stream = child.stdio[fd];
      if (!(stream instanceof NodeStream.Duplex)) {
        this.#failure ??= new Error("Cold child pipe was not captured.");
        this.#signal();
        continue;
      }
      const owned = { fd, stream, closed: false };
      this.#streams.push(owned);
      stream.once("close", () => {
        owned.closed = true;
      });
      stream.on("error", (cause) => {
        this.#failure ??= cause;
      });
      if ([1, 2, 5, 7].includes(fd)) {
        stream.on("data", (chunk: Buffer) => {
          this.#outputBytes += chunk.length;
          if (this.#outputBytes > coldOutputLimit) {
            this.#failure ??= new Error("Cold child output bound exceeded.");
            this.#signal();
          }
        });
        stream.pause();
      } else if (fd !== 8) stream.resume();
    }
    // The manager owns the ordinary FD3–7 consumers; FD8 belongs only to custody.
    const channel = this.#streams.find((item) => item.fd === 8)?.stream;
    if (channel) this.#channel = new ColdChannel(channel, binding);
    if (signal) {
      this.#abortSignal = signal;
      this.#abortListener = () => {
        this.#failure ??= new Error("Cold child cancelled.");
        this.#stopRequestedAt ??= NodePerfHooks.performance.now();
        this.#signal();
      };
      signal.addEventListener("abort", this.#abortListener, { once: true });
      if (signal.aborted) this.#abortListener();
    }
  }
  get pid(): number {
    const pid = this.#child?.pid;
    coldRequire(typeof pid === "number" && pid > 0);
    return pid;
  }
  input(fd: number): NodeStream.Writable {
    coldRequire([3, 4, 6].includes(fd));
    const stream = this.#streams.find((item) => item.fd === fd)?.stream;
    coldRequire(stream !== undefined);
    return stream;
  }
  output(fd: number): NodeStream.Readable {
    coldRequire([1, 2, 5, 7].includes(fd));
    const stream = this.#streams.find((item) => item.fd === fd)?.stream;
    coldRequire(stream !== undefined);
    return stream;
  }
  async exit(): Promise<ColdExitStatus> {
    const result = await this.#exitCompletion.promise;
    if (result instanceof Error) throw result;
    return result;
  }
  #detachAbort() {
    if (this.#abortListener) this.#abortSignal?.removeEventListener("abort", this.#abortListener);
  }
  #signal(kind: NodeJS.Signals = "SIGTERM") {
    if (this.#child && this.#exit === undefined) {
      try {
        if (!this.#child.kill(kind))
          this.#failure ??= new Error("Captured cold child stop was not confirmed.");
      } catch (cause) {
        this.#failure ??= cause;
      }
    }
  }
  async ready(): Promise<ColdFrame> {
    try {
      coldRequire(
        this.#child !== undefined &&
          this.#channel !== undefined &&
          !this.#failure &&
          !this.#readySeen,
      );
      const ready = await this.#channel.receive(["READY"]);
      coldRequire(
        ready.identity !== null &&
          this.#spawned &&
          ready.identity.pid === this.#child.pid &&
          ready.identity.parentPid === NodeProcess.pid,
      );
      // Identity decoding does not qualify native identity. Unit2 must compare a native observer issued in this live scope.
      this.#readySeen = true;
      return ready;
    } catch (cause) {
      this.#failure ??= cause;
      throw cause;
    }
  }
  async stop(): Promise<JonesColdBackendResult> {
    this.#events.push("STOP_REQUEST");
    this.#stopRequestedAt ??= NodePerfHooks.performance.now();
    this.#signal();
    return this.wait();
  }
  wait(): Promise<JonesColdBackendResult> {
    this.#settling ??= this.#finish();
    return this.#settling;
  }
  async #finish(): Promise<JonesColdBackendResult> {
    const deadline = NodePerfHooks.performance.now() + 3000;
    while (
      !this.#closed &&
      this.#child &&
      NodePerfHooks.performance.now() < deadline &&
      (this.#stopRequestedAt === undefined ||
        NodePerfHooks.performance.now() < this.#stopRequestedAt + 100)
    )
      await new Promise<void>((resolve) => NodeTimers.setTimeout(resolve, 5));
    if (!this.#closed && this.#child) {
      this.#failure ??= new Error("Cold child closure was not observed.");
      // Stop only this captured object. A timeout never becomes closure evidence.
      this.#signal();
      const grace = NodePerfHooks.performance.now() + 100;
      while (!this.#closed && NodePerfHooks.performance.now() < grace)
        await new Promise<void>((resolve) => NodeTimers.setTimeout(resolve, 5));
      if (!this.#closed) this.#signal("SIGKILL");
      const drain = NodePerfHooks.performance.now() + 1000;
      while (!this.#closed && NodePerfHooks.performance.now() < drain)
        await new Promise<void>((resolve) => NodeTimers.setTimeout(resolve, 5));
    }
    const pipesClosed = this.#streams.length === 8 && this.#streams.every((item) => item.closed);
    const exitStatus = this.#exit === undefined ? null : Object.freeze({ ...this.#exit });
    const abnormalExit = exitStatus === null || exitStatus.code !== 0 || exitStatus.signal !== null;
    const settled =
      this.#spawned &&
      !this.#spawnUnknown &&
      this.#exit !== undefined &&
      this.#closed &&
      pipesClosed;
    if (settled) this.#events.push("BACKEND_SETTLED", "TERMINAL_CLOSED");
    return Object.freeze({
      dispatchId: this.dispatchId,
      spawned: this.#spawned,
      spawnUnknown: this.#spawnUnknown,
      exitStatus,
      abnormalExit,
      stopRequested: this.#stopRequestedAt !== undefined,
      exitObserved: this.#exit !== undefined,
      closeObserved: this.#closed,
      pipesClosed,
      settled,
      held: true,
      lifecycleFailed:
        !settled ||
        abnormalExit ||
        !this.#readySeen ||
        this.#failure !== undefined ||
        this.#channel?.failed === true,
      events: Object.freeze([...this.#events]),
      nativeIdentityQualified: false,
      chromiumWritersQualified: false,
      stateAdmissionAllowed: false,
      activationAllowed: false,
      restorationAllowed: false,
    });
  }
}

export function startJonesColdBackend(
  permit: JonesColdBackendPermit,
  signal?: AbortSignal,
): JonesColdBackendOwner {
  // Unit2 must add the private, source-validated issuer. Serialized objects and qualification reports cannot issue permits.
  const command =
    permit !== null && typeof permit === "object" ? coldPermits.get(permit) : undefined;
  coldRequire(command !== undefined);
  coldPermits.delete(permit);
  return new CapturedColdBackend(command, command.binding, signal);
}
