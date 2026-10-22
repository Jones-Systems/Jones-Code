// @effect-diagnostics nodeBuiltinImport:off -- This opt-in test entry point owns its protocol, synthetic files, and process lifetime.
import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";
import * as NodeTimers from "node:timers";
import * as NodeURL from "node:url";
import type * as NodeStream from "node:stream";
import type * as EffectType from "effect/Effect";
import type * as ScopeType from "effect/Scope";
import type * as NodeServicesType from "@effect/platform-node/NodeServices";

export const protocol = "jones-je08h-control/v1";
export const fixtureDirectory = "apps/server/integration/jones-e2e";
const requiredSources = [
  `${fixtureDirectory}/physicalHost.worker.ts`,
  `${fixtureDirectory}/physicalHost.ts`,
  `${fixtureDirectory}/serverFixture.ts`,
  `${fixtureDirectory}/cross-server-message.integration.test.ts`,
  "pnpm-lock.yaml",
];
export type SourceBinding = {
  repository: string;
  node: string;
  nodeVersion: string;
  identity: string;
  hashes: Record<string, string>;
};
export type DomainSnapshot = {
  tables: Record<string, readonly Record<string, unknown>[]>;
  sha256: string;
};
const tableOrders = {
  orchestration_events: "sequence",
  orchestration_command_receipts: "command_id",
  orchestration_v2_projection_threads: "thread_id",
  orchestration_v2_projection_messages: "message_id",
  orchestration_v2_projection_runs: "run_id",
  orchestration_v2_projection_provider_turns: "provider_turn_id",
} as const;

export const object = (value: unknown): Record<string, unknown> => {
  NodeAssert.ok(value !== null && typeof value === "object" && !Array.isArray(value));
  return value as Record<string, unknown>;
};
export const keys = (value: Record<string, unknown>, names: readonly string[]) => {
  NodeAssert.deepEqual(Object.keys(value).sort(), [...names].sort());
};
export const text = (value: unknown): string => {
  NodeAssert.ok(typeof value === "string" && value.length > 0 && value.length <= 4096);
  return value;
};
export const absolute = (value: unknown): string => {
  const path = text(value);
  NodeAssert.ok(NodePath.isAbsolute(path) && NodePath.normalize(path) === path && path !== "/");
  NodeAssert.ok(!/[\r\n\0]/u.test(path));
  return path;
};
export const parse = (value: string): unknown => {
  return JSON.parse(value);
};
export const canonical = (value: unknown): string => {
  const normalize = (input: unknown): unknown => {
    if (Array.isArray(input)) return input.map(normalize);
    if (input !== null && typeof input === "object") {
      return Object.fromEntries(
        Object.entries(input)
          .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
          .map(([key, entry]) => [key, normalize(entry)]),
      );
    }
    return input;
  };
  const encoded = JSON.stringify(normalize(value));
  NodeAssert.ok(typeof encoded === "string");
  return encoded;
};
export const digest = (value: string | Buffer): string =>
  NodeCrypto.createHash("sha256").update(value).digest("hex");
export const validateSource = (input: unknown): SourceBinding => {
  const value = object(input);
  keys(value, ["repository", "node", "nodeVersion", "identity", "hashes"]);
  const hashes = object(value.hashes);
  NodeAssert.ok(Object.keys(hashes).length <= 10000);
  for (const [path, hash] of Object.entries(hashes)) {
    NodeAssert.ok(!NodePath.isAbsolute(path) && !path.split("/").includes("..") && path !== "");
    NodeAssert.match(text(hash), /^[a-f0-9]{64}$/u);
  }
  for (const path of requiredSources)
    NodeAssert.ok(Object.hasOwn(hashes, path), `Missing source: ${path}`);
  return {
    repository: absolute(value.repository),
    node: absolute(value.node),
    nodeVersion: text(value.nodeVersion),
    identity: text(value.identity),
    hashes: hashes as Record<string, string>,
  };
};
export const verifySource = (binding: SourceBinding) => {
  NodeAssert.equal(NodeFS.realpathSync(process.cwd()), NodeFS.realpathSync(binding.repository));
  NodeAssert.equal(NodeFS.realpathSync(process.execPath), NodeFS.realpathSync(binding.node));
  NodeAssert.equal(process.versions.node, binding.nodeVersion);
  for (const [relative, expected] of Object.entries(binding.hashes)) {
    const path = NodePath.join(binding.repository, relative);
    NodeAssert.ok(NodeFS.lstatSync(path).isFile() && !NodeFS.lstatSync(path).isSymbolicLink());
    NodeAssert.ok(
      NodeFS.realpathSync(path).startsWith(`${NodeFS.realpathSync(binding.repository)}/`),
    );
    NodeAssert.equal(digest(NodeFS.readFileSync(path)), expected, `Source differs: ${relative}`);
  }
};
export const processIdentity = () => {
  NodeAssert.equal(
    // oxlint-disable-next-line t3code/no-global-process-runtime -- Verify the actual worker OS before loading the fixture runtime; an injected platform would not prove host identity.
    process.platform,
    "linux",
    "Physical-host qualification requires the bound Linux hosts",
  );
  const stat = NodeFS.readFileSync("/proc/self/stat", "utf8");
  const fields = stat
    .slice(stat.lastIndexOf(")") + 2)
    .trim()
    .split(/\s+/u);
  return {
    pid: process.pid,
    ppid: process.ppid,
    pgid: Number(fields[2]),
    startTicks: text(fields[19]),
    executable: NodeFS.realpathSync(process.execPath),
    nodeVersion: process.versions.node,
    cwd: NodeFS.realpathSync(process.cwd()),
    bootIdSha256: digest(NodeFS.readFileSync("/proc/sys/kernel/random/boot_id")),
  };
};
export const ownedRoot = (parent: string, prefix: string) => {
  NodeAssert.equal(
    NodeFS.realpathSync(parent),
    parent,
    "Scratch/evidence parent must be canonical",
  );
  const type = NodeFS.statfsSync(parent).type;
  NodeAssert.ok(type !== 0x01021994 && type !== 0x858458f6, "Use disk-backed task storage");
  return NodeFS.mkdtempSync(NodePath.join(parent, prefix));
};
export const isolate = (root: string) => {
  NodeAssert.ok(
    !process.env.T3CODE_DESKTOP_USER_DATA_DIR && !process.env.T3CODE_JONES_TRIAL_DESCRIPTOR,
  );
  for (const key of Object.keys(process.env)) delete process.env[key];
  for (const name of ["home", "tmp", "cache"]) NodeFS.mkdirSync(NodePath.join(root, name));
  Object.assign(process.env, {
    HOME: NodePath.join(root, "home"),
    TMPDIR: NodePath.join(root, "tmp"),
    XDG_CACHE_HOME: NodePath.join(root, "cache"),
    PATH: "/usr/bin:/bin",
    LANG: "C.UTF-8",
  });
};
export const removeOwned = (root: string) => {
  NodeAssert.ok(NodeFS.lstatSync(root).isDirectory() && !NodeFS.lstatSync(root).isSymbolicLink());
  NodeFS.rmSync(root, { recursive: true });
  NodeAssert.equal(NodeFS.existsSync(root), false);
};
export const writeEvidence = (root: string, name: string, value: unknown) => {
  NodeAssert.match(name, /^[a-z0-9-]+\.json$/u);
  const bytes = `${canonical(value)}\n`;
  NodeAssert.ok(Buffer.byteLength(bytes) <= 4 * 1024 * 1024);
  NodeFS.writeFileSync(NodePath.join(root, name), bytes, { flag: "wx", mode: 0o600 });
  return digest(bytes);
};
export const deadline = async <A>(promise: Promise<A>, ms: number, label: string): Promise<A> => {
  let timer: ReturnType<typeof NodeTimers.setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        // @effect-diagnostics-next-line globalTimers:off -- This native process boundary must enforce cancellation before the Effect runtime loads and after it closes.
        timer = NodeTimers.setTimeout(() => reject(new Error(`Deadline: ${label}`)), ms);
      }),
    ]);
  } finally {
    if (timer !== undefined) NodeTimers.clearTimeout(timer);
  }
};

// Only complete, bounded frames enter the queue; raw frames can contain a synthetic pairing code.
export class Channel {
  private pending = "";
  private readonly queue: unknown[] = [];
  private waiter: { resolve: (value: unknown) => void; reject: (error: Error) => void } | undefined;
  private failure: Error | undefined;
  private readonly output: NodeStream.Writable;
  constructor(input: NodeStream.Readable, output: NodeStream.Writable) {
    this.output = output;
    input.setEncoding("utf8");
    input.on("data", (chunk: string) => {
      if (this.failure) return;
      this.pending += chunk;
      if (Buffer.byteLength(this.pending) > 2 * 1024 * 1024)
        return this.fail("Control frame limit");
      let end: number;
      while ((end = this.pending.indexOf("\n")) >= 0) {
        const line = this.pending.slice(0, end);
        this.pending = this.pending.slice(end + 1);
        try {
          const value = parse(line);
          if (this.waiter) {
            const waiter = this.waiter;
            this.waiter = undefined;
            waiter.resolve(value);
          } else {
            this.queue.push(value);
            if (this.queue.length > 4) return this.fail("Control queue limit");
          }
        } catch {
          this.fail("Invalid control JSON");
        }
      }
    });
    input.on("end", () => this.fail("Control EOF"));
    input.on("error", () => this.fail("Control input error"));
    output.on("error", () => this.fail("Control output error"));
  }
  fail(message: string) {
    this.failure ??= new Error(message);
    this.waiter?.reject(this.failure);
    this.waiter = undefined;
  }
  read(): Promise<unknown> {
    if (this.queue.length > 0) return Promise.resolve(this.queue.shift());
    if (this.failure) return Promise.reject(this.failure);
    NodeAssert.equal(this.waiter, undefined, "One outstanding control read");
    return new Promise((resolve, reject) => {
      this.waiter = { resolve, reject };
    });
  }
  async send(value: unknown) {
    const line = `${canonical(value)}\n`;
    NodeAssert.ok(Buffer.byteLength(line) <= 2 * 1024 * 1024);
    await new Promise<void>((resolve, reject) =>
      this.output.write(line, (error) => (error ? reject(error) : resolve())),
    );
  }
}

export async function loadRuntime() {
  const [Effect, Scope, Exit, ManagedRuntime, NodeServices, fixture, contracts, Layer, Logger] =
    await Promise.all([
      import("effect/Effect"),
      import("effect/Scope"),
      import("effect/Exit"),
      import("effect/ManagedRuntime"),
      import("@effect/platform-node/NodeServices"),
      import("./serverFixture.ts"),
      import("@t3tools/contracts"),
      import("effect/Layer"),
      import("effect/Logger"),
    ]);
  // Keep service diagnostics, including possible auth request material, off the private protocol stream.
  const runtime = ManagedRuntime.make(Layer.merge(NodeServices.layer, Logger.layer([])));
  const closers = new Set<() => Promise<void>>();
  return {
    Effect,
    Scope,
    Exit,
    fixture,
    contracts,
    runtime,
    async dispose() {
      const results = await Promise.allSettled([...closers].map((close) => close()));
      await runtime.dispose();
      const failed = results.filter((result) => result.status === "rejected");
      if (failed.length > 0) throw new Error("Fixture scope cleanup unknown");
    },
    async open(home: string, signal: AbortSignal) {
      const scope = await runtime.runPromise(Scope.make("sequential"));
      let closing: Promise<void> | undefined;
      const close = () => {
        closing ??= runtime.runPromise(Scope.close(scope, Exit.void));
        return closing;
      };
      closers.add(close);
      const run = <A, E>(
        effect: EffectType.Effect<A, E, ScopeType.Scope | NodeServicesType.NodeServices>,
      ) => runtime.runPromise(effect.pipe(Scope.provide(scope)), { signal });
      try {
        const server = await run(fixture.startServer(home));
        return { server, run, close };
      } catch (error) {
        await close();
        throw error;
      }
    },
  };
}
export type Live = Awaited<ReturnType<Awaited<ReturnType<typeof loadRuntime>>["open"]>>;
export const snapshot = async (live: Live): Promise<DomainSnapshot> => {
  const tables: DomainSnapshot["tables"] = {};
  for (const [table, order] of Object.entries(tableOrders)) {
    tables[table] = await live.run(
      live.server.sql.unsafe<Record<string, unknown>>(`SELECT * FROM ${table} ORDER BY ${order}`),
    );
  }
  return { tables, sha256: digest(canonical(tables)) };
};
export const offlineSnapshot = async (dbPath: string): Promise<DomainSnapshot> => {
  const Sqlite = await import("node:sqlite");
  const db = new Sqlite.DatabaseSync(dbPath, { readOnly: true });
  try {
    const tables: DomainSnapshot["tables"] = {};
    for (const [table, order] of Object.entries(tableOrders)) {
      tables[table] = db.prepare(`SELECT * FROM ${table} ORDER BY ${order}`).all();
    }
    return { tables, sha256: digest(canonical(tables)) };
  } finally {
    db.close();
  }
};
export const closedListener = (url: string) =>
  new Promise<void>((resolve, reject) => {
    const parsed = new globalThis.URL(url);
    NodeAssert.equal(parsed.hostname, "127.0.0.1");
    const socket = NodeNet.connect({ host: "127.0.0.1", port: Number(parsed.port) });
    socket.setTimeout(5000);
    socket.once("connect", () => {
      socket.destroy();
      reject(new Error("Owned listener still reachable"));
    });
    socket.once("timeout", () => {
      socket.destroy();
      reject(new Error("Listener readback timed out"));
    });
    socket.once("error", (error: NodeJS.ErrnoException) => {
      socket.destroy();
      if (error.code === "ECONNREFUSED") resolve();
      else reject(error);
    });
  });

async function workerMain() {
  const channel = new Channel(process.stdin, process.stdout);
  const cancellation = new AbortController();
  const cancel = () => {
    cancellation.abort();
    channel.fail("Worker cancelled");
  };
  process.once("SIGTERM", cancel);
  process.once("SIGINT", cancel);
  process.stdin.once("end", cancel);
  // @effect-diagnostics-next-line globalTimers:off -- This native process boundary must enforce cancellation before the Effect runtime loads and after it closes.
  const lifetime = NodeTimers.setTimeout(cancel, 180000);
  let owned: string | undefined;
  let evidence: string | undefined;
  let loaded: Awaited<ReturnType<typeof loadRuntime>> | undefined;
  let live: Live | undefined;
  let dbPath: string | undefined;
  let runId = "uninitialized";
  let clean = false;
  let failure = false;
  let nextId = 1;
  try {
    await channel.send({ schema: protocol, kind: "boot", process: processIdentity() });
    const init = object(await deadline(channel.read(), 15000, "worker init"));
    keys(init, [
      "schema",
      "runId",
      "id",
      "op",
      "source",
      "scratchParent",
      "evidenceParent",
      "stage",
      "fault",
    ]);
    NodeAssert.equal(init.schema, protocol);
    NodeAssert.equal(init.id, 0);
    NodeAssert.equal(init.op, "init");
    runId = text(init.runId);
    NodeAssert.match(runId, /^[a-zA-Z0-9-]{1,80}$/u);
    NodeAssert.ok(["import-smoke", "lifecycle-smoke", "qualify"].includes(text(init.stage)));
    NodeAssert.ok(
      init.fault === "none" ||
        (init.stage === "lifecycle-smoke" && init.fault === "after-listening"),
    );
    const source = validateSource(init.source);
    verifySource(source);
    evidence = ownedRoot(absolute(init.evidenceParent), `je08h-${runId}-`);
    owned = ownedRoot(absolute(init.scratchParent), `je08h-${runId}-`);
    isolate(owned);
    writeEvidence(evidence, "binding.json", { runId, source, process: processIdentity(), owned });
    loaded = await loadRuntime();
    cancellation.signal.throwIfAborted();
    if (init.stage === "import-smoke") {
      await channel.send({
        schema: protocol,
        runId,
        id: 0,
        ok: true,
        value: { imported: true, evidence },
      });
    } else {
      const home = NodePath.join(owned, "server");
      NodeFS.mkdirSync(home);
      live = await loaded.open(home, cancellation.signal);
      dbPath = live.server.config.dbPath;
      if (init.fault === "after-listening") throw new Error("Injected setup failure");
      await channel.send({
        schema: protocol,
        runId,
        id: 0,
        ok: true,
        value: {
          process: processIdentity(),
          source,
          owned,
          home,
          evidence,
          dbPath,
          dbStat: { dev: NodeFS.statSync(dbPath).dev, ino: NodeFS.statSync(dbPath).ino },
          environmentId: live.server.environmentId,
          url: live.server.url,
          authorityStateDir: live.server.config.authorityStateDir,
        },
      });
    }
    let seeded = false;
    while (!cancellation.signal.aborted) {
      const request = object(await channel.read());
      keys(request, ["schema", "runId", "id", "op"]);
      NodeAssert.equal(request.schema, protocol);
      NodeAssert.equal(request.runId, runId);
      NodeAssert.equal(request.id, nextId++);
      const op = text(request.op);
      if (op === "close") break;
      if (op === "cancel") {
        cancellation.abort();
        break;
      }
      let value: unknown;
      if (op === "pairing") {
        NodeAssert.ok(live && !seeded);
        value = {
          credential: (await live.run(live.server.auth.issuePairingCredential())).credential,
        };
      } else if (op === "seed") {
        NodeAssert.ok(live && !seeded);
        const sessions = (await live.run(live.server.auth.listSessions())).filter((session) =>
          session.client.label?.startsWith("T3 Code · "),
        );
        NodeAssert.equal(sessions.length, 1);
        const session = sessions[0]!;
        const projectId = loaded.contracts.ProjectId.make("project:cross-server");
        const threadId = loaded.contracts.ThreadId.make("thread:cross-server");
        const outside = loaded.contracts.ThreadId.make("thread:outside-link");
        const workspaceRoot = NodePath.join(live.server.config.baseDir, "workspace");
        NodeFS.mkdirSync(workspaceRoot);
        await live.run(
          live.server.projects.create({
            commandId: loaded.contracts.CommandId.make("create:cross-server-project"),
            projectId,
            title: "Same project",
            workspaceRoot,
          }),
        );
        await live.run(
          loaded.fixture.createThread(live.server, {
            projectId,
            threadId,
            linkOrigin: {
              sessionId: session.sessionId,
              label: session.client.label ?? "Synthetic link",
              access: "auto",
            },
          }),
        );
        await live.run(loaded.fixture.createThread(live.server, { projectId, threadId: outside }));
        seeded = true;
        value = { environmentId: live.server.environmentId, linkedSessions: sessions.length };
      } else if (op === "snapshot") {
        NodeAssert.ok(live && seeded);
        value = await snapshot(live);
      } else if (op === "stop-server") {
        NodeAssert.ok(live && dbPath && seeded);
        const { url, config } = live.server;
        await live.close();
        live = undefined;
        await closedListener(url);
        NodeAssert.equal(NodeFS.existsSync(config.serverRuntimeStatePath), false);
        const state = await offlineSnapshot(dbPath);
        const files = [dbPath, `${dbPath}-wal`, `${dbPath}-shm`].map((path) => ({
          path,
          present: NodeFS.existsSync(path),
          sha256: NodeFS.existsSync(path) ? digest(NodeFS.readFileSync(path)) : null,
        }));
        value = { state, files, listenerClosed: true, runtimeStateRemoved: true };
      } else if (op === "closed-snapshot") {
        NodeAssert.ok(!live && dbPath && seeded);
        value = await offlineSnapshot(dbPath);
      } else {
        throw new Error("Unknown worker operation");
      }
      if (op !== "pairing") writeEvidence(evidence, `step-${request.id}.json`, { op, value });
      await channel.send({ schema: protocol, runId, id: request.id, ok: true, value });
    }
  } catch {
    failure = !cancellation.signal.aborted;
    // Do not serialize exceptions: auth failures can contain request material.
  } finally {
    try {
      if (live) {
        const { url, config } = live.server;
        await deadline(live.close(), 20000, "worker server close");
        await closedListener(url);
        NodeAssert.equal(NodeFS.existsSync(config.serverRuntimeStatePath), false);
      }
      if (loaded) await deadline(loaded.dispose(), 20000, "worker runtime close");
      if (owned) removeOwned(owned);
      clean = true;
    } catch {
      failure = true;
    }
    const receipt = {
      schema: protocol,
      runId,
      kind: "closed",
      clean,
      owned: owned ?? null,
      evidence: evidence ?? null,
      cancelled: cancellation.signal.aborted,
      failed: failure,
      process: processIdentity(),
    };
    if (evidence) writeEvidence(evidence, "cleanup.json", receipt);
    await channel.send(receipt).catch(() => {});
    NodeTimers.clearTimeout(lifetime);
    process.removeListener("SIGTERM", cancel);
    process.removeListener("SIGINT", cancel);
    process.stdin.removeListener("end", cancel);
    process.stdin.pause();
    process.exitCode = failure || !clean ? 1 : 0;
  }
}

if (
  process.argv[1] &&
  NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href === import.meta.url
) {
  await workerMain();
}
