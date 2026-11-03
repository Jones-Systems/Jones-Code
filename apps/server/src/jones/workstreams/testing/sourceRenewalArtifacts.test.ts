// @effect-diagnostics nodeBuiltinImport:off -- In-memory child events only; no processes or artifact I/O.
import * as NodeEvents from "node:events";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import {
  acquireConsumerLifecycle,
  lifecycleBudgets,
  makeArtifactCustody,
} from "./sourceRenewalArtifacts.ts";
const { makeWorkerClose } = (await import(
  new URL("./sourceRenewalWorker.mjs", import.meta.url).href
)) as {
  makeWorkerClose: (finalizers: Array<() => Promise<unknown>>) => () => Promise<void>;
};

// The helper import must be inert. These boundaries are forbidden even during collection.
vi.mock("node:child_process", () => ({
  fork: () => {
    throw new Error("Real fork forbidden in mock controls");
  },
  execFile: () => {
    throw new Error("Real producer forbidden in mock controls");
  },
}));
vi.mock(
  "node:fs/promises",
  () =>
    new Proxy(
      {},
      {
        get: (_target, name) =>
          name === "then"
            ? undefined
            : () => {
                throw new Error(`Artifact I/O forbidden: ${String(name)}`);
              },
      },
    ),
);

class FakeChild extends NodeEvents.EventEmitter {
  pid: number | undefined = 42;
  connected = true;
  exitCode: number | null = null;
  signalCode = null;
  stdout = new NodeEvents.EventEmitter();
  stderr = new NodeEvents.EventEmitter();
  sent: Array<{ id: number; command: string }> = [];
  send(message: { id: number; command: string }, callback: (error: Error | null) => void) {
    if (!this.connected || this.exitCode !== null) throw new Error("send on closed IPC");
    this.sent.push(message);
    callback(null);
    return true;
  }
  reply(value: unknown, error?: unknown) {
    this.emit("message", { id: this.sent.at(-1)!.id, value, error });
  }
  disconnect() {
    this.connected = false;
    this.emit("disconnect");
  }
  exit(code = 0) {
    this.exitCode = code;
    this.emit("exit", code, null);
  }
  streamsClose() {
    this.emit("close", this.exitCode, null);
  }
}
function fixture() {
  const child = new FakeChild();
  const custody = makeArtifactCustody();
  const receipts: Array<{ event: string; data: Record<string, unknown> }> = [];
  const fork = vi.fn(
    () => child as unknown as ReturnType<Parameters<typeof acquireConsumerLifecycle>[0]["fork"]>,
  );
  const options = {
    requested: {
      root: "/mock/old",
      artifactDigest: "mock-digest",
      build: {
        repository: "Jones-Systems/Jones-Code" as const,
        sha: "mock-request-sha",
        tree: "mock-request-tree",
      },
      worker: "/mock/old/apps/server/.source-renewal-worker.mjs",
      workerSha256: "mock-worker",
      runtime: { executable: "/mock/node", version: "mock-runtime", sha256: "mock-node" },
    },
    custody,
    fork,
    timers: { setTimeout, clearTimeout },
    record: (event: string, data: unknown) =>
      receipts.push({ event, data: structuredClone(data) as Record<string, unknown> }),
  };
  const lifecycle = acquireConsumerLifecycle(options);
  return { child, custody, fork, options, lifecycle, receipts };
}
const rejection = (promise: Promise<unknown>) =>
  promise.then(
    () => {
      throw new Error("Unexpected success");
    },
    (error: unknown) => error,
  );
beforeEach(() => vi.useFakeTimers());
afterEach(() => {
  expect(vi.getTimerCount()).toBe(0);
  vi.useRealTimers();
});

it("captures custody and requested identity before bootstrap failure, preserving the first error through closed IPC", async () => {
  const f = fixture();
  expect(f.receipts[0]!.data).toMatchObject({ pid: null, requested: f.options.requested });
  expect(f.receipts[1]!.data).toMatchObject({ pid: 42, returnedIdentity: undefined });
  const ready = rejection(
    f.lifecycle.ready(() => {
      throw new Error("identity cannot validate");
    }),
  );
  const original = new Error("bootstrap import failed");
  f.child.stderr.emit("data", "original bootstrap diagnostic");
  f.child.emit("error", original);
  f.child.disconnect();
  f.child.exit(1);
  await Promise.resolve();
  f.child.stderr.emit("data", ": trailing stderr after exit");
  f.child.stdout.emit("data", "separate stdout");
  f.child.streamsClose();
  expect(await ready).toBe(original);
  expect(f.child.sent.map((message) => message.command)).toEqual(["identity"]);
  expect(await rejection(f.lifecycle.close())).toBe(original);
  expect(f.receipts.at(-2)!.data).toMatchObject({
    stderr: "original bootstrap diagnostic: trailing stderr after exit",
    stdout: "separate stdout",
    stdioClosed: true,
    exit: { code: 1, signal: null },
    familyClosure: "unknown",
    outcome: "closed-with-failure",
  });
  expect(() => f.custody.assertRemoval()).toThrow("fenced");
});

it("settles startup on spawn error without inventing a PID or successful closure", async () => {
  const f = fixture();
  f.child.pid = undefined;
  const ready = rejection(f.lifecycle.ready(() => undefined));
  const original = new Error("spawn ENOENT");
  f.child.emit("error", original);
  f.child.connected = false;
  f.child.streamsClose();
  expect(await ready).toBe(original);
  expect(f.receipts.at(-1)!.data).toMatchObject({
    primary: "spawn ENOENT",
    familyClosure: "unknown",
  });
});

it("waits for stdio closure after exit and keeps bounded, separate late output", async () => {
  const f = fixture();
  const ready = rejection(f.lifecycle.ready(() => undefined));
  f.child.exit(1);
  let settled = false;
  void ready.then(() => {
    settled = true;
  });
  await vi.advanceTimersByTimeAsync(lifecycleBudgets.shutdown - 1);
  expect(settled).toBe(false);
  f.child.stderr.emit("data", "x".repeat(6000));
  f.child.stdout.emit("data", "y".repeat(6000));
  f.child.streamsClose();
  expect(String(await ready)).toContain("exited 1/null");
  expect(f.receipts.at(-2)!.data).toMatchObject({
    stderr: "x".repeat(4096),
    stdout: "y".repeat(4096),
  });
});

it.each(["cancel", "deadline"])(
  "settles a no-response call on %s and retains unknown closure",
  async (mode) => {
    const f = fixture();
    const controller = new AbortController();
    const ready = rejection(f.lifecycle.ready(() => undefined, controller.signal));
    if (mode === "cancel") controller.abort(new Error("cancelled by scope"));
    else await vi.advanceTimersByTimeAsync(lifecycleBudgets.startup);
    await vi.advanceTimersByTimeAsync(lifecycleBudgets.operation + lifecycleBudgets.shutdown);
    expect(String(await ready)).toContain(
      mode === "cancel" ? "cancelled by scope" : "identity deadline",
    );
    expect(f.receipts.at(-2)!.data).toMatchObject({
      outcome: "retained-unknown",
      stdioClosed: false,
    });
    expect(f.receipts.at(-1)!.data.cleanupErrors).toEqual([
      "Artifact consumer close deadline exceeded",
      "Artifact consumer stdio-close deadline exceeded",
    ]);
    expect(f.lifecycle.close()).toBe(f.lifecycle.close());
    expect(await rejection(f.lifecycle.close())).toBe(await ready);
  },
);

it("distinguishes returned identity from the request and rejects validation without losing custody", async () => {
  const f = fixture();
  const mismatch = new Error("identity mismatch");
  const ready = rejection(
    f.lifecycle.ready(() => {
      throw mismatch;
    }),
  );
  f.child.reply({ root: "/mock/other", pid: 99 });
  await Promise.resolve();
  f.child.exit();
  f.child.streamsClose();
  expect(await ready).toBe(mismatch);
  expect(
    f.receipts.some((r) => (r.data.returnedIdentity as { pid?: number } | undefined)?.pid === 99),
  ).toBe(true);
  expect(f.receipts[0]!.data.requested).toEqual(f.options.requested);
});

it("shares failed shutdown, attempts stdio drain after cleanup refusal, and never sends again", async () => {
  const f = fixture();
  const ready = f.lifecycle.ready(() => undefined);
  f.child.reply({ root: "/mock/old" });
  await ready;
  const first = f.lifecycle.close();
  const result = rejection(first);
  expect(f.lifecycle.close()).toBe(first);
  await Promise.resolve();
  f.child.reply(undefined, { message: "worker cleanup failed" });
  await Promise.resolve();
  f.child.disconnect();
  f.child.exit(1);
  f.child.streamsClose();
  expect(await result).toMatchObject({ message: "worker cleanup failed" });
  expect(f.child.sent.map((m) => m.command)).toEqual(["identity", "close"]);
  expect(f.receipts.at(-1)!.data.cleanupErrors).toContain("worker cleanup failed");
  expect(await rejection(f.lifecycle.close())).toBe(await result);
});

it("does not certify descendants when a direct child closes successfully", async () => {
  const f = fixture();
  const result = rejection(f.lifecycle.close());
  await Promise.resolve();
  f.child.reply({ closed: true });
  await Promise.resolve();
  f.child.disconnect();
  f.child.exit();
  f.child.streamsClose();
  expect(String(await result)).toContain("family closure unknown");
  expect(() => f.custody.assertActive()).toThrow("fenced");
});

it("refuses every later fork, transition and removal after fencing", async () => {
  const f = fixture();
  f.custody.fence("first failure");
  for (let index = 0; index < 3; index++)
    expect(() => acquireConsumerLifecycle(f.options)).toThrow("first failure");
  const stop = vi.fn();
  const start = vi.fn();
  const remove = vi.fn();
  await expect(f.custody.transition(stop, start)).rejects.toThrow("first failure");
  await expect(f.custody.remove(remove)).rejects.toThrow("first failure");
  expect(f.fork).toHaveBeenCalledTimes(1);
  expect(stop).not.toHaveBeenCalled();
  expect(start).not.toHaveBeenCalled();
  expect(remove).not.toHaveBeenCalled();
  const result = rejection(f.lifecycle.close());
  await Promise.resolve();
  f.child.reply({ closed: true });
  await Promise.resolve();
  f.child.exit();
  f.child.streamsClose();
  await result;
});

it("rechecks the fence between stop and start, and seals removal against a concurrent fork", async () => {
  const custody = makeArtifactCustody();
  const start = vi.fn();
  await expect(
    custody.transition(async () => {
      custody.fence("stop failed");
    }, start),
  ).rejects.toThrow("stop failed");
  expect(start).not.toHaveBeenCalled();
  const empty = makeArtifactCustody();
  await empty.remove(async () => {
    expect(() => empty.acquire()).toThrow("removed");
  });
  expect(() => empty.acquire()).toThrow("removed");
});

it("attempts every worker finalizer once and repeats the same failed completion", async () => {
  const environmentFailure = new Error("environment close failed");
  const scopeFailure = new Error("scope close failed");
  const environment = vi.fn(async () => {
    throw environmentFailure;
  });
  const scope = vi.fn(async () => {
    throw scopeFailure;
  });
  const close = makeWorkerClose([environment, scope]);
  const first = close();
  const result = rejection(first);
  expect(close()).toBe(first);
  const error = (await result) as AggregateError;
  expect(error.errors).toEqual([environmentFailure, scopeFailure]);
  expect(await rejection(close())).toBe(error);
  expect(environment).toHaveBeenCalledTimes(1);
  expect(scope).toHaveBeenCalledTimes(1);
});

it("keeps an operation failure primary when shutdown also fails", async () => {
  const f = fixture();
  const operation = new Error("open failed before acknowledgement");
  f.lifecycle.failOperation(operation);
  const closed = rejection(f.lifecycle.close());
  await Promise.resolve();
  f.child.reply(undefined, {
    message: "cleanup failed too",
    cleanupErrors: ["environment failure", "scope failure"],
  });
  f.child.exit(1);
  f.child.streamsClose();
  expect(await closed).toBe(operation);
  expect(f.receipts.at(-1)!.data).toMatchObject({
    primary: operation.message,
    cleanupErrors: ["cleanup failed too"],
    workerCleanupErrors: ["environment failure", "scope failure"],
  });
});

it("does not fork if the pre-identity receipt cannot be persisted", async () => {
  const custody = makeArtifactCustody();
  const persistFailure = new Error("receipt storage unavailable");
  const fork = vi.fn();
  const options = {
    requested: {
      root: "/mock",
      artifactDigest: "mock",
      build: { repository: "Jones-Systems/Jones-Code" as const, sha: "mock", tree: "mock" },
      worker: "/mock/worker",
      workerSha256: "mock",
      runtime: { executable: "/mock/node", version: "mock", sha256: "mock" },
    },
    custody,
    fork,
    timers: { setTimeout, clearTimeout },
    record: () => {
      throw persistFailure;
    },
  };
  const lifecycle = acquireConsumerLifecycle(options);
  const ready = rejection(lifecycle.ready(() => undefined));
  await vi.advanceTimersByTimeAsync(lifecycleBudgets.shutdown);
  expect(await ready).toBe(persistFailure);
  expect(fork).not.toHaveBeenCalled();
  expect(() => custody.assertActive()).toThrow("receipt storage unavailable");
});
