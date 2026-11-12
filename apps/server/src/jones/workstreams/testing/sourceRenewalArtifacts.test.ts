// @effect-diagnostics nodeBuiltinImport:off -- In-memory child events only; no processes or artifact I/O.
import * as NodeEvents from "node:events";
import * as NodeStream from "node:stream";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import { afterEach, beforeEach, expect, vi } from "vite-plus/test";
import { it } from "@effect/vitest";
import {
  acquireConsumerLifecycle,
  acquireFamilyFixture,
  makeFamilyFixtureReceiver,
  retainFamilyFixtureFailure,
  runFamilyFixtureController,
  assertNaturalFamilyStreams,
  completeFamilyFixture,
  disposeFamilyObserver,
  observeFamilyStreams,
  type FamilyFixtureManifest,
  lifecycleBudgets,
  makeArtifactCustody,
  probeConsumerFamily,
  superviseConsumer,
} from "./sourceRenewalArtifacts.ts";
const { makeWorkerClose, makeWorkerClock } = (await import(
  new URL("./sourceRenewalWorker.mjs", import.meta.url).href
)) as {
  makeWorkerClock: (
    effect: typeof Effect,
    liveClock: Clock.Clock,
    currentMillis: () => number,
  ) => Clock.Clock;
  makeWorkerClose: (finalizers: Array<() => Promise<unknown>>) => () => Promise<void>;
};

// Collection must remain inert, including the capability probe and actual spawn factory.
vi.mock("node:child_process", () => ({
  spawn: () => {
    throw new Error("Real spawn forbidden in mock controls");
  },
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
vi.mock(
  "node:fs",
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

const binding = { custodyRoot: "/mock/custody", custodyRootIdentity: [1, 2, 3] };
type Proof = ReturnType<ReturnType<typeof makeArtifactCustody>["readReceipt"]>;
type Observation = Parameters<ReturnType<typeof makeArtifactCustody>["settle"]>[2];
const observed = (): Observation => ({
  supervisor: { pid: 42, exit: { code: 0, signal: null }, handlesClosed: true },
  streams: {
    stdoutEnded: true,
    stderrEnded: true,
    prematureClose: false,
    parentDisposal: false,
    disposedBeforeEof: false,
  },
  worker: { returnedPid: 99 },
  ipc: { disconnected: true },
  control: "none",
  outputBytes: 0,
});
function leaseFixture(custody = makeArtifactCustody(binding)) {
  const lease = custody.acquire();
  custody.bind(lease, `request-${lease.attempt}`, `/mock/${lease.attempt}.receipt.json`);
  const proof: Proof = {
    schema: "jones-source-renewal-family-receipt/v1",
    ...binding,
    ...lease,
    requestSha256: `request-${lease.attempt}`,
    supervisorIdentity: [42, 100],
    consumerIdentity: [99, 101],
    controlIdentity: [3, 4],
    consumerExit: { code: 0, signal: null },
    outcome: "success",
    signalsSent: [],
    familyClosed: true,
    reaped: true,
    allIdentitiesExited: true,
    ipcFdReleased: true,
    processOverflow: false,
    ownershipUnresolved: false,
  };
  return { custody, lease, proof };
}
class FakeChild extends NodeEvents.EventEmitter {
  pid: number | undefined = 42;
  connected = true;
  exitCode: number | null = null;
  signalCode = null;
  stdout = new NodeStream.PassThrough();
  stderr = new NodeStream.PassThrough();
  control = new NodeStream.PassThrough();
  unref = vi.fn();
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
    const ended = Promise.all([
      new Promise<void>((resolve) => this.stdout.once("end", resolve)),
      new Promise<void>((resolve) => this.stderr.once("end", resolve)),
      new Promise<void>((resolve) => this.control.once("end", resolve)),
    ]);
    this.control.resume();
    this.stdout.end();
    this.stderr.end();
    this.control.end();
    void ended.then(() => {
      this.emit("close", this.exitCode, null);
    });
  }
}
const mockRequest = {
  root: "/mock/old",
  artifactDigest: "mock-digest",
  build: {
    repository: "Jones-Systems/Jones-Code" as const,
    sha: "mock-sha",
    tree: "mock-tree",
  },
  worker: "/mock/worker",
  workerSha256: "mock-worker",
  runtime: { executable: "/mock/node", version: "mock", sha256: "mock-node" },
};
function fixture(custody = makeArtifactCustody(binding)) {
  const child = new FakeChild();
  const receipts: Array<{ event: string; data: Record<string, unknown> }> = [];
  let proof!: Proof;
  const fork = vi.fn((lease: Parameters<typeof superviseConsumer>[1]) => {
    custody.bind(lease, "request", `/mock/${lease.attempt}.receipt.json`);
    proof = { ...leaseFixture().proof, ...lease, requestSha256: "request" };
    return child as unknown as ReturnType<typeof superviseConsumer>;
  });
  const readReceipt = vi.fn(() => proof);
  const options = {
    requested: mockRequest,
    custody,
    fork,
    readReceipt,
    timers: { setTimeout, clearTimeout },
    record: (event: string, data: unknown) => {
      receipts.push({ event, data: structuredClone(data) as Record<string, unknown> });
    },
  };
  const lifecycle = acquireConsumerLifecycle(options);
  const ready = async () => {
    const pending = lifecycle.ready<{ pid: number }>(() => undefined);
    child.reply({ pid: 99 });
    await pending;
  };
  const finish = async () => {
    const closing = lifecycle.close();
    await Promise.resolve();
    child.reply({ closed: true });
    await Promise.resolve();
    child.disconnect();
    child.exit();
    child.streamsClose();
    return closing;
  };
  return {
    child,
    custody,
    fork,
    options,
    lifecycle,
    receipts,
    ready,
    finish,
    readReceipt,
    proof: () => proof,
  };
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

it.effect("keeps Effect timing usable while controlling the artifact wall clock", () =>
  Effect.gen(function* () {
    let millis = 1234;
    const clock = makeWorkerClock(Effect, Clock.Clock.defaultValue(), () => millis);
    const usingClock = <A, E>(effect: Effect.Effect<A, E>) =>
      effect.pipe(Effect.provideService(Clock.Clock, clock));
    expect(clock.currentTimeMillisUnsafe()).toBe(1234);
    expect(yield* usingClock(Clock.currentTimeNanos)).toBe(1234000000n);
    millis = 5678;
    expect(yield* usingClock(Clock.currentTimeMillis)).toBe(5678);
    expect((yield* usingClock(Effect.timed(Effect.succeed("ready"))))[1]).toBe("ready");
  }),
);

it("releases positive closure only after recording it and observing all handles", async () => {
  const f = fixture();
  await f.ready();
  const result = f.lifecycle.close();
  await Promise.resolve();
  f.child.reply({ closed: true });
  await Promise.resolve();
  f.child.disconnect();
  f.child.exit();
  expect(f.readReceipt).not.toHaveBeenCalled();
  expect(() => f.custody.assertRemoval()).toThrow("still open");
  f.child.streamsClose();
  await result;
  expect(f.readReceipt).toHaveBeenCalledExactlyOnceWith(f.lifecycle.attempt);
  expect(f.receipts.find((r) => r.event === "source-renewal family closed")?.data).toEqual(
    f.proof(),
  );
  const remove = vi.fn(async () => {
    expect(() => f.custody.acquire()).toThrow("removed");
  });
  await f.custody.remove(remove);
  expect(remove).toHaveBeenCalledOnce();
});

it("transitions only A while B remains open and usable, then requires all attempts for removal", async () => {
  const custody = makeArtifactCustody(binding);
  const a = fixture(custody);
  const b = fixture(custody);
  await a.ready();
  await b.ready();
  let next: ReturnType<typeof fixture> | undefined;
  await custody.transition(a.lifecycle.attempt, a.finish, async () => {
    next = fixture(custody);
    await next.ready();
  });
  expect(b.child.sent.map((m) => m.command)).toEqual(["identity"]);
  expect(b.child.connected).toBe(true);
  const call = b.lifecycle.call("rows", null, 0);
  b.child.reply("B remains usable");
  expect(await call).toBe("B remains usable");
  expect(next!.lifecycle.attempt).toBe(3);
  expect(() => custody.assertRemoval()).toThrow("still open");
  await b.finish();
  expect(() => custody.assertRemoval()).toThrow("still open");
  await next!.finish();
  custody.assertRemoval();
});

it("never starts the next consumer after outgoing close failure", async () => {
  const f = fixture();
  await f.ready();
  const start = vi.fn();
  const result = rejection(f.custody.transition(f.lifecycle.attempt, f.lifecycle.close, start));
  await Promise.resolve();
  f.child.reply(undefined, { message: "cleanup failed" });
  await Promise.resolve();
  f.child.disconnect();
  f.child.exit(1);
  f.child.streamsClose();
  expect(await result).toMatchObject({ message: "cleanup failed" });
  expect(start).not.toHaveBeenCalled();
});

type Mutation = (proof: Proof, observed: Observation) => void;
const refusals: Array<[string, Mutation]> = [
  [
    "schema",
    (p) => {
      p.schema = "foreign";
    },
  ],
  [
    "nonce",
    (p) => {
      p.nonce = "foreign";
    },
  ],
  [
    "attempt",
    (p) => {
      p.attempt += 1;
    },
  ],
  [
    "root",
    (p) => {
      p.custodyRoot = "/foreign";
    },
  ],
  [
    "root identity",
    (p) => {
      p.custodyRootIdentity = [2, 3, 4];
    },
  ],
  [
    "request hash",
    (p) => {
      p.requestSha256 = "foreign";
    },
  ],
  [
    "outcome",
    (p) => {
      p.outcome = "survivors";
    },
  ],
  [
    "consumer code",
    (p) => {
      p.consumerExit.code = 1;
    },
  ],
  [
    "consumer signal",
    (p) => {
      p.consumerExit.signal = "SIGTERM";
    },
  ],
  [
    "signals",
    (p) => {
      p.signalsSent = [15];
    },
  ],
  ...(["familyClosed", "reaped", "allIdentitiesExited", "ipcFdReleased"] as const).map(
    (key) =>
      [
        key,
        (p: Proof) => {
          p[key] = false;
        },
      ] as [string, Mutation],
  ),
  [
    "overflow",
    (p) => {
      p.processOverflow = true;
    },
  ],
  [
    "ownership",
    (p) => {
      p.ownershipUnresolved = true;
    },
  ],
  [
    "supervisor code",
    (_p, o) => {
      o.supervisor.exit!.code = 1;
    },
  ],
  [
    "supervisor signal",
    (_p, o) => {
      o.supervisor.exit!.signal = "SIGKILL";
    },
  ],
  [
    "natural stdout EOF",
    (_p, o) => {
      o.streams.stdoutEnded = false;
    },
  ],
  [
    "natural stderr EOF",
    (_p, o) => {
      o.streams.stderrEnded = false;
    },
  ],
  [
    "premature stream close",
    (_p, o) => {
      o.streams.prematureClose = true;
    },
  ],
  [
    "observer disposal",
    (_p, o) => {
      o.streams.parentDisposal = true;
    },
  ],
  [
    "premature observer disposal",
    (_p, o) => {
      o.streams.disposedBeforeEof = true;
    },
  ],
  [
    "handles",
    (_p, o) => {
      o.supervisor.handlesClosed = false;
    },
  ],
  [
    "IPC",
    (_p, o) => {
      o.ipc.disconnected = false;
    },
  ],
  [
    "control",
    (_p, o) => {
      o.control = "cancel-sent";
    },
  ],
  [
    "output",
    (_p, o) => {
      o.outputBytes = 262145;
    },
  ],
  [
    "supervisor PID",
    (p) => {
      p.supervisorIdentity[0] = 43;
    },
  ],
  [
    "worker PID",
    (p) => {
      p.consumerIdentity[0] = 100;
    },
  ],
  [
    "same PID",
    (p, o) => {
      p.consumerIdentity[0] = 42;
      o.worker.returnedPid = 42;
    },
  ],
  [
    "worker starttime",
    (p) => {
      p.consumerIdentity[1] = 0;
    },
  ],
  [
    "supervisor starttime",
    (p) => {
      p.supervisorIdentity[1] = 0;
    },
  ],
];
it.each(refusals)("permanently fences a failed %s predicate", (_name, mutate) => {
  const f = leaseFixture();
  const o = observed();
  mutate(f.proof, o);
  expect(() => f.custody.settle(f.lease.attempt, f.proof, o)).toThrow();
  expect(() => f.custody.assertActive()).toThrow("fenced");
  expect(() => f.custody.settle(f.lease.attempt, leaseFixture().proof, observed())).toThrow(
    "fenced",
  );
});
it.each(["unknown", "duplicate", "foreign custody", "removed"])(
  "refuses %s settlement without disk adoption",
  async (mode) => {
    const f = leaseFixture();
    if (mode === "duplicate" || mode === "removed")
      f.custody.settle(f.lease.attempt, f.proof, observed());
    if (mode === "removed") await f.custody.remove(async () => undefined);
    const target = mode === "foreign custody" ? makeArtifactCustody(binding) : f.custody;
    expect(() =>
      target.settle(mode === "unknown" ? 8 : f.lease.attempt, f.proof, observed()),
    ).toThrow();
  },
);

it("rejects receipt read failure and record failure before settling", async () => {
  for (const failure of ["read", "record"]) {
    const f = fixture();
    await f.ready();
    if (failure === "read")
      f.readReceipt.mockImplementation(() => {
        throw new Error("receipt refused");
      });
    else
      f.options.record = (event) => {
        if (event === "source-renewal family closed") throw new Error("record refused");
      };
    expect(String(await rejection(f.finish()))).toContain("refused");
    expect(() => f.custody.assertActive()).toThrow("fenced");
  }
});

it("retains the bootstrap failure, drains streams, and keeps supervisor and worker separate", async () => {
  const f = fixture();
  const original = new Error("bootstrap import failed");
  const ready = rejection(f.lifecycle.ready(() => undefined));
  f.child.stderr.write("original bootstrap diagnostic");
  f.child.emit("error", original);
  f.child.disconnect();
  f.child.exit(1);
  await Promise.resolve();
  f.child.stderr.write(": trailing stderr");
  f.child.streamsClose();
  expect(await ready).toBe(original);
  expect(await rejection(f.lifecycle.close())).toBe(original);
  expect(f.readReceipt).not.toHaveBeenCalled();
  expect(f.receipts.at(-2)!.data).toMatchObject({
    stderr: "original bootstrap diagnostic: trailing stderr",
    supervisor: { pid: 42, exit: { code: 1, signal: null }, handlesClosed: true },
    worker: { returnedPid: null },
  });
});

it("settles spawn failure without inventing a worker identity", async () => {
  const f = fixture();
  f.child.pid = undefined;
  const ready = rejection(f.lifecycle.ready(() => undefined));
  const original = new Error("spawn ENOENT");
  f.child.emit("error", original);
  f.child.connected = false;
  f.child.streamsClose();
  expect(await ready).toBe(original);
  expect(f.readReceipt).not.toHaveBeenCalled();
});

it.each(["cancel", "deadline"])(
  "bounds %s cleanup, disposes every handle, and ignores late proof",
  async (mode) => {
    const f = fixture();
    const controller = new AbortController();
    const ready = rejection(f.lifecycle.ready(() => undefined, controller.signal));
    if (mode === "cancel") controller.abort(new Error("scope cancelled"));
    else await vi.advanceTimersByTimeAsync(lifecycleBudgets.startup);
    await vi.advanceTimersByTimeAsync(16_000);
    expect(String(await ready)).toContain(
      mode === "cancel" ? "scope cancelled" : "identity deadline",
    );
    expect(f.child.stdout.destroyed && f.child.stderr.destroyed && f.child.control.destroyed).toBe(
      true,
    );
    expect(f.child.connected).toBe(false);
    expect(f.child.unref).toHaveBeenCalledOnce();
    f.child.exit();
    f.child.streamsClose();
    expect(f.readReceipt).not.toHaveBeenCalled();
    expect(() => f.custody.assertActive()).toThrow("fenced");
    expect(f.lifecycle.close()).toBe(f.lifecycle.close());
  },
);

it("fences cumulative output overflow and bounds the retained output tail", async () => {
  const f = fixture();
  await f.ready();
  f.child.stdout.write("x".repeat(200_000));
  f.child.stderr.write("y".repeat(100_000));
  expect(String(await rejection(f.finish()))).toContain("output limit");
  expect(f.receipts.at(-2)!.data).toMatchObject({
    stdout: "x".repeat(4096),
    stderr: "y".repeat(4096),
    outputBytes: 300_000,
  });
});

it("allows ordinary command refusal while identity and close refusal fence", async () => {
  const f = fixture();
  await f.ready();
  const call = rejection(f.lifecycle.call("open", null, 0));
  f.child.reply(undefined, { message: "expected mismatch" });
  expect(await call).toMatchObject({ message: "expected mismatch" });
  f.custody.assertActive();
  await f.finish();
  f.custody.assertRemoval();
});

it("rejects invalid returned identity and explicit open failure without release", async () => {
  for (const mode of ["identity", "open"]) {
    const f = fixture();
    const error = new Error(`${mode} failed`);
    if (mode === "identity") {
      const ready = rejection(
        f.lifecycle.ready(() => {
          throw error;
        }),
      );
      f.child.reply({ pid: 99 });
      await Promise.resolve();
      await Promise.resolve();
      f.child.reply({ closed: true });
      await Promise.resolve();
      f.child.disconnect();
      f.child.exit();
      f.child.streamsClose();
      expect(await ready).toBe(error);
    } else {
      await f.ready();
      f.lifecycle.failOperation(error);
      expect(await rejection(f.finish())).toBe(error);
    }
    expect(() => f.custody.assertRemoval()).toThrow("fenced");
  }
});

it("refuses unsupported platforms before process or file work", async () => {
  await expect(probeConsumerFamily("darwin")).rejects.toThrow("requires Linux");
  const f = leaseFixture();
  expect(() =>
    superviseConsumer(
      f.custody,
      f.lease,
      { executable: "/mock/node", args: [], cwd: "/mock", env: {} },
      "darwin",
    ),
  ).toThrow("requires Linux");
});

it("rechecks the transition fence and prevents unsettled or unknown outgoing transitions", async () => {
  for (const mode of ["fenced", "open", "unknown"]) {
    const f = leaseFixture();
    const start = vi.fn();
    await expect(
      f.custody.transition(
        mode === "unknown" ? 8 : f.lease.attempt,
        async () => {
          if (mode === "fenced") f.custody.fence("stop failed");
        },
        start,
      ),
    ).rejects.toThrow();
    expect(start).not.toHaveBeenCalled();
  }
});

it("limits original attempts and refuses duplicate binding", () => {
  const f = leaseFixture();
  expect(() => f.custody.bind(f.lease, "foreign", "/foreign")).toThrow();
  const custody = makeArtifactCustody(binding);
  for (let i = 0; i < 8; i++) custody.acquire();
  expect(() => custody.acquire()).toThrow("fork attempt limit");
});

it("attempts every worker finalizer once and preserves the failed completion", async () => {
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
  expect(((await result) as AggregateError).errors).toEqual([environmentFailure, scopeFailure]);
  expect(await rejection(close())).toBe(await result);
  expect(environment).toHaveBeenCalledOnce();
  expect(scope).toHaveBeenCalledOnce();
});

it("does not spawn when the acquisition record cannot persist", async () => {
  const f = fixture();
  await f.ready();
  await f.finish();
  const fork = vi.fn();
  const original = new Error("receipt storage unavailable");
  const lifecycle = acquireConsumerLifecycle({
    ...f.options,
    custody: makeArtifactCustody(binding),
    fork,
    record: () => {
      throw original;
    },
  });
  const ready = rejection(lifecycle.ready(() => undefined));
  await vi.advanceTimersByTimeAsync(11_000);
  expect(await ready).toBe(original);
  expect(fork).not.toHaveBeenCalled();
});

function deferred<A>() {
  let resolve!: (value: A) => void;
  const promise = new Promise<A>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function outerFixture() {
  const fixture = leaseFixture();
  const manifest: FamilyFixtureManifest = {
    ...binding,
    ...fixture.lease,
    fixtureId: "/mock/fresh-run-v1",
    requestSha256: fixture.proof.requestSha256,
    receiptPath: "/mock/exact.receipt.json",
    supervisorIdentity: [42, 100],
    consumerIdentity: [99, 101],
    controllerIdentity: [40, 98],
  };
  const controller = deferred<void>();
  const family = deferred<void>();
  const streams = observed().streams;
  streams.stdoutEnded = false;
  streams.stderrEnded = false;
  const readReceipt = vi.fn(() => ({ ...fixture.proof, outcome: "parent-loss" }));
  const remove = vi.fn();
  const persist = vi.fn();
  const disposeObserver = vi.fn();
  const options = {
    manifest,
    controllerExit: controller.promise,
    familyCompletion: family.promise,
    streams,
    deadlineMs: 1000,
    readReceipt,
    expectedOutcome: "parent-loss",
    allowInheritedStreamClosure: false,
    persist,
    remove,
    disposeObserver,
  };
  const complete = () => {
    streams.stdoutEnded = true;
    streams.stderrEnded = true;
    family.resolve();
  };
  return {
    manifest,
    controller,
    family,
    complete,
    readReceipt,
    remove,
    persist,
    disposeObserver,
    options,
  };
}

it("does zero receipt reads and removals after controller close until genuine family completion", async () => {
  const f = outerFixture();
  const completion = completeFamilyFixture(f.options);
  f.controller.resolve();
  await vi.advanceTimersByTimeAsync(100);
  expect(f.readReceipt).not.toHaveBeenCalled();
  expect(f.remove).not.toHaveBeenCalled();
  f.complete();
  await completion;
  expect(f.readReceipt).toHaveBeenCalledOnce();
  expect(f.remove).toHaveBeenCalledOnce();
  expect(f.persist.mock.invocationCallOrder[0]).toBeLessThan(f.remove.mock.invocationCallOrder[0]!);
  expect(f.persist).toHaveBeenCalledWith(
    expect.objectContaining({
      manifest: f.manifest,
      completion: expect.objectContaining({
        proof: expect.objectContaining({ outcome: "parent-loss" }),
      }),
    }),
  );
});

it("retains the exact root, receipt path and birth identities when family completion times out", async () => {
  const f = outerFixture();
  const failure = rejection(completeFamilyFixture(f.options));
  f.controller.resolve();
  await vi.advanceTimersByTimeAsync(1000);
  expect(String(await failure)).toContain(JSON.stringify(f.manifest));
  expect(f.readReceipt).not.toHaveBeenCalled();
  expect(f.remove).not.toHaveBeenCalled();
  expect(f.persist).toHaveBeenCalledWith(
    expect.objectContaining({
      manifest: f.manifest,
      retained: true,
      cleanupFailure: "Fixture overall deadline exceeded",
    }),
  );
  expect(f.disposeObserver).toHaveBeenCalledOnce();
  f.complete();
  await Promise.resolve();
  expect(f.readReceipt).not.toHaveBeenCalled();
  expect(f.remove).not.toHaveBeenCalled();
});

it.each([
  "request",
  "root",
  "nonce",
  "attempt",
  "supervisor birth",
  "consumer birth",
  "closure",
  "outcome",
])("retains a fresh fixture on mismatched %s proof", async (field) => {
  const f = outerFixture();
  f.readReceipt.mockImplementation(() => {
    const proof = { ...leaseFixture().proof, ...f.manifest, outcome: "parent-loss" };
    if (field === "request") proof.requestSha256 = "foreign";
    if (field === "root") proof.custodyRoot = "/foreign";
    if (field === "nonce") proof.nonce = "foreign";
    if (field === "attempt") proof.attempt += 1;
    if (field === "supervisor birth") proof.supervisorIdentity = [42, 999];
    if (field === "consumer birth") proof.consumerIdentity = [99, 999];
    if (field === "closure") proof.familyClosed = false;
    if (field === "outcome") proof.outcome = "success";
    return proof;
  });
  const failure = rejection(completeFamilyFixture(f.options));
  f.controller.resolve();
  f.complete();
  await failure;
  expect(f.remove).not.toHaveBeenCalled();
  expect(f.persist).toHaveBeenCalledWith(expect.objectContaining({ retained: true }));
});

it("refuses release and stream cleanup after destruction before natural EOF despite valid-looking proof and close events", async () => {
  const f = fixture();
  await f.ready();
  const streams = observeFamilyStreams(f.child);
  const closing = rejection(f.lifecycle.close());
  await Promise.resolve();
  f.child.reply({ closed: true });
  await Promise.resolve();
  f.child.stdout.destroy();
  f.child.disconnect();
  f.child.exit();
  f.child.emit("close", 0, null);
  expect(String(await closing)).toContain("natural EOF");
  expect(f.readReceipt).not.toHaveBeenCalled();
  expect(() => f.custody.assertRemoval()).toThrow("fenced");
  expect(streams).toMatchObject({
    stdoutEnded: false,
    parentDisposal: true,
    disposedBeforeEof: true,
  });
  const outer = outerFixture();
  outer.controller.resolve();
  outer.family.resolve();
  const remove = vi.fn();
  await expect(
    completeFamilyFixture({ ...outer.options, streams, allowInheritedStreamClosure: true, remove }),
  ).rejects.toThrow("retained");
  expect(outer.readReceipt).not.toHaveBeenCalled();
  expect(remove).not.toHaveBeenCalled();
});

it("records disposal before destroying any observer handle", () => {
  const child = new FakeChild();
  const streams = observeFamilyStreams(child);
  const destroy = vi.spyOn(child.stdout, "destroy").mockImplementation(() => {
    expect(streams).toMatchObject({ parentDisposal: true, disposedBeforeEof: true });
    return child.stdout;
  });
  disposeFamilyObserver(child);
  expect(destroy).toHaveBeenCalledOnce();
  expect(() => assertNaturalFamilyStreams(streams)).toThrow();
});

it("allows diagnostic inherited-stream cleanup only with natural EOF and no premature disposal", async () => {
  const f = outerFixture();
  const readReceipt = vi.fn(() => undefined);
  const completion = completeFamilyFixture({
    ...f.options,
    readReceipt,
    allowInheritedStreamClosure: true,
  });
  f.controller.resolve();
  f.complete();
  await completion;
  expect(f.remove).toHaveBeenCalledOnce();
  expect(f.persist).toHaveBeenCalledWith(
    expect.objectContaining({
      completion: expect.objectContaining({ inheritedStreamClosure: true }),
    }),
  );
});

it.each([null, false, 0, ""])(
  "retains a present falsy receipt %j despite natural inherited EOF",
  async (value) => {
    const f = outerFixture();
    const failure = rejection(
      completeFamilyFixture({
        ...f.options,
        readReceipt: () => value as unknown as Proof,
        allowInheritedStreamClosure: true,
      }),
    );
    f.controller.resolve();
    f.complete();
    const error = await failure;
    expect(String(error)).toContain("retained");
    expect(f.remove).not.toHaveBeenCalled();
    expect(f.persist).toHaveBeenCalledWith(
      expect.objectContaining({ manifest: f.manifest, retained: true }),
    );
  },
);

it("retains scratch when diagnostic persistence fails and preserves primary plus cleanup failures", async () => {
  const f = outerFixture();
  const persist = vi.fn(() => {
    throw new Error("evidence write failed");
  });
  const failure = rejection(
    completeFamilyFixture({ ...f.options, persist, primaryFailure: "assertion failed" }),
  );
  f.controller.resolve();
  f.complete();
  await failure;
  expect(f.remove).not.toHaveBeenCalled();
  expect(persist).toHaveBeenLastCalledWith(
    expect.objectContaining({
      primaryFailure: "assertion failed",
      cleanupFailure: "evidence write failed",
      retained: true,
    }),
  );
});

it("keeps diagnostic manifest projections separate from the original release binding", () => {
  const f = leaseFixture();
  const projection = f.custody.requestBinding(f.lease.attempt);
  projection.custodyRootIdentity[0] = 999;
  projection.nonce = "foreign";
  projection.receiptPath = "/foreign";
  expect(f.custody.rootBinding()).toEqual(binding);
  expect(f.custody.requestBinding(f.lease.attempt)).toMatchObject({
    nonce: f.lease.nonce,
    receiptPath: `/mock/${f.lease.attempt}.receipt.json`,
  });
  f.custody.settle(f.lease.attempt, f.proof, observed());
  f.custody.assertRemoval();
});

it("preserves assertion and removal failures in durable diagnostic records", async () => {
  const f = outerFixture();
  const remove = vi.fn(() => {
    throw new Error("remove refused");
  });
  const failure = rejection(
    completeFamilyFixture({ ...f.options, remove, primaryFailure: "assertion failed" }),
  );
  f.controller.resolve();
  f.complete();
  await failure;
  expect(f.persist).toHaveBeenLastCalledWith(
    expect.objectContaining({
      manifest: f.manifest,
      primaryFailure: "assertion failed",
      cleanupFailure: "remove refused",
      retained: true,
      observedCompletion: expect.objectContaining({
        controllerExited: true,
        familyCompleted: true,
      }),
    }),
  );
  expect(f.disposeObserver).toHaveBeenCalledOnce();
});

function acquiredFixture(persist = vi.fn((_value: unknown): void => {})) {
  const child = new FakeChild();
  const custody = makeArtifactCustody(binding);
  const remove = vi.spyOn(custody, "remove");
  const owned = acquireFamilyFixture({
    fixtureId: "/mock/fresh-run-v1",
    controllerIdentity: [40, 98],
    birthIdentity: (pid) => {
      if (pid === 42) return [42, 100];
      if (pid === 99) return [99, 101];
      throw new Error("Birth identity unavailable");
    },
    persist,
    lifecycle: {
      requested: mockRequest,
      custody,
      record: () => undefined,
      fork: (lease) => {
        custody.bind(lease, "request", "/mock/exact.receipt.json");
        return child as unknown as ReturnType<typeof superviseConsumer>;
      },
    },
  });
  return { child, custody, owned, persist, remove };
}

it("retains original partial acquisition before worker readiness on the controller deadline", async () => {
  const f = acquiredFixture();
  const sendManifest = vi.fn();
  const acknowledgement = vi.fn(() => new Promise<string>(() => undefined));
  const failure = rejection(
    runFamilyFixtureController({
      fixture: f.owned,
      deadlineMs: 100,
      sendManifest,
      acknowledgement,
      persist: f.persist,
    }),
  );
  expect(f.persist).toHaveBeenLastCalledWith({
    trust: "diagnostic-only",
    acquisition: {
      fixtureId: "/mock/fresh-run-v1",
      ...binding,
      ...f.custody.requestBinding(f.owned.lifecycle.attempt),
      supervisorIdentity: [42, 100],
      consumerIdentity: null,
      controllerIdentity: [40, 98],
    },
  });
  await vi.advanceTimersByTimeAsync(100);
  const error = await failure;
  expect(String(error)).toContain(JSON.stringify(f.owned.diagnostics()));
  expect(f.persist).toHaveBeenLastCalledWith(
    expect.objectContaining({
      retained: true,
      diagnostics: f.owned.diagnostics(),
      primaryFailure: "Controller deadline exceeded before acknowledgement",
    }),
  );
  expect(sendManifest).not.toHaveBeenCalled();
  expect(acknowledgement).not.toHaveBeenCalled();
  expect(f.child.unref).toHaveBeenCalledOnce();
  expect(f.remove).not.toHaveBeenCalled();
  expect(() => f.child.emit("error", new Error("late child error"))).not.toThrow();
  // The rejected identity lifecycle finishes its separately bounded close after observer disposal.
  await vi.advanceTimersByTimeAsync(11_000);
});

it("captures worker birth and original binding before a caller assertion can fail", async () => {
  const f = acquiredFixture();
  const assertion = new Error("caller assertion failed");
  const failure = rejection(
    f.owned.ready<{ pid: number }>(() => {
      expect(f.persist).toHaveBeenLastCalledWith(
        expect.objectContaining({
          trust: "diagnostic-only",
          acquisition: expect.objectContaining({
            ...f.custody.requestBinding(f.owned.lifecycle.attempt),
            supervisorIdentity: [42, 100],
            consumerIdentity: [99, 101],
          }),
        }),
      );
      throw assertion;
    }),
  );
  f.child.reply({ pid: 99 });
  await Promise.resolve();
  await Promise.resolve();
  f.child.reply({ closed: true });
  await Promise.resolve();
  f.child.disconnect();
  f.child.exit();
  f.child.streamsClose();
  expect(await failure).toBe(assertion);
  expect(f.owned.manifest()).toBeUndefined();
  expect(() =>
    retainFamilyFixtureFailure({
      diagnostics: f.owned.diagnostics(),
      primaryFailure: assertion,
      persist: f.persist,
      disposeObserver: () => disposeFamilyObserver(f.child),
    }),
  ).toThrow("retained");
  expect(f.persist).toHaveBeenLastCalledWith(
    expect.objectContaining({
      diagnostics: expect.objectContaining({ consumerIdentity: [99, 101] }),
      retained: true,
    }),
  );
  expect(f.remove).not.toHaveBeenCalled();
});

it.each(["missing", "foreign", "exact"])(
  "requires the exact nonce acknowledgement (%s)",
  async (mode) => {
    const f = acquiredFixture();
    const sendManifest = vi.fn();
    const acknowledgement = vi.fn(() =>
      mode === "missing"
        ? new Promise<string>(() => undefined)
        : Promise.resolve(mode === "exact" ? f.owned.diagnostics().nonce + "\n" : "foreign\n"),
    );
    const pending = runFamilyFixtureController({
      fixture: f.owned,
      deadlineMs: 100,
      sendManifest,
      acknowledgement,
      persist: f.persist,
    });
    const outcome = pending.then(
      () => "success",
      (error: unknown) => error,
    );
    f.child.reply({ pid: 99 });
    await vi.advanceTimersByTimeAsync(mode === "missing" ? 100 : 0);
    expect(sendManifest).toHaveBeenCalledWith(f.owned.manifest());
    expect(f.owned.diagnostics()).toMatchObject({
      supervisorIdentity: [42, 100],
      consumerIdentity: [99, 101],
    });
    if (mode === "exact") {
      expect(await outcome).toBe("success");
      disposeFamilyObserver(f.child);
    } else {
      expect(String(await outcome)).toContain("retained");
      expect(f.persist).toHaveBeenLastCalledWith(
        expect.objectContaining({ diagnostics: f.owned.diagnostics(), retained: true }),
      );
    }
    expect(f.child.unref).toHaveBeenCalledOnce();
    expect(f.remove).not.toHaveBeenCalled();
  },
);

it.each(["incomplete shared fixture", "parent-loss controller"])(
  "owns errors and disposes after acquisition persistence failure: %s",
  async (wrapper) => {
    const acquisitionFailure = new Error("acquisition persistence failed");
    const retentionFailure = new Error("retained record failed");
    const disposalFailure = new Error("stdout destroy failed");
    let writes = 0;
    const persist = vi.fn((_value: unknown) => {
      if (++writes === 2) throw acquisitionFailure;
    });
    const f = acquiredFixture(persist);
    expect(f.owned.child()).toBe(f.child);
    expect(f.owned.diagnostics()).toMatchObject({
      ...f.custody.requestBinding(f.owned.lifecycle.attempt),
      supervisorIdentity: [42, 100],
      consumerIdentity: null,
    });
    expect(() =>
      f.child.emit("error", new Error("spawn error after capture failure")),
    ).not.toThrow();
    vi.spyOn(f.child.stdout, "destroy").mockImplementation(() => {
      throw disposalFailure;
    });
    const retained = vi.fn((_value: unknown) => {
      throw retentionFailure;
    });
    let error: unknown;
    if (wrapper === "parent-loss controller") {
      error = await rejection(
        runFamilyFixtureController({
          fixture: f.owned,
          deadlineMs: 100,
          persist: retained,
          sendManifest: vi.fn(),
          acknowledgement: vi.fn(),
        }),
      );
    } else {
      const primary = await rejection(f.owned.ready(() => undefined));
      expect(primary).toBe(acquisitionFailure);
      try {
        retainFamilyFixtureFailure({
          diagnostics: f.owned.diagnostics(),
          primaryFailure: primary,
          persist: retained,
          disposeObserver: () => disposeFamilyObserver(f.child),
        });
      } catch (caught) {
        error = caught;
      }
    }
    expect(String(error)).toContain(JSON.stringify(f.owned.diagnostics()));
    const errors = ((error as Error).cause as AggregateError).errors;
    expect(errors.slice(0, 2)).toEqual([acquisitionFailure, retentionFailure]);
    expect((errors[2] as AggregateError).errors).toEqual([disposalFailure]);
    expect(String(error)).toContain("stdout destroy failed");
    expect(retained).toHaveBeenCalledWith(
      expect.objectContaining({ diagnostics: f.owned.diagnostics(), retained: true }),
    );
    expect(f.child.stderr.destroyed && f.child.control.destroyed).toBe(true);
    expect(f.child.connected).toBe(false);
    expect(f.child.unref).toHaveBeenCalledOnce();
    expect(() => f.child.emit("error", new Error("late child error"))).not.toThrow();
    expect(f.remove).not.toHaveBeenCalled();
  },
);

it.each(["invalid manifest", "received diagnostic write"])(
  "retains UNTRUSTED received binding before validation and always disposes: %s",
  async (mode) => {
    const f = acquiredFixture();
    const validationFailure = new Error("manifest binding invalid");
    const receivedFailure = new Error("received record failed");
    const retainedFailure = new Error("retained record failed");
    const disposalFailure = new Error("stdout destroy failed");
    const raw = JSON.stringify({
      manifest: { ...outerFixture().manifest, custodyRoot: "/foreign" },
    });
    const persist = vi.fn((_value: unknown) => {
      if (mode === "received diagnostic write") throw receivedFailure;
    });
    const validate = vi.fn(() => {
      throw validationFailure;
    });
    const receiver = makeFamilyFixtureReceiver({ persist, validate });
    let primary: unknown;
    try {
      receiver.receive(raw);
    } catch (error) {
      primary = error;
    }
    expect(primary).toBe(mode === "invalid manifest" ? validationFailure : receivedFailure);
    expect(persist).toHaveBeenCalledWith({ trust: "UNTRUSTED", raw });
    if (mode === "invalid manifest")
      expect(persist.mock.invocationCallOrder[0]).toBeLessThan(
        validate.mock.invocationCallOrder[0]!,
      );
    else expect(validate).not.toHaveBeenCalled();
    expect(persist).not.toHaveBeenCalledWith(expect.objectContaining({ trust: "validated" }));
    vi.spyOn(f.child.stdout, "destroy").mockImplementation(() => {
      throw disposalFailure;
    });
    let error: unknown;
    const manifestChannel = new NodeStream.PassThrough();
    const ackChannel = new NodeStream.PassThrough();
    const retained = vi.fn((_value: unknown) => {
      throw retainedFailure;
    });
    try {
      retainFamilyFixtureFailure({
        diagnostics: { ...binding, received: receiver.diagnostics(), manifest: null },
        primaryFailure: primary,
        persist: retained,
        disposeObserver: () => disposeFamilyObserver(f.child, [manifestChannel, ackChannel]),
      });
    } catch (caught) {
      error = caught;
    }
    const errors = ((error as Error).cause as AggregateError).errors;
    expect(errors.slice(0, 2)).toEqual([primary, retainedFailure]);
    expect((errors[2] as AggregateError).errors).toEqual([disposalFailure]);
    expect(String(error)).toContain(JSON.stringify(receiver.diagnostics()));
    expect(f.child.stderr.destroyed && f.child.control.destroyed).toBe(true);
    expect(f.child.connected).toBe(false);
    expect(manifestChannel.destroyed && ackChannel.destroyed).toBe(true);
    expect(f.child.unref).toHaveBeenCalledOnce();
    expect(f.remove).not.toHaveBeenCalled();
    expect(() => f.child.emit("error", new Error("late error"))).not.toThrow();
  },
);

it("retains bounded partial received diagnostics before another channel chunk or validation", () => {
  const persist = vi.fn();
  const validate = vi.fn();
  const receiver = makeFamilyFixtureReceiver({ persist, validate });
  const partial = '{"manifest":{"supervisorIdentity":[42,100]';
  receiver.capture(partial);
  expect(persist).toHaveBeenLastCalledWith({ trust: "UNTRUSTED", raw: partial });
  expect(validate).not.toHaveBeenCalled();
  expect(() => receiver.capture("x".repeat(8193))).toThrow("bound exceeded");
  expect(receiver.diagnostics()).toEqual({ trust: "UNTRUSTED", raw: partial });
});

it("preserves controller acquisition failure diagnostics at the outer receiver without accepting a manifest", () => {
  const f = acquiredFixture();
  const persist = vi.fn();
  const validate = vi.fn();
  const receiver = makeFamilyFixtureReceiver({ persist, validate });
  const diagnostic = f.owned.diagnostics();
  const raw = JSON.stringify({
    failure: "controller acquisition persistence failed",
    diagnostics: diagnostic,
  });
  let primary: unknown;
  try {
    receiver.receive(raw);
  } catch (error) {
    primary = error;
  }
  expect(String(primary)).toContain("controller acquisition persistence failed");
  expect(validate).not.toHaveBeenCalled();
  const retentionFailure = new Error("outer retained record failed");
  const retained = vi.fn((_value: unknown) => {
    throw retentionFailure;
  });
  let result: unknown;
  try {
    retainFamilyFixtureFailure({
      diagnostics: { ...binding, received: receiver.diagnostics(), manifest: null },
      primaryFailure: primary,
      persist: retained,
      disposeObserver: () => disposeFamilyObserver(f.child),
    });
  } catch (error) {
    result = error;
  }
  expect(((result as Error).cause as AggregateError).errors).toEqual([primary, retentionFailure]);
  expect(retained).toHaveBeenCalledWith(
    expect.objectContaining({
      diagnostics: { ...binding, received: { trust: "UNTRUSTED", raw }, manifest: null },
      retained: true,
    }),
  );
  expect(f.child.unref).toHaveBeenCalledOnce();
  expect(f.child.connected).toBe(false);
  expect(() => f.child.emit("error", new Error("late controller error"))).not.toThrow();
  expect(f.remove).not.toHaveBeenCalled();
});
