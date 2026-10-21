import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeEvents from "node:events";
import * as NodeStream from "node:stream";
import * as NodeReadline from "node:readline";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as HostProcess from "../../../packages/shared/src/HostProcess.ts";
import {
  assertVisible,
  assertInactiveDenied,
  assertBrowserDisabledDenied,
  atomicJson,
  redact,
  scenarioFromPrompt,
  createMilestoneChannel,
  publishMilestone,
  awaitMilestoneRelease,
  readAcknowledgedMilestone,
  runScenario,
  scenarioSecrets,
} from "./install-preview-agent.mjs";
import {
  hashTree,
  recordBuild,
  readBuildReceipt,
  isolatedEnvironment,
  readRendererManifest,
  serveRendererFixture,
  stopChild,
  withOwnedScratch,
  fixtureProxy,
  scenarioSteps,
  rendererSteps,
  indexedDbBrowserProof,
  waitInactive,
  waitProviderReady,
  openRpc,
  waitForFixtureStartup,
  issueFixturePairing,
} from "./install-preview-qualification.mjs";

const nativePlatform = HostProcess.Platform.defaultValue();

const source = NodePath.resolve(
  NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
  "../../..",
);

function startupChildFixture() {
  return Object.assign(new NodeEvents.EventEmitter(), {
    stdout: new NodeStream.PassThrough(),
    stderr: new NodeStream.PassThrough(),
  });
}

NodeTest.test(
  "fixture readiness uses the nonsecret issuance marker, never a printed token",
  async () => {
    const child = startupChildFixture();
    let ready = false;
    const startup = waitForFixtureStartup(child, "synthetic", undefined, 5_000).then((output) => {
      ready = true;
      return output;
    });
    child.stdout.write("Pairing URL: http://127.0.0.1:1234/pair#token=SYNTHETIC_OLD_TOKEN\n");
    await Promise.resolve();
    NodeAssert.equal(ready, false);
    child.stdout.write("Startup pairing credential issued; credential ");
    child.stdout.write("output withheld.\n");
    const output = await startup;
    NodeAssert.match(output(), /credential output withheld/);
    child.emit("exit", 0);
    NodeAssert.equal(child.stdout.listenerCount("data"), 0);
    NodeAssert.equal(child.stderr.listenerCount("data"), 0);
    NodeAssert.equal(child.listenerCount("exit"), 0);
  },
);

for (const outcome of ["exit", "error", "abort", "timeout"]) {
  NodeTest.test(
    `fixture startup ${outcome} keeps output private and removes listeners`,
    async () => {
      const child = startupChildFixture();
      const controller = new AbortController();
      const startup = waitForFixtureStartup(child, "synthetic", controller.signal, 20);
      child.stderr.write("SYNTHETIC_PRIVATE_DIAGNOSTIC\n");
      const rejected = NodeAssert.rejects(startup, (error) => {
        NodeAssert.match(error.message, /^Owned synthetic server /);
        NodeAssert.equal(JSON.stringify(error).includes("SYNTHETIC_PRIVATE"), false);
        NodeAssert.equal(error.message.includes("SYNTHETIC_PRIVATE"), false);
        return true;
      });
      if (outcome === "exit") child.emit("exit", 1);
      if (outcome === "error") child.emit("error", new Error("SYNTHETIC_PRIVATE_ERROR"));
      if (outcome === "abort") controller.abort(new Error("SYNTHETIC_PRIVATE_ABORT"));
      await rejected;
      NodeAssert.equal(child.stdout.listenerCount("data"), 0);
      NodeAssert.equal(child.stderr.listenerCount("data"), 0);
      NodeAssert.equal(child.listenerCount("exit"), 0);
      NodeAssert.equal(child.listenerCount("error"), 0);
    },
  );
}

async function withPairingCommandFixture(program, use) {
  let capturedRoot;
  try {
    return await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
      capturedRoot = root;
      const entry = NodePath.join(root, "apps/server/dist/bin.mjs");
      await NodeFSP.mkdir(NodePath.dirname(entry), { recursive: true });
      const env = isolatedEnvironment(root);
      await Promise.all([NodeFSP.mkdir(env.HOME), NodeFSP.mkdir(env.TMPDIR)]);
      await NodeFSP.writeFile(entry, program);
      const processes = [];
      try {
        return await use({
          source: root,
          owned: root,
          baseDir: NodePath.join(root, "t3"),
          processes,
        });
      } finally {
        await Promise.all(processes.map(stopChild));
        NodeAssert.ok(
          processes.every((child) => child.exitCode !== null || child.signalCode !== null),
        );
      }
    });
  } finally {
    if (capturedRoot) await NodeAssert.rejects(NodeFSP.lstat(capturedRoot), { code: "ENOENT" });
  }
}

NodeTest.test(
  "fixture explicitly pairs only its own base with administrative scopes and isolated environment",
  async () => {
    await withPairingCommandFixture(
      `import assert from "node:assert/strict";
     import path from "node:path";
     const args = process.argv.slice(2);
     assert.deepEqual(args.slice(0, 3), ["pair", "--base-dir", path.join(process.cwd(), "t3")]);
     assert.equal(process.env.HOME, path.join(process.cwd(), "home"));
     assert.equal(process.env.TMPDIR, path.join(process.cwd(), "tmp"));
     assert.equal(process.env.T3CODE_DESKTOP_USER_DATA_DIR, undefined);
     assert.equal(process.env.T3CODE_DEV_AUTH_TOKEN, undefined);
     assert.equal(process.env.T3CODE_REVEAL_STARTUP_PAIRING_CREDENTIAL, undefined);
     const scopes = [];
     for (let i = 3; i < args.length; i += 2) {
       assert.equal(args[i], "--scope");
       scopes.push(args[i + 1]);
     }
     assert.deepEqual(scopes.sort(), [
       "orchestration:read", "orchestration:operate", "settings:write", "providers:manage",
       "environment:maintain", "preview:operate", "diagnostics:read", "terminal:read",
       "terminal:operate", "source-control:write", "filesystem:read", "filesystem:write",
       "relay:read", "access:read", "access:write", "relay:write"
     ].sort());
     console.log("Token: SYNTHETIC_EXPLICIT_PAIR_TOKEN");`,
      async (options) => {
        NodeAssert.equal(await issueFixturePairing(options), "SYNTHETIC_EXPLICIT_PAIR_TOKEN");
        NodeAssert.equal(options.processes.length, 1);
      },
    );
  },
);

for (const outcome of ["failure", "malformed", "timeout", "abort"]) {
  NodeTest.test(
    `fixture pairing ${outcome} hides credentials and reaps its child`,
    { timeout: 10_000 },
    async () => {
      const program =
        outcome === "failure"
          ? 'console.log("Token: SYNTHETIC_PRIVATE_TOKEN"); console.error("SYNTHETIC_PRIVATE_ERROR"); process.exitCode = 1;'
          : outcome === "malformed"
            ? 'console.log("SYNTHETIC_PRIVATE_MALFORMED");'
            : 'console.log("Token: SYNTHETIC_PRIVATE_TOKEN"); setInterval(() => {}, 1000);';
      await withPairingCommandFixture(program, async (options) => {
        const controller = new AbortController();
        const pairing = issueFixturePairing({
          ...options,
          signal: controller.signal,
          timeoutMs: outcome === "timeout" ? 50 : 5_000,
        });
        const rejected = NodeAssert.rejects(pairing, (error) => {
          NodeAssert.match(
            error.message,
            /^Owned fixture pairing command (failed|returned invalid output)$/,
          );
          NodeAssert.equal(error.message.includes("SYNTHETIC_PRIVATE"), false);
          NodeAssert.equal(JSON.stringify(error).includes("SYNTHETIC_PRIVATE"), false);
          NodeAssert.equal(error.cause, undefined);
          return true;
        });
        if (outcome === "abort") {
          await NodeEvents.once(options.processes[0].stdout, "data");
          controller.abort(new Error("SYNTHETIC_PRIVATE_ABORT"));
        }
        await rejected;
        NodeAssert.equal(options.processes.length, 1);
        NodeAssert.ok(
          options.processes[0].exitCode !== null || options.processes[0].signalCode !== null,
        );
      });
    },
  );
}

NodeTest.test("fixture pairing refuses another base before spawning", async () => {
  const processes = [];
  await NodeAssert.rejects(
    issueFixturePairing({
      source: "/synthetic",
      owned: "/synthetic/owned",
      baseDir: "/synthetic/other/t3",
      processes,
    }),
    /Pair only the owned fixture base/,
  );
  NodeAssert.deepEqual(processes, []);
});

NodeTest.test(
  "native ACP framing retains the exact scenario through T3's instruction wrapper",
  () => {
    const scenario = { mode: "active", steps: [{ action: "wait", text: "Jones main" }] };
    const marker = `JONES_PREVIEW_SCENARIO:${Buffer.from(JSON.stringify(scenario)).toString("base64")}`;
    NodeAssert.deepEqual(
      scenarioFromPrompt([
        {
          type: "text",
          text: `<t3_code_instructions>\nNative context\n</t3_code_instructions>\n\n<user_request>\n${marker}\n</user_request>`,
        },
      ]),
      scenario,
    );
    NodeAssert.throws(
      () => scenarioFromPrompt([{ type: "text", text: JSON.stringify(scenario) }]),
      /scenario marker/,
    );
  },
);

NodeTest.test("a deliberately wrong renderer assertion cannot pass", () => {
  const snapshot = { visibleText: "Jones main · Setup required" };
  assertVisible(snapshot, "Setup required");
  assertVisible(snapshot, "Committed", true);
  NodeAssert.throws(() => assertVisible(snapshot, "Committed"), /Missing visible text/);
  NodeAssert.throws(
    () => assertVisible(snapshot, "Setup required", true),
    /Unexpected visible text/,
  );
  NodeAssert.throws(
    () => assertVisible({ screenshotPath: "/owned/image.png" }, "Setup required"),
    /Missing visible text/,
  );
});

function shellSubscriptionFixture(expectedTag = "orchestration.subscribeShell") {
  let onItem;
  let resolveExit, rejectExit;
  let interrupts = 0;
  let settled = false;
  const result = new Promise((resolve, reject) => {
    resolveExit = resolve;
    rejectExit = reject;
  });
  return {
    rpc: {
      call(tag, payload, receive) {
        NodeAssert.equal(tag, expectedTag);
        NodeAssert.deepEqual(payload, {});
        NodeAssert.equal(onItem, undefined);
        onItem = receive;
        return {
          result,
          interrupt: () => {
            if (!settled) interrupts += 1;
          },
        };
      },
    },
    emit: (item) => onItem(item),
    snapshot: (...threads) =>
      onItem({ kind: "snapshot", snapshot: { threads, archivedThreads: [] } }),
    update: (thread) => onItem({ kind: "thread.updated", thread }),
    fail: (error) => {
      settled = true;
      rejectExit(error);
    },
    end: () => {
      settled = true;
      resolveExit();
    },
    acknowledgeInterrupt: () => {
      NodeAssert.equal(interrupts, 1);
      settled = true;
      rejectExit(
        Object.assign(new Error("Native RPC interrupted"), { nativeRpcInterrupted: true }),
      );
    },
    get interrupts() {
      return interrupts;
    },
  };
}

NodeTest.test(
  "initial idle shell waits for native interrupt completion before release",
  async () => {
    const fixture = shellSubscriptionFixture();
    let completed = false;
    const waiting = waitInactive(fixture.rpc, "owned-thread").then((value) => {
      completed = true;
      return value;
    });
    fixture.snapshot({ id: "owned-thread", activeRunId: null });
    await Promise.resolve();
    NodeAssert.equal(fixture.interrupts, 1);
    NodeAssert.equal(completed, false);
    fixture.acknowledgeInterrupt();
    NodeAssert.deepEqual(await waiting, { thread: { id: "owned-thread", activeRunId: null } });
  },
);

NodeTest.test(
  "shell inactivity ignores unrelated updates and observes the matching active-to-idle transition",
  async () => {
    const fixture = shellSubscriptionFixture();
    const waiting = waitInactive(fixture.rpc, "owned-thread");
    fixture.snapshot(
      { id: "other", activeRunId: null },
      { id: "owned-thread", activeRunId: "run:active" },
    );
    fixture.update({ id: "other", activeRunId: null });
    fixture.emit({ kind: "synchronized" });
    await Promise.resolve();
    NodeAssert.equal(fixture.interrupts, 0);
    fixture.update({ id: "owned-thread", activeRunId: null });
    await Promise.resolve();
    fixture.acknowledgeInterrupt();
    NodeAssert.equal((await waiting).thread.activeRunId, null);
  },
);

NodeTest.test(
  "missing, inherited and invalid activeRunId never count as native inactivity",
  async () => {
    for (const thread of [
      { id: "owned-thread" },
      Object.assign(Object.create({ activeRunId: null }), { id: "owned-thread" }),
      { id: "owned-thread", activeRunId: "" },
      { id: "owned-thread", activeRunId: 0 },
    ]) {
      const fixture = shellSubscriptionFixture();
      const rejected = NodeAssert.rejects(
        waitInactive(fixture.rpc, "owned-thread"),
        /missing a valid activeRunId/,
      );
      fixture.snapshot(thread);
      await Promise.resolve();
      fixture.acknowledgeInterrupt();
      await rejected;
    }
  },
);

NodeTest.test(
  "native shell failure, deadline and cancellation settle without another subscription",
  async () => {
    for (const reason of [
      "Native RPC socket closed",
      "Native RPC timed out: orchestration.subscribeShell",
      "Owned qualification cancelled",
    ]) {
      const fixture = shellSubscriptionFixture();
      const original = new Error(reason);
      const rejected = NodeAssert.rejects(
        waitInactive(fixture.rpc, "owned-thread"),
        (error) => error === original,
      );
      fixture.snapshot({ id: "owned-thread", activeRunId: "run:active" });
      fixture.fail(original);
      await rejected;
      NodeAssert.equal(fixture.interrupts, 0);
    }
    const fixture = shellSubscriptionFixture();
    const rejected = NodeAssert.rejects(
      waitInactive(fixture.rpc, "owned-thread"),
      /ended before inactivity/,
    );
    fixture.end();
    await rejected;
    NodeAssert.equal(fixture.interrupts, 0);
  },
);

NodeTest.test(
  "native subscription cleanup failures cannot produce successful inactivity or replace a prior error",
  async () => {
    for (const malformed of [false, true]) {
      const fixture = shellSubscriptionFixture();
      const cleanup = new Error("Native finalizer failed");
      const rejected = NodeAssert.rejects(waitInactive(fixture.rpc, "owned-thread"), (error) => {
        if (malformed) {
          NodeAssert.ok(error instanceof AggregateError);
          NodeAssert.match(error.errors[0].message, /missing a valid activeRunId/);
          NodeAssert.equal(error.errors[1], cleanup);
        } else NodeAssert.equal(error, cleanup);
        return true;
      });
      fixture.snapshot(
        malformed ? { id: "owned-thread" } : { id: "owned-thread", activeRunId: null },
      );
      await Promise.resolve();
      NodeAssert.equal(fixture.interrupts, 1);
      fixture.fail(cleanup);
      await rejected;
    }
  },
);

const ownedPreviewProvider = {
  instanceId: "preview-browser-disabled",
  driver: "acpRegistry",
  enabled: true,
};

NodeTest.test(
  "native provider readiness accepts an existing instance only after subscription drain",
  async () => {
    for (const availability of [undefined, "available"]) {
      const fixture = shellSubscriptionFixture("subscribeServerConfig");
      let settled = false;
      const waiting = waitProviderReady(fixture.rpc, ownedPreviewProvider.instanceId).then(
        (value) => {
          settled = true;
          return value;
        },
      );
      fixture.emit({
        type: "snapshot",
        config: { providers: [{ ...ownedPreviewProvider, availability }] },
      });
      await Promise.resolve();
      NodeAssert.equal(fixture.interrupts, 1);
      NodeAssert.equal(settled, false);
      fixture.acknowledgeInterrupt();
      NodeAssert.deepEqual(await waiting, ownedPreviewProvider);
    }
  },
);

NodeTest.test(
  "native provider launch remains held through absent and unrelated registration events",
  async () => {
    const fixture = shellSubscriptionFixture("subscribeServerConfig");
    let launches = 0;
    const launched = waitProviderReady(fixture.rpc, ownedPreviewProvider.instanceId).then(() => {
      launches += 1;
    });
    fixture.emit({ type: "snapshot", config: { providers: [] } });
    fixture.emit({
      type: "settingsUpdated",
      payload: {
        settings: {
          providerInstances: { [ownedPreviewProvider.instanceId]: ownedPreviewProvider },
        },
      },
    });
    fixture.emit({
      type: "providerStatuses",
      payload: { providers: [{ ...ownedPreviewProvider, instanceId: "preview-active" }] },
    });
    await Promise.resolve();
    NodeAssert.equal(launches, 0);
    NodeAssert.equal(fixture.interrupts, 0);
    fixture.emit({ type: "providerStatuses", payload: { providers: [ownedPreviewProvider] } });
    await Promise.resolve();
    NodeAssert.equal(launches, 0);
    NodeAssert.equal(fixture.interrupts, 1);
    fixture.acknowledgeInterrupt();
    await launched;
    NodeAssert.equal(launches, 1);
  },
);

NodeTest.test(
  "native provider readiness rejects unavailable, disabled, wrong-driver and malformed observations",
  async () => {
    for (const [providers, message] of [
      [[{ ...ownedPreviewProvider, availability: "unavailable" }], /unavailable/],
      [[{ ...ownedPreviewProvider, availability: "unknown" }], /unavailable/],
      [[{ ...ownedPreviewProvider, driver: "codex" }], /wrong driver/],
      [[{ ...ownedPreviewProvider, enabled: false }], /disabled/],
      [[{ ...ownedPreviewProvider, enabled: undefined }], /disabled/],
      [[ownedPreviewProvider, ownedPreviewProvider], /duplicate/],
      [undefined, /missing providers/],
    ]) {
      const fixture = shellSubscriptionFixture("subscribeServerConfig");
      const rejected = NodeAssert.rejects(
        waitProviderReady(fixture.rpc, ownedPreviewProvider.instanceId),
        message,
      );
      fixture.emit({ type: "snapshot", config: { providers } });
      await Promise.resolve();
      NodeAssert.equal(fixture.interrupts, 1);
      fixture.acknowledgeInterrupt();
      await rejected;
    }
  },
);

NodeTest.test(
  "native provider readiness preserves failure, deadline, cancellation and premature end",
  async () => {
    for (const reason of [
      "Native provider stream failed",
      "Native RPC timed out: subscribeServerConfig",
      "Owned qualification cancelled",
    ]) {
      const fixture = shellSubscriptionFixture("subscribeServerConfig");
      const original = new Error(reason);
      const rejected = NodeAssert.rejects(
        waitProviderReady(fixture.rpc, ownedPreviewProvider.instanceId),
        (error) => error === original,
      );
      fixture.emit({ type: "snapshot", config: { providers: [] } });
      fixture.fail(original);
      await rejected;
      NodeAssert.equal(fixture.interrupts, 0);
    }
    const fixture = shellSubscriptionFixture("subscribeServerConfig");
    const rejected = NodeAssert.rejects(
      waitProviderReady(fixture.rpc, ownedPreviewProvider.instanceId),
      /ended before provider readiness/,
    );
    fixture.end();
    await rejected;
    NodeAssert.equal(fixture.interrupts, 0);
  },
);

NodeTest.test(
  "native provider cleanup failure prevents launch and preserves an earlier readiness failure",
  async () => {
    for (const disabled of [false, true]) {
      const fixture = shellSubscriptionFixture("subscribeServerConfig");
      const cleanup = new Error("Native provider subscription finalizer failed");
      const rejected = NodeAssert.rejects(
        waitProviderReady(fixture.rpc, ownedPreviewProvider.instanceId),
        (error) => {
          if (disabled) {
            NodeAssert.ok(error instanceof AggregateError);
            NodeAssert.match(error.errors[0].message, /disabled/);
            NodeAssert.equal(error.errors[1], cleanup);
          } else NodeAssert.equal(error, cleanup);
          return true;
        },
      );
      fixture.emit({
        type: "snapshot",
        config: { providers: [{ ...ownedPreviewProvider, enabled: !disabled }] },
      });
      await Promise.resolve();
      NodeAssert.equal(fixture.interrupts, 1);
      fixture.fail(cleanup);
      await rejected;
    }
  },
);

NodeTest.test(
  "RPC interruption classification uses actual encoded Exit reasons and drains the request",
  async (t) => {
    for (const cause of [
      [{ _tag: "Interrupt", fiberId: null }],
      [
        { _tag: "Interrupt", fiberId: 1 },
        { _tag: "Die", defect: "failed cleanup" },
      ],
      [],
    ]) {
      const sent = [];
      let socket;
      const mock = t.mock.method(globalThis, "WebSocket", function () {
        socket = new EventTarget();
        socket.send = (value) => sent.push(JSON.parse(value));
        socket.close = () => socket.dispatchEvent(new Event("close"));
        queueMicrotask(() => socket.dispatchEvent(new Event("open")));
        return socket;
      });
      let rpc;
      try {
        rpc = await openRpc("ws://owned-fixture.invalid", new AbortController().signal);
        const subscription = rpc.call("orchestration.subscribeShell", {}, () => {});
        const requestId = sent[0].id;
        const rejected = NodeAssert.rejects(subscription.result, (error) => {
          NodeAssert.equal(error.nativeRpcInterrupted, cause.length === 1);
          return true;
        });
        subscription.interrupt();
        NodeAssert.deepEqual(sent[1], { _tag: "Interrupt", requestId });
        socket.dispatchEvent(
          new MessageEvent("message", {
            data: JSON.stringify({
              _tag: "Exit",
              requestId,
              exit: { _tag: "Failure", cause },
            }),
          }),
        );
        await rejected;
        subscription.interrupt();
        NodeAssert.equal(sent.length, 2);
      } finally {
        await rpc?.close();
        mock.mock.restore();
      }
    }
  },
);

// A deterministic IDB event model checks the emitted expression's guards and cleanup;
// only the hosted native Preview scenario establishes actual browser compatibility.
function indexedDbExpressionFixture({ disabledEnvironmentIds = ["synthetic-remote"] } = {}) {
  const runtimeName = "t3code:connection-runtime";
  const hintName = "t3code:connection-compatibility-hints";
  const catalog = JSON.stringify({
    schemaVersion: 1,
    targets: [
      {
        _tag: "BearerConnectionTarget",
        environmentId: "synthetic-remote",
        connectionId: "synthetic-connection",
        label: "Synthetic incompatible Jones host",
      },
    ],
    profiles: [
      {
        _tag: "BearerConnectionProfile",
        environmentId: "synthetic-remote",
        connectionId: "synthetic-connection",
        httpBaseUrl: "http://127.0.0.1:4100",
      },
    ],
    disabledEnvironmentIds,
    credentials: ["DO-NOT-PUBLISH"],
  });
  const data = new Map([
    [runtimeName, { version: 4, stores: new Map([["catalog", catalog]]) }],
    [
      hintName,
      {
        version: 1,
        stores: new Map([
          [
            "compatibility-hints",
            [
              {
                environmentId: "synthetic-remote",
                routeBinding: "DO-NOT-PUBLISH",
                kind: "outdated-update",
              },
            ],
          ],
        ]),
      },
    ],
  ]);
  const handles = new Set();
  const events = [];
  const window = new EventTarget();
  let deferOpen = false;
  let failUpgrade = false;
  let failDelete = false;
  const pending = [];
  const database = (name, entry) => {
    const handle = {
      name,
      version: entry.version,
      closed: false,
      objectStoreNames: { contains: (store) => entry.stores.has(store) },
      createObjectStore: (store) => entry.stores.set(store, []),
      close() {
        handle.closed = true;
      },
      transaction(store) {
        if (handle.closed) throw Object.assign(new Error("closed"), { name: "InvalidStateError" });
        if (!entry.stores.has(store)) throw new Error("missing store");
        let aborted = false;
        const tx = {
          abort: () => {
            aborted = true;
            queueMicrotask(() => tx.onabort?.());
          },
          objectStore: () => ({
            get: () => request(),
            getAll: () => request(),
            put: (value, key) => {
              NodeAssert.equal(key, "document");
              entry.stores.set(store, value);
            },
          }),
        };
        const request = () => {
          const result = { result: entry.stores.get(store) };
          queueMicrotask(() => {
            result.onsuccess?.();
            if (!aborted) tx.oncomplete?.();
          });
          return result;
        };
        return tx;
      },
    };
    handles.add(handle);
    return handle;
  };
  const indexedDB = {
    databases: async () => [...data].map(([name, entry]) => ({ name, version: entry.version })),
    open(name, version) {
      events.push(["open", name, version]);
      const request = {};
      const complete = () => {
        if (failUpgrade && version === 5) {
          request.error = { name: "AbortError" };
          request.onerror?.();
          return;
        }
        const previous = data.get(name);
        const oldVersion = previous?.version ?? 0;
        if (oldVersion > version) {
          request.error = { name: "VersionError" };
          request.onerror?.();
          return;
        }
        if (oldVersion < version) {
          for (const handle of handles)
            if (handle.name === name && !handle.closed) handle.onversionchange?.();
          if ([...handles].some((handle) => handle.name === name && !handle.closed)) {
            request.onblocked?.();
            return;
          }
        }
        const entry = previous ?? { version, stores: new Map() };
        const result = database(name, entry);
        request.result = result;
        let aborted = false;
        request.transaction = {
          abort: () => {
            aborted = true;
          },
        };
        if (oldVersion < version) request.onupgradeneeded?.({ oldVersion });
        if (aborted) {
          result.close();
          request.error = { name: "AbortError" };
          request.onerror?.();
          return;
        }
        entry.version = version;
        result.version = version;
        data.set(name, entry);
        request.onsuccess?.();
      };
      if (deferOpen) pending.push(complete);
      else queueMicrotask(complete);
      return request;
    },
    deleteDatabase(name) {
      events.push(["delete", name]);
      const request = {};
      queueMicrotask(() => {
        if (failDelete) {
          request.onerror?.();
          return;
        }
        if ([...handles].some((handle) => handle.name === name && !handle.closed))
          request.onblocked?.();
        else {
          data.delete(name);
          request.onsuccess?.();
        }
      });
      return request;
    },
  };
  const run = new Function(
    "window",
    "indexedDB",
    "crypto",
    "phase",
    "syntheticOrigin",
    `return (${indexedDbBrowserProof.toString()})(phase, syntheticOrigin)`,
  );
  return {
    run: (phase, syntheticOrigin = "http://127.0.0.1:4100") =>
      run(window, indexedDB, { randomUUID: () => "owned-negative" }, phase, syntheticOrigin),
    data,
    handles,
    events,
    window,
    runtimeName,
    hintName,
    failUpgrade: () => {
      failUpgrade = true;
    },
    failDelete: () => {
      failDelete = true;
    },
    delayOpen: () => {
      deferOpen = true;
    },
    releaseOpen: () => {
      deferOpen = false;
      for (const complete of pending.splice(0)) complete();
    },
  };
}

NodeTest.test(
  "native IDB scenario holds v4 across real pairing and check, then releases before hard reload",
  () => {
    const steps = scenarioSteps("http://fixture.invalid/pair#token=DO-NOT-PUBLISH");
    const at = (label) => steps.findIndex((step) => step.label === label);
    const hold = at("Hold the actual legacy version-4 catalog connection");
    const checkpoint = at("Capture the saved Off catalog before the authenticated check");
    const verify = at("New hint persistence preserves the held legacy catalog");
    const close = at("Release the legacy catalog handle before hard reload");
    NodeAssert.ok(at("Native IndexedDB detects the legacy upgrade hazard") < hold);
    NodeAssert.ok(hold < steps.findIndex((step) => step.action === "type"));
    NodeAssert.ok(checkpoint > steps.findIndex((step) => step.text === "Source matches this app."));
    NodeAssert.ok(
      checkpoint <
        steps.findIndex((step) => step.locator === 'button:enabled:text-is("Check for builds")'),
    );
    NodeAssert.ok(
      verify > steps.findIndex((step) => step.text === "Synthetic authenticated check completed."),
    );
    NodeAssert.ok(close > verify);
    NodeAssert.deepEqual(steps[close + 1], { action: "navigate", path: "/settings/connections" });
    const seed = at("Save Off for the exact isolated synthetic target");
    const seededHold = at("Hold the saved Off version-4 catalog connection");
    NodeAssert.ok(hold < seed && seed < seededHold && seededHold < checkpoint);
    NodeAssert.deepEqual(steps[seed + 2], { action: "navigate", path: "/settings/connections" });
    NodeAssert.equal(steps.filter((step) => step.action === "navigate").length, 3);
    NodeAssert.ok(steps.some((step) => step.name === "saved-connection-reload"));
  },
);

NodeTest.test(
  "saved Off seeding changes only the isolated target preference before checkpoint",
  async () => {
    const fixture = indexedDbExpressionFixture({ disabledEnvironmentIds: [] });
    const store = fixture.data.get(fixture.runtimeName).stores;
    const before = JSON.parse(store.get("catalog"));
    await fixture.run("hold");
    NodeAssert.deepEqual(await fixture.run("seed-off"), { savedOffFixtureSeeded: true });
    NodeAssert.deepEqual(JSON.parse(store.get("catalog")), {
      ...before,
      disabledEnvironmentIds: ["synthetic-remote"],
    });
    await fixture.run("close");
    await fixture.run("hold");
    await fixture.run("checkpoint");
    NodeAssert.ok(Object.values(await fixture.run("verify")).every((value) => value === true));
    await fixture.run("close");
    NodeAssert.ok([...fixture.handles].every((handle) => handle.closed));
  },
);

NodeTest.test(
  "saved Off seeding refuses nonmatching or ambiguous synthetic catalogs without writes",
  async () => {
    for (const mutate of [
      (catalog) => {
        catalog.targets[0].label = "Unrelated host";
      },
      (catalog) => {
        catalog.targets.push({ ...catalog.targets[0] });
      },
      (catalog) => {
        catalog.profiles[0].httpBaseUrl = "https://unrelated.invalid";
      },
      (catalog) => {
        catalog.profiles[0].environmentId = "other";
      },
      (catalog) => {
        catalog.disabledEnvironmentIds = ["other"];
      },
    ]) {
      const fixture = indexedDbExpressionFixture({ disabledEnvironmentIds: [] });
      const store = fixture.data.get(fixture.runtimeName).stores;
      const catalog = JSON.parse(store.get("catalog"));
      mutate(catalog);
      const before = JSON.stringify(catalog);
      store.set("catalog", before);
      await fixture.run("hold");
      await NodeAssert.rejects(
        fixture.run("seed-off"),
        /exactly one newly paired synthetic target/,
      );
      NodeAssert.equal(store.get("catalog"), before);
      NodeAssert.ok([...fixture.handles].every((handle) => handle.closed));
    }
    const fixture = indexedDbExpressionFixture({ disabledEnvironmentIds: [] });
    const before = fixture.data.get(fixture.runtimeName).stores.get("catalog");
    await fixture.run("hold");
    await NodeAssert.rejects(
      fixture.run("seed-off", "https://unrelated.invalid"),
      /exact synthetic origin/,
    );
    NodeAssert.equal(fixture.data.get(fixture.runtimeName).stores.get("catalog"), before);
    NodeAssert.ok([...fixture.handles].every((handle) => handle.closed));
  },
);

NodeTest.test(
  "IDB proof emits only booleans and detects a real-shaped version upgrade with exact cleanup",
  async () => {
    const fixture = indexedDbExpressionFixture();
    NodeAssert.deepEqual(await fixture.run("upgrade-control"), {
      versionChangeObserved: true,
      legacyConnectionClosed: true,
      oldVersionRejected: true,
      temporaryDatabaseRemoved: true,
    });
    NodeAssert.deepEqual(
      fixture.events.filter(([action]) => action === "delete"),
      [["delete", "jones-install-preview-idb-owned-negative"]],
    );
    NodeAssert.deepEqual(await fixture.run("hold"), { legacyV4Held: true });
    await fixture.run("checkpoint");
    const result = await fixture.run("verify");
    NodeAssert.ok(Object.values(result).every((value) => value === true));
    NodeAssert.ok(!JSON.stringify(result).includes("DO-NOT-PUBLISH"));
    NodeAssert.deepEqual(await fixture.run("close"), { legacyProofReleased: true });
    NodeAssert.ok([...fixture.handles].every((handle) => handle.closed));
    NodeAssert.equal(fixture.window.__jonesLegacyIndexedDbProof, undefined);
  },
);

NodeTest.test(
  "IDB proof rejects closed or mutated catalogs and missing, wrong-version or empty hint stores",
  async () => {
    for (const fault of ["closed", "changed", "absent", "version", "empty", "wrong-environment"]) {
      const fixture = indexedDbExpressionFixture();
      await fixture.run("hold");
      await fixture.run("checkpoint");
      if (fault === "closed") fixture.window.__jonesLegacyIndexedDbProof.database.onversionchange();
      if (fault === "changed")
        fixture.data.get(fixture.runtimeName).stores.set("catalog", "changed");
      if (fault === "absent") fixture.data.delete(fixture.hintName);
      if (fault === "version") fixture.data.get(fixture.hintName).version = 2;
      if (fault === "empty")
        fixture.data.get(fixture.hintName).stores.set("compatibility-hints", []);
      if (fault === "wrong-environment")
        fixture.data
          .get(fixture.hintName)
          .stores.set("compatibility-hints", [
            { environmentId: "other", kind: "outdated-update", routeBinding: "other" },
          ]);
      try {
        if (fault === "closed") await NodeAssert.rejects(fixture.run("verify"));
        else {
          const result = await fixture.run("verify");
          NodeAssert.ok(Object.values(result).includes(false), fault);
          if (fault === "absent" || fault === "version") {
            NodeAssert.ok(
              !fixture.events.some(
                ([action, name]) => action === "open" && name === fixture.hintName,
              ),
            );
          }
        }
      } finally {
        fixture.window.dispatchEvent(new Event("pagehide"));
      }
      NodeAssert.ok([...fixture.handles].every((handle) => handle.closed));
      NodeAssert.equal(fixture.window.__jonesLegacyIndexedDbProof, undefined);
    }
  },
);

NodeTest.test(
  "IDB pagehide closes late handles and a colliding control name is never deleted",
  async () => {
    const fixture = indexedDbExpressionFixture();
    fixture.delayOpen();
    const held = fixture.run("hold");
    await Promise.resolve();
    fixture.window.dispatchEvent(new Event("pagehide"));
    fixture.releaseOpen();
    await NodeAssert.rejects(held, /proof ended/);
    NodeAssert.ok([...fixture.handles].every((handle) => handle.closed));
    NodeAssert.equal(fixture.window.__jonesLegacyIndexedDbProof, undefined);
    const collision = indexedDbExpressionFixture();
    collision.data.set("jones-install-preview-idb-owned-negative", {
      version: 4,
      stores: new Map(),
    });
    await NodeAssert.rejects(collision.run("upgrade-control"), /already exists/);
    NodeAssert.ok(collision.data.has("jones-install-preview-idb-owned-negative"));
    NodeAssert.ok(!collision.events.some(([action]) => action === "delete"));
  },
);

NodeTest.test(
  "IDB control failure closes handles, preserves its error, and reports deletion failure",
  async () => {
    for (const cleanupFails of [false, true]) {
      const fixture = indexedDbExpressionFixture();
      fixture.failUpgrade();
      if (cleanupFails) fixture.failDelete();
      await NodeAssert.rejects(fixture.run("upgrade-control"), (error) => {
        if (cleanupFails) {
          NodeAssert.ok(error instanceof AggregateError);
          NodeAssert.equal(error.errors[0].name, "AbortError");
          NodeAssert.match(error.errors[1].message, /cleanup failed/);
        } else NodeAssert.equal(error.name, "AbortError");
        return true;
      });
      NodeAssert.ok([...fixture.handles].every((handle) => handle.closed));
      NodeAssert.equal(fixture.data.has("jones-install-preview-idb-owned-negative"), cleanupFails);
      NodeAssert.ok(fixture.data.has(fixture.runtimeName));
      NodeAssert.ok(fixture.data.has(fixture.hintName));
    }
    for (const version of [null, 5]) {
      const fixture = indexedDbExpressionFixture();
      if (version === null) fixture.data.delete(fixture.runtimeName);
      else fixture.data.get(fixture.runtimeName).version = version;
      await NodeAssert.rejects(fixture.run("hold"), /must remain version 4/);
      NodeAssert.equal(fixture.events.length, 0);
      NodeAssert.equal(fixture.window.__jonesLegacyIndexedDbProof, undefined);
    }
  },
);

NodeTest.test(
  "saved-host assertion requires the Base UI switch to be present, off and ARIA-disabled",
  () => {
    const assertions = scenarioSteps("http://127.0.0.1/pair#token=SYNTHETIC").filter(
      (step) => step.label === "Update access must preserve the saved disabled host",
    );
    NodeAssert.equal(assertions.length, 3);
    const shape = (checked, disabled) => ({
      getAttribute: (name) =>
        ({ "aria-checked": checked, "aria-disabled": disabled })[name] ?? null,
    });
    for (const step of assertions) {
      const read = (toggle) =>
        new Function("document", `return ${step.expression}`)({
          querySelector: (selector) => {
            NodeAssert.equal(
              selector,
              '[role="switch"][aria-label="Switch on Synthetic incompatible Jones host"]',
            );
            return toggle;
          },
        });
      const disabledSpan = shape("false", "true");
      NodeAssert.equal(disabledSpan.disabled, undefined);
      NodeAssert.deepEqual(JSON.parse(JSON.stringify(read(disabledSpan))), step.expected);
      for (const invalid of [
        null,
        shape("false", null),
        shape("false", "false"),
        shape("true", "true"),
        shape(null, "true"),
      ]) {
        NodeAssert.throws(() => NodeAssert.deepEqual(read(invalid), step.expected));
      }
    }
  },
);

NodeTest.test(
  "pending install assertion selects one disabled Install control despite copy buttons",
  () => {
    const assertions = rendererSteps("http://fixture.invalid", "a".repeat(40), "synthetic").filter(
      (step) => step.label === "Both production controls refuse duplicate pending installation",
    );
    NodeAssert.equal(assertions.length, 2);
    const copy = { textContent: "1020·1713", disabled: false };
    const install = (disabled) => ({ textContent: " Install ", disabled });
    const sidebar = (disabled) => ({ getAttribute: () => disabled });
    for (const step of assertions) {
      const read = (settingsButtons, sidebarButtons) =>
        new Function("document", "window", `return ${step.expression}`)(
          {
            querySelectorAll: (selector) => {
              if (selector === '[data-testid="settings-updates"] button') return settingsButtons;
              NodeAssert.equal(
                selector,
                '[data-testid="sidebar-updates"] button[aria-label]:not([aria-label="Nightly update release notes"])',
              );
              return sidebarButtons;
            },
          },
          { __jonesInstallPreview: { snapshot: () => ({}) } },
        );
      NodeAssert.equal(read([copy, install(true)], [sidebar("true")]), true);
      for (const [settingsButtons, sidebarButtons] of [
        [[copy, install(false)], [sidebar("true")]],
        [[copy], [sidebar("true")]],
        [[copy, install(true), install(true)], [sidebar("true")]],
        [[copy, install(true)], [sidebar("false")]],
        [[copy, install(true)], []],
        [
          [copy, install(true)],
          [sidebar("true"), sidebar("true")],
        ],
      ])
        NodeAssert.equal(read(settingsButtons, sidebarButtons), false);
    }
  },
);

NodeTest.test(
  "notice assertions require short labels with exact full source and version details",
  () => {
    const steps = rendererSteps("http://fixture.invalid", "d".repeat(40), "synthetic");
    const labels = [
      "Initial preview pair renders the expected full source and abbreviation",
      "Changed full source retains the same displayed abbreviation",
      "Stable guidance retains full version details and update-or-reload instructions",
    ];
    for (const label of labels) {
      const step = steps.find((item) => item.label === label);
      NodeAssert.ok(step);
      const sha = "a".repeat(39) + (label.startsWith("Changed") ? "c" : "b");
      const version = label.startsWith("Stable") ? "0.1.0" : "0.0.45-preview.20261011.1.1";
      const badge = (source = sha, fullVersion = version, text = sha.slice(0, 7)) => ({
        textContent: text,
        getAttribute: () => `Build time not recorded\n${fullVersion}\nsource ${source}`,
      });
      const read = (badges) =>
        new Function("document", "window", `return ${step.expression}`)(
          {
            querySelector: (selector) => ({
              textContent: selector.includes("notice-server")
                ? `Synthetic server ${version}; source ${sha}`
                : "Client ddddddd; Server aaaaaaa. Update this app or reload an updated web client.",
            }),
            querySelectorAll: (selector) => {
              NodeAssert.equal(selector, '[data-testid="notice-banners"] [aria-label]');
              return badges;
            },
          },
          { __jonesInstallPreview: { snapshot: () => ({}) } },
        );
      NodeAssert.equal(read([badge()]), true);
      for (const invalid of [
        [],
        [badge(), badge()],
        [badge(sha.slice(0, -1) + "e")],
        [badge(sha, version + "-other")],
        [badge(sha, version, "wrong")],
      ])
        NodeAssert.equal(read(invalid), false);
    }
  },
);

NodeTest.test(
  "a failed controlled page action is captured before native tab closure without retry",
  async () => {
    const failure = new Error(
      "Preview automation waitFor timed out; reconcile via preview_snapshot before repeating",
    );
    const calls = [];
    const snapshot = { visibleText: "Could not add backend", screenshotPath: "/owned/failure.png" };
    let waits = 0;
    const client = {
      callTool: async ({ name }) => {
        calls.push(name);
        if (name === "preview_wait_for" && ++waits === 2) throw failure;
        return {
          structuredContent:
            name === "preview_open"
              ? { tabId: "owned-tab" }
              : name === "preview_snapshot"
                ? snapshot
                : {},
        };
      },
    };
    const report = { calls: [], captures: [], negativeControls: {} };
    await NodeAssert.rejects(
      runScenario(
        client,
        {
          origin: "http://127.0.0.1",
          pairingUrl: "http://127.0.0.1/pair",
          initialText: "Project",
          steps: [{ action: "wait", text: "Source matches this app." }],
        },
        report,
      ),
      (error) => error === failure,
    );
    NodeAssert.deepEqual(calls.slice(-3), [
      "preview_wait_for",
      "preview_snapshot",
      "t3_preview_close",
    ]);
    NodeAssert.equal(waits, 2);
    NodeAssert.deepEqual(report.failedAction, {
      index: 0,
      action: "wait",
      text: "Source matches this app.",
    });
    NodeAssert.equal(report.calls.filter((call) => call.ok === false).length, 1);
    NodeAssert.deepEqual(report.captures, []);
    NodeAssert.equal(report.failureReconciliation.kind, "text-only");
    NodeAssert.equal(report.failureReconciliation.contentOmitted, true);
    NodeAssert.equal(report.controlledPageReconciled, true);
    NodeAssert.equal(report.nativeTabClosed, true);
  },
);

NodeTest.test(
  "inactive-provider qualification rejects unrelated failures and successful calls",
  async () => {
    for (const response of [
      undefined,
      { isError: false },
      { isError: true, content: [{ type: "text", text: "browser crashed" }] },
    ]) {
      await NodeAssert.rejects(assertInactiveDenied({ callTool: async () => response }));
    }
    await NodeAssert.rejects(
      assertInactiveDenied({
        callTool: async () => {
          throw new Error("socket closed");
        },
      }),
    );
    await NodeAssert.rejects(
      assertInactiveDenied({
        callTool: async () => {
          throw new Error("preview failed after 4010 milliseconds");
        },
      }),
    );
    NodeAssert.equal(
      await assertInactiveDenied({
        callTool: async () => ({
          isError: true,
          content: [{ type: "text", text: "parent_not_active" }],
        }),
      }),
      "tool authorization refused",
    );
    NodeAssert.equal(
      await assertInactiveDenied({
        callTool: async () => {
          throw new Error("HTTP 401 Unauthorized");
        },
      }),
      "transport authorization refused",
    );
  },
);

NodeTest.test(
  "inactive-provider qualification accepts only the exact native refusal text with isError",
  async () => {
    const text = "The calling provider no longer owns an active thread run.";
    const native = { content: [{ type: "text", text }], isError: true };
    NodeAssert.equal(
      await assertInactiveDenied({ callTool: async () => native }),
      "tool authorization refused",
    );
    for (const response of [
      { ...native, isError: false },
      { content: native.content },
      { ...native, content: [{ type: "text", text: text + " Retry later." }] },
      { ...native, content: [{ type: "text", text: "Reported: " + text }] },
      { ...native, content: [{ type: "text", text: text.slice(0, -1) }] },
      { ...native, content: [{ type: "text", text: text.toLowerCase() }] },
      { ...native, content: [{ type: "text", text: "The browser no longer owns an active tab." }] },
      { ...native, content: [{ type: "image", text }] },
      { isError: true, message: text },
    ]) {
      await NodeAssert.rejects(assertInactiveDenied({ callTool: async () => response }));
    }
  },
);

NodeTest.test(
  "failed pairing reconciliation publishes no pixels, pasted URL or separated pairing code",
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
      await withFixtureCleanup(async (defer) => {
        const code = "SYNTHETIC+PAIRING/CODE";
        const pairingUrl = `http://127.0.0.1:41001/pair#token=${encodeURIComponent(code)}`;
        const primaryCode = "SYNTHETIC-PRIMARY-CODE";
        const scenario = {
          origin: "http://127.0.0.1:41000",
          pairingUrl: `http://127.0.0.1:41000/pair#token=${primaryCode}`,
          initialText: "Project",
          steps: [
            { action: "type", locator: 'role=textbox[name="Host"]', text: pairingUrl },
            { action: "click", locator: 'role=button[name="Add environment"]' },
            { action: "wait", text: "Source matches this app." },
          ],
        };
        const failure = new Error(`Pairing failed for ${pairingUrl}; displayed code ${code}`);
        const imageBytes = Buffer.from(`pixels containing ${code}`).toString("base64");
        const snapshotCalls = [];
        let waits = 0;
        const client = {
          callTool: async ({ name, arguments: args }) => {
            if (name === "preview_wait_for" && ++waits === 2) throw failure;
            if (name === "preview_snapshot") {
              snapshotCalls.push(args);
              return {
                structuredContent: {
                  url: pairingUrl,
                  visibleText: `Could not add backend: Failed to fetch remote environment endpoint. Host 127.0.0.1 Pairing code ${code}`,
                  interactiveElements: [{ value: code }],
                  screenshot: { data: imageBytes },
                  screenshotPath: "/owned/must-not-publish.png",
                },
              };
            }
            return { structuredContent: name === "preview_open" ? { tabId: "pairing-tab" } : {} };
          },
        };
        const report = { calls: [], captures: [], negativeControls: {} };
        await NodeAssert.rejects(
          runScenario(client, scenario, report),
          (error) => error === failure,
        );
        report.error = failure.message;
        NodeAssert.deepEqual(snapshotCalls, [
          { tabId: "pairing-tab", save: false, includeImage: false },
        ]);
        NodeAssert.equal(waits, 2);
        NodeAssert.deepEqual(report.captures, []);
        NodeAssert.equal(report.nativeTabClosed, true);
        const secrets = scenarioSecrets(scenario);
        NodeAssert.ok(secrets.includes(code) && secrets.includes(primaryCode));
        NodeAssert.ok(
          scenarioSecrets({
            pairingUrl: `http://127.0.0.1/pair?token=${encodeURIComponent(code)}`,
          }).includes(code),
        );
        const files = milestoneFiles(root);
        const channel = await createMilestoneChannel({
          runId: "failed-pairing",
          caseId: "redaction",
          files,
        });
        defer(() => channel.close());
        const published = JSON.parse(redact(report, secrets));
        await publishMilestone(channel.binding, "final", files.final, published);
        const readback = await readAcknowledgedMilestone(
          files.final,
          await channel.wait(["final"]),
        );
        const text = JSON.stringify(readback);
        for (const secret of [pairingUrl, scenario.pairingUrl, code, primaryCode, imageBytes])
          NodeAssert.equal(text.includes(secret), false);
        NodeAssert.equal(text.includes("screenshotPath"), false);
        NodeAssert.deepEqual(Object.keys(readback.failureReconciliation).sort(), [
          "contentOmitted",
          "kind",
          "ui",
          "visibleTextLength",
        ]);
        NodeAssert.equal(readback.failureReconciliation.contentOmitted, true);
        NodeAssert.equal(readback.failureReconciliation.ui.addFailed, true);
        NodeAssert.equal(readback.failureReconciliation.ui.fetchFailed, true);
        NodeAssert.equal(readback.failureReconciliation.ui.sourceMatches, false);
        NodeAssert.ok(
          Object.values(readback.failureReconciliation.ui).every(
            (value) => typeof value === "boolean",
          ),
        );
        NodeAssert.deepEqual(await NodeFSP.readdir(root), ["final.json"]);
      });
    });
  },
);

NodeTest.test(
  "disabled-browser qualification requires the native preview capability refusal",
  () => {
    for (const result of [
      undefined,
      { isError: false },
      {
        isError: true,
        content: [
          { type: "text", text: "PreviewAutomationUnavailableError: Chrome launch failed" },
        ],
      },
    ]) {
      NodeAssert.throws(() => assertBrowserDisabledDenied(result));
    }
    assertBrowserDisabledDenied({
      isError: true,
      content: [
        {
          type: "text",
          text: "MCP credential does not grant the preview capability: browser preview tools are off for this thread.",
        },
      ],
    });
  },
);

async function withBuildReceiptFixture(use) {
  await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
    const checkout = NodePath.join(root, "source");
    const evidence = NodePath.join(root, "evidence");
    const env = {
      ...isolatedEnvironment(root),
      GIT_CONFIG_NOSYSTEM: "1",
      GIT_CONFIG_GLOBAL: NodePath.join(root, "gitconfig"),
      GIT_AUTHOR_NAME: "Synthetic Preview",
      GIT_AUTHOR_EMAIL: "preview@example.invalid",
      GIT_COMMITTER_NAME: "Synthetic Preview",
      GIT_COMMITTER_EMAIL: "preview@example.invalid",
    };
    await Promise.all(
      [checkout, evidence, env.HOME, env.TMPDIR].map((directory) => NodeFSP.mkdir(directory)),
    );
    await NodeFSP.writeFile(env.GIT_CONFIG_GLOBAL, "");
    const git = (...args) =>
      NodeChildProcess.execFileSync("git", ["-C", checkout, ...args], { env });
    git("init", "--quiet", "--initial-branch=fixture");
    await NodeFSP.writeFile(
      NodePath.join(checkout, ".gitignore"),
      ".env\napps/server/dist/\napps/web/dist/\n",
    );
    await NodeFSP.writeFile(NodePath.join(checkout, "source.txt"), "original\n");
    await NodeFSP.writeFile(NodePath.join(checkout, "binary.bin"), Buffer.from([0, 1, 2, 255]));
    git("add", ".gitignore", "source.txt", "binary.bin");
    git("commit", "--quiet", "-m", "Synthetic Preview base");
    for (const directory of ["apps/server/dist", "apps/web/dist"]) {
      await NodeFSP.mkdir(NodePath.join(checkout, directory), { recursive: true });
      await NodeFSP.writeFile(NodePath.join(checkout, directory, "index.html"), "synthetic build");
    }
    await NodeFSP.writeFile(NodePath.join(checkout, ".env"), "SYNTHETIC_IGNORED_CREDENTIAL");
    await NodeFSP.writeFile(
      NodePath.join(checkout, "credentials.txt"),
      "SYNTHETIC_UNTRACKED_CREDENTIAL",
    );
    await use({
      checkout,
      git,
      env,
      receipt: NodePath.join(evidence, "install-preview-build.json"),
      patch: NodePath.join(evidence, "install-preview-build.patch"),
    });
  });
}

const patchDigest = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");

NodeTest.test(
  "build receipt preserves replayable binary bytes and the unchanged normalized v1 digest",
  async () => {
    await withBuildReceiptFixture(async (fixture) => {
      const binary = Buffer.from([0, 3, 2, 254]);
      await NodeFSP.writeFile(NodePath.join(fixture.checkout, "source.txt"), "café\n\n");
      await NodeFSP.writeFile(NodePath.join(fixture.checkout, "binary.bin"), binary);
      const ordinaryPatch = fixture.git("diff", "HEAD", "--", ".").toString("utf8").trim();
      const expectedPatch = fixture.git("diff", "--binary", "--full-index", "HEAD", "--", ".");
      const receipt = await recordBuild(fixture.checkout, fixture.receipt);
      const savedPatch = await NodeFSP.readFile(fixture.patch);
      NodeAssert.deepEqual(savedPatch, expectedPatch);
      NodeAssert.match(savedPatch.toString("utf8"), /GIT binary patch/);
      NodeAssert.match(savedPatch.toString("utf8"), /index [a-f0-9]{40}\.\.[a-f0-9]{40}/);
      NodeAssert.ok(savedPatch.includes(Buffer.from("café\n")));
      NodeAssert.equal(savedPatch.at(-1), 10);
      NodeAssert.ok(!savedPatch.includes(Buffer.from("SYNTHETIC_")));
      NodeAssert.equal(receipt.schema, "jones-install-preview-build/v1");
      NodeAssert.equal(receipt.trackedPatchSha256, patchDigest(ordinaryPatch));
      NodeAssert.equal(receipt.cleanTrackedSource, false);
      NodeAssert.deepEqual(receipt.trackedPatchArtifact, {
        format: "git-diff-binary-full-index/v1",
        file: "install-preview-build.patch",
        sha256: patchDigest(savedPatch),
        bytes: savedPatch.length,
      });
      NodeAssert.equal((await NodeFSP.stat(fixture.patch)).mode & 0o777, 0o600);
      fixture.git("apply", "--reverse", fixture.patch);
      NodeAssert.deepEqual(
        await NodeFSP.readFile(NodePath.join(fixture.checkout, "binary.bin")),
        Buffer.from([0, 1, 2, 255]),
      );
      fixture.git("apply", fixture.patch);
      NodeAssert.deepEqual(
        await NodeFSP.readFile(NodePath.join(fixture.checkout, "binary.bin")),
        binary,
      );
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(fixture.checkout, "source.txt"), "utf8"),
        "café\n\n",
      );
      NodeAssert.deepEqual(await readBuildReceipt(fixture.checkout, fixture.receipt), receipt);
    });
  },
);

NodeTest.test(
  "clean tracked source records an empty patch despite untracked credentials",
  async () => {
    await withBuildReceiptFixture(async (fixture) => {
      const receipt = await recordBuild(fixture.checkout, fixture.receipt);
      NodeAssert.equal(receipt.cleanTrackedSource, true);
      NodeAssert.equal(receipt.trackedPatchSha256, patchDigest(Buffer.alloc(0)));
      NodeAssert.equal(receipt.trackedPatchArtifact.bytes, 0);
      NodeAssert.equal(receipt.trackedPatchArtifact.sha256, patchDigest(Buffer.alloc(0)));
      NodeAssert.deepEqual(await NodeFSP.readFile(fixture.patch), Buffer.alloc(0));
      NodeAssert.deepEqual(await readBuildReceipt(fixture.checkout, fixture.receipt), receipt);
    });
  },
);

NodeTest.test(
  "build receipt refuses a FIFO without a writer before qualification",
  { skip: nativePlatform === "win32" ? "Named FIFOs require POSIX mkfifo" : false },
  async () => {
    await withBuildReceiptFixture(async (fixture) => {
      await recordBuild(fixture.checkout, fixture.receipt);
      await NodeFSP.unlink(fixture.patch);
      NodeChildProcess.execFileSync("mkfifo", [fixture.patch], {
        env: fixture.env,
        timeout: 5_000,
        killSignal: "SIGKILL",
        maxBuffer: 64 * 1024,
      });
      NodeAssert.ok((await NodeFSP.lstat(fixture.patch)).isFIFO());
      const moduleUrl = new URL("./install-preview-qualification.mjs", import.meta.url).href;
      // A blocking-open regression is killed and reaped before the owned FIFO is removed.
      NodeChildProcess.execFileSync(
        process.execPath,
        [
          "--input-type=module",
          "--eval",
          `import * as NodeAssert from "node:assert/strict";
           import { readBuildReceipt } from ${JSON.stringify(moduleUrl)};
           await NodeAssert.rejects(
             readBuildReceipt(process.argv[1], process.argv[2]),
             /Tracked patch artifact must be a regular file/,
           );`,
          fixture.checkout,
          fixture.receipt,
        ],
        {
          env: fixture.env,
          timeout: 10_000,
          killSignal: "SIGKILL",
          maxBuffer: 64 * 1024,
        },
      );
    });
  },
);

for (const fault of ["missing", "tampered", "current-binary", "metadata-bound"]) {
  NodeTest.test(`build receipt refuses ${fault} patch evidence before qualification`, async () => {
    await withBuildReceiptFixture(async (fixture) => {
      await NodeFSP.writeFile(
        NodePath.join(fixture.checkout, "binary.bin"),
        Buffer.from([0, 3, 2, 254]),
      );
      const receipt = await recordBuild(fixture.checkout, fixture.receipt);
      if (fault === "missing") await NodeFSP.unlink(fixture.patch);
      if (fault === "tampered") {
        const bytes = await NodeFSP.readFile(fixture.patch);
        bytes[0] ^= 1;
        await NodeFSP.writeFile(fixture.patch, bytes);
      }
      if (fault === "current-binary") {
        await NodeFSP.writeFile(
          NodePath.join(fixture.checkout, "binary.bin"),
          Buffer.from([0, 4, 2, 253]),
        );
        // Refresh only the legacy digest to isolate the current binary-byte check.
        receipt.trackedPatchSha256 = patchDigest(
          fixture.git("diff", "HEAD", "--", ".").toString("utf8").trim(),
        );
        await atomicJson(fixture.receipt, receipt);
      }
      if (fault === "metadata-bound") {
        receipt.trackedPatchArtifact.bytes = 16 * 1024 * 1024 + 1;
        await atomicJson(fixture.receipt, receipt);
      }
      await NodeAssert.rejects(
        readBuildReceipt(fixture.checkout, fixture.receipt),
        fault === "missing"
          ? { code: "ENOENT" }
          : fault === "tampered"
            ? /artifact digest differs/
            : fault === "current-binary"
              ? /differs from current source/
              : /exceeds its byte bound/,
      );
    });
  });
}

NodeTest.test("patch collision preserves the existing file and publishes no receipt", async () => {
  await withBuildReceiptFixture(async (fixture) => {
    await NodeFSP.writeFile(fixture.patch, "preserved prior evidence");
    await NodeAssert.rejects(recordBuild(fixture.checkout, fixture.receipt), { code: "EEXIST" });
    NodeAssert.equal(await NodeFSP.readFile(fixture.patch, "utf8"), "preserved prior evidence");
    await NodeAssert.rejects(NodeFSP.lstat(fixture.receipt), { code: "ENOENT" });
  });
});

NodeTest.test("oversized binary diff publishes neither sidecar nor receipt", async () => {
  await withBuildReceiptFixture(async (fixture) => {
    // Incompressible bytes exceed the 16 MiB binary-patch limit;
    // the ordinary binary summary stays small.
    const binary = NodeCrypto.randomBytes(16 * 1024 * 1024);
    binary[0] = 0;
    await NodeFSP.writeFile(NodePath.join(fixture.checkout, "binary.bin"), binary);
    await NodeAssert.rejects(
      recordBuild(fixture.checkout, fixture.receipt),
      (error) => error.code === "ENOBUFS" || error.code === "ERR_CHILD_PROCESS_STDIO_MAXBUFFER",
    );
    await NodeAssert.rejects(NodeFSP.lstat(fixture.patch), { code: "ENOENT" });
    await NodeAssert.rejects(NodeFSP.lstat(fixture.receipt), { code: "ENOENT" });
  });
});

NodeTest.test(
  "renderer manifest refuses a stale source revision, tree or undeclared boundary",
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
      const manifest = {
        schema: "jones-install-renderer-fixture/v1",
        sourceSha: "a".repeat(40),
        sourceTree: "b".repeat(40),
        entry: "jones-install-fixture.html",
        boundary:
          "Production renderer components with synthetic desktop bridge; no Electron IPC or native installation",
      };
      const file = NodePath.join(root, "jones-install-fixture-manifest.json");
      await atomicJson(file, manifest);
      NodeAssert.deepEqual(
        await readRendererManifest(root, manifest.sourceSha, manifest.sourceTree),
        manifest,
      );
      await NodeAssert.rejects(
        readRendererManifest(root, "c".repeat(40), manifest.sourceTree),
        /candidate source revision/,
      );
      await NodeAssert.rejects(
        readRendererManifest(root, manifest.sourceSha, "d".repeat(40)),
        /candidate source tree/,
      );
      await atomicJson(file, { ...manifest, boundary: "Native installation verified" });
      await NodeAssert.rejects(readRendererManifest(root, manifest.sourceSha, manifest.sourceTree));
    });
  },
);

NodeTest.test(
  "owned scratch removes only its exact directory after success, failure and cancellation",
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (parent) => {
      const sibling = NodePath.join(parent, "preserved");
      await NodeFSP.writeFile(sibling, "keep");
      for (const result of ["success", "failure", "cancellation"]) {
        let owned;
        const run = withOwnedScratch(parent, async (root) => {
          owned = root;
          await NodeFSP.mkdir(NodePath.join(root, "nested"));
          await NodeFSP.writeFile(NodePath.join(root, "nested/evidence"), result);
          if (result !== "success") throw new Error(result);
        });
        if (result === "success") await run;
        else await NodeAssert.rejects(run, new RegExp(result));
        await NodeAssert.rejects(NodeFSP.lstat(owned), { code: "ENOENT" });
        NodeAssert.equal(await NodeFSP.readFile(sibling, "utf8"), "keep");
      }
    });
  },
);

function milestoneFiles(root, label = "") {
  return Object.fromEntries(
    ["started", "custody", "report", "final", "release"].map((name) => [
      name,
      NodePath.join(root, `${label}${name}.json`),
    ]),
  );
}

async function withFixtureCleanup(use) {
  const cleanups = [];
  let primary;
  try {
    return await use((cleanup) => cleanups.push(cleanup));
  } catch (error) {
    primary = error;
    throw error;
  } finally {
    const results = await Promise.allSettled(
      cleanups.reverse().map((cleanup) => Promise.resolve().then(cleanup)),
    );
    const errors = results
      .filter((result) => result.status === "rejected")
      .map((result) => result.reason);
    if (errors.length) {
      if (primary) {
        primary.cleanupErrors = errors;
        primary.retainScratch = true;
      } else {
        throw Object.assign(new AggregateError(errors, "Owned fixture cleanup failed"), {
          retainScratch: true,
        });
      }
    }
  }
}

async function assertChannelClosed(channel) {
  NodeAssert.equal(channel.evidence().closed, true);
  NodeAssert.equal(channel.evidence().pendingWaiters, 0);
  NodeAssert.equal(channel.evidence().heldRequests, 0);
  await NodeAssert.rejects(
    fetch(channel.binding.endpoint, { signal: AbortSignal.timeout(5_000) }),
    (error) => ["ECONNREFUSED", "UND_ERR_SOCKET"].includes(error.cause?.code),
  );
}

NodeTest.test(
  "the synthetic older host explicitly advertises its supervised update route",
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
      await withFixtureCleanup(async (defer) => {
        const descriptorPath = NodePath.join(root, ".well-known/t3/environment");
        await NodeFSP.mkdir(NodePath.dirname(descriptorPath), { recursive: true });
        await atomicJson(descriptorPath, {
          label: "Unmanaged fixture origin",
          capabilities: { serverBrowser: true },
          orchestrationProtocolVersion: 42,
        });
        const upstream = await serveRendererFixture(root);
        defer(() => upstream.close());
        const sourceIdentity = {
          repository: "Jones-Systems/Jones-Code",
          sha: "a".repeat(40),
          tree: "b".repeat(40),
        };
        const proxy = await fixtureProxy(upstream, sourceIdentity);
        defer(() => proxy.close());
        const response = await fetch(`${proxy.origin}/.well-known/t3/environment`);
        NodeAssert.equal(response.status, 200);
        const descriptor = await response.json();
        NodeAssert.deepEqual(descriptor.capabilities, {
          serverBrowser: true,
          serverSelfUpdate: "boot-service",
        });
        NodeAssert.equal(descriptor.orchestrationProtocolVersion, 0);
        NodeAssert.deepEqual(descriptor.jonesSource, sourceIdentity);
        NodeAssert.deepEqual(proxy.evidence().requests, [
          {
            method: "GET",
            path: "/.well-known/t3/environment",
            status: 200,
            forwardingStarted: true,
          },
        ]);
        NodeAssert.match(
          proxy.evidence().fixtureBoundary,
          /synthetic older boot-service descriptor/,
        );
        NodeAssert.equal(proxy.evidence().checks, 0);
      });
    });
  },
);

NodeTest.test(
  "fixture preflight permits native trace headers and omits unknown header values from evidence",
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
      await withFixtureCleanup(async (defer) => {
        const upstream = await serveRendererFixture(root);
        defer(() => upstream.close());
        const proxy = await fixtureProxy(upstream, {});
        defer(() => proxy.close());
        const headers = [
          "authorization",
          "b3",
          "traceparent",
          "content-type",
          "dpop",
          "x-t3-orchestration-protocol",
        ];
        const rejectedHeader = "x-synthetic-secret-must-not-be-recorded";
        const response = await fetch(`${proxy.origin}/.well-known/t3/environment`, {
          method: "OPTIONS",
          headers: {
            origin: "http://127.0.0.1:41000",
            "access-control-request-method": "GET",
            "access-control-request-headers": [...headers, rejectedHeader].join(","),
          },
        });
        NodeAssert.equal(response.status, 204);
        NodeAssert.deepEqual(
          response.headers.get("access-control-allow-headers").split(","),
          headers,
        );
        NodeAssert.equal(
          response.headers.get("access-control-allow-headers").includes(rejectedHeader),
          false,
        );
        NodeAssert.equal(
          response.headers.get("access-control-allow-origin"),
          "http://127.0.0.1:41000",
        );
        NodeAssert.deepEqual(proxy.evidence().requests, [
          {
            method: "OPTIONS",
            path: "/.well-known/t3/environment",
            status: 204,
            requestedHeaders: Object.fromEntries(headers.map((name) => [name, true])),
            unsupportedHeaders: true,
          },
        ]);
        NodeAssert.deepEqual(upstream.evidence().requests, []);
        NodeAssert.equal(JSON.stringify(proxy.evidence()).includes(rejectedHeader), false);
        const invalid = await fetch(
          `${proxy.origin}/unexpected-SYNTHETIC-SECRET?token=SYNTHETIC-SECRET`,
        );
        NodeAssert.equal(invalid.status, 500);
        NodeAssert.deepEqual(proxy.evidence().requests.at(-1), {
          method: "GET",
          path: "other",
          status: 500,
        });
        NodeAssert.equal(JSON.stringify(proxy.evidence()).includes("SYNTHETIC-SECRET"), false);
      });
    });
  },
);

NodeTest.test(
  "a failed second channel acquisition closes the first owned listener",
  { timeout: 10_000 },
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
      let left;
      const acquisitionError = new Error("forced second acquisition failure");
      await NodeAssert.rejects(
        withFixtureCleanup(async (defer) => {
          left = await createMilestoneChannel({
            runId: "acquisition",
            caseId: "left",
            files: milestoneFiles(root, "left-"),
          });
          defer(() => left.close());
          const response = await fetch(left.binding.endpoint, { headers: { connection: "close" } });
          NodeAssert.equal(response.status, 409);
          await response.json();
          const right = await createMilestoneChannel({
            runId: "acquisition",
            caseId: "right",
            files: milestoneFiles(root, "right-"),
            signal: AbortSignal.abort(acquisitionError),
          });
          defer(() => right.close());
        }),
        (error) => error === acquisitionError,
      );
      await assertChannelClosed(left);
    });
  },
);

NodeTest.test(
  "a rejected child cleanup still closes its channel and readline without masking the primary failure",
  { timeout: 10_000 },
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
      for (const failBody of [false, true]) {
        let channel;
        let child;
        let linesClosed = false;
        const primary = new Error("forced fixture body failure");
        const cleanupFailure = new Error("forced child cleanup rejection after reaping");
        await NodeAssert.rejects(
          withFixtureCleanup(async (defer) => {
            channel = await createMilestoneChannel({
              runId: "cleanup",
              caseId: String(failBody),
              files: milestoneFiles(root, `${failBody}-`),
            });
            defer(() => channel.close());
            child = NodeChildProcess.spawn(
              process.execPath,
              ["--input-type=module", "-e", "process.stdin.resume()"],
              { env: isolatedEnvironment(root), stdio: ["pipe", "pipe", "pipe"] },
            );
            defer(async () => {
              await stopChild(child);
              throw cleanupFailure;
            });
            defer(() => child.stdin.end());
            const lines = NodeReadline.createInterface({ input: child.stdout });
            defer(() => lines.close());
            lines.once("close", () => {
              linesClosed = true;
            });
            await NodeEvents.once(child, "spawn");
            if (failBody) throw primary;
          }),
          (error) => {
            NodeAssert.equal(error.retainScratch, true);
            if (failBody) {
              NodeAssert.equal(error, primary);
              NodeAssert.deepEqual(error.cleanupErrors, [cleanupFailure]);
            } else {
              NodeAssert.ok(error instanceof AggregateError);
              NodeAssert.deepEqual(error.errors, [cleanupFailure]);
            }
            return true;
          },
        );
        NodeAssert.ok(child.exitCode !== null || child.signalCode !== null);
        NodeAssert.equal(linesClosed, true);
        await assertChannelClosed(channel);
      }
    });
  },
);

NodeTest.test(
  "acknowledged milestones handle existing, new and cancelled waits without filesystem notifications",
  { timeout: 10_000 },
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
      const files = milestoneFiles(root);
      const channel = await createMilestoneChannel({ runId: "unit", caseId: "milestones", files });
      try {
        // Writing a file alone must never satisfy a milestone; only a correlated acknowledgment does.
        await atomicJson(files.report, { ready: true });
        const cancellation = new AbortController();
        const unacknowledged = channel.wait(["report"], cancellation.signal);
        cancellation.abort(new Error("owned cancellation"));
        await NodeAssert.rejects(unacknowledged, /owned cancellation/);
        NodeAssert.deepEqual(channel.evidence().receipts, []);
        await publishMilestone(channel.binding, "started", files.started, { beforeWait: true });
        NodeAssert.deepEqual(
          await readAcknowledgedMilestone(files.started, await channel.wait(["started"])),
          { beforeWait: true },
        );
        const pending = channel.wait(["report"], AbortSignal.timeout(5_000));
        await publishMilestone(channel.binding, "report", files.report, { ready: true });
        NodeAssert.deepEqual(await readAcknowledgedMilestone(files.report, await pending), {
          ready: true,
        });
        const release = awaitMilestoneRelease(
          channel.binding,
          files.release,
          AbortSignal.timeout(5_000),
        );
        NodeAssert.equal(await channel.waitingForRelease, true);
        await channel.release({ inactive: true });
        NodeAssert.deepEqual(await release, { inactive: true });
        const abandoned = channel.wait(["final"]);
        const rejected = NodeAssert.rejects(abandoned, /channel closed/);
        await channel.close();
        await rejected;
        NodeAssert.equal(channel.evidence().pendingWaiters, 0);
        NodeAssert.equal(channel.evidence().heldRequests, 0);
        await NodeAssert.rejects(channel.wait(["started"]), /channel closed/);
      } finally {
        await channel.close();
      }
      NodeAssert.deepEqual((await NodeFSP.readdir(root)).sort(), [
        "release.json",
        "report.json",
        "started.json",
      ]);
    });
  },
);

NodeTest.test(
  "owned milestone channels reject malformed correlation and remain isolated with bounded cleanup",
  { timeout: 10_000 },
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
      await withFixtureCleanup(async (defer) => {
        const leftFiles = milestoneFiles(root, "left-");
        const rightFiles = milestoneFiles(root, "right-");
        const left = await createMilestoneChannel({
          runId: "same-run",
          caseId: "left",
          files: leftFiles,
        });
        defer(() => left.close());
        const cancellation = new AbortController();
        const right = await createMilestoneChannel({
          runId: "same-run",
          caseId: "right",
          files: rightFiles,
          signal: cancellation.signal,
        });
        defer(() => right.close());
        const rejected = async (body) => {
          const response = await fetch(left.binding.endpoint, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body,
          });
          NodeAssert.equal(response.status, 409);
          await response.body.cancel();
        };
        await rejected("not-json");
        await rejected(
          JSON.stringify({
            ...right.binding,
            kind: "publish",
            name: "report",
            sha256: "0".repeat(64),
          }),
        );
        await rejected(
          JSON.stringify({ ...left.binding, runId: "wrong-run", kind: "wait", name: "release" }),
        );
        await rejected(
          JSON.stringify({ ...left.binding, caseId: "wrong-case", kind: "wait", name: "release" }),
        );
        await rejected(
          JSON.stringify({ ...left.binding, nonce: "0".repeat(64), kind: "wait", name: "release" }),
        );
        await rejected(
          JSON.stringify({
            ...left.binding,
            kind: "publish",
            name: "release",
            sha256: "0".repeat(64),
          }),
        );
        await rejected(
          JSON.stringify({
            ...left.binding,
            kind: "publish",
            name: "report",
            sha256: "0".repeat(64),
            excess: "x".repeat(2048),
          }),
        );
        await publishMilestone(left.binding, "report", leftFiles.report, { owner: "left" });
        NodeAssert.deepEqual(right.evidence().receipts, []);
        await publishMilestone(right.binding, "report", rightFiles.report, { owner: "right" });
        const receipt = await left.wait(["report"]);
        await atomicJson(leftFiles.report, { owner: "tampered" });
        await NodeAssert.rejects(
          readAcknowledgedMilestone(leftFiles.report, receipt),
          /changed before readback/,
        );
        await right.release({ inactive: true });
        NodeAssert.deepEqual(await awaitMilestoneRelease(right.binding, rightFiles.release), {
          inactive: true,
        });
        const cancelled = NodeAssert.rejects(right.wait(["final"]), /channel closed/);
        cancellation.abort(new Error("fixture cancelled"));
        await right.close();
        await cancelled;
        NodeAssert.equal(right.evidence().pendingWaiters, 0);
        const held = awaitMilestoneRelease(left.binding, leftFiles.release);
        const refused = NodeAssert.rejects(held);
        NodeAssert.equal(await left.waitingForRelease, true);
        await left.close();
        await refused;
        NodeAssert.equal(left.evidence().heldRequests, 0);
        await NodeAssert.rejects(
          publishMilestone(left.binding, "final", leftFiles.final, { done: true }),
        );
      });
    });
  },
);

NodeTest.test(
  "unresolved process or evidence cleanup retains its owned scratch without masking the failure",
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (parent) => {
      let owned;
      const original = Object.assign(new Error("native cleanup failed"), { retainScratch: true });
      await NodeAssert.rejects(
        withOwnedScratch(parent, async (root) => {
          owned = root;
          await NodeFSP.writeFile(NodePath.join(root, "evidence"), "preserve");
          throw original;
        }),
        (error) => error === original && error.cleanupError.includes(owned),
      );
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(owned, "evidence"), "utf8"),
        "preserve",
      );
    });
  },
);

NodeTest.test(
  "artifact identity detects a changed served bundle and refuses symlink traversal",
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
      await NodeFSP.writeFile(NodePath.join(root, "index.html"), "first");
      const before = await hashTree(root);
      await NodeFSP.writeFile(NodePath.join(root, "index.html"), "second");
      NodeAssert.notDeepEqual(await hashTree(root), before);
      await NodeFSP.symlink(NodePath.join(root, "index.html"), NodePath.join(root, "linked.html"));
      await NodeAssert.rejects(hashTree(root), /contains a symlink/);
    });
  },
);

NodeTest.test("fixture environment and evidence exclude ambient credentials", () => {
  const environment = isolatedEnvironment("/owned/fixture");
  NodeAssert.equal(environment.HOME, "/owned/fixture/home");
  NodeAssert.equal(environment.T3CODE_SERVER_BROWSER_SANDBOX, "1");
  NodeAssert.deepEqual(
    Object.keys(environment).sort(),
    [
      "CI",
      "HOME",
      "LANG",
      "NO_COLOR",
      "PATH",
      "T3CODE_DISABLE_TELEMETRY",
      "T3CODE_SERVER_BROWSER_SANDBOX",
      "TMPDIR",
      "XDG_CACHE_HOME",
      "XDG_CONFIG_HOME",
      "XDG_DATA_HOME",
    ].sort(),
  );
  NodeAssert.equal(
    redact("Bearer own-session token=PAIRCODE wsTicket=WS own-secret", ["own-secret"]),
    "Bearer [redacted] token=[redacted] wsTicket=[redacted] [redacted]",
  );
});

NodeTest.test(
  "owned renderer serving records byte identity and rejects paths outside its output",
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (parent) => {
      const root = NodePath.join(parent, "renderer");
      await NodeFSP.mkdir(root);
      await NodeFSP.writeFile(
        NodePath.join(root, "page.html"),
        "<title>Owned renderer fixture</title>",
      );
      await NodeFSP.writeFile(NodePath.join(parent, "outside"), "must not serve");
      await NodeFSP.symlink(NodePath.join(parent, "outside"), NodePath.join(root, "outside"));
      const server = await serveRendererFixture(root);
      try {
        const response = await fetch(`${server.origin}/page.html`);
        NodeAssert.equal(response.status, 200);
        NodeAssert.match(await response.text(), /Owned renderer fixture/);
        NodeAssert.equal((await fetch(`${server.origin}/outside`)).status, 404);
        NodeAssert.equal(
          (await fetch(`${server.origin}/page.html`, { method: "POST" })).status,
          404,
        );
        NodeAssert.deepEqual(
          server.evidence().requests.map(({ path }) => path),
          ["page.html"],
        );
        NodeAssert.match(server.evidence().requests[0].sha256, /^[a-f0-9]{64}$/);
      } finally {
        await server.close();
      }
    });
  },
);

NodeTest.test(
  "real verifier process refuses an ACP session without native T3 MCP injection",
  { timeout: 15_000 },
  async () => {
    await withOwnedScratch(NodeOS.tmpdir(), async (root) => {
      let child;
      await withFixtureCleanup(async (defer) => {
        const report = NodePath.join(root, "report.json");
        const files = {
          started: `${report}.started`,
          custody: `${report}.custody`,
          report,
          final: `${report}.final`,
          release: NodePath.join(root, "release.json"),
        };
        const channel = await createMilestoneChannel({
          runId: "actual-child",
          caseId: "missing-injection",
          files,
        });
        defer(() => channel.close());
        child = NodeChildProcess.spawn(
          process.execPath,
          [NodePath.join(source, "scripts/jones/updates/install-preview-agent.mjs")],
          {
            env: {
              ...isolatedEnvironment(root),
              JONES_PREVIEW_SOURCE: source,
              JONES_PREVIEW_REPORT: report,
              JONES_PREVIEW_RELEASE: files.release,
              JONES_PREVIEW_MILESTONE: JSON.stringify(channel.binding),
            },
            stdio: ["pipe", "pipe", "pipe"],
          },
        );
        defer(() => stopChild(child));
        defer(() => child.stdin.end());
        const lines = NodeReadline.createInterface({ input: child.stdout });
        defer(() => lines.close());
        const pending = new Map();
        let next = 0;
        let stderr = "";
        child.stderr.on("data", (bytes) => {
          stderr += bytes;
        });
        lines.on("line", (line) => {
          const response = JSON.parse(line);
          pending.get(response.id)?.(response);
          pending.delete(response.id);
        });
        const request = (method, params) =>
          new Promise((resolve, reject) => {
            const id = ++next;
            const timer = setTimeout(() => {
              pending.delete(id);
              reject(new Error(`ACP ${method} timeout`));
            }, 5_000);
            pending.set(id, (value) => {
              clearTimeout(timer);
              resolve(value);
            });
            child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`);
          });
        await NodeEvents.once(child, "spawn");
        const initialized = await request("initialize", {
          protocolVersion: 1,
          clientCapabilities: {},
        });
        NodeAssert.equal(initialized.result.protocolVersion, 1);
        const created = await request("session/new", { cwd: root, mcpServers: [] });
        const scenario = Buffer.from(JSON.stringify({ mode: "active", steps: [] })).toString(
          "base64",
        );
        const result = await request("session/prompt", {
          sessionId: created.result.sessionId,
          prompt: [{ type: "text", text: `JONES_PREVIEW_SCENARIO:${scenario}` }],
        });
        NodeAssert.match(result.error.message, /Normal T3 MCP injection is required/);
        const final = await readAcknowledgedMilestone(
          files.final,
          await channel.wait(["final"], AbortSignal.timeout(5_000)),
        );
        NodeAssert.deepEqual(
          channel.evidence().receipts.map((receipt) => receipt.name),
          ["started", "final"],
        );
        NodeAssert.equal(final.outcome, "failed");
        NodeAssert.equal(final.providerPid, child.pid);
        NodeAssert.equal(final.transportCreated, false);
        NodeAssert.equal(final.transportClosed, false);
        NodeAssert.equal(final.mcpBridgePid, undefined);
        NodeAssert.deepEqual(final.calls, []);
        NodeAssert.equal(stderr, "");
      });
      NodeAssert.ok(child.exitCode !== null || child.signalCode !== null);
    });
  },
);
