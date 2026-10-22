import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeEvents from "node:events";
import {
  SECONDARY,
  SecondaryServer,
  secondaryLaunch,
  assertPortClosed,
  readOwnedIdentity,
  validateReady,
  validateSeed,
  stopOwnedProcess,
} from "./support/secondary-server.mjs";

const standardScopes =
  "orchestration:read orchestration:operate settings:write providers:manage " +
  "environment:maintain preview:operate diagnostics:read terminal:read terminal:operate " +
  "source-control:write filesystem:read filesystem:write relay:read";

function fakeServer({
  occupied = false,
  runtimePid = 40,
  startFailure = false,
  hangCli = false,
} = {}) {
  const processes = new Map(),
    spawns = [],
    signals = [],
    requests = [],
    directories = [];
  let clock = 0,
    nextPid = 40,
    pairings = 0;
  const missing = () => Object.assign(new Error("gone"), { code: "ENOENT" });
  const dependencies = {
    now: () => clock,
    sleep: async (ms) => {
      clock += ms;
    },
    portClosed: async () => {
      if (occupied) throw new Error("b-port-occupied");
    },
    fs: {
      mkdir: async (path, options) => {
        directories.push({ path, options });
      },
      readFile: async (path) => {
        if (path.endsWith("/environment-id")) return "environment-b\n";
        if (path.endsWith("/server-runtime.json"))
          return JSON.stringify({
            version: 1,
            pid: runtimePid,
            port: 43774,
            origin: SECONDARY.origin,
          });
        throw new Error("unexpected-fs-read");
      },
    },
    readIdentity: async (pid) => {
      if (!processes.has(pid)) throw missing();
      return { pid, startTicks: String(pid * 100) };
    },
    kill: (pid, name) => {
      signals.push({ pid, name });
      processes.delete(pid);
    },
    spawn: (executable, args, options) => {
      const child = new NodeEvents.EventEmitter();
      Object.assign(child, {
        pid: nextPid++,
        exitCode: null,
        signalCode: null,
        stdout: new NodeEvents.EventEmitter(),
        stderr: new NodeEvents.EventEmitter(),
      });
      processes.set(child.pid, child);
      spawns.push({ executable, args, options });
      if (args.includes("auth")) {
        const credential = `synthetic-pair-${++pairings}`;
        if (!hangCli)
          setImmediate(() => {
            child.stdout.emit(
              "data",
              Buffer.from(JSON.stringify({ credential, scopes: standardScopes.split(" ") })),
            );
            child.exitCode = 0;
            processes.delete(child.pid);
            child.emit("close", 0);
          });
      } else setImmediate(() => child.stderr.emit("data", Buffer.from("synthetic diagnostic\n")));
      return child;
    },
    fetch: async (value, options) => {
      const url = new URL(value);
      requests.push({ url, options });
      if (startFailure) throw new Error("private diagnostic must not escape");
      if (url.pathname === "/.well-known/t3/environment")
        return {
          ok: true,
          json: async () => ({
            environmentId: url.port === "43774" ? "environment-b" : "environment-a",
            label: "same label",
          }),
        };
      if (url.pathname === "/oauth/token")
        return {
          ok: true,
          json: async () => ({
            scope: standardScopes,
            token_type: "Bearer",
            access_token: "synthetic-seeder-bearer",
          }),
        };
      if (url.pathname === "/api/orchestration/shell")
        return {
          ok: url.port === "43774",
          status: url.port === "43774" ? 200 : 401,
          body: { cancel: async () => {} },
          json: async () => ({ threads: [] }),
        };
      throw new Error("unexpected-http");
    },
  };
  return { dependencies, processes, spawns, signals, requests, directories };
}

NodeTest.test("B's launch is fixed to the built server with a closed synthetic environment", () => {
  const launch = secondaryLaunch();
  NodeAssert.equal(launch.executable, "/electron/electron");
  NodeAssert.deepEqual(launch.args, [
    "/app/apps/server/dist/bin.mjs",
    "serve",
    "--mode",
    "web",
    "--host",
    "127.0.0.1",
    "--port",
    "43774",
    "--base-dir",
    "/scratch/tmp/je03-b/home",
    "--no-browser",
  ]);
  NodeAssert.equal(launch.options.cwd, SECONDARY.workspace);
  NodeAssert.equal(launch.options.env.ELECTRON_RUN_AS_NODE, "1");
  NodeAssert.equal(launch.options.env.PATH, "/usr/bin:/bin");
  NodeAssert.deepEqual(
    Object.keys(launch.options.env).sort(),
    [
      "ELECTRON_RUN_AS_NODE",
      "PATH",
      "HOME",
      "XDG_CONFIG_HOME",
      "XDG_CACHE_HOME",
      "XDG_DATA_HOME",
      "XDG_STATE_HOME",
      "XDG_RUNTIME_DIR",
      "TMPDIR",
      "LANG",
      "TZ",
      "T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD",
      "T3CODE_DISABLE_AUTO_UPDATE",
      "OTEL_SDK_DISABLED",
    ].sort(),
  );
  for (const [key, value] of Object.entries(launch.options.env))
    if (key === "HOME" || key.startsWith("XDG_") || key === "TMPDIR")
      NodeAssert.ok(value.startsWith(SECONDARY.user));
});

NodeTest.test(
  "only ECONNREFUSED admits B; occupied and uncertain ports never spawn or allocate",
  async () => {
    for (const code of ["ECONNREFUSED", "EACCES", null]) {
      let destroyed = false;
      const probe = assertPortClosed(() => {
        const socket = new NodeEvents.EventEmitter();
        socket.setTimeout = () => {};
        socket.destroy = () => {
          destroyed = true;
        };
        queueMicrotask(() => (code ? socket.emit("error", { code }) : socket.emit("connect")));
        return socket;
      });
      if (code === "ECONNREFUSED") await probe;
      else await NodeAssert.rejects(probe, /b-port-occupied/);
      NodeAssert.equal(destroyed, true);
    }
    const fake = fakeServer({ occupied: true });
    await NodeAssert.rejects(new SecondaryServer(fake.dependencies).start(), /b-port-occupied/);
    NodeAssert.equal(fake.spawns.length, 0);
    NodeAssert.equal(fake.directories.length, 0);
  },
);

NodeTest.test("readiness binds the descriptor, disk identity, origin and captured PID", () => {
  const valid = {
    descriptor: { environmentId: "b", label: "same" },
    primary: { environmentId: "a" },
    persistedId: "b\n",
    runtime: { version: 1, pid: 40, port: 43774, origin: SECONDARY.origin },
    identity: { pid: 40, startTicks: "900" },
  };
  NodeAssert.equal(validateReady(valid).environmentId, "b");
  for (const changed of [
    { ...valid, persistedId: "a" },
    { ...valid, primary: { environmentId: "b" } },
    { ...valid, runtime: { ...valid.runtime, pid: 99 } },
    { ...valid, runtime: { ...valid.runtime, origin: "http://127.0.0.1:43773" } },
  ])
    NodeAssert.throws(() => validateReady(changed), /runtime-state-unbound|secondary-start-failed/);
});

NodeTest.test(
  "metadata seeding rejects providers, outside workspaces and extra command effects",
  () => {
    validateSeed({ type: "project.create", workspaceRoot: SECONDARY.workspace });
    validateSeed({ type: "thread.metadata.update", threadId: "t", title: "updated" });
    for (const command of [
      { type: "thread.turn.start" },
      { type: "thread.pin" },
      { type: "project.create", workspaceRoot: "/outside" },
      { type: "thread.metadata.update", threadId: "t", title: "updated", pinnedAt: "now" },
      {
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        branch: "main",
        worktreePath: null,
      },
    ])
      NodeAssert.throws(() => validateSeed(command), /secondary-seed-outside-/);
  },
);

NodeTest.test(
  "Linux stat parsing handles parenthesized names and validates a captured positive PID",
  async () => {
    const fields = ["S", "1", ...Array(17).fill("0"), "12345"];
    const identity = await readOwnedIdentity(40, async (path) => {
      NodeAssert.equal(path, "/proc/40/stat");
      return `40 (server ) name) ${fields.join(" ")}`;
    });
    NodeAssert.deepEqual(identity, { pid: 40, startTicks: "12345" });
    await NodeAssert.rejects(
      readOwnedIdentity(-1, async () => ""),
      /identity-unbound/,
    );
  },
);

NodeTest.test(
  "stop escalates only the captured PID, refuses unreadable identity, and never signals a reused PID",
  async () => {
    let now = 0,
      alive = true;
    const signals = [],
      identity = { pid: 40, startTicks: "900" };
    const deps = {
      now: () => now,
      sleep: async (ms) => {
        now += ms;
      },
      readIdentity: async () => {
        if (!alive) throw Object.assign(new Error("absent"), { code: "ENOENT" });
        return identity;
      },
      kill: (pid, name) => {
        signals.push([pid, name]);
        if (name === "SIGKILL") alive = false;
      },
    };
    const stopped = await stopOwnedProcess(identity, deps);
    NodeAssert.equal(stopped.outcome, "stopped");
    NodeAssert.equal(stopped.escalated, true);
    NodeAssert.deepEqual(signals, [
      [40, "SIGTERM"],
      [40, "SIGKILL"],
    ]);
    NodeAssert.ok(now <= 15000);
    signals.length = 0;
    await stopOwnedProcess(identity, {
      ...deps,
      readIdentity: async () => ({ pid: 40, startTicks: "901" }),
    });
    NodeAssert.deepEqual(signals, []);
    now = 0;
    await NodeAssert.rejects(
      stopOwnedProcess(identity, {
        ...deps,
        readIdentity: async () => identity,
        kill: (pid, name) => signals.push([pid, name]),
      }),
      /stop-unknown/,
    );
    NodeAssert.deepEqual(signals, [
      [40, "SIGTERM"],
      [40, "SIGKILL"],
    ]);
    signals.length = 0;
    await NodeAssert.rejects(
      stopOwnedProcess(identity, {
        ...deps,
        readIdentity: async () => {
          throw new Error("unreadable");
        },
      }),
      /stop-unknown/,
    );
    NodeAssert.deepEqual(signals, []);
  },
);

NodeTest.test(
  "two CLI grants stay in memory, fence is endpoint-specific, and stop is idempotent",
  async () => {
    const fake = fakeServer(),
      b = new SecondaryServer(fake.dependencies);
    const ready = await b.start();
    NodeAssert.equal(ready.environmentId, "environment-b");
    NodeAssert.equal(b.pairingCode, "synthetic-pair-1");
    NodeAssert.equal(fake.spawns.length, 3);
    NodeAssert.equal(
      fake.requests
        .find((entry) => entry.url.pathname === "/oauth/token")
        .options.body.get("subject_token"),
      "synthetic-pair-2",
    );
    NodeAssert.deepEqual(await b.credentialFence(), { ok: true, aStatus: 401, bStatus: 200 });
    for (const secret of ["synthetic-pair-1", "synthetic-pair-2", "synthetic-seeder-bearer"])
      NodeAssert.throws(() => b.assertSafeEvidence({ nested: [secret] }), /secret-in-evidence/);
    b.assertSafeEvidence(b.evidence);
    NodeAssert.equal(JSON.stringify(b.evidence).includes("synthetic diagnostic"), false);
    fake.processes.get(40).stderr.emit("data", Buffer.alloc(70000, 120));
    NodeAssert.equal(b.evidence.stderr.bytes, 65536);
    NodeAssert.ok(
      fake.directories.every(
        ({ path, options }) => path.startsWith(SECONDARY.root) && options.mode === 0o700,
      ),
    );
    const first = b.stop(),
      second = b.stop();
    NodeAssert.equal(first, second);
    NodeAssert.equal((await first).outcome, "stopped");
    NodeAssert.equal(fake.processes.size, 0);
    NodeAssert.throws(() => b.pairingCode, /pairing-unavailable/);
    const requestsAfterStop = fake.requests.length;
    NodeAssert.throws(() => b.readSnapshot(), /secondary-auth-unavailable/);
    NodeAssert.equal(fake.requests.length, requestsAfterStop);
  },
);

NodeTest.test("failed readiness closes the captured server and hides raw diagnostics", async () => {
  for (const options of [{ runtimePid: 999 }, { startFailure: true }]) {
    const fake = fakeServer(options),
      b = new SecondaryServer(fake.dependencies);
    await NodeAssert.rejects(b.start(), /runtime-state-unbound|secondary-start-failed/);
    NodeAssert.deepEqual(fake.signals, [{ pid: 40, name: "SIGTERM" }]);
    NodeAssert.equal(fake.processes.size, 0);
    NodeAssert.equal(b.evidence.stop.outcome, "stopped");
  }
});

NodeTest.test(
  "cancellation during CLI pairing closes both exact owned PIDs and never exposes stdout",
  async () => {
    const fake = fakeServer({ hangCli: true }),
      controller = new AbortController();
    const b = new SecondaryServer({ ...fake.dependencies, signal: controller.signal });
    const starting = b.start();
    setImmediate(() => controller.abort());
    await NodeAssert.rejects(starting, /secondary-start-failed/);
    NodeAssert.equal(fake.processes.size, 0);
    NodeAssert.deepEqual(
      fake.signals.map((entry) => entry.pid),
      [41, 40],
    );
    NodeAssert.equal(JSON.stringify(b.evidence).includes("synthetic-pair"), false);
  },
);

NodeTest.test("cancellation before start has no spawn or filesystem effect", async () => {
  const fake = fakeServer(),
    controller = new AbortController();
  controller.abort();
  const b = new SecondaryServer({ ...fake.dependencies, signal: controller.signal });
  await NodeAssert.rejects(b.start(), /secondary-start-failed/);
  NodeAssert.equal(fake.spawns.length, 0);
  NodeAssert.equal(fake.directories.length, 0);
});
