import * as NodeAssert from "node:assert/strict";
import * as NodeEvents from "node:events";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeStream from "node:stream";
import * as NodeTest from "node:test";
import { recordCompletion } from "../../../apps/server/src/jones/workspaceRetirement/CompletionObservation.ts";
import { availableBytes, parseOptions, runSetupWorktree, setupWorktree } from "./setup.ts";

const GIB = 1024n ** 3n;
const SINK_TEST_DEADLINE_MS = 10_000;
async function fixture(run) {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-setup-"));
  const cleanups = [];
  try {
    const project = NodePath.join(root, "project");
    const worktree = NodePath.join(root, "worktree");
    for (const base of [project, worktree])
      NodeFS.mkdirSync(NodePath.join(base, "infra/relay"), { recursive: true });
    await run({ root, project, worktree, onCleanup: (cleanup) => cleanups.push(cleanup) });
  } finally {
    try {
      for (const cleanup of cleanups.reverse()) await cleanup();
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  }
}
function harness(capacity = () => 40n * GIB, statuses = []) {
  const events = [];
  return {
    events,
    dependencies: {
      platform: "linux",
      available(destination) {
        events.push(["capacity", destination]);
        return capacity();
      },
      run(command, args, options) {
        events.push(["run", command, args, options]);
        return statuses.length ? statuses.shift() : 0;
      },
      report() {},
    },
  };
}

NodeTest.test("lightweight setup preserves real files and replaces only existing symlinks", () =>
  fixture(async ({ project, worktree }) => {
    const source = NodePath.join(project, ".env");
    const target = NodePath.join(worktree, ".env");
    NodeFS.writeFileSync(source, "SYNTHETIC_SOURCE=1");
    NodeFS.writeFileSync(target, "SYNTHETIC_TARGET=1");
    NodeFS.writeFileSync(NodePath.join(project, "infra/relay/.env"), "SYNTHETIC_RELAY=1");
    NodeFS.symlinkSync(
      NodePath.join(project, "missing"),
      NodePath.join(worktree, "infra/relay/.env"),
    );
    const { events, dependencies } = harness(() => {
      throw Error("must not measure");
    });
    NodeAssert.equal(await setupWorktree([], project, worktree, dependencies), 0);
    NodeAssert.equal(NodeFS.readFileSync(target, "utf8"), "SYNTHETIC_TARGET=1");
    NodeAssert.equal(
      NodeFS.readlinkSync(NodePath.join(worktree, "infra/relay/.env")),
      NodePath.join(project, "infra/relay/.env"),
    );
    NodeAssert.deepEqual(events, []);
    NodeAssert.equal(await setupWorktree([], project, project, dependencies), 0);
    NodeAssert.equal(NodeFS.readFileSync(source, "utf8"), "SYNTHETIC_SOURCE=1");
  }),
);

NodeTest.test("source symlinks are skipped and missing sources have no effect", () =>
  fixture(async ({ project, worktree }) => {
    NodeFS.symlinkSync(NodePath.join(project, "missing"), NodePath.join(project, ".env"));
    const { dependencies } = harness();
    NodeAssert.equal(await setupWorktree([], project, worktree, dependencies), 0);
    NodeAssert.equal(NodeFS.existsSync(NodePath.join(worktree, ".env")), false);
  }),
);

NodeTest.test("install uses frozen unattended command and warming measures capacity again", () =>
  fixture(async ({ project, worktree }) => {
    const { events, dependencies } = harness();
    NodeAssert.equal(
      await setupWorktree(["--install", "--warm"], project, worktree, dependencies),
      0,
    );
    NodeAssert.deepEqual(
      events.map((event) => event[0]),
      ["capacity", "run", "capacity", "run"],
    );
    NodeAssert.deepEqual(events[1], [
      "run",
      "vp i --frozen-lockfile -- --yes",
      [],
      {
        cwd: worktree,
        shell: true,
        stdio: ["ignore", "pipe", "pipe"],
      },
    ]);
    NodeAssert.equal(events[3][1], process.execPath);
    NodeAssert.deepEqual(events[3][2], [
      NodePath.join(worktree, "apps/web/scripts/warm-dep-cache.ts"),
    ]);
  }),
);

NodeTest.test("install-only omits warming; installer and warmer failures propagate", () =>
  fixture(async ({ project, worktree }) => {
    const installOnly = harness();
    NodeAssert.equal(
      await setupWorktree(["--install"], project, worktree, installOnly.dependencies),
      0,
    );
    NodeAssert.equal(installOnly.events.filter((event) => event[0] === "run").length, 1);
    for (const status of [27, null]) {
      const failure = harness(undefined, [status]);
      NodeAssert.equal(
        await setupWorktree(["--install", "--warm"], project, worktree, failure.dependencies),
        status ?? 1,
      );
      NodeAssert.equal(failure.events.length, 2);
    }
    for (const status of [19, null]) {
      const failure = harness(undefined, [0, status]);
      NodeAssert.equal(
        await setupWorktree(["--install", "--warm"], project, worktree, failure.dependencies),
        status ?? 1,
      );
    }
  }),
);

NodeTest.test("one byte short or unknown capacity starts no child and preserves output", () =>
  fixture(async ({ project, worktree }) => {
    const output = NodePath.join(worktree, "node_modules");
    NodeFS.mkdirSync(output);
    NodeFS.writeFileSync(NodePath.join(output, "sentinel"), "existing");
    for (const capacity of [
      () => 40n * GIB - 1n,
      () => -1n,
      () => 40,
      () => {
        throw Error("unavailable");
      },
    ]) {
      const { events, dependencies } = harness(capacity);
      await NodeAssert.rejects(
        () => setupWorktree(["--install", "--warm"], project, worktree, dependencies),
        /refused/,
      );
      NodeAssert.equal(events.length, 1);
      NodeAssert.equal(NodeFS.readFileSync(NodePath.join(output, "sentinel"), "utf8"), "existing");
    }
  }),
);

NodeTest.test("fresh post-install refusal prevents warmer", () =>
  fixture(async ({ project, worktree }) => {
    for (const second of [39n * GIB, undefined]) {
      let count = 0;
      const { events, dependencies } = harness(() => {
        if (++count === 1) return 40n * GIB;
        if (second === undefined) throw Error("unknown");
        return second;
      });
      await NodeAssert.rejects(
        () => setupWorktree(["--install", "--warm"], project, worktree, dependencies),
        /Cache warm refused/,
      );
      NodeAssert.deepEqual(
        events.map((event) => event[0]),
        ["capacity", "run", "capacity"],
      );
    }
  }),
);

NodeTest.test(
  "budgets can only increase and equality admits without subtracting existing output",
  () =>
    fixture(async ({ project, worktree }) => {
      const args = ["--install", "--reserve-gib", "31", "--peak-gib", "11"];
      const short = harness(() => 42n * GIB - 1n);
      await NodeAssert.rejects(
        () => setupWorktree(args, project, worktree, short.dependencies),
        /refused/,
      );
      NodeFS.mkdirSync(NodePath.join(worktree, "node_modules"));
      const equal = harness(() => 42n * GIB);
      NodeAssert.equal(await setupWorktree(args, project, worktree, equal.dependencies), 0);
    }),
);

NodeTest.test("invalid options fail before filesystem or producer effects", async () => {
  for (const args of [
    ["--warm"],
    ["--force"],
    ["--install", "--install"],
    ["--reserve-gib"],
    ...["0", "-1", "29", "30.5", "NaN", "Infinity", "1e3", " 30", "030"].map((value) => [
      "--reserve-gib",
      value,
    ]),
    ["--peak-gib", "9"],
    ["--peak-gib", "0"],
  ])
    NodeAssert.throws(() => parseOptions(args));
  const { events, dependencies } = harness();
  await NodeAssert.rejects(
    () => setupWorktree(["--warm"], "missing", "missing", dependencies),
    /requires --install/,
  );
  await NodeAssert.rejects(
    () => setupWorktree([], undefined, "missing", dependencies),
    /T3CODE_PROJECT_ROOT/,
  );
  NodeAssert.deepEqual(events, []);
});

NodeTest.test(
  "capacity follows destination symlinks and missing descendants without creating them",
  () =>
    fixture(({ root, worktree }) => {
      const link = NodePath.join(root, "linked-worktree");
      NodeFS.symlinkSync(worktree, link);
      const missing = NodePath.join(link, "future", "output");
      NodeAssert.equal(typeof availableBytes(missing), "bigint");
      NodeAssert.ok(availableBytes(missing) >= 0n);
      NodeAssert.equal(NodeFS.existsSync(NodePath.join(worktree, "future")), false);
      NodeFS.writeFileSync(NodePath.join(worktree, "file"), "synthetic");
      NodeAssert.throws(() => availableBytes(NodePath.join(worktree, "file", "child")));
    }),
);

function lifecycleHarness(worktree, onCleanup) {
  const children = [];
  const completions = [];
  const childWaiters = new Map();
  let cleaningUp = false;
  const forwarded = [];
  const dependencies = {
    platform: "linux",
    available: () => 40n * GIB,
    report() {},
    stdout: { write: (chunk) => forwarded.push(chunk) },
    stderr: { write: (chunk) => forwarded.push(chunk) },
    spawn(command, args, options) {
      const child = new NodeEvents.EventEmitter();
      child.stdout = new NodeStream.PassThrough();
      child.stderr = new NodeStream.PassThrough();
      child.signals = [];
      child.kill = (signal) => {
        child.signals.push(signal);
        return true;
      };
      children.push({ child, command, args, options });
      childWaiters.get(children.length - 1)?.(child);
      if (cleaningUp) queueMicrotask(() => child.emit("close", 1, null));
      return child;
    },
  };
  const marker = (name) => NodePath.join(worktree, ".t3", `setup-${name}`);
  const assertRunning = () => {
    NodeAssert.equal(NodeFS.readFileSync(marker("running"), "utf8"), `${process.pid}\n`);
    NodeAssert.equal(NodeFS.existsSync(marker("done")), false);
    NodeAssert.equal(NodeFS.existsSync(marker("failed")), false);
  };
  onCleanup(async () => {
    cleaningUp = true;
    for (const { child } of children) child.emit("close", 1, null);
    await Promise.allSettled(completions);
    for (const { child } of children) {
      child.stdout.destroy();
      child.stderr.destroy();
    }
  });
  const run = (args, project) => {
    const completion = runSetupWorktree(args, project, worktree, dependencies);
    completions.push(completion);
    return completion;
  };
  return {
    children,
    forwarded,
    dependencies,
    marker,
    assertRunning,
    run,
    started: (index = 0) =>
      children[index]
        ? Promise.resolve(children[index].child)
        : new Promise((resolve) => childWaiters.set(index, resolve)),
  };
}

NodeTest.test(
  "lightweight lifecycle clears stale markers without inspecting install metadata",
  () =>
    fixture(async ({ project, worktree, onCleanup }) => {
      const harness = lifecycleHarness(worktree, onCleanup);
      NodeFS.mkdirSync(NodePath.join(worktree, ".t3"));
      NodeFS.writeFileSync(harness.marker("failed"), "stale failure");
      NodeFS.writeFileSync(harness.marker("done"), "stale completion");
      NodeFS.mkdirSync(NodePath.join(worktree, "node_modules"));
      NodeFS.writeFileSync(
        NodePath.join(worktree, "node_modules/.modules.yaml"),
        "invalid metadata",
      );
      NodeAssert.equal(await harness.run([], project), 0);
      NodeAssert.equal(NodeFS.existsSync(harness.marker("done")), true);
      NodeAssert.equal(NodeFS.existsSync(harness.marker("running")), false);
      NodeAssert.equal(NodeFS.existsSync(harness.marker("failed")), false);
      NodeAssert.equal(harness.children.length, 0);
    }),
);

NodeTest.test("install completion waits for output closure and records the failure tail", () =>
  fixture(async ({ project, worktree, onCleanup }) => {
    const harness = lifecycleHarness(worktree, onCleanup);
    const pending = harness.run(["--install", "--warm"], project);
    await harness.started();
    const { child, options } = harness.children[0];
    NodeAssert.deepEqual(options.stdio, ["ignore", "pipe", "pipe"]);
    harness.assertRunning();
    child.stdout.write("early output\n");
    child.emit("exit", 27, null);
    await Promise.resolve();
    harness.assertRunning();
    const tail = Array.from({ length: 40 }, (_, index) => `failure line ${index}`).join("\n");
    child.stderr.write(`${tail}\n`);
    child.emit("close", 27, null);
    NodeAssert.equal(await pending, 27);
    NodeAssert.equal(NodeFS.existsSync(harness.marker("running")), false);
    NodeAssert.equal(NodeFS.existsSync(harness.marker("done")), false);
    NodeAssert.equal(
      NodeFS.readFileSync(harness.marker("failed"), "utf8"),
      `Setup exited with code 27.\n${tail.split("\n").slice(-30).join("\n")}\n`,
    );
    NodeAssert.equal(harness.forwarded.join(""), `early output\n${tail}\n`);
    NodeAssert.equal(harness.children.length, 1);
  }),
);

NodeTest.test(
  "cancellation forwards each supported signal and waits for close before failure",
  () =>
    fixture(async ({ project, worktree, onCleanup }) => {
      for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) {
        const harness = lifecycleHarness(worktree, onCleanup);
        const before = process.listenerCount(signal);
        const pending = harness.run(["--install", "--warm"], project);
        await harness.started();
        const { child } = harness.children[0];
        process.emit(signal);
        NodeAssert.deepEqual(child.signals, [signal]);
        child.emit("exit", 0, null);
        await Promise.resolve();
        harness.assertRunning();
        child.stdout.write("output after shell exit\n");
        child.emit("close", 0, null);
        NodeAssert.equal(await pending, 1);
        NodeAssert.equal(NodeFS.existsSync(harness.marker("running")), false);
        NodeAssert.equal(NodeFS.existsSync(harness.marker("done")), false);
        NodeAssert.match(
          NodeFS.readFileSync(harness.marker("failed"), "utf8"),
          new RegExp(`Cancelled by ${signal}`),
        );
        NodeAssert.equal(harness.children.length, 1);
        NodeAssert.equal(process.listenerCount(signal), before);
      }
    }),
);

NodeTest.test("successful install and warm retain running marker until both outputs close", () =>
  fixture(async ({ project, worktree, onCleanup }) => {
    const harness = lifecycleHarness(worktree, onCleanup);
    NodeFS.mkdirSync(NodePath.join(worktree, "node_modules"));
    NodeFS.writeFileSync(
      NodePath.join(worktree, "node_modules/.modules.yaml"),
      JSON.stringify({ storeDir: worktree }),
    );
    let warmed;
    const startedWarm = new Promise((resolve) => {
      warmed = resolve;
    });
    const spawn = harness.dependencies.spawn;
    harness.dependencies.spawn = (...args) => {
      const child = spawn(...args);
      if (harness.children.length === 2) warmed();
      return child;
    };
    const pending = harness.run(["--install", "--warm"], project);
    await harness.started();
    const installer = harness.children[0].child;
    installer.emit("exit", 0, null);
    await Promise.resolve();
    harness.assertRunning();
    NodeAssert.equal(harness.children.length, 1);
    installer.emit("close", 0, null);
    await startedWarm;
    harness.assertRunning();
    const warmer = harness.children[1].child;
    warmer.emit("exit", 0, null);
    await Promise.resolve();
    harness.assertRunning();
    warmer.emit("close", 0, null);
    NodeAssert.equal(await pending, 0);
    NodeAssert.equal(NodeFS.existsSync(harness.marker("done")), true);
    NodeAssert.equal(NodeFS.existsSync(harness.marker("running")), false);
    NodeAssert.equal(NodeFS.existsSync(harness.marker("failed")), false);
  }),
);

NodeTest.test("spawn errors wait for close and setup exceptions leave a failed marker", () =>
  fixture(async ({ project, worktree, onCleanup }) => {
    const harness = lifecycleHarness(worktree, onCleanup);
    const pending = harness.run(["--install"], project);
    await harness.started();
    const { child } = harness.children[0];
    child.emit("error", new Error("synthetic spawn failure"));
    await Promise.resolve();
    harness.assertRunning();
    child.emit("close", -2, null);
    NodeAssert.equal(await pending, -2);
    NodeAssert.match(
      NodeFS.readFileSync(harness.marker("failed"), "utf8"),
      /synthetic spawn failure/,
    );
    await NodeAssert.rejects(harness.run(["--warm"], project), /requires --install/);
    NodeAssert.equal(NodeFS.existsSync(harness.marker("running")), false);
    NodeAssert.equal(NodeFS.existsSync(harness.marker("done")), false);
    NodeAssert.match(NodeFS.readFileSync(harness.marker("failed"), "utf8"), /requires --install/);
    NodeAssert.equal(harness.children.length, 1);
  }),
);

NodeTest.test(
  "producer finalization records cheap success, failure and drained cancellation in real Git admin state",
  () =>
    fixture(async ({ project, worktree, onCleanup }) => {
      const { spawnSync } = await import("node:child_process");
      function git(args) {
        const r = spawnSync("git", ["-C", worktree, ...args], {
          encoding: "utf8",
          env: {
            PATH: process.env.PATH,
            GIT_CONFIG_NOSYSTEM: "1",
            GIT_CONFIG_GLOBAL: "/dev/null",
            GIT_AUTHOR_NAME: "Fixture",
            GIT_AUTHOR_EMAIL: "fixture@example.invalid",
            GIT_COMMITTER_NAME: "Fixture",
            GIT_COMMITTER_EMAIL: "fixture@example.invalid",
          },
        });
        NodeAssert.equal(r.status, 0, r.stderr);
      }
      git(["init", "-b", "fixture"]);
      git(["commit", "--allow-empty", "-m", "fixture"]);
      const readRecords = () =>
        NodeFS.readdirSync(NodePath.join(worktree, ".git/workspace-completions")).map((name) =>
          JSON.parse(
            NodeFS.readFileSync(
              NodePath.join(worktree, ".git/workspace-completions", name),
              "utf8",
            ),
          ),
        );
      const harness = lifecycleHarness(worktree, onCleanup);
      // This case asserts the records, not the sink's 100 ms bound; a slow host must not drop them.
      harness.dependencies.record = (observation) =>
        recordCompletion(observation, undefined, SINK_TEST_DEADLINE_MS);
      NodeAssert.equal(await harness.run([], project), 0);
      NodeAssert.equal(readRecords()[0].status, "succeeded");
      NodeAssert.equal(readRecords()[0].proofStatus, "deferred");
      NodeAssert.equal("manifest" in readRecords()[0], false);
      NodeAssert.equal(NodeFS.existsSync(harness.marker("unfinalized")), false);
      for (const [index, cancel] of [false, true].entries()) {
        const pending = harness.run(["--install"], project);
        const child = await harness.started(index);
        if (cancel) process.emit("SIGTERM");
        child.emit("exit", 1, null);
        await Promise.resolve();
        harness.assertRunning();
        NodeAssert.equal(readRecords().length, index + 1);
        child.emit("close", 1, null);
        NodeAssert.equal(await pending, 1);
        NodeAssert.ok(readRecords().some((r) => r.status === (cancel ? "cancelled" : "failed")));
      }
      NodeAssert.equal(NodeFS.existsSync(NodePath.join(worktree, ".git/workspace-outputs")), false);
      NodeAssert.equal(NodeFS.existsSync(harness.marker("unfinalized")), false);
    }),
);

NodeTest.test(
  "optional producer metadata failure preserves successful ordinary setup without manufacturing an obligation",
  () =>
    fixture(async ({ project, worktree, onCleanup }) => {
      const harness = lifecycleHarness(worktree, onCleanup);
      NodeAssert.equal(await harness.run([], project), 0);
      NodeAssert.equal(NodeFS.existsSync(harness.marker("done")), true);
      NodeAssert.equal(NodeFS.existsSync(harness.marker("unfinalized")), false);
      NodeAssert.equal(harness.children.length, 0);
    }),
);

const {
  createLocalMacAdmission,
  cpuEvidence,
  memoryEvidence,
  producerEnvironment,
  utilityReader,
  UnknownSamplerClosure,
} = await import("./local-mac-admission.ts");
const vm = (ins = 0, outs = 0, page = 4096) =>
  `Mach Virtual Memory Statistics: (page size of ${page} bytes)\nSwapins: ${ins}.\nSwapouts: ${outs}.\n`;
const cpu = (user, idle) => [{ times: { user, idle, sys: 0, nice: 0, irq: 0 } }];
function syntheticSampler({ busy = 10, pressure = "1", swap = 0, utility, wait, secondCpu } = {}) {
  let time = 0;
  let cpuCalls = 0;
  let vmCalls = 0;
  const commands = [];
  const admission = createLocalMacAdmission({
    now: () => time,
    cpu: () =>
      cpuCalls++ % 2 === 0
        ? cpu(0, 0)
        : (secondCpu ?? cpu(Math.round(busy * 1000), 100000 - Math.round(busy * 1000))),
    wait: async (signal) => {
      time += 1000;
      await wait?.(signal);
      signal.throwIfAborted();
    },
    utility: async (command, args, signal) => {
      commands.push([command, args]);
      signal.throwIfAborted();
      if (utility) return utility(command, args, signal);
      return command.endsWith("sysctl") ? pressure : vm(0, vmCalls++ % 2 === 0 ? 0 : swap);
    },
  });
  return {
    admission,
    commands,
    advance: (amount) => {
      time += amount;
    },
  };
}
NodeTest.test(
  "native CPU and memory policies retain exact thresholds and reject invalid evidence",
  () => {
    NodeAssert.equal(cpuEvidence(cpu(0, 0), cpu(84999, 15001)).usedPercent, 84.999);
    NodeAssert.equal(cpuEvidence(cpu(0, 0), cpu(85, 15)).usedPercent, 85);
    for (const pair of [
      [[], []],
      [cpu(0, 0), []],
      [cpu(1, 0), cpu(0, 100)],
      [cpu(0, 0), cpu(NaN, 1)],
      [cpu(0, 0), cpu(0, 0)],
    ])
      NodeAssert.throws(() => cpuEvidence(...pair));
    for (const pressure of ["1", "2", "4"])
      NodeAssert.equal(memoryEvidence(vm(), vm(2560), 1000, pressure).swapBytesPerSecond, 10485760);
    for (const args of [
      [vm(1), vm(), 1000, "1"],
      [vm(), vm(0, 0, 8192), 1000, "1"],
      [vm(), vm(), 0, "1"],
      [vm(), vm(), 5001, "1"],
      [vm(), vm(), 1000, "3"],
      [vm(), vm() + "Swapins: 1.\n", 1000, "1"],
      ["", vm(), 1000, "1"],
      [vm(), vm(Number.MAX_SAFE_INTEGER), 1000, "1"],
    ])
      NodeAssert.throws(() => memoryEvidence(...args));
  },
);
NodeTest.test("Mac refusal evidence launches zero producers, including CI=true", async () => {
  const priorCI = process.env.CI;
  process.env.CI = "true";
  try {
    for (const config of [
      { busy: 85 },
      { busy: 100 },
      { pressure: "4", swap: 2561 },
      { pressure: "unknown" },
      { secondCpu: [] },
      { utility: () => "malformed" },
      {
        utility: () => {
          throw Error("missing utility");
        },
      },
    ]) {
      await fixture(async ({ project, worktree }) => {
        const h = harness();
        Object.assign(h.dependencies, {
          platform: "darwin",
          admission: syntheticSampler(config).admission,
        });
        await NodeAssert.rejects(
          setupWorktree(["--install", "--warm"], project, worktree, h.dependencies),
        );
        NodeAssert.equal(h.events.filter((e) => e[0] === "run").length, 0);
      });
    }
  } finally {
    if (priorCI === undefined) delete process.env.CI;
    else process.env.CI = priorCI;
  }
});
NodeTest.test(
  "normal and warning high swap pass; critical equality passes; samples are single-use and invocation-bound",
  async () => {
    for (const config of [
      { pressure: "1", swap: 9999 },
      { pressure: "2", swap: 9999 },
      { pressure: "4", swap: 2560 },
      { busy: 84.999 },
    ]) {
      const s = syntheticSampler(config);
      const attempt = {};
      const signal = new AbortController().signal;
      const receipt = await s.admission.collect(attempt, signal);
      s.admission.consume(receipt, attempt, signal);
      NodeAssert.throws(() => s.admission.consume(receipt, attempt, signal));
    }
    for (const mode of ["wrong", "stale", "future", "foreign"]) {
      const s = syntheticSampler();
      const attempt = {};
      const signal = new AbortController().signal;
      const receipt = await s.admission.collect(attempt, signal);
      if (mode === "stale") s.advance(1001);
      if (mode === "future") s.advance(-1);
      const gate = mode === "foreign" ? syntheticSampler().admission : s.admission;
      NodeAssert.throws(() => gate.consume(receipt, mode === "wrong" ? {} : attempt, signal));
    }
  },
);
NodeTest.test(
  "Mac setup independently samples install and warm and only scopes supported child controls",
  () =>
    fixture(async ({ project, worktree }) => {
      const h = harness();
      const s = syntheticSampler();
      Object.assign(h.dependencies, { platform: "darwin", admission: s.admission });
      const original = { ...process.env };
      NodeAssert.equal(
        await setupWorktree(["--install", "--warm"], project, worktree, h.dependencies),
        0,
      );
      NodeAssert.equal(s.commands.length, 6);
      const runs = h.events.filter((e) => e[0] === "run");
      NodeAssert.equal(runs[0][3].env.PNPM_CONFIG_CHILD_CONCURRENCY, "1");
      NodeAssert.equal(runs[1][3].env.ROLLDOWN_WORKER_THREADS, "1");
      NodeAssert.equal(runs[1][3].env.ROLLDOWN_MAX_BLOCKING_THREADS, "1");
      NodeAssert.ok(
        JSON.stringify({ ...process.env }) === JSON.stringify(original),
        "parent environment unchanged",
      );
      const env = producerEnvironment("install", {
        pnpm_config_child_concurrency: "9",
        PnPm_CoNfIg_ChIlD_CoNcUrReNcY: "8",
        KEEP: "yes",
      });
      NodeAssert.deepEqual(env, { KEEP: "yes", PNPM_CONFIG_CHILD_CONCURRENCY: "1" });
    }),
);
NodeTest.test(
  "fresh storage and final abort/freshness checks prevent a launch after sampling",
  async () => {
    for (const mode of ["stale", "abort", "second-hold", "between", "during", "before"]) {
      await fixture(async ({ project, worktree }) => {
        const controller = new AbortController();
        const s = syntheticSampler({
          wait: () => {
            if (mode === "during") controller.abort();
          },
        });
        const h = harness();
        let capacities = 0;
        Object.assign(h.dependencies, {
          platform: "darwin",
          signal: controller.signal,
          admission: s.admission,
          available: () => {
            capacities++;
            if (mode === "stale") s.advance(1001);
            if (mode === "abort") controller.abort();
            return mode === "second-hold" && capacities === 2 ? 0n : 40n * GIB;
          },
          afterInstall: () => {
            if (mode === "between") controller.abort();
          },
        });
        if (mode === "before") controller.abort();
        await NodeAssert.rejects(
          setupWorktree(["--install", "--warm"], project, worktree, h.dependencies),
        );
        NodeAssert.equal(
          h.events.filter((e) => e[0] === "run").length,
          ["second-hold", "between"].includes(mode) ? 1 : 0,
        );
      });
    }
  },
);
NodeTest.test("Mac link-only never samples and failed install never samples warm", () =>
  fixture(async ({ project, worktree }) => {
    const h = harness(undefined, [7]);
    const s = syntheticSampler();
    Object.assign(h.dependencies, { platform: "darwin", admission: s.admission });
    NodeAssert.equal(await setupWorktree([], project, worktree, h.dependencies), 0);
    NodeAssert.equal(s.commands.length, 0);
    NodeAssert.equal(
      await setupWorktree(["--install", "--warm"], project, worktree, h.dependencies),
      7,
    );
    NodeAssert.equal(s.commands.length, 3);
  }),
);
NodeTest.test(
  "unknown sampler closure retains the unfinalized obligation without success metadata",
  () =>
    fixture(async ({ project, worktree, onCleanup }) => {
      const h = lifecycleHarness(worktree, onCleanup);
      let recorded = false;
      Object.assign(h.dependencies, {
        platform: "darwin",
        admission: syntheticSampler({
          utility: () => {
            throw new UnknownSamplerClosure("synthetic unclosed utility");
          },
        }).admission,
        record: () => {
          recorded = true;
        },
      });
      await NodeAssert.rejects(h.run(["--install"], project), UnknownSamplerClosure);
      NodeAssert.equal(h.children.length, 0);
      NodeAssert.equal(recorded, false);
      await NodeAssert.rejects(h.run(["--install"], project), /cannot be replayed/);
      NodeAssert.equal(h.children.length, 0);
      NodeAssert.equal(NodeFS.existsSync(h.marker("unfinalized")), true);
      NodeAssert.equal(NodeFS.existsSync(h.marker("done")), false);
    }),
);
NodeTest.test(
  "utility timeout, abort, oversize and nonzero await close; unclosed output stays unknown",
  async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    try {
      for (const mode of [
        "timeout",
        "abort",
        "oversize",
        "nonzero",
        "missing",
        "unclosed",
        "kill-error",
      ]) {
        const child = new NodeEvents.EventEmitter();
        child.stdout = new NodeStream.PassThrough();
        child.stderr = new NodeStream.PassThrough();
        child.kill = () => {
          if (mode === "kill-error") throw Error("signal refused");
          return true;
        };
        const controller = new AbortController();
        const read = utilityReader(() => child);
        let finished = false;
        const pending = read("/usr/bin/vm_stat", [], controller.signal);
        const rejected = NodeAssert.rejects(
          pending,
          ["unclosed", "kill-error"].includes(mode) ? UnknownSamplerClosure : Error,
        );
        pending.then(
          () => {
            finished = true;
          },
          () => {
            finished = true;
          },
        );
        if (mode === "abort") controller.abort();
        if (mode === "oversize") child.stdout.emit("data", Buffer.alloc(65537));
        if (["timeout", "unclosed", "kill-error"].includes(mode)) t.mock.timers.tick(2000);
        if (mode === "missing") child.emit("error", Error("ENOENT"));
        child.emit("exit", 1);
        await Promise.resolve();
        NodeAssert.equal(finished, false);
        if (["unclosed", "kill-error"].includes(mode)) t.mock.timers.tick(250);
        else child.emit("close", 1);
        await rejected;
        child.stdout.destroy();
        child.stderr.destroy();
      }
    } finally {
      t.mock.timers.reset();
    }
  },
);
NodeTest.test(
  "overall deadline cancels sampling and invalid collection clocks refuse",
  async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    try {
      const s = syntheticSampler({
        wait: (signal) =>
          new Promise((resolve, reject) => {
            signal.addEventListener("abort", () => reject(Error("deadline")), { once: true });
          }),
      });
      const pending = s.admission.collect({}, new AbortController().signal);
      const rejected = NodeAssert.rejects(pending, /deadline/);
      await Promise.resolve();
      await Promise.resolve();
      t.mock.timers.tick(5000);
      await rejected;
    } finally {
      t.mock.timers.reset();
    }
    for (const elapsed of [0, -1, NaN, 5001]) {
      const gate = createLocalMacAdmission({
        cpu: (() => {
          let n = 0;
          return () => (n++ ? cpu(1, 99) : cpu(0, 0));
        })(),
        now: (() => {
          let n = 0;
          return () => (n++ ? elapsed : 0);
        })(),
        wait: async () => {},
        utility: async (command) => (command.endsWith("sysctl") ? "1" : vm()),
      });
      await NodeAssert.rejects(gate.collect({}, new AbortController().signal));
    }
  },
);
