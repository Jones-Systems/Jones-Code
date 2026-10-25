import * as NodeAssert from "node:assert/strict";
import * as NodeEvents from "node:events";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeStream from "node:stream";
import * as NodeTest from "node:test";
import { availableBytes, parseOptions, runSetupWorktree, setupWorktree } from "./setup.ts";

const GIB = 1024n ** 3n;
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
  let cleaningUp = false;
  const forwarded = [];
  const dependencies = {
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
  return { children, forwarded, dependencies, marker, assertRunning, run };
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
