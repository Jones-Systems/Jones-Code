import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import * as NodeChildProcess from "node:child_process";
import * as NodeEvents from "node:events";
import { tryCapacityLock } from "./capacity-lock.ts";
import { runChild } from "./patch.ts";
import {
  compilerBudget,
  usesLocalMacCapacity,
  withLocalMacCapacity,
} from "./local-mac-capacity.ts";
function fixture() {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-capacity-"));
  let now = 0;
  const dependencies = {
    now: () => now,
    wait: async (ms) => {
      now += ms;
    },
    cpu: () => Array.from({ length: 12 }, () => ({ idle: now, total: now + 1 })),
    memory: async () => ({ normal: true }),
    storage: () => {},
    report: () => {},
  };
  return {
    directory,
    dependencies,
    options: {
      directory,
      root: directory,
      platform: "darwin",
      environment: {},
      signal: new AbortController().signal,
      jobs: 1,
    },
    close: () => NodeFS.rmSync(directory, { recursive: true }),
  };
}
NodeTest.test("adaptive budget uses both windows and rejects unknown requests/evidence", () => {
  NodeAssert.equal(usesLocalMacCapacity("darwin", { CI: "true" }), true);
  const hosted = {
    GITHUB_ACTIONS: "true",
    RUNNER_ENVIRONMENT: "github-hosted",
    RUNNER_OS: "macOS",
  };
  NodeAssert.equal(usesLocalMacCapacity("darwin", hosted), false);
  NodeAssert.equal(
    usesLocalMacCapacity("darwin", { ...hosted, RUNNER_ENVIRONMENT: "self-hosted" }),
    true,
  );
  NodeAssert.equal(usesLocalMacCapacity("darwin", { RUNNER_ENVIRONMENT: "github-hosted" }), true);
  NodeAssert.equal(usesLocalMacCapacity("linux", {}), false);
  for (const [u, n] of [
    [0.2, 6],
    [0.5, 3],
    [0.7, 1],
    [0.75, 0],
  ])
    NodeAssert.equal(compilerBudget(12, u, u), n);
  NodeAssert.equal(compilerBudget(12, 0.2, 0.75), 0);
  NodeAssert.equal(compilerBudget(12, 0.75, 0.2), 0);
  NodeAssert.equal(compilerBudget(12, 0.2, 0.2, 2), 2);
  for (const x of [NaN, Infinity, -1, 1.1]) NodeAssert.throws(() => compilerBudget(12, x, 0));
  for (const n of [0, -1, 1.5]) NodeAssert.throws(() => compilerBudget(12, 0, 0, n));
});
NodeTest.test(
  "lease spans native completion and contention starts no second producer",
  async () => {
    const f = fixture();
    let done, started;
    const waiting = new Promise((r) => {
      done = r;
    });
    const entered = new Promise((r) => {
      started = r;
    });
    try {
      const first = withLocalMacCapacity(
        f.options,
        async () => {
          started();
          await waiting;
          return 7;
        },
        f.dependencies,
      );
      await entered;
      NodeAssert.ok(NodeFS.existsSync(NodePath.join(f.directory, "build.lock")));
      await NodeAssert.rejects(
        withLocalMacCapacity(
          { ...f.options, deadlineMs: 100 },
          async () => NodeAssert.fail("contended launch"),
          f.dependencies,
        ),
        /deadline/,
      );
      done();
      NodeAssert.equal(await first, 7);
      NodeAssert.ok(!NodeFS.existsSync(NodePath.join(f.directory, "build.lock")));
    } finally {
      done?.();
      f.close();
    }
  },
);

async function crashedOwner(directory, phase) {
  const child = NodeChildProcess.spawn(
    process.execPath,
    [
      "--input-type=module",
      "-e",
      `import { tryCapacityLock } from ${JSON.stringify(new URL("./capacity-lock.ts", import.meta.url).href)};
     const lease = tryCapacityLock(process.argv[1]);
     if (process.argv[2] === 'producer') lease.producer();
     if (process.argv[2] === 'closed') { lease.producer(); lease.confirmClosure(); }
     process.send('ready');
     setInterval(() => {}, 1000);`,
      directory,
      phase,
    ],
    { stdio: ["ignore", "ignore", "inherit", "ipc"] },
  );
  const closed = NodeEvents.once(child, "close");
  try {
    const [ready] = await NodeEvents.once(child, "message", { signal: AbortSignal.timeout(10000) });
    NodeAssert.equal(ready, "ready");
    NodeAssert.equal(tryCapacityLock(directory), undefined, "live owner stays protected");
  } finally {
    child.kill("SIGKILL");
    await closed;
  }
}

NodeTest.test(
  "crashed admission/closed owners recover, while producer and legacy states fail closed",
  async () => {
    for (const phase of ["admission", "closed", "producer", "legacy"]) {
      const f = fixture();
      try {
        await crashedOwner(f.directory, phase === "legacy" ? "producer" : phase);
        const ownerPath = NodePath.join(f.directory, "build.lock/owner.json");
        if (phase === "legacy") {
          const { pid, nonce } = JSON.parse(NodeFS.readFileSync(ownerPath, "utf8"));
          NodeFS.writeFileSync(ownerPath, JSON.stringify({ pid, nonce }));
        }
        const before = NodeFS.readFileSync(ownerPath, "utf8");
        if (phase === "producer" || phase === "legacy") {
          NodeAssert.throws(() => tryCapacityLock(f.directory), /no producer-closure proof/);
          NodeAssert.equal(NodeFS.readFileSync(ownerPath, "utf8"), before);
        } else {
          NodeAssert.equal(tryCapacityLock(f.directory), undefined);
          const lease = tryCapacityLock(f.directory);
          NodeAssert.ok(lease);
          NodeAssert.equal(tryCapacityLock(f.directory), undefined);
          lease.release();
        }
      } finally {
        f.close();
      }
    }
  },
);

NodeTest.test(
  "recovery preserves a claimed generation, malformed metadata and foreign entries",
  async () => {
    for (const kind of ["claim", "extra", "invalid", "symlink"]) {
      const f = fixture();
      try {
        await crashedOwner(f.directory, "admission");
        const ownerPath = NodePath.join(f.directory, "build.lock/owner.json");
        const before = NodeFS.readFileSync(ownerPath, "utf8");
        if (kind === "claim")
          NodeFS.writeFileSync(
            NodePath.join(f.directory, `reclaim-${JSON.parse(before).nonce}`),
            "",
            { mode: 0o600 },
          );
        if (kind === "extra")
          NodeFS.writeFileSync(NodePath.join(f.directory, "build.lock/foreign"), "preserve");
        if (kind === "invalid") NodeFS.writeFileSync(ownerPath, "{}");
        if (kind === "symlink") {
          NodeFS.renameSync(ownerPath, NodePath.join(f.directory, "target"));
          NodeFS.symlinkSync(NodePath.join(f.directory, "target"), ownerPath);
        }
        NodeAssert.throws(() => tryCapacityLock(f.directory), /preserved/);
        NodeAssert.ok(NodeFS.existsSync(NodePath.join(f.directory, "build.lock")));
        if (["claim", "extra", "symlink"].includes(kind))
          NodeAssert.equal(NodeFS.readFileSync(ownerPath, "utf8"), before);
      } finally {
        f.close();
      }
    }
  },
);

NodeTest.test(
  "interrupted owner publication is bounded and never treated as stale authority",
  () => {
    const f = fixture();
    try {
      const lock = NodePath.join(f.directory, "build.lock");
      NodeFS.mkdirSync(lock, { mode: 0o700 });
      NodeAssert.equal(tryCapacityLock(f.directory), undefined);
      const old = new Date(Date.now() - 10000);
      NodeFS.utimesSync(lock, old, old);
      NodeAssert.throws(() => tryCapacityLock(f.directory), /owner record unavailable; preserved/);
      NodeAssert.ok(NodeFS.existsSync(lock));
    } finally {
      f.close();
    }
  },
);

NodeTest.test("cancelled native child is closed before rejection releases capacity", async () => {
  const f = fixture();
  const stop = new AbortController();
  let observed = false;
  try {
    await NodeAssert.rejects(
      withLocalMacCapacity(
        f.options,
        async (_budget, lease) => {
          await runChild({
            executable: process.execPath,
            args: ["-e", "setInterval(() => {}, 1000)"],
            cwd: f.directory,
            signal: stop.signal,
            spawned: async () => {
              stop.abort("SIGTERM");
            },
            closed: () => {
              observed = true;
              lease.confirmClosure();
            },
          });
          NodeAssert.ok(observed);
          throw Error("post-close failure");
        },
        f.dependencies,
      ),
      /post-close failure/,
    );
    NodeAssert.ok(observed);
    NodeAssert.ok(!NodeFS.existsSync(NodePath.join(f.directory, "build.lock")));
  } finally {
    f.close();
  }
});

NodeTest.test("competing reclaimers never remove a successor generation", async () => {
  const f = fixture();
  const children = [];
  try {
    await crashedOwner(f.directory, "admission");
    for (let index = 0; index < 2; index++) {
      const child = NodeChildProcess.spawn(
        process.execPath,
        [
          "--input-type=module",
          "-e",
          `
        import { tryCapacityLock } from ${JSON.stringify(new URL("./capacity-lock.ts", import.meta.url).href)};
        import { setImmediate } from 'node:timers/promises';
        process.once('message', async () => {
          let lease;
          for (let i = 0; i < 50 && !lease; i++) {
            try { lease = tryCapacityLock(process.argv[1]); }
            catch (error) { if (!String(error).includes('recovery already claimed')) throw error; }
            if (!lease) await setImmediate();
          }
          if (lease) {
            process.once('message', () => { lease.release(); process.disconnect(); });
            process.send('holding');
          } else { process.send('contended'); process.disconnect(); }
        });
      `,
          f.directory,
        ],
        { stdio: ["ignore", "ignore", "inherit", "ipc"] },
      );
      children.push({
        child,
        closed: NodeEvents.once(child, "close"),
        message: NodeEvents.once(child, "message", { signal: AbortSignal.timeout(10000) }),
      });
    }
    for (const { child } of children) child.send("start");
    const outcomes = await Promise.all(children.map(({ message }) => message));
    NodeAssert.equal(outcomes.filter(([state]) => state === "holding").length, 1);
    NodeAssert.equal(outcomes.filter(([state]) => state === "contended").length, 1);
    NodeAssert.ok(NodeFS.existsSync(NodePath.join(f.directory, "build.lock/owner.json")));
    for (let i = 0; i < children.length; i++)
      if (outcomes[i][0] === "holding") children[i].child.send("release");
    await Promise.all(children.map(({ closed }) => closed));
    NodeAssert.ok(!NodeFS.existsSync(NodePath.join(f.directory, "build.lock")));
  } finally {
    for (const { child } of children)
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    await Promise.all(children.map(({ closed }) => closed));
    f.close();
  }
});
NodeTest.test(
  "missing/cancelled/stale/storage refusal starts nothing and releases only owned lock",
  async () => {
    for (const deadlineMs of [0, -1, NaN, Infinity, 600_001]) {
      const f = fixture();
      try {
        await NodeAssert.rejects(
          withLocalMacCapacity(
            { ...f.options, deadlineMs },
            async () => NodeAssert.fail("invalid deadline launch"),
            f.dependencies,
          ),
          /deadline/,
        );
      } finally {
        f.close();
      }
    }
    for (const kind of ["missing", "busy", "cancel", "stale", "memory", "storage"]) {
      const f = fixture();
      const c = new AbortController();
      f.options.signal = c.signal;
      if (kind === "missing") f.dependencies.cpu = () => [];
      if (kind === "busy") {
        f.options.deadlineMs = 60_001;
        f.dependencies.cpu = () =>
          Array.from({ length: 12 }, () => ({ idle: 0, total: f.dependencies.now() + 1 }));
      }
      if (kind === "memory")
        f.dependencies.memory = async () => {
          throw Error("native pressure refused");
        };
      if (kind === "cancel")
        f.dependencies.wait = async () => {
          c.abort();
        };
      if (kind === "stale")
        f.dependencies.memory = async () => {
          await f.dependencies.wait(6000);
        };
      if (kind === "storage")
        f.dependencies.storage = () => {
          throw Error("unknown storage");
        };
      try {
        await NodeAssert.rejects(
          withLocalMacCapacity(
            f.options,
            async () => NodeAssert.fail("must not launch"),
            f.dependencies,
          ),
        );
        NodeAssert.ok(!NodeFS.existsSync(NodePath.join(f.directory, "build.lock")));
      } finally {
        f.close();
      }
    }
  },
);
NodeTest.test(
  "unknown producer rejection retains its lease; Linux policy remains unchanged",
  async () => {
    const f = fixture();
    try {
      await NodeAssert.rejects(
        withLocalMacCapacity(
          f.options,
          async () => {
            throw Error("native outcome unknown");
          },
          f.dependencies,
        ),
        /native outcome/,
      );
      NodeAssert.ok(NodeFS.existsSync(NodePath.join(f.directory, "build.lock/owner.json")));
      NodeAssert.equal(
        await withLocalMacCapacity({ ...f.options, platform: "linux" }, async (n) => n, {
          ...f.dependencies,
          cpu: () => NodeAssert.fail("Linux must not sample"),
        }),
        1,
      );
    } finally {
      f.close();
    }
  },
);
