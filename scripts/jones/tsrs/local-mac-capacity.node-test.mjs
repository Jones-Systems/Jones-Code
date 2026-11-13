import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
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
NodeTest.test(
  "missing/cancelled/stale/storage refusal starts nothing and releases only owned lock",
  async () => {
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
