import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import { availableBytes, parseOptions, setupWorktree } from "./setup.ts";

const GIB = 1024n ** 3n;
function fixture(run) {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-setup-"));
  try {
    const project = NodePath.join(root, "project");
    const worktree = NodePath.join(root, "worktree");
    for (const base of [project, worktree])
      NodeFS.mkdirSync(NodePath.join(base, "infra/relay"), { recursive: true });
    run({ root, project, worktree });
  } finally {
    NodeFS.rmSync(root, { recursive: true, force: true });
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
  fixture(({ project, worktree }) => {
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
    NodeAssert.equal(setupWorktree([], project, worktree, dependencies), 0);
    NodeAssert.equal(NodeFS.readFileSync(target, "utf8"), "SYNTHETIC_TARGET=1");
    NodeAssert.equal(
      NodeFS.readlinkSync(NodePath.join(worktree, "infra/relay/.env")),
      NodePath.join(project, "infra/relay/.env"),
    );
    NodeAssert.deepEqual(events, []);
    NodeAssert.equal(setupWorktree([], project, project, dependencies), 0);
    NodeAssert.equal(NodeFS.readFileSync(source, "utf8"), "SYNTHETIC_SOURCE=1");
  }),
);

NodeTest.test("source symlinks are skipped and missing sources have no effect", () =>
  fixture(({ project, worktree }) => {
    NodeFS.symlinkSync(NodePath.join(project, "missing"), NodePath.join(project, ".env"));
    const { dependencies } = harness();
    NodeAssert.equal(setupWorktree([], project, worktree, dependencies), 0);
    NodeAssert.equal(NodeFS.existsSync(NodePath.join(worktree, ".env")), false);
  }),
);

NodeTest.test("install uses frozen unattended command and warming measures capacity again", () =>
  fixture(({ project, worktree }) => {
    const { events, dependencies } = harness();
    NodeAssert.equal(setupWorktree(["--install", "--warm"], project, worktree, dependencies), 0);
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
        stdio: ["ignore", "inherit", "inherit"],
      },
    ]);
    NodeAssert.equal(events[3][1], process.execPath);
    NodeAssert.deepEqual(events[3][2], [
      NodePath.join(worktree, "apps/web/scripts/warm-dep-cache.ts"),
    ]);
  }),
);

NodeTest.test("install-only omits warming; installer and warmer failures propagate", () =>
  fixture(({ project, worktree }) => {
    const installOnly = harness();
    NodeAssert.equal(setupWorktree(["--install"], project, worktree, installOnly.dependencies), 0);
    NodeAssert.equal(installOnly.events.filter((event) => event[0] === "run").length, 1);
    for (const status of [27, null]) {
      const failure = harness(undefined, [status]);
      NodeAssert.equal(
        setupWorktree(["--install", "--warm"], project, worktree, failure.dependencies),
        status ?? 1,
      );
      NodeAssert.equal(failure.events.length, 2);
    }
    for (const status of [19, null]) {
      const failure = harness(undefined, [0, status]);
      NodeAssert.equal(
        setupWorktree(["--install", "--warm"], project, worktree, failure.dependencies),
        status ?? 1,
      );
    }
  }),
);

NodeTest.test("one byte short or unknown capacity starts no child and preserves output", () =>
  fixture(({ project, worktree }) => {
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
      NodeAssert.throws(
        () => setupWorktree(["--install", "--warm"], project, worktree, dependencies),
        /refused/,
      );
      NodeAssert.equal(events.length, 1);
      NodeAssert.equal(NodeFS.readFileSync(NodePath.join(output, "sentinel"), "utf8"), "existing");
    }
  }),
);

NodeTest.test("fresh post-install refusal prevents warmer", () =>
  fixture(({ project, worktree }) => {
    for (const second of [39n * GIB, undefined]) {
      let count = 0;
      const { events, dependencies } = harness(() => {
        if (++count === 1) return 40n * GIB;
        if (second === undefined) throw Error("unknown");
        return second;
      });
      NodeAssert.throws(
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
    fixture(({ project, worktree }) => {
      const args = ["--install", "--reserve-gib", "31", "--peak-gib", "11"];
      const short = harness(() => 42n * GIB - 1n);
      NodeAssert.throws(
        () => setupWorktree(args, project, worktree, short.dependencies),
        /refused/,
      );
      NodeFS.mkdirSync(NodePath.join(worktree, "node_modules"));
      const equal = harness(() => 42n * GIB);
      NodeAssert.equal(setupWorktree(args, project, worktree, equal.dependencies), 0);
    }),
);

NodeTest.test("invalid options fail before filesystem or producer effects", () => {
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
  NodeAssert.throws(
    () => setupWorktree(["--warm"], "missing", "missing", dependencies),
    /requires --install/,
  );
  NodeAssert.throws(
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
