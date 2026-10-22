import assert from "node:assert/strict";
import * as fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import { availableBytes, parseOptions, setupWorktree } from "./setup.ts";

const GIB = 1024n ** 3n;
function fixture(run) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "jones-setup-"));
  try {
    const project = path.join(root, "project");
    const worktree = path.join(root, "worktree");
    for (const base of [project, worktree])
      fs.mkdirSync(path.join(base, "infra/relay"), { recursive: true });
    run({ root, project, worktree });
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
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

test("lightweight setup preserves real files and replaces only existing symlinks", () =>
  fixture(({ project, worktree }) => {
    const source = path.join(project, ".env");
    const target = path.join(worktree, ".env");
    fs.writeFileSync(source, "SYNTHETIC_SOURCE=1");
    fs.writeFileSync(target, "SYNTHETIC_TARGET=1");
    fs.writeFileSync(path.join(project, "infra/relay/.env"), "SYNTHETIC_RELAY=1");
    fs.symlinkSync(path.join(project, "missing"), path.join(worktree, "infra/relay/.env"));
    const { events, dependencies } = harness(() => {
      throw Error("must not measure");
    });
    assert.equal(setupWorktree([], project, worktree, dependencies), 0);
    assert.equal(fs.readFileSync(target, "utf8"), "SYNTHETIC_TARGET=1");
    assert.equal(
      fs.readlinkSync(path.join(worktree, "infra/relay/.env")),
      path.join(project, "infra/relay/.env"),
    );
    assert.deepEqual(events, []);
    assert.equal(setupWorktree([], project, project, dependencies), 0);
    assert.equal(fs.readFileSync(source, "utf8"), "SYNTHETIC_SOURCE=1");
  }));

test("source symlinks are skipped and missing sources have no effect", () =>
  fixture(({ project, worktree }) => {
    fs.symlinkSync(path.join(project, "missing"), path.join(project, ".env"));
    const { dependencies } = harness();
    assert.equal(setupWorktree([], project, worktree, dependencies), 0);
    assert.equal(fs.existsSync(path.join(worktree, ".env")), false);
  }));

test("install uses frozen unattended command and warming measures capacity again", () =>
  fixture(({ project, worktree }) => {
    const { events, dependencies } = harness();
    assert.equal(setupWorktree(["--install", "--warm"], project, worktree, dependencies), 0);
    assert.deepEqual(
      events.map((event) => event[0]),
      ["capacity", "run", "capacity", "run"],
    );
    assert.deepEqual(events[1], [
      "run",
      "vp i --frozen-lockfile -- --yes",
      [],
      {
        cwd: worktree,
        shell: true,
        stdio: ["ignore", "inherit", "inherit"],
      },
    ]);
    assert.equal(events[3][1], process.execPath);
    assert.deepEqual(events[3][2], [path.join(worktree, "apps/web/scripts/warm-dep-cache.ts")]);
  }));

test("install-only omits warming; installer and warmer failures propagate", () =>
  fixture(({ project, worktree }) => {
    const installOnly = harness();
    assert.equal(setupWorktree(["--install"], project, worktree, installOnly.dependencies), 0);
    assert.equal(installOnly.events.filter((event) => event[0] === "run").length, 1);
    for (const status of [27, null]) {
      const failure = harness(undefined, [status]);
      assert.equal(
        setupWorktree(["--install", "--warm"], project, worktree, failure.dependencies),
        status ?? 1,
      );
      assert.equal(failure.events.length, 2);
    }
    for (const status of [19, null]) {
      const failure = harness(undefined, [0, status]);
      assert.equal(
        setupWorktree(["--install", "--warm"], project, worktree, failure.dependencies),
        status ?? 1,
      );
    }
  }));

test("one byte short or unknown capacity starts no child and preserves output", () =>
  fixture(({ project, worktree }) => {
    const output = path.join(worktree, "node_modules");
    fs.mkdirSync(output);
    fs.writeFileSync(path.join(output, "sentinel"), "existing");
    for (const capacity of [
      () => 40n * GIB - 1n,
      () => -1n,
      () => 40,
      () => {
        throw Error("unavailable");
      },
    ]) {
      const { events, dependencies } = harness(capacity);
      assert.throws(
        () => setupWorktree(["--install", "--warm"], project, worktree, dependencies),
        /refused/,
      );
      assert.equal(events.length, 1);
      assert.equal(fs.readFileSync(path.join(output, "sentinel"), "utf8"), "existing");
    }
  }));

test("fresh post-install refusal prevents warmer", () =>
  fixture(({ project, worktree }) => {
    for (const second of [39n * GIB, undefined]) {
      let count = 0;
      const { events, dependencies } = harness(() => {
        if (++count === 1) return 40n * GIB;
        if (second === undefined) throw Error("unknown");
        return second;
      });
      assert.throws(
        () => setupWorktree(["--install", "--warm"], project, worktree, dependencies),
        /Cache warm refused/,
      );
      assert.deepEqual(
        events.map((event) => event[0]),
        ["capacity", "run", "capacity"],
      );
    }
  }));

test("budgets can only increase and equality admits without subtracting existing output", () =>
  fixture(({ project, worktree }) => {
    const args = ["--install", "--reserve-gib", "31", "--peak-gib", "11"];
    const short = harness(() => 42n * GIB - 1n);
    assert.throws(() => setupWorktree(args, project, worktree, short.dependencies), /refused/);
    fs.mkdirSync(path.join(worktree, "node_modules"));
    const equal = harness(() => 42n * GIB);
    assert.equal(setupWorktree(args, project, worktree, equal.dependencies), 0);
  }));

test("invalid options fail before filesystem or producer effects", () => {
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
    assert.throws(() => parseOptions(args));
  const { events, dependencies } = harness();
  assert.throws(
    () => setupWorktree(["--warm"], "missing", "missing", dependencies),
    /requires --install/,
  );
  assert.throws(() => setupWorktree([], undefined, "missing", dependencies), /T3CODE_PROJECT_ROOT/);
  assert.deepEqual(events, []);
});

test("capacity follows destination symlinks and missing descendants without creating them", () =>
  fixture(({ root, worktree }) => {
    const link = path.join(root, "linked-worktree");
    fs.symlinkSync(worktree, link);
    const missing = path.join(link, "future", "output");
    assert.equal(typeof availableBytes(missing), "bigint");
    assert.ok(availableBytes(missing) >= 0n);
    assert.equal(fs.existsSync(path.join(worktree, "future")), false);
    fs.writeFileSync(path.join(worktree, "file"), "synthetic");
    assert.throws(() => availableBytes(path.join(worktree, "file", "child")));
  }));
