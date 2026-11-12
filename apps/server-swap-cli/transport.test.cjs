"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const crypto = require("node:crypto");
const { EventEmitter } = require("node:events");
const { remoteArgs, remote, quote, workerMain } = require("./transport.cjs");
const { runFleet } = require("./fleet.cjs");

const digest = "a".repeat(64);
const target = {
  id: "test",
  ssh: "existing-test-alias",
  worker: "/opt/tools/server swap",
  plan: "/tmp/operator's plan.json",
  planDigest: digest,
};

test("remote arguments retain spaces/quotes without permitting shell interpretation", () => {
  const args = remoteArgs(target, "launch");
  assert.equal(
    args.at(-1),
    "'/opt/tools/server swap' 'worker' 'launch' '/tmp/operator'\\''s plan.json' '" + digest + "'",
  );
  assert.equal(quote("$(touch /tmp/anything)"), "'$(touch /tmp/anything)'");
  assert.throws(() => remoteArgs({ ...target, ssh: "-oProxyCommand=evil" }, "launch"));
  assert.throws(() => remoteArgs({ ...target, plan: "/tmp/a\nb" }, "launch"));
});

test("lost SSH response stays unknown without retry", async () => {
  let calls = 0;
  const result = await remote(target, "launch", async () => {
    calls++;
    throw new Error("lost");
  });
  assert.equal(result.status, "unknown");
  assert.equal(calls, 1);
});

test("fleet starts only first unfinished host and leaves laptop pending", async () => {
  const calls = [];
  const result = await runFleet(
    { targets: [target, { ...target, id: "vps" }] },
    "apply",
    async (host, command) => {
      calls.push([host.id, command]);
      return { status: command === "status" ? "not-started" : "launched", planDigest: digest };
    },
  );
  assert.deepEqual(calls, [
    ["test", "status"],
    ["test", "launch"],
  ]);
  assert.equal(result.status, "pending");
  assert.deepEqual(result.remaining, ["vps"]);
});

test("fleet advances only past committed hosts and pins plan identity", async () => {
  const calls = [];
  const result = await runFleet(
    { targets: [target, { ...target, id: "vps" }] },
    "resume",
    async (host, command) => {
      calls.push([host.id, command]);
      return {
        status: host.id === "test" ? "committed" : "not-started",
        planDigest: host.id === "test" ? digest : "b".repeat(64),
      };
    },
  );
  assert.deepEqual(calls, [
    ["test", "status"],
    ["test", "reconcile"],
    ["vps", "status"],
  ]);
  assert.equal(result.targets[1].observation.status, "unknown");
});

test("fleet completion gives manual laptop handoff and never calls a laptop API", async () => {
  const result = await runFleet({ targets: [target] }, "resume", async () => ({
    status: "committed",
    planDigest: digest,
  }));
  assert.equal(result.status, "committed");
  assert.match(result.laptop, /manually/);
});

test("a lost launch response followed by committed journal is not replayed", async () => {
  let committed = false;
  let launches = 0;
  const invoke = async (_host, command) => {
    if (command === "launch") {
      launches++;
      committed = true;
      return { status: "unknown" };
    }
    return { status: committed ? "committed" : "not-started", planDigest: digest };
  };
  assert.equal((await runFleet({ targets: [target] }, "apply", invoke)).status, "pending");
  assert.equal((await runFleet({ targets: [target] }, "resume", invoke)).status, "committed");
  assert.equal(launches, 1);
});

function workerFixture(t) {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "swap-worker-test-")));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const plan = path.join(root, "plan.json");
  const bytes = Buffer.from('{"synthetic":true}\n');
  fs.writeFileSync(plan, bytes);
  const planDigest = crypto.createHash("sha256").update(bytes).digest("hex");
  const transactionDir = path.join(root, "state", "transaction");
  return {
    plan,
    transactionDir,
    core: {
      loadPlan: () => ({ digest: planDigest, transactionDir }),
      plan: async () => ({ status: "ready" }),
    },
  };
}

test("detached worker keeps an immutable snapshot and is launched once", async (t) => {
  const f = workerFixture(t);
  let starts = 0;
  const launch = (_file, args, options) => {
    starts++;
    assert.equal(options.detached, true);
    assert.match(args.at(-1), /worker-launch\/plan.json$/);
    const child = new EventEmitter();
    child.pid = 12345;
    child.unref = () => {};
    process.nextTick(() => child.emit("spawn"));
    return child;
  };
  const deps = { core: f.core, spawn: launch };
  assert.equal((await workerMain(["launch", f.plan], deps)).status, "launched");
  assert.equal((await workerMain(["launch", f.plan], deps)).status, "launched");
  assert.equal(starts, 1);
  assert.equal(
    fs.readFileSync(path.join(f.transactionDir, "worker-launch", "plan.json"), "utf8"),
    fs.readFileSync(f.plan, "utf8"),
  );
});

test("failure after launch intent cannot blindly spawn again", async (t) => {
  const f = workerFixture(t);
  let starts = 0;
  const deps = {
    core: f.core,
    spawn: () => {
      starts++;
      throw new Error("spawn interrupted");
    },
  };
  await assert.rejects(workerMain(["launch", f.plan], deps), /interrupted/);
  assert.equal((await workerMain(["launch", f.plan], deps)).status, "unknown");
  assert.equal(starts, 1);
});

test("fleet-pinned digest is enforced on the host before launch side effects", async (t) => {
  const f = workerFixture(t);
  let starts = 0;
  await assert.rejects(
    workerMain(["launch", f.plan, "b".repeat(64)], {
      core: f.core,
      spawn: () => {
        starts++;
      },
    }),
    /digest changed/,
  );
  assert.equal(starts, 0);
  assert.equal(fs.existsSync(f.transactionDir), false);
});

test("historical commit cannot authorize the laptop handoff without current readback", async () => {
  const result = await runFleet({ targets: [target] }, "status", async (_host, command) => ({
    status: command === "status" ? "committed-unverified" : "unknown",
    planDigest: digest,
  }));
  assert.equal(result.status, "pending");
  assert.match(result.laptop, /pending/);
});
