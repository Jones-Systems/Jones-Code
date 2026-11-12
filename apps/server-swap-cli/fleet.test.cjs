"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const { runUntilComplete } = require("./fleet.cjs");
const { exitCode } = require("./cli.cjs");
const digest = "a".repeat(64);
const targets = ["test", "vps", "mini"].map((id) => ({ id, planDigest: digest }));
const options = { timeoutMs: 1000, pollMs: 100 };
function clock(invoke) {
  let time = 0;
  return {
    now: () => time,
    sleep: async (ms) => {
      time += ms;
    },
    invoke,
  };
}

test("fleet run waits for each host, launches once, and finally revalidates all hosts", async () => {
  const calls = [],
    counts = new Map();
  const result = await runUntilComplete(
    { targets },
    options,
    clock(async (target, command) => {
      calls.push(`${target.id}:${command}`);
      if (command === "launch") return { status: "launched", planDigest: digest };
      if (command === "reconcile") return { status: "committed", planDigest: digest };
      const count = counts.get(target.id) || 0;
      counts.set(target.id, count + 1);
      return {
        status: ["not-started", "not-started", "in-progress", "committed-unverified"][
          Math.min(count, 3)
        ],
        planDigest: digest,
      };
    }),
  );
  assert.equal(result.status, "committed");
  assert.match(result.laptop, /manually/);
  assert.deepEqual(
    calls,
    targets
      .flatMap((t) => [
        `${t.id}:status`,
        `${t.id}:launch`,
        `${t.id}:status`,
        `${t.id}:status`,
        `${t.id}:status`,
        `${t.id}:reconcile`,
      ])
      .concat(targets.map((t) => `${t.id}:reconcile`)),
  );
});

for (const status of ["unknown", "failed-before-start", "recovery-required"]) {
  test(`fleet run stops on ${status} without touching the next host`, async () => {
    const calls = [];
    const result = await runUntilComplete(
      { targets },
      options,
      clock(async (target, command) => {
        calls.push([target.id, command]);
        return { status, planDigest: digest };
      }),
    );
    assert.equal(result.status, status);
    assert.equal(result.stoppedAt, "test");
    assert.deepEqual(result.remaining, ["vps", "mini"]);
    assert.deepEqual(calls, [["test", "status"]]);
    assert.equal(exitCode(result), 2);
  });
}

test("fleet run returns bounded timeout without cancelling or relaunching worker", async () => {
  const calls = [];
  const result = await runUntilComplete(
    { targets },
    options,
    clock(async (target, command, remaining) => {
      assert.ok(remaining > 0 && remaining <= 1000);
      calls.push([target.id, command]);
      return { status: command === "launch" ? "launched" : "not-started", planDigest: digest };
    }),
  );
  assert.equal(result.status, "timed-out");
  assert.equal(result.stoppedAt, "test");
  assert.equal(calls.filter(([, command]) => command === "launch").length, 1);
  assert.ok(calls.every(([id]) => id === "test"));
  assert.equal(exitCode(result), 2);
});

test("lost launch response stops; a later run observes completion without replay", async () => {
  let complete = false,
    starts = 0;
  const invoke = async (_target, command) => {
    if (command === "launch") {
      complete = true;
      starts++;
      throw new Error("lost SSH response");
    }
    return { status: complete ? "committed" : "not-started", planDigest: digest };
  };
  const manifest = { targets: targets.slice(0, 1) };
  assert.equal((await runUntilComplete(manifest, options, clock(invoke))).status, "unknown");
  assert.equal((await runUntilComplete(manifest, options, clock(invoke))).status, "committed");
  assert.equal(starts, 1);
});

test("fleet run fences changed digest and final drift on an earlier host", async () => {
  let calls = 0;
  const mismatch = await runUntilComplete(
    { targets },
    options,
    clock(async () => {
      calls++;
      return { status: "not-started", planDigest: "b".repeat(64) };
    }),
  );
  assert.equal(mismatch.status, "unknown");
  assert.equal(calls, 1);
  let reconciles = 0;
  const drift = await runUntilComplete(
    { targets },
    options,
    clock(async (_target, command) => {
      if (command === "reconcile") reconciles++;
      return { status: reconciles > 3 ? "unknown" : "committed", planDigest: digest };
    }),
  );
  assert.equal(drift.status, "unknown");
  assert.equal(drift.stoppedAt, "test");
  assert.match(drift.laptop, /pending/);
});

test("deadline expiry during observation cannot launch another host", async () => {
  let time = 0;
  const calls = [];
  const result = await runUntilComplete({ targets }, options, {
    now: () => time,
    sleep: async () => {},
    invoke: async (target, command) => {
      calls.push([target.id, command]);
      time = 1000;
      return { status: "not-started", planDigest: digest };
    },
  });
  assert.equal(result.status, "timed-out");
  assert.deepEqual(calls, [["test", "status"]]);
});

test("invalid deadline is rejected before transport and checkpoint failures exit nonzero", async () => {
  await assert.rejects(
    runUntilComplete(
      { targets },
      { timeoutMs: 0, pollMs: 100 },
      { invoke: () => assert.fail("transport") },
    ),
    /deadline/,
  );
  assert.equal(
    exitCode({ status: "pending", targets: [{ observation: { status: "unknown" } }] }),
    2,
  );
  assert.equal(
    exitCode({ status: "pending", targets: [{ observation: { status: "launched" } }] }),
    0,
  );
});
