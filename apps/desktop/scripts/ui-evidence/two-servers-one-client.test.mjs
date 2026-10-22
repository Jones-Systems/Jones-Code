import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeEvents from "node:events";
import { EndpointTrace } from "./support/endpoint-trace.mjs";
import {
  canonical,
  selectedSnapshot,
  rowSelector,
  environmentRowSelector,
  environmentRowCount,
  assertRowCounts,
  assertPinSnapshot,
  inspectEncryptedCatalog,
  finalizeEvidence,
  recordRestartStep,
} from "./scenarios/two-servers-one-client.mjs";

const fixture = { projectId: "p", threadId: "t", controlThreadId: "c", liveThreadId: "live" };
const snapshot = () => ({
  schemaVersion: 2,
  snapshotSequence: 4,
  projects: [{ id: "p", title: "colliding project", workspaceRoot: "/synthetic" }],
  threads: ["t", "c", "live"].map((id) => ({
    id,
    projectId: "p",
    title: id,
    updatedAt: "2026-10-22T00:00:00Z",
    pinnedAt: null,
    pinOrderKey: null,
  })),
  archivedThreads: [],
});

function finalizationFixture({
  pending = false,
  primary,
  writeFailure,
  cleanupFailure,
  scanFailure,
} = {}) {
  const page = new NodeEvents.EventEmitter();
  const trace = new EndpointTrace("t", { drainTimeoutMs: 10 });
  trace.attach(page);
  if (pending)
    page.emit("request", {
      url: () => "http://127.0.0.1:43773/api",
      allHeaders: () => new Promise(() => {}),
    });
  const writes = [],
    events = [];
  let stops = 0,
    attempts = 0;
  const secondary = {
    evidence: { stop: { outcome: "not-requested" } },
    assertSafeEvidence(value) {
      events.push("scan");
      NodeAssert.equal(JSON.stringify(value).includes("private-secret"), false);
      if (scanFailure) throw new Error("private-secret");
    },
    async stop() {
      events.push("stop");
      stops++;
      this.evidence = { stop: { outcome: cleanupFailure ? "unknown" : "stopped" } };
      if (cleanupFailure) throw new Error("private-secret");
    },
  };
  const evidence = {
    status: primary ? "failed" : "passed",
    steps: [{ name: "restart-disabled-b", status: primary ? "failed" : "passed" }],
  };
  const run = () =>
    finalizeEvidence({
      evidence,
      trace,
      secondary,
      path: "/synthetic/je03-evidence.json",
      failed: primary ? new Error(primary) : undefined,
      writeFile: async (path, bytes, options) => {
        attempts++;
        events.push("write");
        if (attempts === writeFailure) throw new Error("private-secret");
        writes.push({ path, value: JSON.parse(bytes), options });
      },
    });
  return { run, trace, page, evidence, writes, events, stops: () => stops };
}

NodeTest.test(
  "primary failure is persisted before a stuck drain and survives cleanup failure",
  { timeout: 250 },
  async () => {
    const f = finalizationFixture({
      pending: true,
      primary: "restart-disabled-b-timeout",
      cleanupFailure: true,
    });
    const flushing = f.trace.flush.bind(f.trace);
    f.trace.flush = async () => {
      NodeAssert.equal(f.writes.length, 1);
      NodeAssert.equal(f.writes[0].value.failure, "restart-disabled-b-timeout");
      NodeAssert.equal(f.writes[0].value.finalization.drain, "pending");
      NodeAssert.equal(f.writes[0].options.flag, "wx");
      NodeAssert.equal(f.writes[0].options.flush, true);
      return flushing();
    };
    const failed = await f.run();
    NodeAssert.equal(failed.message, "restart-disabled-b-timeout");
    NodeAssert.equal(f.stops(), 1);
    const final = f.writes.at(-1).value;
    NodeAssert.equal(final.status, "failed");
    NodeAssert.equal(final.failure, "restart-disabled-b-timeout");
    NodeAssert.equal(final.trace.incomplete, true);
    NodeAssert.equal(final.trace.pendingAtTimeout, 1);
    NodeAssert.deepEqual(final.finalization, { drain: "failed", cleanup: "failed" });
  },
);

NodeTest.test(
  "a successful scenario cannot pass when trace draining fails",
  { timeout: 250 },
  async () => {
    const f = finalizationFixture({ pending: true });
    NodeAssert.equal((await f.run()).message, "renderer-network-unobservable");
    NodeAssert.equal(f.stops(), 1);
    NodeAssert.ok(f.writes.every(({ value }) => value.status === "failed"));
    NodeAssert.equal(f.writes.at(-1).value.finalization.cleanup, "completed");
  },
);

NodeTest.test(
  "evidence rejection still performs one cleanup and keeps the earlier failure",
  async () => {
    for (const fault of [{ writeFailure: 1 }, { writeFailure: 2 }, { scanFailure: true }]) {
      for (const primary of [undefined, "restart-disabled-b-timeout", "Error https://private-secret"]) {
        const f = finalizationFixture({ ...fault, primary });
        const failed = await f.run();
        NodeAssert.equal(f.stops(), 1);
        NodeAssert.equal(f.evidence.status, "failed");
        NodeAssert.equal(JSON.stringify(f.evidence).includes("private-secret"), false);
        if (primary) NodeAssert.equal(failed.message, primary.includes(" ") ? "je03-failed" : primary);
        else NodeAssert.match(failed.message, /evidence.*failed/);
        NodeAssert.throws(() => f.trace.fingerprint("secret"), /trace-disposed/);
        if (fault.writeFailure === 1 || fault.scanFailure) NodeAssert.equal(f.writes.length, 0);
      }
    }
  },
);

NodeTest.test("only completed drain, cleanup and evidence can finalize success", async () => {
  const f = finalizationFixture();
  NodeAssert.equal(await f.run(), undefined);
  NodeAssert.equal(f.writes[0].value.status, "failed");
  NodeAssert.equal(f.writes[1].value.status, "passed");
  NodeAssert.deepEqual(f.writes[1].value.finalization, { drain: "completed", cleanup: "completed" });
  NodeAssert.equal(f.stops(), 1);
  NodeAssert.deepEqual(f.events, ["scan", "write", "scan", "stop", "write"]);
  const cleanup = finalizationFixture({ cleanupFailure: true });
  NodeAssert.equal((await cleanup.run()).message, "secondary-stop-unknown");
  NodeAssert.equal(cleanup.writes.at(-1).value.status, "failed");
});

NodeTest.test(
  "observations completed during drain are secret-scanned before cleanup wipes matchers",
  async () => {
    const f = finalizationFixture();
    let resolve;
    f.page.emit("request", {
      url: () => "http://private-secret/api",
      method: () => "GET",
      allHeaders: () =>
        new Promise((done) => {
          resolve = done;
        }),
    });
    const flush = f.trace.flush.bind(f.trace);
    f.trace.flush = () => {
      resolve({});
      return flush();
    };
    NodeAssert.equal((await f.run()).message, "secret-in-evidence-or-write-failed");
    NodeAssert.equal(f.stops(), 1);
    NodeAssert.equal(f.writes.length, 1);
    NodeAssert.equal(f.writes[0].value.status, "failed");
    NodeAssert.equal(JSON.stringify(f.writes).includes("private-secret"), false);
    NodeAssert.deepEqual(f.events, ["scan", "write", "scan", "stop"]);
  },
);

NodeTest.test(
  "restart, reload and trace checkpoint retain the last entered boundary",
  { timeout: 250 },
  async () => {
    for (const phase of ["restart", "reload", "checkpoint"]) {
      const records = [];
      const f = finalizationFixture({ pending: phase === "checkpoint" });
      try {
        await NodeAssert.rejects(
          recordRestartStep(
            "/synthetic",
            phase,
            async () => {
              NodeAssert.deepEqual(records, [{ phase, state: "entered" }]);
              if (phase === "checkpoint") return f.trace.checkpoint();
              throw new Error("synthetic-boundary-failure");
            },
            async (path, bytes, options) => {
              NodeAssert.equal(path, "/synthetic/je03-restart-progress.ndjson");
              NodeAssert.equal(options.flush, true);
              records.push(JSON.parse(bytes));
            },
          ),
          /synthetic-boundary-failure|drain-timeout/,
        );
        NodeAssert.deepEqual(records, [{ phase, state: "entered" }]);
      } finally {
        f.trace.dispose();
      }
    }
  },
);

NodeTest.test(
  "identical native IDs have disjoint exact environment selectors on both shelves",
  () => {
    NodeAssert.equal(rowSelector("env-b", "t"), 'li[data-thread-item="env-b:t"]');
    for (const pinned of [true, false, undefined]) {
      NodeAssert.notEqual(rowSelector("env-a", "t", pinned), rowSelector("env-b", "t", pinned));
      NodeAssert.equal(rowSelector("env-b", "t", pinned).includes('$="'), false);
    }
    NodeAssert.throws(() => rowSelector('env"]', "t"), /row-identity-not-environment-scoped/);
  },
);

NodeTest.test(
  "strict canonical comparison ignores object key order but no data or sequence changes",
  () => {
    NodeAssert.equal(
      canonical({ b: 2, a: { z: 0, c: 1 } }),
      canonical({ a: { c: 1, z: 0 }, b: 2 }),
    );
    const before = snapshot();
    for (const mutate of [
      (value) => value.snapshotSequence++,
      (value) => {
        value.threads[0].updatedAt = "changed";
      },
      (value) => {
        value.projects[0].workspaceRoot = "/other";
      },
      (value) => {
        value.threads[0].pinnedAt = "2026-10-22T01:00:00Z";
      },
    ]) {
      const after = structuredClone(before);
      mutate(after);
      NodeAssert.notEqual(canonical(before), canonical(after));
    }
  },
);

NodeTest.test(
  "selected invariance permits the deliberate live-thread change and still catches collisions",
  () => {
    const before = snapshot(),
      after = snapshot();
    after.threads[2].title = "A live updated";
    after.snapshotSequence++;
    NodeAssert.equal(selectedSnapshot(after, fixture), selectedSnapshot(before, fixture));
    for (const mutate of [
      (value) => {
        value.threads[0].title = "wrong server";
      },
      (value) => {
        value.threads[1].pinnedAt = "wrong pin";
      },
      (value) => {
        value.projects[0].title = "wrong project";
      },
      (value) => {
        value.threads.push({ id: "unexpected" });
      },
      (value) => {
        value.archivedThreads.push({ id: "unexpected" });
      },
    ]) {
      const changed = structuredClone(after);
      mutate(changed);
      NodeAssert.notEqual(selectedSnapshot(changed, fixture), selectedSnapshot(before, fixture));
    }
    after.threads.push({ ...after.threads[0] });
    NodeAssert.throws(() => selectedSnapshot(after, fixture), /missing-or-duplicated/);
  },
);

NodeTest.test(
  "pin oracle is false before the action and rejects collateral control pinning",
  () => {
    const value = snapshot();
    assertPinSnapshot(value, fixture, false);
    NodeAssert.throws(() => assertPinSnapshot(value, fixture, true), /backend-pin-mismatch/);
    Object.assign(value.threads[0], { pinnedAt: "2026-10-22T01:00:00Z", pinOrderKey: "a0" });
    assertPinSnapshot(value, fixture, true);
    NodeAssert.throws(() => assertPinSnapshot(value, fixture, false), /backend-pin-mismatch/);
    value.threads[1].pinnedAt = "2026-10-22T01:00:00Z";
    NodeAssert.throws(() => assertPinSnapshot(value, fixture, true), /control-pin-changed/);
  },
);

NodeTest.test(
  "encrypted wrapper must be exact, bounded, canonical and contain no B address or bearer marker",
  () => {
    const encoded = (encryptedCatalog, extra = {}) =>
      Buffer.from(JSON.stringify({ version: 1, encryptedCatalog, ...extra }));
    const cipher = Buffer.from([0, 1, 2, 255]).toString("base64");
    NodeAssert.equal(inspectEncryptedCatalog(encoded(cipher)).encrypted, true);
    for (const bytes of [
      encoded(cipher, { plaintext: "unexpected" }),
      encoded(`${cipher}\n`),
      encoded(""),
      Buffer.alloc(1024 * 1024 + 1),
      Buffer.from("invalid"),
    ])
      NodeAssert.throws(() => inspectEncryptedCatalog(bytes), /catalog-/);
    for (const secret of ["127.0.0.1:43774", "bearer:env:origin"])
      NodeAssert.throws(
        () => inspectEncryptedCatalog(encoded(Buffer.from(secret).toString("base64"))),
        /plaintext-present/,
      );
  },
);

NodeTest.test(
  "environment row counts use the complete environment namespace and attached rows",
  async () => {
    const a = environmentRowSelector("env-a");
    const b = environmentRowSelector("env-ab");
    NodeAssert.equal(a, 'li[data-thread-item^="env-a:"]');
    NodeAssert.equal(b, 'li[data-thread-item^="env-ab:"]');
    NodeAssert.notEqual(a, b);
    const attachedCounts = new Map([
      [a, 3],
      [b, 2],
    ]);
    const observedSelectors = [];
    const page = {
      locator(selector) {
        observedSelectors.push(selector);
        return { count: async () => attachedCounts.get(selector) ?? 0 };
      },
    };
    NodeAssert.equal(await environmentRowCount(page, "env-a"), 3);
    NodeAssert.equal(await environmentRowCount(page, "env-ab"), 2);
    attachedCounts.set(b, 0);
    NodeAssert.equal(await environmentRowCount(page, "env-ab"), 0);
    NodeAssert.deepEqual(observedSelectors, [a, b, b]);
    for (const invalid of ["", "env:a", 'env"]', "env*", null, undefined, 1]) {
      NodeAssert.throws(
        () => environmentRowSelector(invalid),
        /row-identity-not-environment-scoped/,
      );
      NodeAssert.throws(
        () => environmentRowCount(page, invalid),
        /row-identity-not-environment-scoped/,
      );
    }
    NodeAssert.equal(observedSelectors.length, 3);
  },
);

NodeTest.test(
  "disabled-row predicates require a non-vacuous baseline, hidden B and unchanged A",
  () => {
    NodeAssert.deepEqual(assertRowCounts("preOff", { A: 3, B: 2 }), { A: 3, B: 2 });
    for (const counts of [
      { A: 0, B: 0 },
      { A: 2, B: 2 },
      { A: 3, B: 1 },
    ])
      NodeAssert.throws(() => assertRowCounts("preOff", counts), /pre-off-rows-missing/);
    for (const phase of ["off", "offRecheck"]) {
      NodeAssert.deepEqual(assertRowCounts(phase, { A: 4, B: 0 }, 4), { A: 4, B: 0 });
      for (const A of [0, 3, 5])
        NodeAssert.throws(() => assertRowCounts(phase, { A, B: 0 }, 4), /a-row-count-changed/);
      NodeAssert.throws(
        () => assertRowCounts(phase, { A: 4, B: 1 }, 4),
        /b-rows-visible-while-disabled/,
      );
      for (const baseline of [undefined, NaN, 0, 2, 3.5])
        NodeAssert.throws(
          () => assertRowCounts(phase, { A: 4, B: 0 }, baseline),
          /row-count-baseline-invalid/,
        );
    }
  },
);

NodeTest.test("restart absence and re-enable presence are distinct gating states", () => {
  NodeAssert.deepEqual(assertRowCounts("restart", { A: 3, B: 0 }), { A: 3, B: 0 });
  NodeAssert.throws(
    () => assertRowCounts("restart", { A: 3, B: 2 }),
    /b-rows-visible-after-restart/,
  );
  for (const B of [0, 1])
    NodeAssert.throws(() => assertRowCounts("reenable", { A: 3, B }), /b-rows-missing-on-reenable/);
  NodeAssert.deepEqual(assertRowCounts("reenable", { A: 3, B: 2 }), { A: 3, B: 2 });
  for (const invalid of [-1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, "3", true])
    for (const field of ["A", "B"])
      NodeAssert.throws(
        () => assertRowCounts("preOff", { A: 3, B: 2, [field]: invalid }),
        /row-count-invalid/,
      );
  NodeAssert.throws(() => assertRowCounts("unknown", { A: 3, B: 0 }), /row-count-phase-invalid/);
});

NodeTest.test("row-count evidence includes only integer counts for each lifecycle phase", () => {
  const preOff = assertRowCounts("preOff", { A: 3, B: 2, title: "not evidence" });
  const rowCounts = {
    preOff,
    off: assertRowCounts("off", { A: 3, B: 0 }, preOff.A),
    offRecheck: assertRowCounts("offRecheck", { A: 3, B: 0 }, preOff.A),
    restart: assertRowCounts("restart", { A: 3, B: 0 }),
    reenable: assertRowCounts("reenable", { A: 3, B: 2 }),
  };
  NodeAssert.deepEqual(JSON.parse(JSON.stringify(rowCounts)), {
    preOff: { A: 3, B: 2 },
    off: { A: 3, B: 0 },
    offRecheck: { A: 3, B: 0 },
    restart: { A: 3, B: 0 },
    reenable: { A: 3, B: 2 },
  });
  for (const counts of Object.values(rowCounts)) {
    NodeAssert.deepEqual(Object.keys(counts), ["A", "B"]);
    NodeAssert.ok(Object.values(counts).every(Number.isSafeInteger));
  }
});
