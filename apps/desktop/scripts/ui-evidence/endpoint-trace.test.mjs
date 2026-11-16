import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeEvents from "node:events";
import { EndpointTrace, assertCrossover, assertPinRouting } from "./support/endpoint-trace.mjs";

const A = "http://127.0.0.1:43773",
  B = "http://127.0.0.1:43774";
const expected = { 0: { a: "a-fingerprint", b: "b-fingerprint" } };
const request = (origin, authFp) => ({ generation: 0, kind: "request", origin, authFp });
const ticket = (origin, ticketFp) => ({ generation: 0, kind: "websocket", origin, ticketFp });

function pendingRequest(url) {
  const headers = Promise.withResolvers();
  return {
    request: { url: () => url, method: () => "GET", allHeaders: () => headers.promise },
    headers,
  };
}

function observeRequest(page, request) {
  const listeners = page.listeners("request");
  NodeAssert.equal(listeners.length, 1);
  return listeners[0](request);
}

NodeTest.test(
  "timeout diagnostics distinguish closed, detached, live and asset requests",
  async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const trace = new EndpointTrace("t");
    t.after(() => trace.dispose());
    const closed = new NodeEvents.EventEmitter();
    const detached = new NodeEvents.EventEmitter();
    const current = new NodeEvents.EventEmitter();
    const old = pendingRequest(A),
      abandoned = pendingRequest(B);
    const live = pendingRequest(B),
      asset = pendingRequest("t3code://app/assets/private.js");
    trace.attach(closed);
    closed.emit("request", old.request);
    closed.emit("requestfinished", old.request);
    closed.emit("close");
    trace.attach(detached);
    detached.emit("request", abandoned.request);
    trace.detach();
    detached.emit("close");
    detached.emit("requestfailed", abandoned.request);
    trace.attach(current);
    current.emit("request", live.request);
    current.emit("request", asset.request);
    current.emit("requestfailed", asset.request);
    const drain = NodeAssert.rejects(trace.flush(), /renderer-trace-drain-timeout/);
    t.mock.timers.tick(999);
    NodeAssert.equal(trace.summary().incomplete, false);
    t.mock.timers.tick(1);
    await drain;
    const summary = trace.summary();
    NodeAssert.equal(summary.pendingAtTimeout, 4);
    NodeAssert.equal(summary.drainTimeoutMs, 1000);
    NodeAssert.equal(summary.pendingDiagnosticsAtTimeout.currentGeneration, 2);
    NodeAssert.equal(summary.pendingDiagnosticsAtTimeout.omittedCount, 0);
    const samples = summary.pendingDiagnosticsAtTimeout.samples;
    NodeAssert.deepEqual(
      samples.map(
        ({
          sequence,
          generation,
          originClass,
          headerState,
          requestFinished,
          requestFailed,
          pageClosed,
          detached,
        }) => ({
          sequence,
          generation,
          originClass,
          headerState,
          requestFinished,
          requestFailed,
          pageClosed,
          detached,
        }),
      ),
      [
        {
          sequence: 0,
          generation: 0,
          originClass: "A",
          headerState: "pending",
          requestFinished: true,
          requestFailed: false,
          pageClosed: true,
          detached: true,
        },
        {
          sequence: 1,
          generation: 1,
          originClass: "B",
          headerState: "pending",
          requestFinished: false,
          requestFailed: false,
          pageClosed: false,
          detached: true,
        },
        {
          sequence: 2,
          generation: 2,
          originClass: "B",
          headerState: "pending",
          requestFinished: false,
          requestFailed: false,
          pageClosed: false,
          detached: false,
        },
        {
          sequence: 3,
          generation: 2,
          originClass: "packaged",
          headerState: "pending",
          requestFinished: false,
          requestFailed: true,
          pageClosed: false,
          detached: false,
        },
      ],
    );
    NodeAssert.deepEqual(
      samples.map(({ requestClass, pathTemplate, reloadTiming }) => ({
        requestClass,
        pathTemplate,
        reloadTiming,
      })),
      ["/", "/", "/", "not-endpoint"].map((pathTemplate) => ({
        requestClass: "unavailable",
        pathTemplate,
        reloadTiming: { state: "reload-unmarked", requestObservedRelativeToReloadStartMs: null },
      })),
    );
    for (const sample of samples) NodeAssert.ok(Number.isFinite(sample.ageMs) && sample.ageMs >= 0);
    NodeAssert.ok(samples[0].ageMs >= samples[3].ageMs);
    NodeAssert.deepEqual(trace.entries, []);
    NodeAssert.equal(summary.localAssetRequests, 0);
    for (const page of [closed, detached, current]) NodeAssert.deepEqual(page.eventNames(), []);
  },
);

NodeTest.test(
  "reload diagnostics use one clock, preserve generation binding and never release header waits",
  async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    let now = 10;
    let clockReads = 0;
    const trace = new EndpointTrace("t", {
      diagnosticNow: () => {
        clockReads++;
        return now;
      },
    });
    t.after(() => trace.dispose());
    const first = new NodeEvents.EventEmitter(),
      second = new NodeEvents.EventEmitter();
    const held = [];
    let headerReads = 0;
    const emit = (page, type) => {
      const pending = pendingRequest(A);
      held.push(pending);
      pending.observed = observeRequest(page, {
        ...pending.request,
        resourceType: () => type,
        allHeaders: () => {
          headerReads++;
          return pending.headers.promise;
        },
      });
    };
    trace.attach(first);
    emit(first, "fetch");
    now = 20;
    const reloadPromise = Promise.resolve("reload-return");
    NodeAssert.equal(
      trace.observeReload(() => {
        emit(first, "document");
        return reloadPromise;
      }),
      reloadPromise,
    );
    NodeAssert.equal(await reloadPromise, "reload-return");
    now = 25;
    emit(first, "eventsource");
    trace.attach(second);
    now = 100;
    const reloadValue = {};
    NodeAssert.equal(
      trace.observeReload(() => {
        now = 105;
        emit(second, "websocket");
        return reloadValue;
      }),
      reloadValue,
    );
    NodeAssert.equal(headerReads, 4);
    NodeAssert.equal(trace.summary().pending, 4);
    const drain = NodeAssert.rejects(trace.flush(), /renderer-trace-drain-timeout/);
    t.mock.timers.tick(999);
    NodeAssert.equal(trace.summary().incomplete, false);
    t.mock.timers.tick(1);
    await drain;
    const saved = trace.summary().pendingDiagnosticsAtTimeout;
    NodeAssert.equal(saved.currentGeneration, 1);
    NodeAssert.deepEqual(
      saved.samples.map(({ generation, requestClass, pathTemplate, reloadTiming, detached }) => ({
        generation,
        requestClass,
        pathTemplate,
        reloadTiming,
        detached,
      })),
      [
        [0, "fetch", -10, true],
        [0, "document", 0, true],
        [0, "eventsource", 5, true],
        [1, "websocket", 5, false],
      ].map(([generation, requestClass, delta, detached]) => ({
        generation,
        requestClass,
        pathTemplate: "/",
        reloadTiming: { state: "observed", requestObservedRelativeToReloadStartMs: delta },
        detached,
      })),
    );
    NodeAssert.equal(trace.summary().drainTimeoutMs, 1000);
    const copy = trace.summary().pendingDiagnosticsAtTimeout;
    copy.samples[0].reloadTiming.state = "tampered";
    copy.samples[0].reloadTiming.requestObservedRelativeToReloadStartMs = Infinity;
    const readsAtTimeout = clockReads;
    const original = new Error("private-reload-error");
    NodeAssert.throws(
      () =>
        trace.observeReload(() => {
          throw original;
        }),
      (error) => error === original,
    );
    NodeAssert.equal(clockReads, readsAtTimeout);
    let fingerprints = 0;
    trace.fingerprint = () => {
      fingerprints++;
      return "invalid";
    };
    for (const pending of held) pending.headers.resolve({ authorization: "Bearer private-header" });
    await Promise.all(held.map((pending) => pending.observed));
    NodeAssert.deepEqual(trace.summary().pendingDiagnosticsAtTimeout, saved);
    NodeAssert.equal(trace.summary().pending, 0);
    NodeAssert.equal(trace.summary().pendingAtTimeout, 4);
    NodeAssert.equal(fingerprints, 0);
    NodeAssert.deepEqual(trace.entries, []);
    NodeAssert.deepEqual(first.eventNames(), []);
    NodeAssert.deepEqual(second.eventNames(), []);
    await NodeAssert.rejects(trace.checkpoint(), /renderer-trace-drain-timeout/);
  },
);

NodeTest.test(
  "request classes and endpoint templates are allowlisted without exposing request identity",
  async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const trace = new EndpointTrace("t");
    t.after(() => trace.dispose());
    const page = new NodeEvents.EventEmitter();
    trace.attach(page);
    const cases = [
      [A + "/ws?wsTicket=private-ticket", "websocket", "/ws"],
      [
        "http://private-user:private-password@127.0.0.1:43773/api/orchestration/threads/private-id?private-query",
        "fetch",
        "/api/orchestration/threads/:threadId",
      ],
      [
        B + "/api/orchestration/threads/private-id/commands/private-command?private-query",
        "xhr",
        "/api/orchestration/threads/:threadId/commands/:commandId",
      ],
      [
        A + "/api/orchestration/threads/private-id/provider-goal-state",
        "fetch",
        "/api/orchestration/threads/:threadId/provider-goal-state",
      ],
      [
        B + "/api/orchestration/threads/private-id/transcript?private-query",
        "eventsource",
        "/api/orchestration/threads/:threadId/transcript",
      ],
      [A + "/private-path?private-query", "fetch", "other"],
      ["https://private-host/api/orchestration/shell?private-query", "document", "not-endpoint"],
      ["blob:" + A + "/private-path", "image", "not-endpoint"],
      ["t3code://app/private-path?private-query", "script", "not-endpoint"],
      [A + "/ws?private-query", "private-type", "/ws", "unavailable"],
      [A + "/oauth/token?private-query", new Error("private-error"), "/oauth/token", "unavailable"],
      [A + "/?private-query", undefined, "/", "unavailable"],
      [
        A + "/api/orchestration/shell",
        Promise.resolve("fetch"),
        "/api/orchestration/shell",
        "unavailable",
      ],
      ["file:///private-path", 7, "not-endpoint", "unavailable"],
      [A + "/api/auth/websocket-ticket?private-query", "other", "/api/auth/websocket-ticket"],
      [B + "/api/orchestration/dispatch?private-query", "fetch", "/api/orchestration/dispatch"],
    ];
    let headerReads = 0;
    for (const [url, type] of cases) {
      const pending = pendingRequest(url);
      page.emit("request", {
        ...pending.request,
        resourceType:
          type === undefined
            ? undefined
            : () => {
                if (type instanceof Error) throw type;
                return type;
              },
        allHeaders: () => {
          headerReads++;
          return pending.headers.promise;
        },
      });
    }
    NodeAssert.equal(trace.summary().failure, null);
    const drain = NodeAssert.rejects(trace.flush(), /renderer-trace-drain-timeout/);
    t.mock.timers.tick(1000);
    await drain;
    NodeAssert.equal(headerReads, cases.length);
    const summary = trace.summary();
    NodeAssert.equal(summary.pendingAtTimeout, cases.length);
    NodeAssert.equal(summary.pendingDiagnosticsAtTimeout.omittedCount, 0);
    NodeAssert.deepEqual(
      summary.pendingDiagnosticsAtTimeout.samples.map(({ requestClass, pathTemplate }) => ({
        requestClass,
        pathTemplate,
      })),
      cases.map(([, type, pathTemplate, expectedClass = type]) => ({
        requestClass: expectedClass,
        pathTemplate,
      })),
    );
    NodeAssert.equal(JSON.stringify(summary).includes("private-"), false);
    NodeAssert.deepEqual(trace.entries, []);
  },
);

NodeTest.test(
  "invalid clocks, ambiguous reloads and out-of-range offsets remain unavailable",
  async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const cases = [
      { times: [0, NaN], state: "clock-unavailable" },
      { times: [0, Infinity], state: "clock-unavailable" },
      { times: [0, -1], state: "clock-unavailable" },
      { times: [0, "private-time"], state: "clock-unavailable" },
      { times: [0, Number.MAX_SAFE_INTEGER + 1], state: "clock-unavailable" },
      { times: [0, new Error("private-clock-error")], state: "clock-unavailable" },
      { times: [new Error("private-clock-error"), 1], state: "clock-unavailable" },
      { times: [10, 9], state: "clock-unavailable" },
      { times: [0, 1, NaN], extraRequest: true, state: "clock-unavailable" },
      { times: [0, 3_600_001], state: "out-of-range" },
      { times: [0, 3_600_000], state: "observed", delta: 3_600_000 },
      { times: [0, 3_600_001], eventFirst: true, state: "out-of-range" },
      { times: [0, 3_600_000], eventFirst: true, state: "observed", delta: -3_600_000 },
      { times: [0, 1], repeatReload: true, state: "multiple-reloads" },
    ];
    for (const { times, eventFirst, extraRequest, repeatReload, state, delta = null } of cases) {
      let index = 0;
      const trace = new EndpointTrace("t", {
        diagnosticNow: () => {
          const value = times[index++];
          if (value instanceof Error) throw value;
          return value;
        },
      });
      t.after(() => trace.dispose());
      const page = new NodeEvents.EventEmitter();
      trace.attach(page);
      let headerReads = 0;
      const emit = () =>
        page.emit("request", {
          url: () => A,
          allHeaders: () => {
            headerReads++;
            return new Promise(() => {});
          },
        });
      if (eventFirst) emit();
      let reloadCalls = 0;
      NodeAssert.equal(
        trace.observeReload(() => {
          reloadCalls++;
          if (!eventFirst) emit();
          return "reload-return";
        }),
        "reload-return",
      );
      if (extraRequest) emit();
      if (repeatReload)
        trace.observeReload(() => {
          reloadCalls++;
        });
      NodeAssert.equal(reloadCalls, repeatReload ? 2 : 1);
      NodeAssert.equal(headerReads, extraRequest ? 2 : 1);
      NodeAssert.equal(trace.summary().failure, null);
      const drain = NodeAssert.rejects(trace.flush(), /renderer-trace-drain-timeout/);
      t.mock.timers.tick(1000);
      await drain;
      const summary = trace.summary();
      NodeAssert.equal(summary.drainTimeoutMs, 1000);
      NodeAssert.equal(summary.pendingAtTimeout, extraRequest ? 2 : 1);
      for (const sample of summary.pendingDiagnosticsAtTimeout.samples)
        NodeAssert.deepEqual(sample.reloadTiming, {
          state,
          requestObservedRelativeToReloadStartMs: delta,
        });
      NodeAssert.equal(JSON.stringify(summary).includes("private-"), false);
      NodeAssert.deepEqual(trace.entries, []);
      trace.dispose();
    }
  },
);

NodeTest.test(
  "timeout origin classes never retain secret URLs, paths, queries or headers",
  async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const trace = new EndpointTrace("t");
    t.after(() => trace.dispose());
    const page = new NodeEvents.EventEmitter();
    trace.attach(page);
    const urls = [
      ["http://private-user:private-password@127.0.0.1:43773/private-path?private-query", "A"],
      [`${B}/private-path?private-query`, "B"],
      ["t3code://app/private-path?private-query", "packaged"],
      ["file:///private-path?private-query", "file"],
      ["data:text/plain,private-payload", "data"],
      ["blob:http://private-host/private-path", "blob"],
      [`blob:${A}/private-path`, "blob"],
      ["https://private-host/private-path?private-query", "other"],
      ["t3code://private-host/private-path", "other"],
    ];
    let headerReads = 0;
    for (const [url] of urls) {
      const pending = pendingRequest(url);
      page.emit("request", {
        ...pending.request,
        allHeaders: () => {
          headerReads++;
          return pending.headers.promise;
        },
      });
    }
    const drain = NodeAssert.rejects(trace.flush(), /drain-timeout/);
    t.mock.timers.tick(1000);
    await drain;
    NodeAssert.equal(headerReads, urls.length);
    const summary = trace.summary();
    NodeAssert.deepEqual(
      summary.pendingDiagnosticsAtTimeout.samples.map((sample) => sample.originClass),
      urls.map(([, kind]) => kind),
    );
    NodeAssert.equal(JSON.stringify(summary).includes("private-"), false);
  },
);

NodeTest.test(
  "timeout samples cap at sixteen while pending tracking obeys the event limit",
  async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    for (const count of [16, 17, 5001]) {
      const trace = new EndpointTrace("t");
      t.after(() => trace.dispose());
      const page = new NodeEvents.EventEmitter();
      trace.attach(page);
      let headerReads = 0;
      for (let i = 0; i < count; i++) {
        page.emit("request", {
          url: () => A,
          allHeaders: () => {
            headerReads++;
            return new Promise(() => {});
          },
        });
      }
      const expectedPending = Math.min(count, 5000);
      NodeAssert.equal(headerReads, expectedPending);
      NodeAssert.equal(trace.mark(), expectedPending);
      const drain = NodeAssert.rejects(
        trace.flush(),
        count > 5000 ? /trace-overflow/ : /drain-timeout/,
      );
      t.mock.timers.tick(1000);
      await drain;
      const summary = trace.summary();
      NodeAssert.equal(summary.pending, expectedPending);
      NodeAssert.equal(summary.pendingAtTimeout, expectedPending);
      NodeAssert.equal(summary.pendingDiagnosticsAtTimeout.samples.length, 16);
      NodeAssert.equal(summary.pendingDiagnosticsAtTimeout.omittedCount, expectedPending - 16);
      NodeAssert.deepEqual(
        summary.pendingDiagnosticsAtTimeout.samples.map((sample) => sample.sequence),
        Array.from({ length: 16 }, (_, i) => i),
      );
      NodeAssert.equal(summary.incomplete, true);
      trace.dispose();
      NodeAssert.deepEqual(page.eventNames(), []);
    }
  },
);

NodeTest.test(
  "late headers, lifecycle events and summary edits cannot change the timeout snapshot",
  async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const trace = new EndpointTrace("t");
    t.after(() => trace.dispose());
    const page = new NodeEvents.EventEmitter();
    trace.attach(page);
    const fulfilled = pendingRequest(`${B}/private-path?private-query`);
    const rejected = pendingRequest(A);
    const observations = [
      observeRequest(page, fulfilled.request),
      observeRequest(page, rejected.request),
    ];
    const lateClose = page.listeners("close");
    const lateFinished = page.listeners("requestfinished");
    const drain = NodeAssert.rejects(trace.flush(), /drain-timeout/);
    t.mock.timers.tick(1000);
    await drain;
    const before = trace.summary().pendingDiagnosticsAtTimeout;
    const saved = structuredClone(before);
    before.samples[0].originClass = "tampered";
    before.samples.length = 0;
    before.omittedCount = 999;
    let fingerprints = 0;
    trace.fingerprint = () => {
      fingerprints++;
      return "invalid";
    };
    fulfilled.headers.resolve({ authorization: "Bearer private-header" });
    rejected.headers.reject(new Error("private-browser-error"));
    for (const close of lateClose) close();
    for (const finished of lateFinished) finished(fulfilled.request);
    page.emit("requestfailed", rejected.request);
    trace.dispose();
    await Promise.all(observations);
    const after = trace.summary();
    NodeAssert.deepEqual(after.pendingDiagnosticsAtTimeout, saved);
    NodeAssert.equal(after.pending, 0);
    NodeAssert.equal(after.pendingAtTimeout, 2);
    NodeAssert.equal(after.failure, "renderer-trace-drain-timeout");
    NodeAssert.equal(after.incomplete, true);
    NodeAssert.equal(fingerprints, 0);
    NodeAssert.equal(JSON.stringify(after).includes("private-"), false);
    NodeAssert.deepEqual(trace.entries, []);
    NodeAssert.deepEqual(page.eventNames(), []);
    await NodeAssert.rejects(trace.checkpoint(), /drain-timeout/);
  },
);

NodeTest.test("overlapping drains preserve the first timeout's lifecycle snapshot", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const trace = new EndpointTrace("t");
  t.after(() => trace.dispose());
  const page = new NodeEvents.EventEmitter();
  trace.attach(page);
  page.emit("request", pendingRequest(A).request);
  const first = NodeAssert.rejects(trace.flush(), /drain-timeout/);
  t.mock.timers.tick(500);
  const second = NodeAssert.rejects(trace.flush(), /drain-timeout/);
  t.mock.timers.tick(500);
  await first;
  const snapshot = trace.summary().pendingDiagnosticsAtTimeout;
  NodeAssert.equal(snapshot.samples[0].detached, false);
  t.mock.timers.tick(500);
  await second;
  NodeAssert.deepEqual(trace.summary().pendingDiagnosticsAtTimeout, snapshot);
  NodeAssert.equal(trace.summary().failure, "renderer-trace-drain-timeout");
});

NodeTest.test(
  "settled observations retain generation, fingerprints and successful summary behavior",
  async (t) => {
    const trace = new EndpointTrace("t", { key: Buffer.alloc(32, 7) });
    t.after(() => trace.dispose());
    const first = new NodeEvents.EventEmitter(),
      second = new NodeEvents.EventEmitter();
    const fingerprints = { a: trace.fingerprint("Bearer a"), b: trace.fingerprint("Bearer b") };
    const old = pendingRequest(`${A}/api`);
    trace.attach(first);
    await trace.catalog({ evaluate: async () => fingerprints }, "b");
    first.emit("request", old.request);
    first.emit("close");
    trace.attach(second);
    await trace.catalog({ evaluate: async () => fingerprints }, "b");
    second.emit("request", {
      url: () => B,
      method: () => "GET",
      allHeaders: async () => ({ authorization: "Bearer b" }),
    });
    second.emit("request", { url: () => "t3code://app/asset", allHeaders: async () => ({}) });
    old.headers.resolve({ authorization: "Bearer a" });
    NodeAssert.deepEqual(await trace.checkpoint(), { ok: true });
    NodeAssert.deepEqual(
      trace.entries.map(({ generation, authFp }) => ({ generation, authFp })),
      [
        { generation: 0, authFp: fingerprints.a },
        { generation: 1, authFp: fingerprints.b },
      ],
    );
    NodeAssert.deepEqual(trace.summary(), {
      count: 2,
      localAssetRequests: 1,
      counts: { [`0 ${A} request `]: 1, [`1 ${B} request `]: 1 },
      fingerprints: { 0: fingerprints, 1: fingerprints },
      failure: null,
      incomplete: false,
      pending: 0,
      pendingAtTimeout: null,
      drainTimeoutMs: 1000,
      pendingDiagnosticsAtTimeout: null,
    });
    trace.dispose();
    for (const page of [first, second]) NodeAssert.deepEqual(page.eventNames(), []);
  },
);

NodeTest.test(
  "never-settling headers fail closed within the drain budget",
  { timeout: 250 },
  async (t) => {
    // Keep the pre-fix pending promise alive until the explicit test timeout reports the defect.
    const alive = setInterval(() => {}, 1000);
    const page = new NodeEvents.EventEmitter();
    const trace = new EndpointTrace("t", { drainTimeoutMs: 10 });
    t.after(() => {
      clearInterval(alive);
      trace.dispose();
    });
    trace.attach(page);
    page.emit("request", {
      url: () => A,
      allHeaders: () => new Promise(() => {}),
    });
    await NodeAssert.rejects(trace.flush(), /renderer-trace-drain-timeout/);
    await NodeAssert.rejects(trace.flush(), /renderer-trace-drain-timeout/);
    await NodeAssert.rejects(trace.checkpoint(), /renderer-trace-drain-timeout/);
    NodeAssert.equal(trace.summary().incomplete, true);
    NodeAssert.equal(trace.summary().pendingAtTimeout, 1);
    NodeAssert.equal(page.listenerCount("request"), 0);
    NodeAssert.deepEqual(trace.entries, []);
  },
);

NodeTest.test("late headers cannot append or fingerprint after timeout or disposal", async () => {
  for (const finish of ["timeout", "dispose"]) {
    const page = new NodeEvents.EventEmitter();
    const trace = new EndpointTrace("t", { drainTimeoutMs: 10 });
    let resolve;
    trace.attach(page);
    const observed = observeRequest(page, {
      url: () => `${B}/api?token=private`,
      method: () => "GET",
      allHeaders: () =>
        new Promise((done) => {
          resolve = done;
        }),
    });
    if (finish === "timeout") await NodeAssert.rejects(trace.flush(), /drain-timeout/);
    trace.dispose();
    const before = trace.summary();
    let fingerprints = 0;
    trace.fingerprint = () => {
      fingerprints++;
      return "invalid";
    };
    resolve({ authorization: "Bearer late-secret" });
    await observed;
    NodeAssert.deepEqual(trace.entries, []);
    NodeAssert.equal(fingerprints, 0);
    NodeAssert.deepEqual(trace.summary().fingerprints, before.fingerprints);
    NodeAssert.equal(trace.summary().failure, before.failure);
    NodeAssert.equal(trace.summary().incomplete, true);
    await NodeAssert.rejects(trace.checkpoint(), /drain-timeout|trace-disposed/);
  }
});

NodeTest.test("late catalog replies cannot qualify a disposed trace", async () => {
  const trace = new EndpointTrace("t");
  let reply;
  const catalog = trace.catalog(
    {
      evaluate: () =>
        new Promise((resolve) => {
          reply = resolve;
        }),
    },
    "b",
  );
  trace.dispose();
  reply({ a: "a", b: "b" });
  await NodeAssert.rejects(catalog, /trace-disposed/);
  NodeAssert.deepEqual(trace.summary().fingerprints, {});
  NodeAssert.throws(() => trace.fingerprint("secret"), /trace-disposed/);
});

NodeTest.test(
  "the crossover oracle rejects either misplaced bearer and cross-origin ticket reuse",
  () => {
    const clean = [
      request(A, expected[0].a),
      request(B, expected[0].b),
      request(B, null),
      ticket(A, "ticket-a"),
      ticket(B, "ticket-b"),
    ];
    NodeAssert.deepEqual(assertCrossover(clean, expected), { ok: true });
    for (const injected of [
      request(A, expected[0].b),
      request(B, expected[0].a),
      request(B, "unknown"),
    ])
      NodeAssert.throws(
        () => assertCrossover([...clean, injected], expected),
        /credential-crossover/,
      );
    NodeAssert.throws(
      () => assertCrossover([...clean, ticket(B, "ticket-a")], expected),
      /ticket-crossover/,
    );
    NodeAssert.throws(
      () => assertCrossover([...clean, request("http://127.0.0.1:9", null)], expected),
      /unexpected-origin/,
    );
    NodeAssert.throws(
      () => assertCrossover(clean, { 0: { a: "same", b: "same" } }),
      /fingerprints-unbound/,
    );
  },
);

NodeTest.test(
  "A's bootstrap may rotate on restart while B's bearer is checked in every generation",
  () => {
    const entries = [
      request(A, expected[0].a),
      { ...request(A, "new-a"), generation: 1 },
      { ...request(B, expected[0].b), generation: 1 },
    ];
    NodeAssert.equal(
      assertCrossover(entries, { ...expected, 1: { a: "new-a", b: expected[0].b } }).ok,
      true,
    );
    NodeAssert.throws(() => assertCrossover(entries, expected), /fingerprints-unbound/);
  },
);

NodeTest.test("pin proof requires exactly one matching B dispatch within the action window", () => {
  const pin = {
    sequence: 4,
    generation: 0,
    kind: "frame",
    direction: "sent",
    origin: B,
    tag: "orchestration.dispatchCommand",
    commandType: "thread.pin",
    threadMatch: true,
  };
  NodeAssert.equal(assertPinRouting([pin], 4, 5, 0).ok, true);
  for (const entries of [
    [],
    [{ ...pin, origin: A }],
    [pin, { ...pin, sequence: 5 }],
    [pin, { ...pin, origin: A, threadMatch: false }],
    [{ ...pin, threadMatch: false }],
    [{ ...pin, generation: 1 }],
    [{ ...pin, direction: "received" }],
  ])
    NodeAssert.throws(() => assertPinRouting(entries, 4, 6, 0), /pin-endpoint-mismatch/);
  NodeAssert.throws(() => assertPinRouting([pin], 5, 6, 0), /pin-endpoint-mismatch/);
});

NodeTest.test(
  "real event shape is attributed to the socket endpoint and strips query, header and payload secrets",
  async () => {
    const page = new NodeEvents.EventEmitter();
    const trace = new EndpointTrace("collision", { key: Buffer.alloc(32, 7) });
    const socket = new NodeEvents.EventEmitter();
    socket.url = () => "ws://127.0.0.1:43774/ws?wsTicket=private-ticket&extra=hidden-query";
    trace.attach(page);
    page.emit("request", {
      url: () => `${B}/api/orchestration/shell?secret=hidden-query`,
      method: () => "GET",
      allHeaders: async () => ({ authorization: "Bearer private-bearer" }),
    });
    page.emit("websocket", socket);
    socket.emit("framesent", {
      payload: JSON.stringify({
        _tag: "Request",
        tag: "orchestration.dispatchCommand",
        payload: { type: "thread.pin", threadId: "collision", unrecorded: "private-payload" },
      }),
    });
    socket.emit("framereceived", { payload: "private-server-frame" });
    socket.emit("close");
    await trace.flush();
    const entries = trace.entries;
    NodeAssert.equal(entries.length, 5);
    NodeAssert.equal(entries[0].authFp, trace.fingerprint("Bearer private-bearer"));
    NodeAssert.equal(entries[1].ticketFp, trace.fingerprint("private-ticket"));
    NodeAssert.equal(entries[2].origin, B);
    NodeAssert.equal(entries[2].threadMatch, true);
    NodeAssert.equal(entries[3].direction, "received");
    const serialized = JSON.stringify(entries);
    for (const secret of [
      "private-ticket",
      "hidden-query",
      "private-bearer",
      "private-payload",
      "private-server-frame",
    ])
      NodeAssert.equal(serialized.includes(secret), false);
    trace.dispose();
    NodeAssert.equal(page.listenerCount("request"), 0);
    NodeAssert.equal(socket.listenerCount("framesent"), 0);
  },
);

NodeTest.test(
  "reattachment drops old page listeners; missing tickets and bounded trace overflow fail closed",
  async () => {
    const first = new NodeEvents.EventEmitter(),
      second = new NodeEvents.EventEmitter();
    const trace = new EndpointTrace("t", { limit: 1 });
    trace.attach(first);
    trace.attach(second);
    NodeAssert.equal(first.listenerCount("websocket"), 0);
    const socket = new NodeEvents.EventEmitter();
    socket.url = () => "ws://127.0.0.1:43773/ws?wsTicket=t";
    second.emit("websocket", socket);
    socket.emit("close");
    await NodeAssert.rejects(trace.flush(), /trace-overflow/);
    NodeAssert.equal(trace.entries.length, 1);
    trace.dispose();
    const noTicket = new EndpointTrace("t");
    noTicket.attach(second);
    socket.url = () => "ws://127.0.0.1:43773/ws";
    second.emit("websocket", socket);
    await NodeAssert.rejects(noTicket.flush(), /network-unobservable/);
    noTicket.dispose();
  },
);

NodeTest.test(
  "failed request header observation is a failure, never a clean empty trace",
  async () => {
    const page = new NodeEvents.EventEmitter(),
      trace = new EndpointTrace("t");
    trace.attach(page);
    page.emit("request", {
      url: () => A,
      allHeaders: async () => {
        throw new Error("unavailable");
      },
    });
    await NodeAssert.rejects(trace.flush(), /network-unobservable/);
    trace.dispose();
  },
);

NodeTest.test(
  "packaged asset reads are counted separately; an authorization header on that scheme still fails",
  async () => {
    const page = new NodeEvents.EventEmitter(),
      trace = new EndpointTrace("t");
    trace.attach(page);
    const asset = {
      url: () => "t3code://app/assets/index.js",
      method: () => "GET",
      allHeaders: async () => ({}),
    };
    page.emit("request", asset);
    await trace.flush();
    NodeAssert.equal(trace.entries.length, 0);
    NodeAssert.equal(trace.summary().localAssetRequests, 1);
    page.emit("request", { ...asset, allHeaders: async () => ({ authorization: "Bearer wrong" }) });
    await trace.flush();
    NodeAssert.throws(() => assertCrossover(trace.entries, expected), /unexpected-origin/);
    trace.dispose();
  },
);
