import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeEvents from "node:events";
import { EndpointTrace, assertCrossover, assertPinRouting } from "./support/endpoint-trace.mjs";

const A = "http://127.0.0.1:43773",
  B = "http://127.0.0.1:43774";
const expected = { 0: { a: "a-fingerprint", b: "b-fingerprint" } };
const request = (origin, authFp) => ({ generation: 0, kind: "request", origin, authFp });
const ticket = (origin, ticketFp) => ({ generation: 0, kind: "websocket", origin, ticketFp });

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
