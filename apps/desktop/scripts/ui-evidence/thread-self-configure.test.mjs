import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { loadScenario } from "./scenario-api.mjs";
import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import {
  issuedConnection,
  decodeMcpResponse,
  assertContinuity,
  assertFreshAdmission,
  selectionA,
  selectionB,
} from "./thread-self-configure-support.mjs";

NodeTest.test("MCP connection comes only from app-issued thread configuration", () => {
  const params = {
    config: {
      mcp_servers: {
        "t3-code": {
          url: "http://127.0.0.1:43773/mcp",
          http_headers: { Authorization: "Bearer synthetic" },
        },
      },
    },
  };
  NodeAssert.deepEqual(issuedConnection(params), {
    url: "http://127.0.0.1:43773/mcp",
    authorization: "Bearer synthetic",
  });
  for (const value of [
    {},
    { endpoint: "http://127.0.0.1:1/mcp" },
    {
      config: {
        mcp_servers: {
          "t3-code": {
            url: "http://example.test/mcp",
            http_headers: { Authorization: "Bearer synthetic" },
          },
        },
      },
    },
    { config: { mcp_servers: { "t3-code": { url: "http://127.0.0.1:1/mcp" } } } },
  ])
    NodeAssert.throws(() => issuedConnection(value), /MCP connection/);
});

NodeTest.test(
  "MCP response decoder correlates JSON and SSE without mistaking notifications for receipts",
  () => {
    NodeAssert.deepEqual(decodeMcpResponse('{"id":7,"result":{"ok":true}}', 7), { ok: true });
    NodeAssert.deepEqual(
      decodeMcpResponse(
        'event: message\ndata: {"method":"notification"}\n\n' +
          'event: message\ndata: {"id":7,"result":{"ok":true}}\n\n',
        7,
      ),
      { ok: true },
    );
    for (const text of ['{"id":8,"result":{}}', '{"id":7,"error":{"message":"private"}}'])
      NodeAssert.throws(() => decodeMcpResponse(text, 7), /protocol response rejected/);
  },
);

function continuity() {
  const executionPrecondition = {
    runId: "run-a",
    attemptId: "attempt-a",
    providerThreadId: "provider-a",
    providerSessionId: "session-a",
    runtimeGeneration: "generation-a",
  };
  return {
    before: { executionPrecondition },
    after: {
      executionPrecondition: structuredClone(executionPrecondition),
      currentExecution: { requestedSelection: structuredClone(selectionA) },
      savedSelection: structuredClone(selectionB),
    },
    projection: { runs: [{ id: "run-a" }], attempts: [{ id: "attempt-a" }] },
    nativeThreadId: "native-a",
    processId: "process-a",
    trace: ["thread/start", "turn/start", "turn/steer"].map((method) => ({
      method,
      nativeThreadId: "native-a",
      processId: "process-a",
      ...(method === "turn/steer" ? { input: "NATIVE_ORDINARY_FOLLOWUP" } : {}),
    })),
  };
}
NodeTest.test(
  "continuity oracle accepts a held A with saved B and one real steering receipt",
  () => {
    NodeAssert.doesNotThrow(() => assertContinuity(continuity()));
  },
);
NodeTest.test(
  "continuity oracle rejects each replacement, duplicate, wrong destination and interrupted execution",
  () => {
    const mutations = [
      (v) => {
        v.after.executionPrecondition.runId = "replacement";
      },
      (v) => {
        v.after.executionPrecondition.attemptId = "replacement";
      },
      (v) => {
        v.after.executionPrecondition.runtimeGeneration = "replacement";
      },
      (v) => {
        v.after.executionPrecondition.providerSessionId = "replacement";
      },
      (v) => {
        v.after.currentExecution.requestedSelection = selectionB;
      },
      (v) => {
        v.after.savedSelection = selectionA;
      },
      (v) => {
        v.projection.runs.push({ id: "extra" });
      },
      (v) => {
        v.projection.attempts.push({ id: "extra" });
      },
      (v) => {
        v.trace.push({ ...v.trace[2] });
      },
      (v) => {
        v.trace.push({ ...v.trace[1] });
      },
      (v) => {
        v.trace.push({ ...v.trace[0], method: "thread/resume" });
      },
      (v) => {
        v.trace.push({ ...v.trace[1], method: "turn/interrupt" });
      },
      (v) => {
        v.trace[2].processId = "replacement";
      },
      (v) => {
        v.trace[2].input = "different";
      },
      (v) => {
        v.trace.pop();
      },
    ];
    for (const mutate of mutations) {
      const value = continuity();
      mutate(value);
      NodeAssert.throws(
        () => assertContinuity(value),
        /SC-COR-001 ordinary UI follow-up continuity/,
      );
    }
  },
);

function fresh() {
  return {
    priorRunId: "a",
    nativeThreadId: "native-a",
    projection: { runs: [{ id: "a" }, { id: "b", modelSelection: selectionB }] },
    trace: [
      {
        nativeThreadId: "native-a",
        method: "turn/start",
        input: "NATIVE_FRESH_B",
        instance: "B",
        model: selectionB.model,
        effort: "high",
        serviceTier: "default",
        collaborationModel: selectionB.model,
        collaborationEffort: "high",
      },
    ],
  };
}
NodeTest.test(
  "fresh admission requires captured selection and actual outbound provider fields",
  () => {
    NodeAssert.equal(assertFreshAdmission(fresh()).id, "b");
    for (const mutate of [
      (v) => {
        v.projection.runs[1].modelSelection = selectionA;
      },
      (v) => {
        v.trace[0].instance = "A";
      },
      (v) => {
        v.trace[0].model = selectionA.model;
      },
      (v) => {
        v.trace[0].effort = "low";
      },
      (v) => {
        v.trace[0].serviceTier = "unexpected-tier";
      },
      (v) => {
        delete v.trace[0].collaborationEffort;
      },
      (v) => {
        v.trace.push({ ...v.trace[0] });
      },
    ]) {
      const value = fresh();
      mutate(value);
      NodeAssert.throws(() => assertFreshAdmission(value));
    }
  },
);

NodeTest.test(
  "staged scenario imports its colocated support with the loader content-hash query",
  async () => {
    const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jcue-staged-self-config-"));
    try {
      for (const [source, target] of [
        ["./scenarios/thread-self-configure.mjs", "scenario.mjs"],
        ["./thread-self-configure-support.mjs", "thread-self-configure-support.mjs"],
      ])
        await NodeFSP.copyFile(new URL(source, import.meta.url), NodePath.join(root, target));
      const scenario = await loadScenario(NodePath.join(root, "scenario.mjs"));
      NodeAssert.equal(typeof scenario.run, "function");
    } finally {
      await NodeFSP.rm(root, { recursive: true, force: true });
    }
  },
);
