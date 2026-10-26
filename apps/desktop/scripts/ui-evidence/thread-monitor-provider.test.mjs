import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import {
  createMonitorProtocol,
  fixture,
  assertPrivateNamespace,
  installMonitorFixture,
} from "./thread-monitor-provider.mjs";
function peer() {
  const frames = [],
    ledger = [];
  let counter = 0;
  const protocol = createMonitorProtocol(
    {
      write: (f) => frames.push(f),
      trace: (e) => ledger.push(e),
      id: () => "synthetic-" + ++counter,
      now: () => 1700000000000,
    },
    fixture,
  );
  protocol.handle({
    id: 1,
    method: "initialize",
  });
  protocol.handle({
    method: "initialized",
  });
  protocol.handle({
    id: 2,
    method: "thread/start",
    params: {
      cwd: "/scratch/workspace/native-self-config-execution",
      model: "native-model-a",
      config: {
        "mcp_servers.t3-code.url": "http://127.0.0.1/unused",
        "mcp_servers.t3-code.http_headers.Authorization": "SYNTHETIC_DO_NOT_RECORD",
      },
    },
  });
  const nativeThreadId = frames.at(-1).result.thread.id;
  const start = (text) =>
    protocol.handle({
      id: ++counter + 100,
      method: "turn/start",
      params: {
        threadId: nativeThreadId,
        input: [
          {
            type: "text",
            text,
          },
        ],
        model: "native-model-a",
        collaborationMode: {
          mode: "default",
        },
      },
    });
  const request = () =>
    frames.findLast(
      (f) => f.method?.endsWith("requestApproval") || f.method === "item/tool/requestUserInput",
    );
  const respond = (result) =>
    protocol.handle({
      id: request().id,
      result,
    });
  const approvalTurns = () => {
    start(fixture.once);
    respond({
      decision: "accept",
    });
    start(fixture.deny);
    respond({
      decision: "decline",
    });
  };
  return {
    protocol,
    frames,
    ledger,
    start,
    request,
    respond,
    approvalTurns,
    nativeThreadId,
  };
}
NodeTest.test(
  "only the bound private namespace admits installation, before any scratch mutation",
  async () => {
    await NodeAssert.rejects(
      assertPrivateNamespace("/home/owner/workspace"),
      /Private native namespace required/,
    );
    await NodeAssert.rejects(
      installMonitorFixture("/scratch/workspace/../other"),
      /Private native namespace required/,
    );
  },
);
NodeTest.test(
  "once/deny are separate current-protocol callbacks and a wrong or duplicate response cannot complete a turn",
  () => {
    const p = peer();
    p.start(fixture.once);
    const first = p.request();
    NodeAssert.equal(first.method, "item/commandExecution/requestApproval");
    NodeAssert.equal(first.params.startedAtMs, 1700000000000);
    NodeAssert.equal(first.params.threadId, p.nativeThreadId);
    NodeAssert.throws(
      () =>
        p.protocol.handle({
          id: first.id + 1,
          result: {
            decision: "accept",
          },
        }),
      /Unexpected synthetic request response/,
    );
    NodeAssert.throws(
      () =>
        p.respond({
          decision: "acceptForSession",
        }),
      /Unexpected synthetic request response/,
    );
    NodeAssert.equal(p.ledger.filter((e) => e.event === "completed").length, 0);
    p.respond({
      decision: "accept",
    });
    NodeAssert.throws(
      () =>
        p.protocol.handle({
          id: first.id,
          result: {
            decision: "accept",
          },
        }),
      /Unexpected synthetic request response/,
    );
    p.start(fixture.deny);
    NodeAssert.notEqual(p.request().id, first.id);
    NodeAssert.notEqual(p.request().params.turnId, first.params.turnId);
    p.respond({
      decision: "decline",
    });
    NodeAssert.deepEqual(
      p.ledger.filter((e) => e.event === "response").map((e) => e.result.decision),
      ["accept", "decline"],
    );
  },
);
NodeTest.test(
  "original question IDs, semantic option labels, and custom text must reach the peer unmodified",
  () => {
    const p = peer();
    p.approvalTurns();
    p.start(fixture.questions);
    NodeAssert.equal(p.request().method, "item/tool/requestUserInput");
    NodeAssert.equal(p.request().params.isBlocking, true);
    NodeAssert.deepEqual(p.request().params.questions, fixture.questionsPayload);
    NodeAssert.ok(p.request().params.questions.every((q) => !Object.hasOwn(q, "multiSelect")));
    const answers = {
      route: {
        answers: ["Isolated route"],
      },
      note: {
        answers: [fixture.custom],
      },
    };
    for (const wrong of [
      {
        ...answers,
        route: {
          answers: ["option-0"],
        },
      },
      {
        ...answers,
        note: {
          answers: ["Other"],
        },
      },
      {
        ...answers,
        invented: {
          answers: ["extra"],
        },
      },
    ])
      NodeAssert.throws(
        () =>
          p.respond({
            answers: wrong,
          }),
        /Unexpected synthetic request response/,
      );
    p.respond({
      answers: {
        note: answers.note,
        route: answers.route,
      },
    });
    NodeAssert.equal(p.ledger.filter((e) => e.event === "completed").length, 3);
  },
);
NodeTest.test(
  "a real plan item and delta precede one exact ordinary implementation continuation",
  () => {
    const p = peer();
    p.approvalTurns();
    p.start(fixture.questions);
    p.respond({
      answers: {
        route: {
          answers: ["Isolated route"],
        },
        note: {
          answers: [fixture.custom],
        },
      },
    });
    p.start(fixture.plan);
    const plan = p.frames.find(
      (f) => f.method === "item/completed" && f.params.item.type === "plan",
    );
    NodeAssert.equal(plan.params.item.text, fixture.markdown);
    const delta = p.frames.find((f) => f.method === "item/plan/delta");
    NodeAssert.equal(delta.params.itemId, plan.params.item.id);
    NodeAssert.equal(delta.params.delta, fixture.markdown);
    NodeAssert.equal(plan.params.completedAtMs, 1700000000000);
    NodeAssert.throws(
      () => p.start("Implement this plan: " + fixture.markdown),
      /Unexpected or duplicate synthetic input/,
    );
    const text = "PLEASE IMPLEMENT THIS PLAN:\n" + fixture.markdown.trim();
    p.start(text);
    NodeAssert.equal(
      p.ledger.filter((e) => e.event === "completed").length,
      4,
      "Implementation stays active until explicit fixture release",
    );
    NodeAssert.throws(() => p.start(text), /Unexpected synthetic turn context/);
    p.protocol.release();
    NodeAssert.equal(p.ledger.filter((e) => e.event === "completed").length, 5);
    NodeAssert.throws(() => p.start(text), /Unexpected or duplicate synthetic input/);
    NodeAssert.throws(() => p.protocol.release(), /without held implementation/);
  },
);
NodeTest.test(
  "unsupported methods, foreign workspace, nonfixture input and implicit steer/interrupt fail closed without recording ambient config",
  () => {
    const p = peer();
    NodeAssert.throws(
      () =>
        p.protocol.handle({
          id: 90,
          method: "thread/start",
          params: {
            cwd: "/home/owner",
          },
        }),
      /Unexpected synthetic session context/,
    );
    NodeAssert.throws(
      () =>
        p.protocol.handle({
          id: 91,
          method: "thread/read",
          params: {
            threadId: "foreign",
          },
        }),
      /Foreign synthetic thread read/,
    );
    NodeAssert.throws(
      () => p.start("ambient private text"),
      /Unexpected or duplicate synthetic input/,
    );
    for (const method of ["turn/steer", "turn/interrupt"])
      NodeAssert.throws(
        () =>
          p.protocol.handle({
            id: 92,
            method,
            params: {},
          }),
        /must never steer or interrupt/,
      );
    NodeAssert.throws(
      () =>
        p.protocol.handle({
          id: 93,
          method: "exec/run",
        }),
      /Unsupported synthetic method/,
    );
    NodeAssert.ok(!JSON.stringify(p.ledger).includes("SYNTHETIC_DO_NOT_RECORD"));
    NodeAssert.ok(!JSON.stringify(p.ledger).includes("ambient private text"));
    NodeAssert.equal(p.ledger.filter((e) => e.event === "start").length, 0);
  },
);
