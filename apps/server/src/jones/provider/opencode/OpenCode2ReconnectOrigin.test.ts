import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  MessageId,
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type ProviderReplayEntry,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderEventOrigin from "@t3tools/provider-core/server/jones/ProviderEventOrigin";
import * as OpenCode2Adapter from "@t3tools/provider-opencode/server/v2/adapter";
import * as OpenCode2Server from "@t3tools/provider-opencode/server/v2/OpenCode2Server";
import { OPENCODE_PROVIDER } from "@t3tools/provider-opencode/testing";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";

import {
  OPENCODE2_HTTP_PROTOCOL,
  replayServer,
} from "../../../orchestration-v2/Adapters/OpenCode2AdapterV2.testkit.ts";

// The adapter retires an event producer only when a reconnect lends a different connection.
// RunExecutionService and ProviderSessionManager revalidate an event's captured producer when
// they dequeue it, so these tests check already-queued events the same way.

const SESSION = "ses_f148ca2deffeJcwCnRQtb0YFNX";
const WORK = "/work/opencode2";
const PROMPT_ID = "msg_0eb735d41001NJee1EvVePJAK5";
const instanceId = ProviderInstanceId.make("opencode");
const threadId = ThreadId.make("thread:opencode2-adapter");
const providerSessionId = ProviderSessionId.make("provider-session:opencode2-adapter");
const bigPickle: ModelSelection = { instanceId, model: "opencode/big-pickle" };
const policy = {
  runtimeMode: "full-access" as const,
  interactionMode: "default" as const,
  cwd: WORK,
};

const out = (type: string, input?: unknown): ProviderReplayEntry => ({
  type: "expect_outbound",
  frame: input === undefined ? { type } : { type, input },
});
const reply = (operation: string, data: unknown): ProviderReplayEntry => ({
  type: "emit_inbound",
  frame: { type: "sdk.response", operation, data },
});
const replyData = (operation: string, data: unknown) => reply(operation, { data });

const modelCatalog = {
  location: { directory: WORK },
  data: [
    {
      id: "big-pickle",
      modelID: "big-pickle",
      providerID: "opencode",
      family: "big-pickle",
      name: "Big Pickle",
      compatibility: { reasoningField: "reasoning_content" },
      package: "@opencode/ai/providers/openai-compatible",
      settings: { apiKey: "public", baseURL: "https://opencode.ai/zen/v1", provider: "opencode" },
      capabilities: { tools: true, input: ["text"], output: ["text"] },
      variants: [],
      time: { released: 1760659200000 },
      cost: [{ input: 0, output: 0, cache: { read: 0, write: 0 } }],
      status: "active",
      enabled: true,
      limit: { context: 200000, input: 160000, output: 32000 },
    },
  ],
};
const sessionInfo = {
  id: SESSION,
  title: "Prime check",
  permissions: [
    { action: "*", resource: "*", effect: "allow" },
    { action: "t3-code-*", resource: "*", effect: "deny" },
    { action: "t3-code-thread_opencode2-adapter_*", resource: "*", effect: "allow" },
  ],
  projectID: "global",
  model: { id: "big-pickle", providerID: "opencode", variant: "default" },
  cost: 0,
  tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
  time: { created: 1790656601394, updated: 1790656601394 },
  location: { directory: WORK },
};

// A turn starts, the event stream ends, and the reconnect backfills the turn's end from history.
const entries: ReadonlyArray<ProviderReplayEntry> = [
  out("event.subscribe"),
  out("model.list", "<any>"),
  reply("model.list", modelCatalog),
  out("session.get", { sessionID: SESSION }),
  replyData("session.get", sessionInfo),
  out("permission.list", { sessionID: SESSION }),
  replyData("permission.list", []),
  out("session.form.list", { sessionID: SESSION }),
  replyData("session.form.list", []),
  out("session.instructions.entry.put", { sessionID: SESSION, key: "t3-code", value: "<any>" }),
  reply("session.instructions.entry.put", null),
  out("session.prompt", { sessionID: SESSION, id: PROMPT_ID, text: "<any>" }),
  replyData("session.prompt", {
    id: PROMPT_ID,
    sessionID: SESSION,
    time: { created: 1790656601410 },
    type: "user",
    payload: { text: "hi" },
    delivery: "steer",
  }),
  { type: "runtime_exit", status: "success" },
  out("event.subscribe"),
  out("session.active"),
  replyData("session.active", {}),
  out("message.list", { sessionID: SESSION, order: "desc", limit: "50" }),
  reply("message.list", {
    data: [
      { id: "msg_idle_gap", time: { created: 3 }, type: "idle", outcome: "succeeded" },
      {
        id: "msg_assistant_gap",
        time: { created: 2 },
        type: "assistant",
        agent: "build",
        model: { id: "big-pickle", providerID: "opencode", variant: "default" },
        content: [{ type: "text", text: "Sent while the stream was down." }],
        finish: "stop",
      },
      { id: PROMPT_ID, time: { created: 1 }, text: "hi", type: "user" },
    ],
    cursor: {},
  }),
];

const runAcrossReconnect = (lend: "same" | "replacement") =>
  Effect.gen(function* () {
    const server = yield* replayServer({
      provider: OPENCODE_PROVIDER,
      protocol: OPENCODE2_HTTP_PROTOCOL,
      version: "2.0.18",
      scenario: `opencode2-reconnect-origin-${lend}`,
      entries,
    });
    let sessionBorrows = 0;
    // A replacement lends a distinct connection on reconnect, as a restarted server does.
    const lending = OpenCode2Server.OpenCode2Server.of({
      withConnection: (use, hooks, kind) =>
        server.withConnection(
          (connection) =>
            use(
              kind === "session" && sessionBorrows++ > 0 && lend === "replacement"
                ? { ...connection }
                : connection,
            ),
          hooks,
          kind,
        ),
    });
    const adapter = yield* OpenCode2Adapter.make(instanceId).pipe(
      Effect.provideService(OpenCode2Server.OpenCode2Server, lending),
    );
    const runtime = yield* adapter.openSession({
      threadId,
      providerSessionId,
      modelSelection: bigPickle,
      runtimePolicy: policy,
    });
    const now = yield* DateTime.now;
    const thread = yield* runtime.resumeThread({
      providerThread: {
        id: ProviderThreadId.make("provider-thread:opencode2-adapter"),
        driver: OPENCODE_PROVIDER,
        providerInstanceId: instanceId,
        providerSessionId,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: { driver: OPENCODE_PROVIDER, nativeId: SESSION, strength: "strong" },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      },
      threadId,
      modelSelection: bigPickle,
      runtimePolicy: policy,
    });
    const events = yield* runtime.events.pipe(
      Stream.takeUntil((event) => event.type === "turn.terminal"),
      Stream.runCollect,
      Effect.forkScoped,
    );
    yield* runtime.startTurn({
      appThread: { title: "Prime check" } as OrchestrationV2AppThread,
      threadId,
      runId: RunId.make("run:opencode2-adapter"),
      runOrdinal: 1,
      providerTurnOrdinal: 1,
      attemptId: RunAttemptId.make("attempt:opencode2-adapter"),
      rootNodeId: NodeId.make("node:opencode2-adapter"),
      providerThread: thread,
      message: {
        messageId: MessageId.make("message:opencode2-adapter"),
        text: "hi",
        attachments: [],
        createdBy: "user" as const,
        creationSource: "web" as const,
        scheduledTaskId: undefined,
        senderThreadId: undefined,
      },
      modelSelection: bigPickle,
      runtimePolicy: policy,
    });
    const collected = yield* Fiber.join(events);
    assert.deepInclude(collected.at(-1), { type: "turn.terminal", status: "completed" });

    // Group queued events by the producer that captured them, in emission order.
    const producers: Array<object> = [];
    const byProducer = new Map<object, Array<ProviderAdapter.ProviderAdapterV2Event>>();
    for (const event of collected) {
      const token = ProviderEventOrigin.readProviderEventOrigin(event)?.producer.token;
      assert.isDefined(token);
      if (!byProducer.has(token!)) {
        producers.push(token!);
        byProducer.set(token!, []);
      }
      byProducer.get(token!)!.push(event);
    }
    assert.lengthOf(producers, 2, "the reconnect starts a second producer");
    const deliverable = (batch: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) =>
      Effect.forEach(batch, (event) =>
        ProviderEventOrigin.revalidateProviderEventOrigin(event, runtime).pipe(
          Effect.exit,
          Effect.map(Exit.isSuccess),
        ),
      );
    return {
      beforeReconnect: yield* deliverable(byProducer.get(producers[0]!)!),
      afterReconnect: yield* deliverable(byProducer.get(producers[1]!)!),
    };
  }).pipe(
    Effect.scoped,
    Effect.provide(
      Layer.mergeAll(
        TestProviderHost.layer().pipe(
          Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
        ),
        IdAllocator.layer,
        McpProviderSessions.layer,
        NodeServices.layer,
        NodeCrypto.layer,
      ),
    ),
  );

describe("OpenCode 2 event origin across an event stream reconnect", () => {
  it.effect("keeps events queued before a reconnect to the same server deliverable", () =>
    Effect.gen(function* () {
      const { beforeReconnect, afterReconnect } = yield* runAcrossReconnect("same");
      assert.isNotEmpty(beforeReconnect);
      assert.isTrue(beforeReconnect.every(Boolean));
      assert.isTrue(afterReconnect.every(Boolean));
    }),
  );

  it.effect("rejects events queued before a reconnect that lends a different connection", () =>
    Effect.gen(function* () {
      const { beforeReconnect, afterReconnect } = yield* runAcrossReconnect("replacement");
      assert.isNotEmpty(beforeReconnect);
      assert.isTrue(beforeReconnect.every((deliverable) => !deliverable));
      assert.isTrue(afterReconnect.every(Boolean));
    }),
  );
});
