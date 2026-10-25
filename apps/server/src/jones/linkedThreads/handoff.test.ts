import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ThreadId,
  RunId,
  OrchestratorMcpFailure,
  type ExecutionEnvironmentDescriptor,
  type RuntimeMode,
} from "@t3tools/contracts";
import {
  ThreadHandoffToolResult,
  type LinkedThreadLink,
  type ThreadHandoffToolInput,
} from "@t3tools/contracts/jones/linkedThreads";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import { McpSchema, McpServer, Toolkit } from "effect/ai";
import * as Environment from "../../environment/ServerEnvironment.ts";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import { type ThreadStartGate, toLayer } from "../../mcp/McpToolAccess.ts";
import * as McpHttpServer from "../../mcp/McpHttpServer.ts";
import {
  DispatchModeLimit,
  exceededDispatchModeLimit,
  type DispatchModeLimitValue,
} from "../../orchestration-v2/DispatchModeLimit.ts";
import * as Orchestrator from "../../mcp/OrchestratorMcpService.ts";
import { liveThreadShell } from "../../mcp/McpToolAccess.testkit.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as PeerLinks from "../../peer/PeerLinks.ts";
import * as Forwarding from "../../peer/PeerForwarding.ts";
import * as Links from "./LinkedThreads.ts";
import * as Launch from "./launchTransport.ts";
import * as Handoff from "./handoff.ts";
import { handoffRequestKey, linkedThreadIdentity } from "./identity.ts";
import { ThreadHandoffTool, threadHandoffHandler } from "./tools.ts";

const here = EnvironmentId.make("here"),
  origin = ThreadId.make("origin"),
  target = ThreadId.make("target");
const input: ThreadHandoffToolInput = {
  to: { type: "existing", threadId: target },
  goal: "Finish feature",
  scope: "One lane",
  status: "Ready",
  clientRequestId: "stable-key",
};
const scope: Invocation.McpInvocationScope = {
  environmentId: here,
  issuedAt: 0,
  requestNamespace: "handoff-test",
  capabilities: new Set(["orchestration"]),
  thread: {
    threadId: origin,
    providerInstanceId: liveThreadShell(origin).providerInstanceId,
    providerSessionId: "session",
  },
  client: undefined,
};
const gate: ThreadStartGate = {
  start: Effect.succeed({
    runtimeMode: "full-access",
    interactionMode: "default",
    linkOrigin: undefined,
  }),
  continueWithin: (_target, effect) => effect,
};
function fixture(
  sendError?: OrchestratorMcpFailure,
  options: {
    runtimeMode?: RuntimeMode;
    send?: Orchestrator.OrchestratorMcpService["Service"]["sendToThread"];
  } = {},
) {
  const messages: string[] = [],
    launchKeys: string[] = [],
    launchTexts: string[] = [],
    linkIds = new Set<string>();
  const services = Layer.mergeAll(
    Layer.succeed(Invocation.McpInvocationContext, scope),
    Layer.mock(Environment.ServerEnvironment)({
      getEnvironmentId: Effect.succeed(here),
      getDescriptor: Effect.succeed({
        environmentId: here,
        label: "Laptop",
      } as ExecutionEnvironmentDescriptor),
    }),
    Layer.mock(Threads.ThreadManagementService)({
      getThreadShell: (id) =>
        Effect.succeed(liveThreadShell(id, { runtimeMode: options.runtimeMode ?? "full-access" })),
    }),
    Layer.mock(PeerLinks.PeerLinks)({ get: () => Effect.succeed(Option.none()) }),
    Layer.mock(Forwarding.PeerForwarding)({}),
    Layer.mock(Orchestrator.OrchestratorMcpService)({
      sendToThread: (_scope, value) => {
        messages.push(value.clientRequestId ?? "");
        if (options.send !== undefined) return options.send(_scope, value);
        return sendError === undefined
          ? Effect.succeed({
              threadId: target,
              messageId: "message" as never,
              runId: RunId.make("run"),
              status: "queued",
              delivery: "queued",
            })
          : Effect.fail(sendError);
      },
    }),
    Layer.mock(Launch.HandoffLaunch)({
      launch: (value) => {
        launchKeys.push(value.clientRequestId ?? "");
        launchTexts.push(value.message ?? "");
        return Effect.succeed({
          threadId: target,
          projectId: liveThreadShell(target).projectId,
          modelSelection: liveThreadShell(target).modelSelection,
          runId: RunId.make("run"),
          status: "queued",
          initialMessage: "accepted",
          replayed: launchKeys.length > 1,
        });
      },
    }),
    Layer.mock(Links.LinkedThreads)({
      link: (_actor, value) => {
        const identity = linkedThreadIdentity(value.kind, value.from, value.to);
        linkIds.add(identity.linkId);
        const link: LinkedThreadLink = {
          ...identity,
          kind: value.kind,
          basis: value.basis,
          assertedBy: "agent",
          reverse: "local",
          acceptance: "offered",
          handoff: value.handoff ?? null,
          remote: null,
          createdAt: "now",
          updatedAt: "now",
          acceptedAt: null,
        };
        return Effect.succeed({ link, outcome: "recorded" });
      },
    }),
  );
  return {
    messages,
    launchKeys,
    launchTexts,
    linkIds,
    layer: Handoff.layer.pipe(Layer.provideMerge(services)),
  };
}
it.effect("portable handoff renders attribution without delivery or a link", () => {
  const f = fixture();
  return Effect.gen(function* () {
    const handoffs = yield* Handoff.ThreadHandoff;
    const result = yield* handoffs.execute({ ...input, to: { type: "portable" } }, gate);
    expect(result.delivery).toBe("not_sent");
    expect(result.link).toBeNull();
    expect(result.destination).toBeNull();
    expect(result.handoffText).toContain("thread `origin` on Laptop");
    expect(result.handoffText).toContain("check `t3_environment_links`");
    expect(f.messages).toEqual([]);
    expect(f.launchKeys).toEqual([]);
    expect(f.linkIds.size).toBe(0);
  }).pipe(Effect.provide(f.layer));
});
it.effect("existing and new handoffs retain keys and offered ownership across retries", () => {
  const f = fixture();
  return Effect.gen(function* () {
    const handoffs = yield* Handoff.ThreadHandoff;
    const existing = yield* handoffs.execute(input, gate);
    expect(existing.delivery).toBe("queued");
    expect(existing.link?.acceptance).toBe("offered");
    const request = { ...input, to: { type: "new" as const, title: "Continue" } };
    const first = yield* handoffs.execute(request, gate);
    const retry = yield* handoffs.execute(request, gate);
    expect(first.destination).toEqual(retry.destination);
    expect(retry.launch?.replayed).toBe(true);
    expect(f.messages).toEqual(["handoff:stable-key:message"]);
    expect(f.launchKeys).toEqual(["handoff:stable-key", "handoff:stable-key"]);
    expect(f.launchTexts[0]).toBe(f.launchTexts[1]);
    expect(f.linkIds.size).toBe(1);
    expect(first.link?.acceptance).toBe("offered");
  }).pipe(Effect.provide(f.layer));
});
it.effect(
  "rejected existing delivery records no continuation; ambiguous delivery remains offered",
  () => {
    const rejected = fixture(
      new OrchestratorMcpFailure({
        code: "capability_denied",
        message: "The receiver blocks messages.",
      }),
    );
    const unknown = fixture(
      new OrchestratorMcpFailure({
        code: "orchestration_error",
        message: "Transport outcome is unknown.",
      }),
    );
    const check = (f: ReturnType<typeof fixture>, delivery: "rejected" | "unknown") =>
      Effect.gen(function* () {
        const result = yield* (yield* Handoff.ThreadHandoff).execute(input, gate);
        expect(result.delivery).toBe(delivery);
        expect(f.linkIds.size).toBe(delivery === "rejected" ? 0 : 1);
        expect(result.link?.acceptance ?? null).toBe(delivery === "rejected" ? null : "offered");
      }).pipe(Effect.provide(f.layer));
    return check(rejected, "rejected").pipe(Effect.andThen(check(unknown, "unknown")));
  },
);

it.effect.each([
  { receiverRaises: false, name: "admits an unchanged receiver" },
  { receiverRaises: true, name: "refuses a receiver mode raise after precheck" },
])("installs the real handoff dispatch limit and $name", ({ receiverRaises }) => {
  let observedLimit: DispatchModeLimitValue | undefined;
  let admitted = 0;
  const f = fixture(undefined, {
    runtimeMode: "approval-required",
    send: (_scope, request) =>
      Effect.gen(function* () {
        observedLimit = yield* DispatchModeLimit;
        const lockedReceiver = liveThreadShell(request.threadId, {
          runtimeMode: receiverRaises ? "full-access" : "approval-required",
        });
        const exceeded =
          observedLimit === undefined
            ? undefined
            : exceededDispatchModeLimit(observedLimit, lockedReceiver);
        if (exceeded !== undefined) {
          if (observedLimit?.refused !== undefined)
            yield* Ref.set(observedLimit.refused, {
              threadId: request.threadId,
              mode: exceeded,
              runtimeMode: lockedReceiver.runtimeMode,
              interactionMode: lockedReceiver.interactionMode,
            });
          return yield* new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: "Locked dispatch refused the raised receiver.",
          });
        }
        admitted++;
        return {
          threadId: target,
          messageId: "message" as never,
          runId: RunId.make("run"),
          status: "queued" as const,
          delivery: "queued" as const,
        };
      }),
  });
  const toolkit = Toolkit.make(ThreadHandoffTool);
  const handlers = toLayer(toolkit, { t3_thread_handoff: threadHandoffHandler });
  const client = McpSchema.McpServerClient.of({
    clientId: 1,
    protocolVersion: "2025-06-18",
    clientCapabilities: {},
    clientInfo: { name: "handoff-race", version: "1" },
    initializePayload: {
      protocolVersion: "2025-06-18",
      capabilities: {},
      clientInfo: { name: "handoff-race", version: "1" },
    },
    getClient: Effect.die("unused"),
  });
  return Effect.gen(function* () {
    const server = yield* McpServer.McpServer;
    const response = yield* server.callTool({ name: "t3_thread_handoff", arguments: input });
    const result = yield* Schema.decodeUnknownEffect(
      Schema.fromJsonString(ThreadHandoffToolResult),
    )(response.content.map((part) => (part.type === "text" ? part.text : "")).join(""));
    expect(observedLimit).toMatchObject({
      runtimeMode: "approval-required",
      interactionMode: "default",
    });
    expect(observedLimit?.refused).toBeDefined();
    if (observedLimit?.refused !== undefined) {
      expect(yield* Ref.get(observedLimit.refused)).toEqual(
        receiverRaises
          ? {
              threadId: target,
              mode: "runtime",
              runtimeMode: "full-access",
              interactionMode: "default",
            }
          : undefined,
      );
    }
    expect(admitted).toBe(receiverRaises ? 0 : 1);
    expect(result.delivery).toBe(receiverRaises ? "unknown" : "queued");
    if (receiverRaises)
      expect(result.detail).toBe("The receiver did not confirm message acceptance.");
  }).pipe(
    Effect.provideService(McpSchema.McpServerClient, client),
    Effect.provide(
      McpHttpServer.toolkitRegistration(toolkit, handlers).pipe(
        Layer.provideMerge(McpServer.McpServer.layer),
        Layer.provideMerge(f.layer),
      ),
    ),
  );
});

it("bounds prefixed keys without losing deterministic retries", () => {
  const key = "x".repeat(256);
  expect(handoffRequestKey(key, true).length).toBeLessThanOrEqual(256);
  expect(handoffRequestKey(key, true)).toBe(handoffRequestKey(key, true));
  expect(handoffRequestKey(key, true)).not.toBe(handoffRequestKey(key + "y", true));
  expect(handoffRequestKey("key", true)).toBe("handoff:key:message");
});
