import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as SqlClient from "effect/sql/SqlClient";
import {
  EnvironmentId,
  ThreadId,
  RunId,
  OrchestratorMcpFailure,
  CommandId,
  type ExecutionEnvironmentDescriptor,
  type RuntimeMode,
} from "@t3tools/contracts";
import {
  ThreadHandoffToolResult,
  ThreadLinkToolResult,
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
import { idleThreadProjection, liveThreadShell } from "../../mcp/McpToolAccess.testkit.ts";
import * as Dispatch from "../../orchestration-v2/Orchestrator.ts";
import * as ProviderRegistry from "../../provider/ProviderRegistry.ts";
import * as ProviderAdapters from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Projects from "../../project/ProjectService.ts";
import * as Secrets from "../../secrets/SecretRequests.ts";
import * as RemoteDelegation from "../../peer/RemoteDelegation.ts";
import * as ScheduledTasks from "../../scheduledTasks/ScheduledTaskService.ts";
import * as ThreadLaunch from "../../orchestration-v2/ThreadLaunchService.ts";
import * as GitDriver from "../../vcs/GitVcsDriver.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as PeerLinks from "../../peer/PeerLinks.ts";
import * as Forwarding from "../../peer/PeerForwarding.ts";
import * as Peers from "../../peer/PeerMcpClient.ts";
import * as Links from "./LinkedThreads.ts";
import * as Launch from "./launchTransport.ts";
import * as Handoff from "./handoff.ts";
import migration from "../persistence/Migrations/122_JonesThreadLinks.ts";
import {
  encodeLinkedMessageOrigin,
  withLinkedMessageOrigin,
} from "../peerTransport/LinkedMessageOrigin.ts";
import { handoffRequestKey, linkedThreadIdentity } from "./identity.ts";
import {
  ThreadHandoffTool,
  threadHandoffHandler,
  ThreadLinkTool,
  threadLinkHandler,
} from "./tools.ts";

const here = EnvironmentId.make("here"),
  origin = ThreadId.make("origin"),
  target = ThreadId.make("target");
const decodeLinkFailure = Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestratorMcpFailure));
const decodeLinkResult = Schema.decodeUnknownEffect(Schema.fromJsonString(ThreadLinkToolResult));
const decodeFailure = Schema.decodeUnknownSync(OrchestratorMcpFailure);
const encodeFailure = Schema.encodeSync(OrchestratorMcpFailure);
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
    forwarding?: Forwarding.PeerForwarding["Service"]["call"];
    links?: Links.LinkedThreads["Service"];
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
    Layer.mock(Forwarding.PeerForwarding)(
      options.forwarding === undefined ? {} : { call: options.forwarding },
    ),
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
    options.links === undefined
      ? Layer.mock(Links.LinkedThreads)({
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
        })
      : Layer.succeed(Links.LinkedThreads, options.links),
  );
  return {
    messages,
    launchKeys,
    launchTexts,
    linkIds,
    layer: Handoff.layer.pipe(Layer.provideMerge(services)),
  };
}

it.effect(
  "a forwarded handoff retry preserves the receiver tombstone and manual relink remains possible",
  () => {
    const away = EnvironmentId.make("away");
    const from = { environmentId: here, threadId: origin };
    const to = { environmentId: away, threadId: target };
    const threads = Layer.mock(Threads.ThreadManagementService)({
      getThreadShell: (id) => Effect.succeed(liveThreadShell(id)),
    });
    const linkedLayer = (environmentId: EnvironmentId) =>
      Links.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            threads,
            Layer.succeed(Environment.ServerEnvironment, {
              getEnvironmentId: Effect.succeed(environmentId),
              getDescriptor: Effect.die("unused"),
            }),
            Layer.effectDiscard(
              Effect.gen(function* () {
                yield* migration;
                const sql = yield* SqlClient.SqlClient;
                yield* sql`CREATE TABLE orchestration_v2_projection_threads (thread_id TEXT PRIMARY KEY, deleted_at TEXT)`;
                yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES (${origin}, NULL), (${target}, NULL)`;
              }),
            ).pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" }))),
          ),
        ),
      );
    return Effect.gen(function* () {
      const source = Context.get(yield* Layer.build(linkedLayer(here)), Links.LinkedThreads);
      const receiver = Context.get(yield* Layer.build(linkedLayer(away)), Links.LinkedThreads);
      let remoteScope: Invocation.McpInvocationScope = {
        ...scope,
        environmentId: away,
        thread: undefined,
        client: {
          sessionId: "peer",
          label: "Laptop",
          access: "full-access",
          linked: true,
        },
      };
      const toolkit = Toolkit.make(ThreadLinkTool);
      const receiverContext = yield* Layer.build(
        McpHttpServer.toolkitRegistration(
          toolkit,
          toLayer(toolkit, { t3_thread_link: threadLinkHandler }),
        ).pipe(
          Layer.provideMerge(McpServer.McpServer.layer),
          Layer.provide(
            Layer.mergeAll(
              threads,
              Layer.succeed(Links.LinkedThreads, receiver),
              Layer.mock(Orchestrator.OrchestratorMcpService)({}),
              Layer.mock(Forwarding.PeerForwarding)({}),
            ),
          ),
        ),
      );
      const server = Context.get(receiverContext, McpServer.McpServer);
      const client = McpSchema.McpServerClient.of({
        clientId: 1,
        protocolVersion: "2025-06-18",
        clientCapabilities: {},
        clientInfo: { name: "handoff-mirror", version: "1" },
        initializePayload: {
          protocolVersion: "2025-06-18",
          capabilities: {},
          clientInfo: { name: "handoff-mirror", version: "1" },
        },
        getClient: Effect.die("unused"),
      });
      const invoke = (args: unknown) =>
        server
          .callTool({ name: "t3_thread_link", arguments: args as Record<string, unknown> })
          .pipe(
            Effect.provideService(Invocation.McpInvocationContext, remoteScope),
            Effect.provideService(McpSchema.McpServerClient, client),
            Effect.provide(receiverContext),
            Effect.flatMap((response) => {
              const text = response.content
                .map((part) => (part.type === "text" ? part.text : ""))
                .join("");
              return response.isError
                ? decodeLinkFailure(text).pipe(Effect.flatMap(Effect.fail))
                : decodeLinkResult(text);
            }),
          );
      const payloads: Record<string, unknown>[] = [];
      const forwarding = Context.get(
        yield* Layer.build(
          Forwarding.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                NodeServices.layer,
                threads,
                Layer.mock(Projects.ProjectService)({
                  getById: () => Effect.succeed(Option.none()),
                }),
                Layer.mock(PeerLinks.PeerLinks)({ get: () => Effect.succeed(Option.none()) }),
                Layer.mock(Peers.PeerMcpClient)({
                  call: (request) => {
                    payloads.push(request.arguments);
                    expect(request.origin?.thread?.threadId).toBe(origin);
                    remoteScope = withLinkedMessageOrigin(
                      remoteScope,
                      encodeLinkedMessageOrigin(request.origin!),
                    )!;
                    return invoke(request.arguments) as never;
                  },
                }),
              ),
            ),
          ),
        ),
        Forwarding.PeerForwarding,
      );
      const f = fixture(undefined, {
        links: source,
        forwarding: (caller, tool, environmentId, request) =>
          tool.name === "t3_thread_link"
            ? forwarding.call(caller, tool, environmentId, request)
            : Effect.succeed({ delivery: "queued" } as never),
      });
      const handoff = (yield* Layer.build(f.layer)).pipe(Context.get(Handoff.ThreadHandoff));
      const request = {
        ...input,
        to: { type: "existing" as const, environmentId: away, threadId: target },
      };
      const first = yield* handoff.execute(request, gate).pipe(Effect.provide(f.layer));
      expect(first.mirror.state).toBe("recorded");
      expect((yield* receiver.list({ type: "user" }, { thread: to })).links).toHaveLength(1);
      yield* receiver.unlink({ type: "user" }, { linkId: first.link!.linkId });
      expect((yield* source.list({ type: "user" }, { thread: from })).links).toHaveLength(1);
      const retry = yield* handoff.execute(request, gate).pipe(Effect.provide(f.layer));
      expect((yield* receiver.list({ type: "user" }, { thread: to })).links).toEqual([]);
      expect(payloads[1]).toMatchObject({ action: "link", basis: "handoff", from, to });
      expect(retry.mirror.state).toBe("unavailable");
      const manual = yield* invoke({ action: "link", kind: "continuation", from, to });
      expect(manual.outcome).toBe("recorded");
      expect((yield* receiver.list({ type: "user" }, { thread: to })).links).toHaveLength(1);
      yield* receiver.unlink({ type: "user" }, { linkId: manual.link!.linkId });
      const authenticated = remoteScope.client!;
      for (const client of [
        { ...authenticated, remoteOrigin: undefined },
        {
          ...authenticated,
          remoteOrigin: { ...authenticated.remoteOrigin!, linkSessionId: "forged" },
        },
        {
          ...authenticated,
          remoteOrigin: { ...authenticated.remoteOrigin!, thread: { threadId: "other" } },
        },
        { ...authenticated, linked: false },
      ]) {
        remoteScope = { ...remoteScope, client };
        expect(
          yield* invoke({ action: "link", kind: "continuation", basis: "handoff", from, to }).pipe(
            Effect.flip,
          ),
        ).toMatchObject({ _tag: "OrchestratorMcpFailure", code: "capability_denied" });
      }
      expect((yield* receiver.list({ type: "user" }, { thread: to })).links).toEqual([]);
    }).pipe(Effect.scoped);
  },
);
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
        message: "Transport outcome is unknown: route not found.",
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

it.effect("preserves production blocked-send refusal locally and through forwarding", () => {
  const away = EnvironmentId.make("away");
  const blocked = new Dispatch.OrchestratorThreadMessagesBlockedError({
    commandId: CommandId.make("blocked"),
    threadId: target,
  });
  const ambiguous = new Dispatch.OrchestratorDispatchError({
    commandId: CommandId.make("unknown"),
    commandType: "message.dispatch",
    cause: new Error("Connection closed"),
  });
  const check = (error: typeof blocked | typeof ambiguous, remote: boolean) => {
    const threads = Layer.mock(Threads.ThreadManagementService)({
      getThreadShell: (id) => Effect.succeed(liveThreadShell(id)),
      getThreadRecords: (id) => Effect.succeed(idleThreadProjection(liveThreadShell(id))),
      getProjectThreadRecords: ({ threadId }) =>
        Effect.succeed(idleThreadProjection(liveThreadShell(threadId))),
      sendToThread: () => Effect.fail(error),
    });
    const mcp = Orchestrator.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          NodeServices.layer,
          threads,
          ServerSettings.layerTest(),
          Layer.mock(ProviderRegistry.ProviderRegistry)({}),
          Layer.mock(ProviderAdapters.ProviderAdapterRegistryV2)({}),
          Layer.mock(Projects.ProjectService)({}),
          Layer.mock(Secrets.SecretRequests)({}),
          Layer.mock(RemoteDelegation.RemoteDelegation)({}),
          Layer.mock(ScheduledTasks.ScheduledTaskService)({}),
          Layer.mock(ThreadLaunch.ThreadLaunchService)({}),
          Layer.mock(GitDriver.GitVcsDriver)({}),
        ),
      ),
    );
    return Effect.gen(function* () {
      const service = yield* Orchestrator.OrchestratorMcpService;
      const forwarding = yield* Forwarding.PeerForwarding;
      const f = fixture(undefined, {
        send: service.sendToThread,
        forwarding: forwarding.call,
      });
      const result = yield* Effect.flatMap(Handoff.ThreadHandoff, (handoff) =>
        handoff.execute(
          { ...input, to: { ...input.to, ...(remote ? { environmentId: away } : {}) } },
          gate,
        ),
      ).pipe(Effect.provide(f.layer));
      expect(result.delivery).toBe(error === blocked ? "rejected" : "unknown");
      expect(f.linkIds.size).toBe(error === blocked ? 0 : 1);
      expect(result.link?.acceptance ?? null).toBe(error === blocked ? null : "offered");
    }).pipe(
      Effect.provide(
        Forwarding.layer.pipe(
          Layer.provideMerge(
            Layer.mergeAll(
              NodeServices.layer,
              threads,
              Layer.mock(Projects.ProjectService)({ getById: () => Effect.succeed(Option.none()) }),
              Layer.mock(PeerLinks.PeerLinks)({ get: () => Effect.succeed(Option.none()) }),
              Layer.mock(Peers.PeerMcpClient)({
                call: (request) =>
                  request.tool === "t3_thread_read"
                    ? Effect.succeed({
                        remoteMessagePolicy: {
                          schemaVersion: "t3.linked-message-policy/v1",
                          authenticatedOrigin: true,
                          incomingAcceptance: true,
                        },
                      } as never)
                    : request.tool === "t3_thread_send"
                      ? (Effect.gen(function* () {
                          const service = yield* Orchestrator.OrchestratorMcpService;
                          return yield* service
                            .sendToThread(
                              {
                                ...scope,
                                environmentId: away,
                                thread: undefined,
                                client: {
                                  sessionId: "peer",
                                  label: "Peer",
                                  access: "full-access",
                                  linked: true,
                                },
                              },
                              { threadId: target, message: "handoff" },
                            )
                            .pipe(
                              Effect.mapError((failure) => decodeFailure(encodeFailure(failure))),
                            );
                        }).pipe(Effect.provide(mcp)) as never)
                      : Effect.fail(
                          new OrchestratorMcpFailure({
                            code: "orchestration_error",
                            message: "Connection closed",
                          }),
                        ),
              }),
            ),
          ),
          Layer.provideMerge(mcp),
        ),
      ),
    );
  };
  return check(blocked, false).pipe(
    Effect.andThen(check(blocked, true)),
    Effect.andThen(check(ambiguous, false)),
    Effect.andThen(check(ambiguous, true)),
  );
});

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
