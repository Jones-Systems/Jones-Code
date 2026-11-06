import { expect, it } from "@effect/vitest";
import * as C from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { McpSchema, McpServer, Toolkit } from "effect/ai";
import * as McpHttpServer from "../../../mcp/McpHttpServer.ts";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import * as Access from "../../../mcp/McpToolAccess.ts";
import { liveThreadShell } from "../../../mcp/McpToolAccess.testkit.ts";
import * as Threads from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Continuity from "./GoalContinuity.ts";
import { GoalContinuityTool, goalContinuityHandler } from "./tool.ts";

const threadId = C.ThreadId.make("synthetic-tool-thread");
const owner: C.ContinuityOwner = {
  environmentId: C.EnvironmentId.make("synthetic-environment"),
  threadId,
  providerInstanceId: C.ProviderInstanceId.make("codex"),
  providerThreadId: C.ProviderThreadId.make("synthetic-provider-thread"),
  driver: C.ProviderDriverKind.make("codex"),
  nativeThreadId: "synthetic-native",
};
const mutation: C.ContinuityRequest = {
  action: "detach",
  clientRequestId: "detach-1",
  reason: "Owner inspection",
  expected: { owner, goalId: "goal", revision: "1", generation: "0" },
};
const kit = Toolkit.make(GoalContinuityTool);
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "continuity-tool-test", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "continuity-tool-test", version: "1" },
  },
  getClient: Effect.die("unused"),
});
const scope = (access: "full-access" | "read-only"): Invocation.McpInvocationScope => ({
  environmentId: owner.environmentId,
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
  requestNamespace: "synthetic-owner",
  thread: undefined,
  client: { sessionId: "synthetic-client", label: "Owner", access },
});
const decode = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Union([
      Schema.Struct({ outcome: Schema.String }),
      Schema.Struct({ code: Schema.String }),
    ]),
  ),
);
function harness(refuse = false) {
  const seen: { target?: C.ThreadId; request?: C.ContinuityRequest }[] = [];
  const caller = {} as Continuity.ContinuityCaller;
  const domain = Layer.mock(Continuity.GoalContinuity)({
    context: () => {
      throw new Error("Runtime recovery is outside the tool harness");
    },
    authorize: (target) =>
      Effect.sync(() => {
        seen.push(target === undefined ? {} : { target });
        return caller;
      }),
    execute: (_caller, request) =>
      Effect.sync(() => {
        seen.push({ request });
      }).pipe(
        Effect.andThen(
          refuse
            ? Effect.fail(new C.GoalContinuityError({ reason: "binding_mismatch" }))
            : Effect.succeed({ outcome: "absent" } as const),
        ),
      ),
  });
  const registration = McpHttpServer.toolkitRegistration(
    kit,
    Access.toLayer(kit, { t3_thread_continuity: goalContinuityHandler }),
  ).pipe(
    Layer.provideMerge(McpServer.McpServer.layer),
    Layer.provideMerge(domain),
    Layer.provideMerge(
      Layer.mock(Threads.ThreadManagementService)({
        getThreadShell: () => Effect.succeed(liveThreadShell(threadId)),
      }),
    ),
  );
  const call = (invocation: Invocation.McpInvocationScope, request: C.ContinuityRequest) =>
    McpServer.McpServer.pipe(
      Effect.flatMap((server) =>
        server.callTool({ name: "t3_thread_continuity", arguments: { threadId, request } }),
      ),
      Effect.flatMap(({ content }) =>
        decode(content.map((part) => (part.type === "text" ? part.text : "")).join("")),
      ),
      Effect.map((result) => ("code" in result ? result.code : result.outcome)),
      Effect.provideService(Invocation.McpInvocationContext, invocation),
      Effect.provideService(McpSchema.McpServerClient, client),
      Effect.provide(registration),
    );
  return { call, seen };
}

it.effect.each([
  { action: "read" },
  {
    action: "selection",
    selection: { goalId: "goal", revision: "1", generation: "0", fields: [{ field: "focus" }] },
  },
  { action: "receipt", clientRequestId: "original-request", requestDigest: "a".repeat(64) },
  { action: "delivery_receipt", deliveryId: "original-delivery" },
  {
    action: "discover_deliveries",
    expected: {
      owner,
      goalId: "goal",
      revision: "1",
      generation: "0",
      snapshotDigest: "a".repeat(64),
    },
  },
] satisfies readonly C.ContinuityRequest[])(
  "read-only owner can use the actual tool read operation %o",
  (request) =>
    Effect.gen(function* () {
      const { call, seen } = harness();
      expect(yield* call(scope("read-only"), request)).toBe("absent");
      expect(seen).toEqual([{ target: threadId }, { request }]);
    }),
);

it.effect("actual tool mutations require write access before invoking domain authorization", () =>
  Effect.gen(function* () {
    const { call, seen } = harness();
    expect(yield* call(scope("read-only"), mutation)).toBe("capability_denied");
    expect(seen).toEqual([]);
    expect(yield* call(scope("full-access"), mutation)).toBe("absent");
    expect(seen).toEqual([{ target: threadId }, { request: mutation }]);
  }),
);

it.effect("transport maps typed domain refusal without exposing snapshot or raw cause", () =>
  Effect.gen(function* () {
    const { call } = harness(true);
    expect(yield* call(scope("full-access"), mutation)).toBe("invalid_request");
  }),
);
