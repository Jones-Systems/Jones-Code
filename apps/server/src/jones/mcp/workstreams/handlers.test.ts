import { expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  EnvironmentId,
  OrchestratorMcpEnvironmentTarget,
  OrchestratorMcpFailure,
  ThreadId,
  WORKSTREAM_CONTRACT_HEADER_VERSION,
  WORKSTREAM_CONTRACT_MANIFEST_SHA256,
  type T3WorkstreamListResult,
} from "@t3tools/contracts";
import * as Operations from "@t3tools/contracts/jones/workstreamOperations";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { Tool } from "effect/ai";
import corpus from "../../../../../../packages/contracts/contracts/workstream-operations/v1/fixtures/conformance.json" with { type: "json" };
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import * as Access from "../../../mcp/McpToolAccess.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import * as PeerForwarding from "../../../peer/PeerForwarding.ts";
import * as PeerLinks from "../../../peer/PeerLinks.ts";
import * as PeerMcpClient from "../../../peer/PeerMcpClient.ts";
import { liveThreadShell } from "../../../mcp/McpToolAccess.testkit.ts";
import { handlers } from "./handlers.ts";
import { WorkstreamToolkit, WorkstreamToolsBackend } from "./tools.ts";

const scope: Invocation.McpInvocationScope = {
  environmentId: EnvironmentId.make("env-1"),
  thread: undefined,
  client: { sessionId: "test", label: "test", access: "full-access" },
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
  requestNamespace: "test",
};
const request = Schema.decodeUnknownSync(Operations.WorkstreamSetPrimaryRequest)({
  ...corpus.cases.find((c) => c.name === "set-primary")!.value,
  origin: { kind: "agent" },
});
const isSetupStatus = Schema.is(Operations.WorkstreamSetupStatus);
const isMcpFailure = Schema.is(OrchestratorMcpFailure);
const decodeCreate = Schema.decodeUnknownEffect(Operations.WorkstreamCreateRequest);
const setup = Schema.decodeUnknownSync(Operations.WorkstreamSetupStatus)(
  corpus.cases.find((c) => isSetupStatus(c.value))!.value,
);
const pending: Operations.WorkstreamOperationResult = {
  contract: Operations.WORKSTREAM_OPERATIONS_CONTRACT,
  operation_id: request.operation_id,
  kind: request.kind,
  origin: request.origin,
  request_sha256: "a".repeat(64),
  state: "in_progress",
  reason: null,
  detail: null,
  observed: null,
  result: null,
  children: [],
  poll_after_ms: 100,
  accepted_at: "2026-10-23T00:00:00Z",
  updated_at: "2026-10-23T00:00:00Z",
};
const threads = Layer.mock(ThreadManagementService)({ getThreadShell: () => Effect.succeed(null) });
const listing: T3WorkstreamListResult = {
  binding: {
    registryId: "https://registry.example",
    ownerId: "owner-1",
    principalId: "principal-1",
    authorizationRevision: 1,
    serverGeneration: 1,
    registryVersion: 1,
    permissions: ["workstreams:read", "workstreams:write"],
    contractVersion: WORKSTREAM_CONTRACT_HEADER_VERSION,
    contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
  },
  items: [],
  nextCursor: null,
  source: "live",
  stale: false,
};
const backend = (calls: unknown[]) =>
  Layer.succeed(WorkstreamToolsBackend, {
    list: (input) =>
      Effect.sync(() => {
        calls.push(input);
        return listing;
      }),
    submit: (input) =>
      Effect.sync(() => {
        calls.push(input);
        return { ...pending, operation_id: input.request.operation_id, kind: input.request.kind };
      }),
    read: (input) =>
      Effect.sync(() => {
        calls.push(input);
        return { operation: pending };
      }),
    setupStatus: () => Effect.succeed(setup),
  });
const build = (calls: unknown[]) =>
  WorkstreamToolkit.pipe(
    Effect.provide(Access.HandlersLayer.layer(handlers).pipe(Layer.provide(backend(calls)))),
  );

it("registers all six tools with operation schemas and annotations", () => {
  expect(Object.keys(WorkstreamToolkit.tools).sort()).toEqual([
    "t3_thread_workstream_read",
    "t3_thread_workstream_set",
    "t3_workstream_create",
    "t3_workstream_operation_status",
    "t3_workstreams_list",
    "t3_workstreams_status",
  ]);
  for (const tool of Object.values(WorkstreamToolkit.tools))
    expect(Tool.getJsonSchema(tool)).toMatchObject({ type: "object" });
  const decode = Schema.decodeUnknownSync(
    WorkstreamToolkit.tools.t3_thread_workstream_set.parametersSchema,
  );
  expect(decode({ request, wait_ms: 0 })).toEqual({ request, wait_ms: 0 });
  for (const bad of [
    { ...request, origin: { kind: "cli" } },
    { ...request, operation_id: "short" },
    { ...request, expected: undefined },
    { ...request, extra: true },
  ])
    expect(() => decode({ request: bad, wait_ms: 0 })).toThrow();
  expect(() => decode({ request, wait_ms: 3001 })).toThrow();
  expect(() => decode({ request, wait_ms: 0, extra: true })).toThrow();
  const decodeSetup = Schema.decodeUnknownSync(
    WorkstreamToolkit.tools.t3_workstreams_status.parametersSchema,
  );
  expect(decodeSetup({})).toEqual({});
  for (const invalid of [{ extra: true }, [], null, "status"])
    expect(() => decodeSetup(invalid)).toThrow();
});

it.effect("delegates identical mutation retries unchanged and reads retained status", () =>
  Effect.gen(function* () {
    const calls: unknown[] = [];
    const toolkit = yield* build(calls);
    for (let n = 0; n < 2; n++) {
      const result = yield* toolkit
        .handle("t3_thread_workstream_set", { request, wait_ms: 0 })
        .pipe(Stream.unwrap, Stream.runCollect);
      expect(result.at(-1)?.result).toEqual(pending);
    }
    yield* toolkit
      .handle("t3_workstream_operation_status", { operation_id: request.operation_id })
      .pipe(Stream.unwrap, Stream.runCollect);
    expect(calls).toEqual([
      { request, wait_ms: 0 },
      { request, wait_ms: 0 },
      { operation_id: request.operation_id },
    ]);
  }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope), Effect.provide(threads)),
);

it.effect("refuses a foreign read before delegation", () =>
  Effect.gen(function* () {
    const calls: unknown[] = [];
    const toolkit = yield* build(calls);
    const result = yield* toolkit
      .handle("t3_thread_workstream_read", {
        thread: { environment_id: "foreign", thread_id: "thread-1" },
      })
      .pipe(Stream.unwrap, Stream.runCollect);
    expect(JSON.stringify(result)).toContain("foreign_thread");
    expect(calls).toEqual([]);
  }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope), Effect.provide(threads)),
);

it.effect("read-only credentials cannot mutate", () =>
  Effect.gen(function* () {
    const calls: unknown[] = [];
    const toolkit = yield* build(calls);
    const result = yield* toolkit
      .handle("t3_thread_workstream_set", { request, wait_ms: 0 })
      .pipe(Stream.unwrap, Stream.runCollect);
    expect(JSON.stringify(result)).toContain("capability_denied");
    expect(calls).toEqual([]);
  }).pipe(
    Effect.provideService(Invocation.McpInvocationContext, {
      ...scope,
      client: { ...scope.client!, access: "read-only" },
    }),
    Effect.provide(threads),
  ),
);

it.effect("requires orchestration capability for setup reads", () =>
  Effect.gen(function* () {
    const toolkit = yield* build([]);
    const result = yield* toolkit
      .handle("t3_workstreams_status", {})
      .pipe(Stream.unwrap, Stream.runCollect);
    expect(JSON.stringify(result)).toContain("McpCapabilityUnavailableError");
  }).pipe(
    Effect.provideService(Invocation.McpInvocationContext, {
      ...scope,
      capabilities: new Set<never>(),
    }),
    Effect.provide(threads),
  ),
);

it.effect("delegates list, create, thread read and setup to the backend", () =>
  Effect.gen(function* () {
    const calls: unknown[] = [];
    const toolkit = yield* build(calls);
    const create = yield* decodeCreate({
      contract: Operations.WORKSTREAM_OPERATIONS_CONTRACT,
      operation_id: "create-operation-0001",
      origin: { kind: "agent" },
      kind: "create_workstream",
      registry: { registry_id: setup.registry.registry_id!, owner_id: setup.registry.owner_id! },
      workstream: {
        name: "Release",
        lifecycle: "active",
        progress: { state: "unknown" },
        sort_order: 0,
      },
    });
    const listed = yield* toolkit
      .handle("t3_workstreams_list", { limit: 10 })
      .pipe(Stream.unwrap, Stream.runCollect);
    expect(listed.at(-1)?.result).toEqual(listing);
    yield* toolkit
      .handle("t3_workstream_create", { request: create, wait_ms: 0 })
      .pipe(Stream.unwrap, Stream.runCollect);
    yield* toolkit
      .handle("t3_thread_workstream_read", { thread: request.thread })
      .pipe(Stream.unwrap, Stream.runCollect);
    const status = yield* toolkit
      .handle("t3_workstreams_status", {})
      .pipe(Stream.unwrap, Stream.runCollect);
    expect(status.at(-1)?.result).toEqual(setup);
    expect(calls).toEqual([
      { limit: 10 },
      { request: create, wait_ms: 0 },
      { thread: request.thread },
    ]);
  }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope), Effect.provide(threads)),
);

it.effect("passes a foreign assignment to local qualification without changing its ID", () =>
  Effect.gen(function* () {
    const calls: unknown[] = [];
    const foreign = { ...request, thread: { ...request.thread, environment_id: "foreign" } };
    const rejected: Operations.WorkstreamOperationResult = {
      ...pending,
      state: "rejected",
      reason: "foreign_thread",
      poll_after_ms: null,
    };
    const toolkit = yield* WorkstreamToolkit.pipe(
      Effect.provide(
        Access.HandlersLayer.layer(handlers).pipe(
          Layer.provide(
            Layer.succeed(WorkstreamToolsBackend, {
              list: () => Effect.succeed(listing),
              setupStatus: () => Effect.succeed(setup),
              read: () => Effect.succeed({ operation: null }),
              submit: (input) =>
                Effect.sync(() => {
                  calls.push(input);
                  return rejected;
                }),
            }),
          ),
        ),
      ),
    );
    const output = yield* toolkit
      .handle("t3_thread_workstream_set", { request: foreign, wait_ms: 0 })
      .pipe(Stream.unwrap, Stream.runCollect);
    expect(output.at(-1)?.result).toEqual(rejected);
    expect(calls).toEqual([{ request: foreign, wait_ms: 0 }]);
  }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope), Effect.provide(threads)),
);

type WorkstreamToolName = keyof typeof WorkstreamToolkit.tools;
const remoteEnvironment = EnvironmentId.make("env-remote");
const routedCreate: Operations.WorkstreamCreateRequest = {
  contract: Operations.WORKSTREAM_OPERATIONS_CONTRACT,
  operation_id: "mcp-routed-create-original-0001",
  origin: { kind: "agent" },
  kind: "create_workstream",
  registry: { registry_id: setup.registry.registry_id!, owner_id: setup.registry.owner_id! },
  workstream: {
    name: "Routed Workstream",
    lifecycle: "active",
    progress: { state: "unknown" },
    sort_order: 0,
  },
};
const routedRequest = {
  ...request,
  operation_id: "mcp-routed-assignment-original-0001",
  thread: { ...request.thread, environment_id: remoteEnvironment },
};
const routedInputs: ReadonlyArray<readonly [WorkstreamToolName, Record<string, unknown>]> = [
  ["t3_workstreams_list", { limit: 10, cursor: "original-cursor" }],
  ["t3_workstream_create", { request: routedCreate, wait_ms: 0 }],
  ["t3_thread_workstream_set", { request: routedRequest, wait_ms: 0 }],
  ["t3_thread_workstream_read", { thread: routedRequest.thread }],
  ["t3_workstream_operation_status", { operation_id: routedRequest.operation_id }],
  ["t3_workstream_operation_status", { open: true }],
  ["t3_workstreams_status", {}],
];
const routedParameterDecoders = {
  t3_workstreams_list: Schema.decodeUnknownEffect(
    WorkstreamToolkit.tools.t3_workstreams_list.parametersSchema,
  ),
  t3_workstream_create: Schema.decodeUnknownEffect(
    WorkstreamToolkit.tools.t3_workstream_create.parametersSchema,
  ),
  t3_thread_workstream_set: Schema.decodeUnknownEffect(
    WorkstreamToolkit.tools.t3_thread_workstream_set.parametersSchema,
  ),
  t3_thread_workstream_read: Schema.decodeUnknownEffect(
    WorkstreamToolkit.tools.t3_thread_workstream_read.parametersSchema,
  ),
  t3_workstream_operation_status: Schema.decodeUnknownEffect(
    WorkstreamToolkit.tools.t3_workstream_operation_status.parametersSchema,
  ),
  t3_workstreams_status: Schema.decodeUnknownEffect(
    WorkstreamToolkit.tools.t3_workstreams_status.parametersSchema,
  ),
};
const routedCall = (
  toolkit: Effect.Success<ReturnType<typeof build>>,
  name: WorkstreamToolName,
  args: unknown,
) =>
  Effect.gen(function* () {
    switch (name) {
      case "t3_workstreams_list": {
        const input = yield* routedParameterDecoders.t3_workstreams_list(args);
        return yield* toolkit
          .handle("t3_workstreams_list", input)
          .pipe(Stream.unwrap, Stream.runCollect);
      }
      case "t3_workstream_create": {
        const input = yield* routedParameterDecoders.t3_workstream_create(args);
        return yield* toolkit
          .handle("t3_workstream_create", input)
          .pipe(Stream.unwrap, Stream.runCollect);
      }
      case "t3_thread_workstream_set": {
        const input = yield* routedParameterDecoders.t3_thread_workstream_set(args);
        return yield* toolkit
          .handle("t3_thread_workstream_set", input)
          .pipe(Stream.unwrap, Stream.runCollect);
      }
      case "t3_thread_workstream_read": {
        const input = yield* routedParameterDecoders.t3_thread_workstream_read(args);
        return yield* toolkit
          .handle("t3_thread_workstream_read", input)
          .pipe(Stream.unwrap, Stream.runCollect);
      }
      case "t3_workstream_operation_status": {
        const input = yield* routedParameterDecoders.t3_workstream_operation_status(args);
        return yield* toolkit
          .handle("t3_workstream_operation_status", input)
          .pipe(Stream.unwrap, Stream.runCollect);
      }
      case "t3_workstreams_status": {
        const input = yield* routedParameterDecoders.t3_workstreams_status(args);
        return yield* toolkit
          .handle("t3_workstreams_status", input)
          .pipe(Stream.unwrap, Stream.runCollect);
      }
    }
  });

interface RoutedObservation {
  readonly environmentId: EnvironmentId;
  readonly tool: string;
  readonly arguments: Record<string, unknown>;
  readonly limits: PeerMcpClient.PeerCallLimits;
}
const routingFixture = (callerThreads = threads) =>
  Effect.gen(function* () {
    const localCalls: unknown[] = [];
    const remoteCalls: unknown[] = [];
    const sent: RoutedObservation[] = [];
    const receiving = yield* build(remoteCalls);
    let unavailable = false;
    let receivingScope: Invocation.McpInvocationScope = {
      ...scope,
      environmentId: remoteEnvironment,
      requestNamespace: "original-receiving-session",
      client: {
        sessionId: "remote-link",
        label: "fixture link",
        access: "full-access",
        linked: true,
      },
    };
    const receive = Effect.fn("WorkstreamRoutingFixture.receive")(
      function* (
        input: Omit<Parameters<PeerMcpClient.PeerMcpClient["Service"]["call"]>[0], "success">,
      ) {
        sent.push({
          environmentId: input.environmentId,
          tool: input.tool,
          arguments: structuredClone(input.arguments),
          limits: { ...input.limits },
        });
        if (unavailable)
          return yield* new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: "The original peer is unavailable.",
          });
        const name = routedInputs.find(([name]) => name === input.tool)?.[0];
        if (name === undefined)
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message: "Unexpected peer tool.",
          });
        const output = yield* routedCall(receiving, name, input.arguments).pipe(
          Effect.provideService(Invocation.McpInvocationContext, {
            ...receivingScope,
            client: { ...receivingScope.client!, narrowedTo: input.limits },
          }),
        );
        return output.at(-1)?.result;
      },
      Effect.provide(threads),
      Effect.mapError((error) =>
        isMcpFailure(error)
          ? error
          : new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "The peer refused or returned an invalid result.",
            }),
      ),
    );
    const call: PeerMcpClient.PeerMcpClient["Service"]["call"] = Effect.fn(
      "WorkstreamRoutingFixture.call",
    )((input) => {
      const success: Schema.Decoder<(typeof input.success)["Type"]> = input.success;
      const decode = Schema.decodeUnknownEffect(success);
      return receive(input).pipe(
        Effect.flatMap((result) =>
          decode(result).pipe(
            Effect.mapError(
              () =>
                new OrchestratorMcpFailure({
                  code: "orchestration_error",
                  message: "The peer refused or returned an invalid result.",
                }),
            ),
          ),
        ),
      );
    });
    const forwarding = PeerForwarding.layer.pipe(
      Layer.provide(Layer.succeed(PeerMcpClient.PeerMcpClient, { call })),
      Layer.provide(Layer.mock(PeerLinks.PeerLinks)({})),
      Layer.provide(NodeCrypto.layer),
      Layer.provide(callerThreads),
    );
    const sending = yield* build(localCalls).pipe(Effect.provide(forwarding));
    return {
      localCalls,
      remoteCalls,
      sent,
      sending,
      makeUnavailable: () => {
        unavailable = true;
      },
      makeReceivingReadOnly: () => {
        receivingScope = {
          ...receivingScope,
          client: { ...receivingScope.client!, access: "read-only" },
        };
      },
    };
  });

it("all six closed Workstreams tool schemas expose the standard environment target", () => {
  for (const [name, args] of routedInputs) {
    const tool = WorkstreamToolkit.tools[name];
    expect(tool.parametersSchema.fields.environmentId).toBe(OrchestratorMcpEnvironmentTarget);
    const decode = Schema.decodeUnknownSync(tool.parametersSchema);
    expect(decode({ ...args, environmentId: remoteEnvironment })).toEqual({
      ...args,
      environmentId: remoteEnvironment,
    });
    expect(decode(args)).toEqual(args);
    for (const invalid of [
      { ...args, environmentId: 1 },
      { ...args, environmentId: null },
      { ...args, environmentId: remoteEnvironment, unexpected: true },
    ])
      expect(() => decode(invalid)).toThrow();
  }
});

it.effect("routes all six Workstreams tools once with unchanged intent to the named server", () =>
  Effect.gen(function* () {
    const f = yield* routingFixture();
    for (const [name, args] of routedInputs) {
      const original = structuredClone(args);
      yield* routedCall(f.sending, name, { ...args, environmentId: remoteEnvironment });
      expect(args).toEqual(original);
    }
    expect(f.localCalls).toEqual([]);
    expect(f.sent).toHaveLength(routedInputs.length);
    for (const [index, [name, args]] of routedInputs.entries()) {
      expect(f.sent[index]).toEqual({
        environmentId: remoteEnvironment,
        tool: name,
        arguments: args,
        limits: { runtimeMode: "full-access", interactionMode: "default" },
      });
      expect(f.sent[index]!.arguments).not.toHaveProperty("environmentId");
    }
    expect(f.remoteCalls).toEqual([
      routedInputs[0]![1],
      routedInputs[1]![1],
      routedInputs[2]![1],
      routedInputs[3]![1],
      routedInputs[4]![1],
      routedInputs[5]![1],
    ]);
  }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope), Effect.provide(threads)),
);

it.effect("an explicit local target never forwards or reaches the backend's request payload", () =>
  Effect.gen(function* () {
    const f = yield* routingFixture();
    yield* routedCall(f.sending, "t3_workstreams_list", {
      environmentId: scope.environmentId,
      limit: 3,
    });
    yield* routedCall(f.sending, "t3_workstream_create", {
      environmentId: scope.environmentId,
      request: routedCreate,
      wait_ms: 0,
    });
    yield* routedCall(f.sending, "t3_workstreams_status", { environmentId: scope.environmentId });
    expect(f.sent).toEqual([]);
    expect(f.remoteCalls).toEqual([]);
    expect(f.localCalls).toEqual([{ limit: 3 }, { request: routedCreate, wait_ms: 0 }]);
    const foreign = yield* routedCall(f.sending, "t3_thread_workstream_read", {
      environmentId: scope.environmentId,
      thread: routedRequest.thread,
    });
    expect(JSON.stringify(foreign)).toContain("foreign_thread");
    expect(f.sent).toEqual([]);
  }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope), Effect.provide(threads)),
);

it.effect("read-only callers may route reads but cannot dispatch either mutation", () =>
  Effect.gen(function* () {
    const f = yield* routingFixture();
    yield* routedCall(f.sending, "t3_workstreams_list", { environmentId: remoteEnvironment });
    expect(f.sent[0]?.limits).toEqual({
      runtimeMode: "approval-required",
      interactionMode: "default",
    });
    for (const name of ["t3_workstream_create", "t3_thread_workstream_set"] as const) {
      const args = routedInputs.find(([candidate]) => candidate === name)![1];
      const result = yield* routedCall(f.sending, name, {
        ...args,
        environmentId: remoteEnvironment,
      });
      expect(JSON.stringify(result)).toContain("capability_denied");
    }
    expect(f.sent).toHaveLength(1);
    expect(f.localCalls).toEqual([]);
  }).pipe(
    Effect.provideService(Invocation.McpInvocationContext, {
      ...scope,
      client: { ...scope.client!, access: "read-only" },
    }),
    Effect.provide(threads),
  ),
);

it.effect("fenced linked callers cannot use these tools for a second relay", () =>
  Effect.gen(function* () {
    const f = yield* routingFixture();
    for (const [name, args] of routedInputs) {
      const result = yield* routedCall(f.sending, name, {
        ...args,
        environmentId: remoteEnvironment,
      });
      expect(JSON.stringify(result)).toContain("capability_denied");
    }
    expect(f.sent).toEqual([]);
    expect(f.localCalls).toEqual([]);
    expect(f.remoteCalls).toEqual([]);
  }).pipe(
    Effect.provideService(Invocation.McpInvocationContext, {
      ...scope,
      client: { ...scope.client!, access: "auto", linked: true },
    }),
    Effect.provide(threads),
  ),
);

it.effect("a missing orchestration capability refuses before peer dispatch", () =>
  Effect.gen(function* () {
    const f = yield* routingFixture();
    for (const [name, args] of routedInputs) {
      const result = yield* routedCall(f.sending, name, {
        ...args,
        environmentId: remoteEnvironment,
      });
      expect(JSON.stringify(result)).toContain("capability_denied");
    }
    expect(f.sent).toEqual([]);
    expect(f.localCalls).toEqual([]);
  }).pipe(
    Effect.provideService(Invocation.McpInvocationContext, {
      ...scope,
      capabilities: new Set<never>(),
    }),
    Effect.provide(threads),
  ),
);

it.effect(
  "peer failure neither retries nor falls back; an explicit retry retains the original operation",
  () =>
    Effect.gen(function* () {
      const f = yield* routingFixture();
      f.makeUnavailable();
      for (let attempt = 0; attempt < 2; attempt++) {
        const output = yield* routedCall(f.sending, "t3_workstream_create", {
          environmentId: remoteEnvironment,
          request: routedCreate,
          wait_ms: 0,
        });
        expect(JSON.stringify(output)).toContain("The original peer is unavailable");
        expect(f.sent).toHaveLength(attempt + 1);
      }
      expect(f.sent[0]).toEqual(f.sent[1]);
      expect(f.localCalls).toEqual([]);
      expect(f.remoteCalls).toEqual([]);
    }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope), Effect.provide(threads)),
);

it.effect("receiving access refusal cannot cause a local mutation or a replacement dispatch", () =>
  Effect.gen(function* () {
    const f = yield* routingFixture();
    f.makeReceivingReadOnly();
    const output = yield* routedCall(f.sending, "t3_workstream_create", {
      environmentId: remoteEnvironment,
      request: routedCreate,
      wait_ms: 0,
    });
    expect(JSON.stringify(output)).toContain("peer refused");
    expect(f.sent).toHaveLength(1);
    expect(f.remoteCalls).toEqual([]);
    expect(f.localCalls).toEqual([]);
  }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope), Effect.provide(threads)),
);

it.effect("provider-run callers forward their original thread mode ceiling", () =>
  Effect.gen(function* () {
    const shell = liveThreadShell(ThreadId.make("scheduled-caller"), {
      runtimeMode: "auto-accept-edits",
      interactionMode: "plan",
    });
    const callerThreads = Layer.mock(ThreadManagementService)({
      getThreadShell: (id) => Effect.succeed(id === shell.id ? shell : null),
    });
    const original: Invocation.McpInvocationScope = {
      ...scope,
      client: undefined,
      thread: {
        threadId: shell.id,
        providerInstanceId: shell.providerInstanceId,
        providerSessionId: "original-scheduled-run-credential",
      },
      requestNamespace: "scheduled-run-original-namespace",
    };
    const f = yield* routingFixture(callerThreads);
    yield* routedCall(f.sending, "t3_workstreams_status", {
      environmentId: remoteEnvironment,
    }).pipe(Effect.provideService(Invocation.McpInvocationContext, original));
    expect(f.sent).toHaveLength(1);
    expect(f.sent[0]?.limits).toEqual({
      runtimeMode: "auto-accept-edits",
      interactionMode: "plan",
    });
    expect(f.sent[0]?.arguments).toEqual({});
    expect(f.localCalls).toEqual([]);
    expect(original.requestNamespace).toBe("scheduled-run-original-namespace");
  }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope), Effect.provide(threads)),
);
