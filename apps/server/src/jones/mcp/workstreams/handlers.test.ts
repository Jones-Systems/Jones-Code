import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
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
import corpus from "../../../../../../packages/contracts/contracts/workstream-operations/v1/fixtures/conformance.json";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import * as Access from "../../../mcp/McpToolAccess.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
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
const setup = Schema.decodeUnknownSync(Operations.WorkstreamSetupStatus)(
  corpus.cases.find((c) => Schema.is(Operations.WorkstreamSetupStatus)(c.value))!.value,
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
    Effect.provideService(Invocation.McpInvocationContext, { ...scope, capabilities: new Set() }),
    Effect.provide(threads),
  ),
);

it.effect("reports unsupported without an operation adapter", () =>
  Effect.gen(function* () {
    const toolkit = yield* WorkstreamToolkit.pipe(
      Effect.provide(Access.HandlersLayer.layer(handlers)),
    );
    const result = yield* toolkit
      .handle("t3_workstreams_status", {})
      .pipe(Stream.unwrap, Stream.runCollect);
    expect(JSON.stringify(result)).toContain("unsupported");
  }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope), Effect.provide(threads)),
);

it.effect("delegates list, create, thread read and setup to the backend", () =>
  Effect.gen(function* () {
    const calls: unknown[] = [];
    const toolkit = yield* build(calls);
    const create = Schema.decodeUnknownSync(Operations.WorkstreamCreateRequest)({
      contract: Operations.WORKSTREAM_OPERATIONS_CONTRACT,
      operation_id: "create-operation-0001",
      origin: { kind: "agent" },
      kind: "create_workstream",
      registry: { registry_id: "https://registry.example", owner_id: "owner-1" },
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
