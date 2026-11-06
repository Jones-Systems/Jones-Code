import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";

vi.mock("../../../../../package.json", () => ({
  default: {
    version: "0.0.0-preview.20261002.100",
    jonesSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
    },
  },
}));

import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as OperationContract from "@t3tools/contracts/jones/workstreamOperations";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Access from "../../../../mcp/McpToolAccess.ts";
import * as Threads from "../../../../orchestration-v2/ThreadManagementService.ts";
import { liveThreadShell } from "../../../../mcp/McpToolAccess.testkit.ts";
import * as Handlers from "../../../mcp/workstreams/handlers.ts";
import * as Tools from "../../../mcp/workstreams/tools.ts";
import * as Cli from "../../operations/cli.ts";
import {
  Effect,
  Layer,
  Wire,
  Gateway,
  Operations,
  OperationStore,
  fresh,
  seed,
  registry,
  principal,
  createOperation,
  assignOperation,
} from "./testkit.ts";

const decodeOperationResult = Schema.decodeUnknownEffect(
  OperationContract.WorkstreamOperationResult,
);
const decodeCreateParameters = Schema.decodeUnknownEffect(
  Tools.WorkstreamToolkit.tools.t3_workstream_create.parametersSchema,
);

const invocation: Mcp.McpInvocationScope = {
  environmentId: Wire.EnvironmentId.make(registry.home_environment_id),
  issuedAt: 0,
  requestNamespace: "ingress-synthetic-mcp",
  thread: undefined,
  capabilities: new Set(["orchestration"]),
  client: { sessionId: "startup-mcp", label: "synthetic agent", access: "full-access" },
};
const toolkit = Tools.WorkstreamToolkit.pipe(
  Effect.provide(
    Access.HandlersLayer.layer(Handlers.handlers).pipe(Layer.provide(Handlers.backendLayer)),
  ),
);
const threads = Layer.mock(Threads.ThreadManagementService)({
  getThreadShell: (id) => Effect.succeed(liveThreadShell(id)),
});

it.effect(
  "real MCP toolkit handlers create list status and enforce native caller capability ceilings",
  () => {
    const f = fresh();
    return Effect.gen(function* () {
      const prepared = yield* seed(f);
      yield* Effect.gen(function* () {
        const built = yield* toolkit;
        const request = createOperation("mcp-ingress-create-000001", { kind: "agent" });
        const result = yield* built
          .handle("t3_workstream_create", { request, wait_ms: 3000 })
          .pipe(Stream.unwrap, Stream.runCollect);
        expect(result.at(-1)?.result).toMatchObject({ state: "committed" });
        const listing = yield* built
          .handle("t3_workstreams_list", { limit: 100 })
          .pipe(Stream.unwrap, Stream.runCollect);
        expect(listing.at(-1)?.result).toMatchObject({
          source: "live",
          stale: false,
          items: [expect.objectContaining({ name: "Synthetic " + request.operation_id })],
        });
        const status = yield* built
          .handle("t3_workstream_operation_status", { operation_id: request.operation_id })
          .pipe(Stream.unwrap, Stream.runCollect);
        expect(status.at(-1)?.result).toMatchObject({
          operation: { operation_id: request.operation_id, state: "committed" },
        });
        const restricted = {
          ...invocation,
          client: { ...invocation.client!, access: "read-only" as const },
        };
        const blocked = createOperation("mcp-ingress-restricted-001", { kind: "agent" });
        const denied = yield* built
          .handle("t3_workstream_create", { request: blocked, wait_ms: 0 })
          .pipe(
            Stream.unwrap,
            Stream.runCollect,
            Effect.provideService(Mcp.McpInvocationContext, restricted),
            Effect.result,
          );
        expect(denied._tag).toBe("Success");
        if (denied._tag === "Success")
          expect(JSON.stringify(denied.success)).toContain("capability_denied");
        expect(yield* (yield* OperationStore.OperationStore).read(blocked.operation_id)).toBeNull();
        const missingCapability = yield* built.handle("t3_workstreams_list", {}).pipe(
          Stream.unwrap,
          Stream.runCollect,
          Effect.provideService(Mcp.McpInvocationContext, {
            ...invocation,
            capabilities: new Set<never>(),
          }),
          Effect.result,
        );
        expect(missingCapability._tag).toBe("Success");
        if (missingCapability._tag === "Success")
          expect(JSON.stringify(missingCapability.success)).toContain(
            "McpCapabilityUnavailableError",
          );
        expect(prepared.provider.calls).toHaveLength(0);
      }).pipe(
        Effect.provideService(Mcp.McpInvocationContext, invocation),
        Effect.provide(Layer.merge(threads, prepared.services)),
      );
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

it.effect("real CLI dispatcher reaches native operation service and reads retained status", () => {
  const f = fresh();
  return Effect.gen(function* () {
    const prepared = yield* seed(f);
    yield* Effect.gen(function* () {
      const gateway = yield* Gateway.WorkstreamGateway,
        operations = yield* Operations.WorkstreamOperations;
      const failure = () => new Cli.WorkstreamCliError({ code: "request_failed" });
      const port: Cli.WorkstreamCliPort = {
        list: (input) => gateway.readMetadata(input).pipe(Effect.mapError(failure)),
        submit: (input) => operations.submit(input).pipe(Effect.mapError(failure)),
        read: (input) => operations.read(input),
        setupStatus: () => operations.setupStatus(),
      };
      const request = createOperation("cli-ingress-create-000001", { kind: "cli" });
      const created = yield* Cli.dispatchWorkstreamCli(port, {
        command: "create",
        request,
        wait_ms: 3000,
      });
      expect(created).toMatchObject({ state: "committed" });
      expect(
        yield* Cli.dispatchWorkstreamCli(port, {
          command: "status",
          query: { operation_id: request.operation_id },
        }),
      ).toMatchObject({ operation: created });
      expect(yield* Cli.dispatchWorkstreamCli(port, { command: "list" })).toMatchObject({
        items: [expect.objectContaining({ name: "Synthetic " + request.operation_id })],
      });
      expect(prepared.provider.calls).toHaveLength(0);
    }).pipe(
      Effect.provideService(Wire.EnvironmentAuthenticatedPrincipal, {
        ...principal,
        sessionId: Wire.AuthSessionId.make("startup-cli"),
        subject: "cli-issued-session",
      }),
      Effect.provide(prepared.services),
    );
  }).pipe(Effect.scoped, Effect.provide(f.layer));
});

it.effect("all six named-home tools use the same authenticated native application services", () => {
  const f = fresh();
  return Effect.gen(function* () {
    const prepared = yield* seed(f);
    yield* Effect.gen(function* () {
      const built = yield* toolkit;
      const environmentId = invocation.environmentId;
      const create = createOperation("mcp-named-home-create-0001", { kind: "agent" });
      const created = yield* built
        .handle("t3_workstream_create", {
          environmentId,
          request: create,
          wait_ms: 3000,
        })
        .pipe(Stream.unwrap, Stream.runCollect);
      const creation = yield* decodeOperationResult(created.at(-1)?.result);
      expect(creation.state).toBe("committed");
      if (creation.state !== "committed" || creation.result.workstream_id === null)
        return yield* Effect.die("Expected committed native creation");
      const assign = assignOperation(
        "mcp-named-home-assignment-0001",
        creation.result.workstream_id,
        { kind: "agent" },
      );
      const assigned = yield* built
        .handle("t3_thread_workstream_set", {
          environmentId,
          request: assign,
          wait_ms: 3000,
        })
        .pipe(Stream.unwrap, Stream.runCollect);
      const placement = yield* decodeOperationResult(assigned.at(-1)?.result);
      expect(placement.state).toBe("committed");
      const read = yield* built
        .handle("t3_thread_workstream_read", {
          environmentId,
          thread: assign.thread,
        })
        .pipe(Stream.unwrap, Stream.runCollect);
      expect(read.at(-1)?.result).toMatchObject({
        assignment: { kind: "primary", workstream_id: creation.result.workstream_id },
      });
      const list = yield* built
        .handle("t3_workstreams_list", {
          environmentId,
          limit: 100,
        })
        .pipe(Stream.unwrap, Stream.runCollect);
      expect(list.at(-1)?.result).toMatchObject({
        items: [expect.objectContaining({ name: create.workstream.name })],
        source: "live",
        stale: false,
      });
      const status = yield* built
        .handle("t3_workstream_operation_status", {
          environmentId,
          operation_id: assign.operation_id,
        })
        .pipe(Stream.unwrap, Stream.runCollect);
      expect(status.at(-1)?.result).toEqual({ operation: placement });
      const setup = yield* built
        .handle("t3_workstreams_status", {
          environmentId,
        })
        .pipe(Stream.unwrap, Stream.runCollect);
      expect(setup.at(-1)?.result).toMatchObject({
        registry: { registry_id: registry.registry_instance_id, owner_id: registry.owner_id },
      });
      const operations = yield* Operations.WorkstreamOperations;
      expect(yield* operations.read({ operation_id: assign.operation_id })).toEqual({
        operation: placement,
      });
      expect(yield* operations.submit({ request: assign, wait_ms: 0 })).toEqual(placement);
      // A tool caller cannot manufacture trusted queue origin by changing its body.
      const forgedId = "mcp-named-home-forged-queue-01";
      const forged = yield* decodeCreateParameters({
        environmentId,
        request: { ...create, operation_id: forgedId, origin: { kind: "queue" } },
        wait_ms: 0,
      }).pipe(Effect.result);
      expect(forged._tag).toBe("Failure");
      expect(yield* (yield* OperationStore.OperationStore).read(forgedId)).toBeNull();
      expect(prepared.provider.calls).toHaveLength(0);
    }).pipe(
      Effect.provideService(Mcp.McpInvocationContext, {
        ...invocation,
        requestNamespace: "authenticated-original-mcp-session",
      }),
      Effect.provide(Layer.merge(threads, prepared.services)),
    );
  }).pipe(Effect.scoped, Effect.provide(f.layer));
});
