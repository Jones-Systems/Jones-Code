import { expect, it } from "@effect/vitest";
import * as Stream from "effect/Stream";
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
} from "./testkit.ts";

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
    }).pipe(Effect.provide(f.layer));
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
  }).pipe(Effect.provide(f.layer));
});
