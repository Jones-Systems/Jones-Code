import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Stream from "effect/Stream";
import * as ControlPlane from "../../../../workstreams/ControlPlaneWorkstreamTransport.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Access from "../../../../mcp/McpToolAccess.ts";
import * as Threads from "../../../../orchestration-v2/ThreadManagementService.ts";
import * as Handlers from "../../../mcp/workstreams/handlers.ts";
import * as Tools from "../../../mcp/workstreams/tools.ts";
import {
  Effect,
  Layer,
  Wire,
  Operations,
  Gateway,
  Startup,
  fresh,
  seed,
  app,
  harness,
  http,
  json,
  createOperation,
  registry,
  enrollmentRequest,
} from "./testkit.ts";

it.effect(
  "cpDenied guards production selection constructor and global fetch through raw operations authenticated HTTP and MCP",
  () => {
    const f = fresh();
    return Effect.gen(function* () {
      const constructor = yield* Effect.acquireRelease(
        Effect.sync(() =>
          vi.spyOn(ControlPlane, "makeControlPlaneWorkstreamTransport").mockImplementation(() => {
            throw new Error("Control Plane transport construction forbidden");
          }),
        ),
        (spy) => Effect.sync(() => spy.mockRestore()),
      );
      const fetch = yield* Effect.acquireRelease(
        Effect.sync(() =>
          vi
            .spyOn(globalThis, "fetch")
            .mockRejectedValue(
              new Error("All global fetch egress forbidden in this in-process qualification"),
            ),
        ),
        (spy) => Effect.sync(() => spy.mockRestore()),
      );
      const prepared = yield* seed(f);
      const selected = Startup.chooseLayer({
        legacy: () => {
          ControlPlane.makeControlPlaneWorkstreamTransport({
            state: "enabled",
            value: {
              baseUrl: new URL("https://192.0.2.1:1"),
              ownerId: registry.owner_id,
              principalId: enrollmentRequest.context.principal_id,
              authorizationRevision: 1,
              keyId: "synthetic-only",
              signingSecret: "synthetic-only-never-used",
            },
          });
          return prepared.services;
        },
        home: () => prepared.services,
        unavailable: () => prepared.services,
      }).pipe(Layer.provide(Layer.succeed(Startup.Startup, prepared.selection)));
      yield* Effect.gen(function* () {
        const h = yield* harness;
        yield* app(h.create("CP-denied raw"));
        const operations = yield* Operations.WorkstreamOperations;
        expect(
          (yield* app(
            operations.submit({
              request: createOperation("cp-denied-operation-0001"),
              wait_ms: 3000,
            }),
          )).state,
        ).toBe("committed");
        const web = yield* http;
        const command = yield* app(
          h.command({
            operation: "create_workstream",
            name: "CP-denied HTTP raw",
            lifecycle: "active",
            progress: { state: "unknown" },
            sort_order: 0,
          }),
        );
        expect((yield* web.request("/commands", { command })).status).toBe(200);
        expect((yield* web.request("/commands/" + command.command_id)).status).toBe(200);
        const httpOp = createOperation("cp-denied-http-operation-01");
        expect(
          yield* json(yield* web.request("/operations/submit", { request: httpOp, wait_ms: 3000 })),
        ).toMatchObject({ state: "committed" });
        expect(
          (yield* web.request("/operations/read", { operation_id: httpOp.operation_id })).status,
        ).toBe(200);
        const built = yield* Tools.WorkstreamToolkit.pipe(
          Effect.provide(
            Access.HandlersLayer.layer(Handlers.handlers).pipe(
              Layer.provide(Handlers.backendLayer),
            ),
          ),
        );
        const invocation: Mcp.McpInvocationScope = {
          environmentId: Wire.EnvironmentId.make(registry.home_environment_id),
          issuedAt: 0,
          requestNamespace: "cp-denied-mcp",
          capabilities: new Set(["orchestration"]),
          thread: undefined,
          client: { sessionId: "startup-mcp", label: "synthetic", access: "full-access" },
        };
        const mcpRequest = createOperation("cp-denied-mcp-operation-01", { kind: "agent" });
        const result = yield* built
          .handle("t3_workstream_create", { request: mcpRequest, wait_ms: 3000 })
          .pipe(
            Stream.unwrap,
            Stream.runCollect,
            Effect.provideService(Mcp.McpInvocationContext, invocation),
            Effect.provide(
              Layer.mock(Threads.ThreadManagementService)({
                getThreadShell: () => Effect.succeed(null),
              }),
            ),
          );
        expect(result.at(-1)?.result).toMatchObject({ state: "committed" });
        expect((yield* app((yield* Gateway.WorkstreamGateway).readMetadata())).items).toHaveLength(
          5,
        );
        expect(constructor).not.toHaveBeenCalled();
        expect(fetch).not.toHaveBeenCalled();
        expect(prepared.provider.calls).toHaveLength(0);
      }).pipe(Effect.provide(selected));
    }).pipe(Effect.provide(f.layer));
  },
);
