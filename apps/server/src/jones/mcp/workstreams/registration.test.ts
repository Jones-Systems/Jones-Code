import { expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { EnvironmentId } from "@t3tools/contracts";
import * as Contract from "@t3tools/contracts/jones/workstreamOperations";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { McpSchema, McpServer } from "effect/ai";
import * as McpHttpServer from "../../../mcp/McpHttpServer.ts";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import { WorkstreamGateway } from "../../../workstreams/WorkstreamGateway.ts";
import { Ports } from "../../workstreams/operations/ports.ts";
import * as Operations from "../../workstreams/operations/service.ts";
import {
  fakeControlPlane,
  environmentId,
  request as fixtureRequest,
} from "../../workstreams/operations/testFixtures.ts";

const registryId = "11111111-1111-4111-8111-111111111111";
const invocation: Invocation.McpInvocationScope = {
  environmentId: EnvironmentId.make(environmentId),
  thread: undefined,
  client: { sessionId: "workstreams-test", label: "test", access: "full-access" },
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
  requestNamespace: "workstreams-test",
};
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  clientCapabilities: {},
  clientInfo: { name: "workstreams-test", version: "1.0.0" },
  protocolVersion: "2025-06-18",
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "workstreams-test", version: "1.0.0" },
  },
  getClient: Effect.die("unused"),
});

it.effect(
  "production MCP registration uses the application operation instance and signed registry identity",
  () => {
    const cp = fakeControlPlane();
    const applicationOperations = Operations.layer.pipe(
      Layer.provide(Layer.succeed(Ports, { ...cp.ports, registryId })),
      Layer.provide(NodeCrypto.layer),
    );
    const registration = McpHttpServer.layerWorkstreamToolkit.pipe(
      Layer.provideMerge(McpServer.McpServer.layer),
      Layer.provide(Layer.mock(WorkstreamGateway)({ readMetadata: cp.ports.gateway.readMetadata })),
      Layer.provideMerge(applicationOperations),
    );
    return Effect.gen(function* () {
      const server = yield* McpServer.McpServer;
      const operations = yield* Operations.WorkstreamOperations;
      const call = (name: string, args: Record<string, unknown>) =>
        server
          .callTool({ name, arguments: args })
          .pipe(
            Effect.provideService(Invocation.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );
      expect(server.tools.map(({ tool }) => tool.name).sort()).toEqual([
        "t3_thread_workstream_read",
        "t3_thread_workstream_set",
        "t3_workstream_create",
        "t3_workstream_operation_status",
        "t3_workstreams_list",
        "t3_workstreams_status",
      ]);
      const setup = Contract.parseWorkstreamSetupStatus(
        (yield* call("t3_workstreams_status", {})).structuredContent,
      );
      expect(setup.registry.registry_id).toBe(registryId);
      expect(setup.registry.registry_id).not.toBe(cp.binding().registryId);
      const request: Contract.WorkstreamSetPrimaryRequest = {
        ...fixtureRequest,
        origin: { kind: "agent" },
        target: {
          ...fixtureRequest.target,
          registry_id: setup.registry.registry_id!,
          owner_id: setup.registry.owner_id!,
        },
      };
      const assigned = yield* call("t3_thread_workstream_set", { request, wait_ms: 3000 });
      expect(assigned.isError).toBe(false);
      const result = Contract.parseWorkstreamOperationResult(assigned.structuredContent);
      expect(result.state).toBe("committed");
      // HTTP consumes this same application service; MCP must not hide its result in another cache.
      expect(yield* operations.read({ operation_id: request.operation_id })).toEqual({
        operation: result,
      });
      const reads = cp.state.reads;
      expect(yield* operations.submit({ request, wait_ms: 0 })).toEqual(result);
      const retained = yield* call("t3_workstream_operation_status", {
        operation_id: request.operation_id,
      });
      expect(retained.structuredContent).toEqual({ operation: result });
      expect(cp.state.reads).toBe(reads);
      expect(cp.state.membershipCommits).toBe(1);
      const conflict = yield* call("t3_thread_workstream_set", {
        request: { ...request, expected: { kind: "any" } },
        wait_ms: 0,
      });
      expect(conflict.isError).toBe(true);
      expect(JSON.stringify(conflict)).toContain("operation_id_conflict");
      const foreign = yield* call("t3_thread_workstream_read", {
        thread: { environment_id: "another-server", thread_id: "thread" },
      });
      expect(foreign.isError).toBe(true);
      expect(JSON.stringify(foreign)).toContain("foreign_thread");
      expect(cp.state.reads).toBe(reads);
      const listed = yield* call("t3_workstreams_list", { limit: 100 });
      expect(listed.isError).toBe(false);
      expect(listed.structuredContent).toMatchObject({
        binding: { registryId: cp.binding().registryId },
      });
      const assignment = yield* call("t3_thread_workstream_read", { thread: request.thread });
      expect(assignment.structuredContent).toMatchObject({
        assignment: { kind: "primary", workstream_id: "target" },
      });
      const create: Contract.WorkstreamCreateRequest = {
        contract: request.contract,
        kind: "create_workstream",
        operation_id: "mcp-create-production-01",
        origin: { kind: "agent" },
        registry: { registry_id: setup.registry.registry_id!, owner_id: setup.registry.owner_id! },
        workstream: {
          name: "MCP create",
          lifecycle: "active",
          progress: { state: "progressing" },
          sort_order: 0,
        },
      };
      const created = yield* call("t3_workstream_create", { request: create, wait_ms: 3000 });
      expect(created.isError).toBe(false);
      expect(Contract.parseWorkstreamOperationResult(created.structuredContent).state).toBe(
        "committed",
      );
      expect(yield* operations.read({ operation_id: create.operation_id })).toEqual({
        operation: created.structuredContent,
      });
      expect(cp.state.creates).toBe(1);
    }).pipe(
      Effect.provide(registration),
      Effect.provide(
        Layer.mock(ThreadManagementService)({ getThreadShell: () => Effect.succeed(null) }),
      ),
    );
  },
);
