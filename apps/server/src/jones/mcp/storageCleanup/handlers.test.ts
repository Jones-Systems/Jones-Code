import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  OrchestratorMcpEnvironmentTarget,
  OrchestratorMcpFailure,
  type StorageCleanupReport,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { McpSchema, McpServer, Tool } from "effect/ai";
import * as InProcessMcp from "../../../mcp/McpHttpServer.ts";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import * as McpToolAccess from "../../../mcp/McpToolAccess.ts";
import * as PeerForwarding from "../../../peer/PeerForwarding.ts";
import * as StorageCleanup from "../../../storageCleanup.ts";
import { StorageCleanupReportHandlersLive } from "./handlers.ts";
import { StorageCleanupReportToolkit } from "./tools.ts";

const scope: Invocation.McpInvocationScope = {
  environmentId: EnvironmentId.make("cleanup-environment"),
  thread: undefined,
  client: { sessionId: "synthetic-read-only", label: "synthetic", access: "read-only" },
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
  requestNamespace: "synthetic-cleanup",
};
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  clientCapabilities: {},
  clientInfo: { name: "synthetic-cleanup", version: "1.0.0" },
  protocolVersion: "2025-06-18",
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "synthetic-cleanup", version: "1.0.0" },
  },
  getClient: Effect.die("unused"),
});
const emptyReport: StorageCleanupReport = {
  trigger: "automatic",
  startedAt: "2026-11-16T12:00:00.000Z",
  finishedAt: "2026-11-16T12:00:01.000Z",
  entries: [],
  counts: { removed: 0, kept: 0, failed: 0 },
  bytesFreed: 0,
  omittedCount: 0,
};
const backend = () => {
  let reads = 0;
  let mutations = 0;
  let report: StorageCleanupReport | null = null;
  const forbidden = Effect.sync(() => {
    mutations++;
  }).pipe(
    Effect.andThen(Effect.die("Report access must never invoke cleanup or subscribe to it.")),
  );
  return {
    get reads() {
      return reads;
    },
    get mutations() {
      return mutations;
    },
    setReport: (next: StorageCleanupReport | null) => {
      report = next;
    },
    layer: Layer.succeed(StorageCleanup.StorageCleanup, {
      latestReport: Effect.sync(() => {
        reads++;
        return report;
      }),
      retirement: () => forbidden,
      runNow: forbidden,
      drain: forbidden,
      reports: Stream.fromEffect(forbidden),
    }),
  };
};

it("declares the native forwarding field and one read-only, nondestructive tool", () => {
  const tool = StorageCleanupReportToolkit.tools.t3_storage_cleanup_report;
  expect(Object.keys(StorageCleanupReportToolkit.tools)).toEqual(["t3_storage_cleanup_report"]);
  expect(tool.parametersSchema.fields.environmentId).toBe(OrchestratorMcpEnvironmentTarget);
  expect(Tool.getJsonSchema(tool)).toMatchObject({ type: "object" });
});

it.effect("production registration reads the supplied live report without mutations", () => {
  const source = backend();
  return Effect.scoped(
    Effect.gen(function* () {
      const server = yield* McpServer.McpServer;
      expect(server.tools.map(({ tool }) => tool.name)).toEqual(["t3_storage_cleanup_report"]);
      expect(server.tools[0]?.tool.annotations).toMatchObject({
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      });
      const call = (args: Record<string, unknown> = {}, caller = scope) =>
        server.callTool({ name: "t3_storage_cleanup_report", arguments: args }).pipe(
          Effect.provideService(Invocation.McpInvocationContext, caller),
          Effect.provideService(McpSchema.McpServerClient, client),
        );
      const missing = yield* call();
      expect(missing.isError).toBe(false);
      expect(missing.structuredContent).toMatchObject({
        authorityEffect: "none",
        environmentId: scope.environmentId,
        report: null,
      });
      source.setReport(emptyReport);
      const finished = yield* call();
      expect(finished.isError).toBe(false);
      expect(finished.structuredContent).toMatchObject({ report: emptyReport });
      const refused = yield* call({ cursor: 1 });
      expect(refused.isError).toBe(true);
      expect(refused.structuredContent).toMatchObject({ reason: "snapshot_required" });
      const linked = yield* call({}, {
        ...scope,
        client: { ...scope.client!, linked: true },
      });
      expect(linked.isError).toBe(false);
      expect(linked.structuredContent).toMatchObject({ report: emptyReport });
      expect(source.reads).toBe(4);
      expect(source.mutations).toBe(0);
    }),
  ).pipe(
    Effect.provide(
      InProcessMcp.layerStorageCleanupReportToolkit.pipe(
        Layer.provideMerge(InProcessMcp.layerInProcessMcpServer),
        Layer.provide(source.layer),
      ),
    ),
  );
});

it.effect("denies callers without orchestration before reading the report", () => {
  const source = backend();
  return Effect.gen(function* () {
    const toolkit = yield* StorageCleanupReportToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(StorageCleanupReportHandlersLive).pipe(
          Layer.provide(source.layer),
        ),
      ),
    );
    const results = yield* toolkit.handle("t3_storage_cleanup_report", {}).pipe(
      Stream.unwrap,
      Stream.runCollect,
      Effect.provideService(Invocation.McpInvocationContext, {
        ...scope,
        capabilities: new Set<Invocation.McpCapability>(),
      }),
      Effect.provide(source.layer),
    );
    expect(results.at(-1)?.result).toMatchObject({
      _tag: "McpCapabilityUnavailableError",
      capability: "orchestration",
    });
    expect(source.reads).toBe(0);
    expect(source.mutations).toBe(0);
  });
});

it.effect("forwards explicit linked targets through native routing without reading the local report", () => {
  const source = backend();
  const remote = EnvironmentId.make("linked-cleanup-environment");
  const calls: unknown[] = [];
  const forwarding = Layer.mock(PeerForwarding.PeerForwarding)({
    call: (caller, tool, environmentId, input) => {
      calls.push({ caller, name: tool.name, environmentId, input });
      return Effect.fail(
        new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: "Synthetic remote response",
        }),
      );
    },
  });
  return Effect.gen(function* () {
    const toolkit = yield* StorageCleanupReportToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(StorageCleanupReportHandlersLive).pipe(
          Layer.provide(source.layer),
          Layer.provide(forwarding),
        ),
      ),
    );
    const input = { environmentId: remote, limit: 1, worktreePath: "/exact/worktree" };
    const results = yield* toolkit.handle("t3_storage_cleanup_report", input).pipe(
      Stream.unwrap,
      Stream.runCollect,
      Effect.provideService(Invocation.McpInvocationContext, scope),
      Effect.provide(source.layer),
    );
    expect(results.at(-1)?.result).toMatchObject({ message: "Synthetic remote response" });
    expect(calls).toEqual([{
      caller: scope,
      name: "t3_storage_cleanup_report",
      environmentId: remote,
      input,
    }]);
    expect(source.reads).toBe(0);
    expect(source.mutations).toBe(0);
  });
});
