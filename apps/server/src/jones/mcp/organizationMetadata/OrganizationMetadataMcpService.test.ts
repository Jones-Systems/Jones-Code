import * as NodePath from "@effect/platform-node/NodePath";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ThreadId,
  ProjectId,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { HttpServer } from "effect/http";
import * as NetAddress from "effect/net/NetAddress";
import { ServerConfig, deriveServerPaths } from "../../../config.ts";
import { DEFAULT_SIGNAL_EXPORT } from "@t3tools/shared/observability";
import * as OtelEnvironment from "@t3tools/shared/otelEnvironment";
import { OrchestratorProjectionError } from "../../../orchestration-v2/Orchestrator.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Metadata from "./OrganizationMetadataMcpService.ts";

const encodeJsonText = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const configLayer = Layer.effect(
  ServerConfig,
  deriveServerPaths("/synthetic/effective-home", undefined).pipe(
    Effect.map((derivedPaths) =>
      ServerConfig.of({
        logLevel: "Error",
        traceMinLevel: "Info",
        traceTimingEnabled: true,
        traceBatchWindowMs: 200,
        traceMaxBytes: 10 * 1024 * 1024,
        traceMaxFiles: 10,
        otlpTracesUrl: undefined,
        otlpMetricsUrl: undefined,
        otlpLogsUrl: undefined,
        otlpTracesExport: DEFAULT_SIGNAL_EXPORT,
        otlpMetricsExport: DEFAULT_SIGNAL_EXPORT,
        otlpLogsExport: DEFAULT_SIGNAL_EXPORT,
        otelEnvironment: OtelEnvironment.none,
        cwd: "/synthetic/workspace",
        baseDir: "/synthetic/effective-home",
        ...derivedPaths,
        mode: "web",
        autoBootstrapProjectFromCwd: false,
        logWebSocketEvents: false,
        tailscaleServeEnabled: false,
        tailscaleServePort: 443,
        port: 0,
        host: undefined,
        desktopBootstrapToken: undefined,
        desktopTelemetryFd: undefined,
        desktopTelemetryControlFd: undefined,
        resourceMonitorPath: undefined,
        staticDir: undefined,
        devUrl: undefined,
        devAllowedOrigins: [],
        noBrowser: false,
        startupPresentation: "browser",
      }),
    ),
    Effect.provide(NodePath.layer),
  ),
);

const environmentId = EnvironmentId.make("environment-1");
const caller = { environmentId, threadId: ThreadId.make("caller") };
const timestamp = DateTime.makeUnsafe("2026-08-01T00:00:00.000Z");
const thread = (id: string, overrides: Partial<OrchestrationV2ThreadShell> = {}) =>
  ({
    id: ThreadId.make(id),
    projectId: ProjectId.make("project-1"),
    title: "Thread",
    pinnedAt: null,
    pinOrderKey: null,
    activeOrderKey: null,
    snoozedUntil: null,
    settledOverride: null,
    settledAt: null,
    archivedAt: null,
    deletedAt: null,
    createdAt: timestamp,
    updatedAt: timestamp,
    latestUserMessageAt: null,
    latestRunId: null,
    activeRunId: null,
    status: "idle",
    pendingRuntimeRequest: null,
    hasActionableProposedPlan: false,
    ...overrides,
  }) as OrchestrationV2ThreadShell;

const harness = (
  options: {
    active?: OrchestrationV2ThreadShell[];
    archived?: OrchestrationV2ThreadShell[];
    archiveSequence?: number;
    failureLocation?: "active" | "archive";
    address?: NetAddress.SocketAddress;
  } = {},
) => {
  const locations: Array<string | undefined> = [];
  const dependencies = Layer.mergeAll(
    configLayer,
    Layer.mock(HttpServer.HttpServer)({
      address: options.address ?? NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43123),
    }),
    Layer.mock(ThreadManagement.ThreadManagementService)({
      getShellSnapshot: (input) => {
        locations.push(input?.location);
        if (input?.location === options.failureLocation)
          return Effect.fail(
            new OrchestratorProjectionError({
              threadId: caller.threadId,
              cause: "synthetic read failure",
            }),
          );
        return Effect.succeed({
          schemaVersion: 1,
          snapshotSequence: input?.location === "archive" ? (options.archiveSequence ?? 7) : 7,
          threads: input?.location === "active" ? (options.active ?? []) : [],
          archivedThreads: input?.location === "archive" ? (options.archived ?? []) : [],
        });
      },
    }),
  );
  return { locations, layer: Metadata.layer.pipe(Layer.provide(dependencies)) };
};

it.effect(
  "combines active and archived members only at one watermark, excluding deleted before pagination",
  () => {
    const h = harness({
      active: [thread("b"), thread("deleted", { deletedAt: timestamp })],
      archived: [thread("a", { archivedAt: timestamp })],
    });
    return Effect.gen(function* () {
      const service = yield* Metadata.OrganizationMetadataMcpService;
      const first = yield* service.listThreadMetadata(environmentId, { limit: 1 });
      expect(first).toMatchObject({
        environmentId,
        snapshotSequence: 7,
        nextOffset: 1,
        threads: [{ threadId: "a" }],
      });
      const second = yield* service.listThreadMetadata(environmentId, {
        limit: 1,
        offset: 1,
        expectedSnapshotSequence: 7,
      });
      expect(second).toMatchObject({ nextOffset: null, threads: [{ threadId: "b" }] });
      expect((yield* service.listThreadMetadata(environmentId, { offset: 20 })).threads).toEqual(
        [],
      );
      expect(h.locations).toEqual(["active", "archive", "active", "archive", "active", "archive"]);
    }).pipe(Effect.provide(h.layer));
  },
);

it.effect("fails closed when active and archived reads have different watermarks", () => {
  const h = harness({ archiveSequence: 8 });
  return Effect.gen(function* () {
    const service = yield* Metadata.OrganizationMetadataMcpService;
    expect(yield* service.listThreadMetadata(environmentId, {}).pipe(Effect.flip)).toMatchObject({
      reason: "snapshot-sequence-changed",
      expectedSnapshotSequence: 7,
      snapshotSequence: 8,
    });
  }).pipe(Effect.provide(h.layer));
});

it.effect("rejects a stale pagination watermark with both sequences", () => {
  const h = harness();
  return Effect.gen(function* () {
    const service = yield* Metadata.OrganizationMetadataMcpService;
    expect(
      yield* service
        .listThreadMetadata(environmentId, { expectedSnapshotSequence: 6 })
        .pipe(Effect.flip),
    ).toMatchObject({
      reason: "snapshot-sequence-changed",
      expectedSnapshotSequence: 6,
      snapshotSequence: 7,
    });
  }).pipe(Effect.provide(h.layer));
});

it.effect(
  "caps titles and preserves null activity without reading body or request payload fields",
  () => {
    const sensitive = thread("private", {
      title: "x".repeat(600),
      pendingRuntimeRequest: {
        kind: "permission",
      } as OrchestrationV2ThreadShell["pendingRuntimeRequest"],
    });
    Object.defineProperties(sensitive, {
      latestVisibleMessage: {
        get: () => {
          throw new Error("body projected");
        },
      },
      lastError: {
        get: () => {
          throw new Error("error projected");
        },
      },
    });
    Object.defineProperty(sensitive.pendingRuntimeRequest, "payload", {
      get: () => {
        throw new Error("request payload projected");
      },
    });
    const h = harness({ active: [sensitive] });
    return Effect.gen(function* () {
      const service = yield* Metadata.OrganizationMetadataMcpService;
      const result = (yield* service.listThreadMetadata(environmentId, {})).threads[0]!;
      expect(result.title.length).toBe(512);
      expect(result).toMatchObject({
        latestUserMessageAt: null,
        latestRunRequestedAt: null,
        latestRunStartedAt: null,
        latestRunCompletedAt: null,
        hasPendingApprovals: true,
        hasPendingUserInput: false,
      });
      expect(Object.keys(result)).not.toContain("latestVisibleMessage");
      expect(Object.keys(result)).not.toContain("lastError");
      expect(Object.keys(result)).not.toContain("pendingRuntimeRequest");
    }).pipe(Effect.provide(h.layer));
  },
);

it.effect("derives user-input and approval flags from the current request kind", () => {
  const h = harness({
    active: [
      thread("approval", {
        pendingRuntimeRequest: {
          kind: "command",
        } as OrchestrationV2ThreadShell["pendingRuntimeRequest"],
      }),
      thread("input", {
        pendingRuntimeRequest: {
          kind: "user_input",
        } as OrchestrationV2ThreadShell["pendingRuntimeRequest"],
      }),
      thread("none"),
    ],
  });
  return Effect.gen(function* () {
    const service = yield* Metadata.OrganizationMetadataMcpService;
    expect(
      (yield* service.listThreadMetadata(environmentId, {})).threads.map(
        ({ hasPendingApprovals, hasPendingUserInput }) => [
          hasPendingApprovals,
          hasPendingUserInput,
        ],
      ),
    ).toEqual([
      [true, false],
      [false, true],
      [false, false],
    ]);
  }).pipe(Effect.provide(h.layer));
});

it.effect("defaults to fifty rows and provides a bounded next offset", () => {
  const h = harness({
    active: Array.from({ length: 51 }, (_, i) => thread(`thread-${String(i).padStart(3, "0")}`)),
  });
  return Effect.gen(function* () {
    const service = yield* Metadata.OrganizationMetadataMcpService;
    const page = yield* service.listThreadMetadata(environmentId, {});
    expect(page.threads).toHaveLength(50);
    expect(page.nextOffset).toBe(50);
  }).pipe(Effect.provide(h.layer));
});

it.effect.each([
  { host: "127.0.0.1", expectedHost: "127.0.0.1" },
  { host: "127.42.0.1", expectedHost: "127.42.0.1" },
  { host: "::1", expectedHost: "[::1]" },
  { host: "0.0.0.0", expectedHost: "127.0.0.1" },
  { host: "::", expectedHost: "[::1]" },
])("uses the actual bound address and port for $host", ({ host, expectedHost }) => {
  const h = harness({ address: NetAddress.inetAddressFromIpStringUnsafe(host, 45678) });
  return Effect.gen(function* () {
    const service = yield* Metadata.OrganizationMetadataMcpService;
    expect(yield* service.getInvocationContext(caller)).toMatchObject({
      ...caller,
      effectiveBaseDir: "/synthetic/effective-home",
      loopbackOrigin: `http://${expectedHost}:45678`,
      serverGeneration: null,
    });
  }).pipe(Effect.provide(h.layer));
});

it.effect.each(
  [
    NetAddress.inetAddressFromIpStringUnsafe("192.0.2.1", 43123),
    NetAddress.inetAddressFromIpStringUnsafe("2001:db8::1", 43123),
    NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 0),
    NetAddress.inetAddressFromStringUnsafe("[::1%2]:43123"),
    NetAddress.unixPathAddress("/synthetic/server.sock"),
  ].map((address) => ({ address, name: String(address) })),
)("reports null when the bound loopback origin is unproved: $name", ({ address }) => {
  const h = harness({ address });
  return Effect.gen(function* () {
    const service = yield* Metadata.OrganizationMetadataMcpService;
    expect((yield* service.getInvocationContext(caller)).loopbackOrigin).toBeNull();
  }).pipe(Effect.provide(h.layer));
});

it.effect.each(["active", "archive"] as const)(
  "maps %s read failures without projecting the cause",
  (failureLocation) => {
    const h = harness({ failureLocation });
    return Effect.gen(function* () {
      const service = yield* Metadata.OrganizationMetadataMcpService;
      const error = yield* service.listThreadMetadata(environmentId, {}).pipe(Effect.flip);
      expect(error.reason).toBe("local-operation-failed");
      expect(encodeJsonText(error)).not.toContain("synthetic read failure");
    }).pipe(Effect.provide(h.layer));
  },
);
