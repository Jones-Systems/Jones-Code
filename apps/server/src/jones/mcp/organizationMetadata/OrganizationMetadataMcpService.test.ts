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
    // Sequence advance per read, as on a server busy with unrelated work.
    sequenceStep?: number;
    fail?: boolean;
    address?: NetAddress.SocketAddress;
  } = {},
) => {
  const locations: Array<string | undefined> = [];
  let sequence = 7;
  const dependencies = Layer.mergeAll(
    configLayer,
    Layer.mock(HttpServer.HttpServer)({
      address: options.address ?? NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43123),
    }),
    Layer.mock(ThreadManagement.ThreadManagementService)({
      getShellSnapshot: (input) => {
        locations.push(input?.location);
        if (options.fail)
          return Effect.fail(
            new OrchestratorProjectionError({
              threadId: caller.threadId,
              cause: "synthetic read failure",
            }),
          );
        const snapshotSequence = sequence;
        sequence += options.sequenceStep ?? 0;
        const active = input?.location === "archive" ? [] : (options.active ?? []);
        const archived = input?.location === "active" ? [] : (options.archived ?? []);
        return Effect.succeed({
          schemaVersion: 1,
          snapshotSequence,
          threads: active,
          archivedThreads: archived,
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
      expect(h.locations).toEqual([undefined, undefined, undefined]);
    }).pipe(Effect.provide(h.layer));
  },
);

it.effect("reads one consistent page while unrelated work advances the sequence", () => {
  const h = harness({
    active: [thread("b")],
    archived: [thread("a", { archivedAt: timestamp })],
    sequenceStep: 49,
  });
  return Effect.gen(function* () {
    const service = yield* Metadata.OrganizationMetadataMcpService;
    const first = yield* service.listThreadMetadata(environmentId, { limit: 100 });
    expect(first.snapshotSequence).toBe(7);
    expect(first.threads.map(({ threadId }) => threadId)).toEqual(["a", "b"]);
    const second = yield* service.listThreadMetadata(environmentId, { limit: 100 });
    expect(second.snapshotSequence).toBe(56);
    expect(h.locations).toEqual([undefined, undefined]);
  }).pipe(Effect.provide(h.layer));
});

it.effect("selects named threads and every pinned thread from the same read", () => {
  const h = harness({
    active: [
      thread("target", { activeOrderKey: "a0" }),
      thread("pin-keyed", { pinnedAt: timestamp, pinOrderKey: "a1" }),
      thread("pin-keyless", { pinnedAt: timestamp }),
      thread("other"),
      thread("pin-deleted", { pinnedAt: timestamp, deletedAt: timestamp }),
    ],
    archived: [thread("archived-target", { archivedAt: timestamp })],
    sequenceStep: 3,
  });
  const ids = (page: { threads: ReadonlyArray<{ threadId: string }> }) =>
    page.threads.map(({ threadId }) => threadId);
  return Effect.gen(function* () {
    const service = yield* Metadata.OrganizationMetadataMcpService;
    const named = yield* service.listThreadMetadata(environmentId, {
      threadIds: [ThreadId.make("target"), ThreadId.make("archived-target"), ThreadId.make("gone")],
    });
    expect(ids(named)).toEqual(["archived-target", "target"]);
    expect(named.nextOffset).toBeNull();
    const pinned = yield* service.listThreadMetadata(environmentId, { pinned: true });
    expect(ids(pinned)).toEqual(["pin-keyed", "pin-keyless"]);
    const union = yield* service.listThreadMetadata(environmentId, {
      threadIds: [ThreadId.make("target"), ThreadId.make("pin-keyed")],
      pinned: true,
    });
    expect(ids(union)).toEqual(["pin-keyed", "pin-keyless", "target"]);
    expect(union.threads.find(({ threadId }) => threadId === "target")).toMatchObject({
      pinnedAt: null,
      pinOrderKey: null,
      activeOrderKey: "a0",
    });
    expect(union.threads.find(({ threadId }) => threadId === "pin-keyed")).toMatchObject({
      pinOrderKey: "a1",
    });
    const unselected = yield* service.listThreadMetadata(environmentId, { pinned: false });
    expect(ids(unselected)).toEqual([
      "archived-target",
      "other",
      "pin-keyed",
      "pin-keyless",
      "target",
    ]);
    expect(h.locations).toEqual([undefined, undefined, undefined, undefined]);
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

it.effect("maps read failures without projecting the cause", () => {
  const h = harness({ fail: true });
  return Effect.gen(function* () {
    const service = yield* Metadata.OrganizationMetadataMcpService;
    const error = yield* service.listThreadMetadata(environmentId, {}).pipe(Effect.flip);
    expect(error.reason).toBe("local-operation-failed");
    expect(encodeJsonText(error)).not.toContain("synthetic read failure");
  }).pipe(Effect.provide(h.layer));
});
