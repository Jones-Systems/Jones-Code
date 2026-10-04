import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2ThreadShell,
  type WorkstreamCommand,
  type WorkstreamDetail,
  type WorkstreamReceipt,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { McpSchema, McpServer, Tool } from "effect/unstable/ai";
import { HttpServer } from "effect/unstable/http";
import * as NetAddress from "effect/unstable/net/NetAddress";
import packageJson from "../../../../package.json" with { type: "json" };
import { ServerConfig } from "../../../config.ts";
import { McpInvocationContext, type McpCapability } from "../../McpInvocationContext.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import {
  OrchestratorDispatchError,
  OrchestratorProjectionError,
} from "../../../orchestration-v2/Orchestrator.ts";
import {
  WorkstreamGateway,
  WorkstreamGatewayError,
} from "../../../workstreams/WorkstreamGateway.ts";
import { OrganizationToolkitHandlersLive } from "./handlers.ts";
import { OrganizationCommand, OrganizationToolkit, OrderKey } from "./tools.ts";

const threadId = ThreadId.make("thread-1");
const commandId = CommandId.make("organization-command-1");
const thread: OrchestrationV2ThreadShell = {
  id: threadId,
  projectId: ProjectId.make("project-1"),
  title: "Thread",
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  pullRequests: [],
  createdAt: DateTime.makeUnsafe("2026-08-01T00:00:00.000Z"),
  updatedAt: DateTime.makeUnsafe("2026-08-01T00:00:00.000Z"),
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  latestUserMessageAt: null,
  hasActionableProposedPlan: false,
  createdBy: "user",
  creationSource: "web",
  providerInstanceId: ProviderInstanceId.make("codex"),
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
  forkedFrom: null,
  activeProviderThreadId: null,
  latestRunId: null,
  activeRunId: null,
  status: "idle",
  pendingRuntimeRequest: null,
  pendingRequestCounts: { approval: 0, userInput: 0 },
  latestVisibleMessage: null,
  itemCount: 0,
  visibleItemCount: 0,
  pendingBackgroundTasks: [],
  providerInstanceHistory: [],
  deletedAt: null,
};
const pendingReceipt: WorkstreamReceipt = {
  command_id: commandId,
  owner_id: "owner-1",
  actor: { principal_id: "principal-1" },
  operation: "attach_primary",
  request_sha256: "a".repeat(64),
  server_generation: 1,
  accepted_at: "2026-08-01T00:00:00Z",
  state: "pending",
  retry_after_seconds: 1,
};
const command: typeof OrganizationCommand.Type = {
  command_id: commandId,
  expected_server_generation: 1,
  expected_registry_version: 0,
  action: {
    operation: "attach_primary",
    workstream_id: "workstream-1",
    expected_version: 1,
    native_reference_id: "reference-1",
  },
};
const makeHarness = Effect.fn("organizationTestHarness")(function* (
  options: {
    thread?: OrchestrationV2ThreadShell | null;
    gatewayFailure?: WorkstreamGatewayError;
    receipt?: WorkstreamReceipt;
    detail?: WorkstreamDetail;
    readbackFails?: boolean;
    dispatchFails?: boolean;
    address?: NetAddress.SocketAddress;
    threads?: OrchestrationV2ThreadShell[];
    archivedThreads?: OrchestrationV2ThreadShell[];
    snapshotSequences?: number[];
    shellFailure?: boolean;
  } = {},
) {
  const commands: OrchestrationV2ServerCommand[] = [];
  const submitted: WorkstreamCommand[] = [];
  let placementCalls = 0;
  let shellCalls = 0;
  let addressReads = 0;
  let shellByIdCalls = 0;
  let gatewayCalls = 0;
  const configReads: PropertyKey[] = [];
  const config = new Proxy({} as ServerConfig["Service"], {
    get: (_target, key) => {
      configReads.push(key);
      if (key === "baseDir") return "/effective/t3-home";
      if (key === "port") return 0;
      throw new Error(`Invocation context accessed unexpected config field: ${String(key)}`);
    },
  });
  const target = options.thread === undefined ? thread : options.thread;
  const dependencies = Layer.mergeAll(
    Layer.succeed(ServerConfig, config),
    Layer.succeed(
      HttpServer.HttpServer,
      HttpServer.HttpServer.of({
        get address() {
          addressReads++;
          return options.address ?? NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43123);
        },
        serve: (() =>
          Effect.die(
            "Invocation context must not serve HTTP",
          )) as HttpServer.HttpServer["Service"]["serve"],
      }),
    ),
    Layer.mock(ThreadManagement.ThreadManagementService)({
      getThreadShell: (id) => {
        shellByIdCalls++;
        return options.readbackFails && commands.length > 0
          ? Effect.fail(new OrchestratorProjectionError({ threadId, cause: "synthetic-readback" }))
          : Effect.succeed(id === threadId ? target : null);
      },
      getShellSnapshot: () =>
        Effect.sync(() => {
          shellCalls++;
          return {
            schemaVersion: 1,
            snapshotSequence: options.snapshotSequences?.[shellCalls - 1] ?? 1,
            archivedThreads: options.archivedThreads ?? [],
            threads:
              options.threads ??
              (target ? [{ ...target, messages: [{ text: "PRIVATE CONTENT" }] }] : []),
          };
        }).pipe(
          Effect.flatMap((snapshot) =>
            options.shellFailure
              ? Effect.fail(
                  new OrchestratorProjectionError({
                    threadId,
                    cause: new Error("PRIVATE SQL CONTENT CREDENTIAL"),
                  }),
                )
              : Effect.succeed(snapshot),
          ),
        ),
      dispatch: (input) =>
        Effect.gen(function* () {
          commands.push(input);
          if (options.dispatchFails)
            return yield* Effect.fail(
              new OrchestratorDispatchError({
                commandId: input.commandId,
                commandType: input.type,
                cause: "synthetic-dispatch",
              }),
            );
          return { sequence: 42, storedEvents: [] };
        }),
    }),
    Layer.mock(WorkstreamGateway)({
      purgeAuthorization: () => {},
      submit: (input) =>
        Effect.gen(function* () {
          submitted.push(input);
          if (options.gatewayFailure) return yield* options.gatewayFailure;
          return options.receipt!;
        }),
      readDetail: () => {
        gatewayCalls++;
        return Effect.succeed(options.detail!);
      },
      pollCommand: () => {
        gatewayCalls++;
        return Effect.succeed(options.receipt!);
      },
      readThreadPlacements: () =>
        Effect.gen(function* () {
          placementCalls++;
          return yield* options.gatewayFailure!;
        }),
    }),
  );
  const toolkit = yield* OrganizationToolkit.pipe(
    Effect.provide(OrganizationToolkitHandlersLive.pipe(Layer.provide(dependencies))),
  );
  const call = <Name extends keyof typeof OrganizationToolkit.tools>(
    name: Name,
    input: Parameters<typeof toolkit.handle<Name>>[1],
    capabilities: McpCapability[] = ["organization"],
    caller: { environmentId: EnvironmentId; threadId: ThreadId } = {
      environmentId: EnvironmentId.make("environment-1"),
      threadId,
    },
  ) =>
    toolkit.handle(name, input).pipe(
      Stream.unwrap,
      Stream.runCollect,
      Effect.map(
        (results) =>
          results.at(-1)!.result as Tool.Success<(typeof OrganizationToolkit.tools)[Name]>,
      ),
      Effect.provideService(McpInvocationContext, {
        environmentId: caller.environmentId,
        threadId: caller.threadId,
        providerInstanceId: ProviderInstanceId.make("codex"),
        providerSessionId: "session-1",
        issuedAt: 1,
        capabilities: new Set(capabilities),
      }),
      Effect.provide(dependencies),
    );
  return {
    call,
    commands,
    submitted,
    dependencies,
    unsafeReads: () => ({ shellByIdCalls, gatewayCalls }),
    counts: () => ({ placementCalls, shellCalls, addressReads, configReads }),
  };
});

it.effect("returns only current invocation context without environment or secret-store reads", () =>
  Effect.gen(function* () {
    const h = yield* makeHarness();
    const result = yield* h.call("get_invocation_context", {});
    expect(result).toEqual({
      environmentId: "environment-1",
      threadId: "thread-1",
      effectiveBaseDir: "/effective/t3-home",
      loopbackOrigin: "http://127.0.0.1:43123",
      serverVersion: packageJson.version,
      serverGeneration: null,
    });
    expect(h.counts()).toEqual({
      placementCalls: 0,
      shellCalls: 0,
      addressReads: 1,
      configReads: ["baseDir"],
    });
    expect(h.commands).toEqual([]);
    expect(h.submitted).toEqual([]);
  }),
);
it.effect(
  "registers native MCP manifests and returns structured results for both metadata tools",
  () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      const client = McpSchema.McpServerClient.of({
        clientId: 1,
        clientCapabilities: {},
        clientInfo: { name: "organization-test", version: "1.0.0" },
        protocolVersion: "2025-06-18",
        initializePayload: {
          protocolVersion: "2025-06-18",
          capabilities: {},
          clientInfo: { name: "organization-test", version: "1.0.0" },
        },
        getClient: Effect.die("unused"),
      });
      const registration = McpServer.toolkit(OrganizationToolkit).pipe(
        Layer.provide(OrganizationToolkitHandlersLive),
        Layer.provideMerge(McpServer.McpServer.layer),
        Layer.provide(h.dependencies),
      );
      yield* Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        for (const name of ["get_invocation_context", "list_organization_thread_metadata"]) {
          const manifest = server.tools.find(({ tool }) => tool.name === name)?.tool;
          expect(manifest).toBeDefined();
          expect(manifest?.inputSchema).toMatchObject({ type: "object" });
          expect(manifest?.outputSchema).toMatchObject({ type: "object" });
          expect(manifest?.annotations).toMatchObject({
            readOnlyHint: true,
            idempotentHint: true,
            destructiveHint: false,
            openWorldHint: false,
          });
          const result = yield* server.callTool({ name, arguments: {} }).pipe(
            Effect.provideService(McpSchema.McpServerClient, client),
            Effect.provideService(McpInvocationContext, {
              environmentId: EnvironmentId.make("environment-1"),
              threadId,
              providerInstanceId: ProviderInstanceId.make("codex"),
              providerSessionId: "PRIVATE SESSION",
              issuedAt: 1,
              capabilities: new Set(["organization"] as const),
            }),
          );
          expect(result.isError).toBe(false);
          expect(result.structuredContent).toMatchObject({ environmentId: "environment-1" });
          const text = result.content[0];
          expect(text?.type).toBe("text");
          if (text?.type === "text") {
            expect(text.text).not.toContain("PRIVATE");
            expect(
              yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(text.text),
            ).toEqual(result.structuredContent);
          }
          if (name === "get_invocation_context") {
            expect(result.structuredContent).toEqual({
              environmentId: "environment-1",
              threadId,
              effectiveBaseDir: "/effective/t3-home",
              loopbackOrigin: "http://127.0.0.1:43123",
              serverVersion: packageJson.version,
              serverGeneration: null,
            });
            expect(h.counts().shellCalls).toBe(0);
          } else {
            expect(result.structuredContent).toMatchObject({
              snapshotSequence: 1,
              observedAt: "1970-01-01T00:00:00.000Z",
              nextOffset: null,
              threads: [
                { threadId, backgroundLiveness: "unknown", session: null, latestTurn: null },
              ],
            });
            expect(h.counts().shellCalls).toBe(1);
          }
        }
        expect(h.unsafeReads()).toEqual({ shellByIdCalls: 0, gatewayCalls: 0 });
        expect(h.commands).toEqual([]);
        expect(h.submitted).toEqual([]);
      }).pipe(Effect.provide(registration));
    }),
);
it.effect("denies invocation context before reading server configuration or its address", () =>
  Effect.gen(function* () {
    const h = yield* makeHarness();
    expect(
      yield* h.call("get_invocation_context", {}, ["preview"]).pipe(Effect.flip),
    ).toMatchObject({
      _tag: "McpCapabilityUnavailableError",
      capability: "organization",
    });
    expect(h.counts()).toEqual({
      placementCalls: 0,
      shellCalls: 0,
      addressReads: 0,
      configReads: [],
    });
  }),
);
it.effect("isolates invocation identity across callers sharing one toolkit", () =>
  Effect.gen(function* () {
    const h = yield* makeHarness();
    const alternate = {
      environmentId: EnvironmentId.make("environment-2"),
      threadId: ThreadId.make("thread-2"),
    };
    expect(yield* h.call("get_invocation_context", {})).toMatchObject({
      environmentId: "environment-1",
      threadId: "thread-1",
    });
    expect(yield* h.call("get_invocation_context", {}, ["organization"], alternate)).toMatchObject(
      alternate,
    );
    expect(yield* h.call("get_invocation_context", {})).toMatchObject({
      environmentId: "environment-1",
      threadId: "thread-1",
    });
  }),
);
it.effect("uses the bound port and address family for proved loopback origins", () =>
  Effect.gen(function* () {
    for (const [host, expectedHost] of [
      ["127.0.0.1", "127.0.0.1"],
      ["127.42.0.1", "127.42.0.1"],
      ["::1", "[::1]"],
      ["0.0.0.0", "127.0.0.1"],
      ["::", "[::1]"],
    ]) {
      const h = yield* makeHarness({
        address: NetAddress.inetAddressFromIpStringUnsafe(host!, 45678),
      });
      expect((yield* h.call("get_invocation_context", {})).loopbackOrigin).toBe(
        `http://${expectedHost}:45678`,
      );
      expect(h.counts().configReads).toEqual(["baseDir"]);
    }
  }),
);
it.effect("reports an unavailable origin when a loopback route or bound port is unproved", () =>
  Effect.gen(function* () {
    for (const address of [
      NetAddress.inetAddressFromIpStringUnsafe("192.0.2.1", 43123),
      NetAddress.inetAddressFromIpStringUnsafe("2001:db8::1", 43123),
      NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 0),
      NetAddress.inetAddressFromStringUnsafe("[::1%2]:43123"),
      NetAddress.unixPathAddress("/synthetic/server.sock"),
    ]) {
      const h = yield* makeHarness({ address });
      const result = yield* h.call("get_invocation_context", {});
      expect(result.loopbackOrigin).toBeNull();
      expect(result.serverGeneration).toBeNull();
    }
  }),
);
it("accepts only empty invocation input and advertises bounded read-only behavior", () => {
  const tool = OrganizationToolkit.tools.get_invocation_context;
  expect(Tool.getJsonSchema(tool)).toMatchObject({ type: "object", maxProperties: 0 });
  const decode = Schema.decodeUnknownSync(tool.parametersSchema);
  expect(decode({})).toEqual({});
  for (const input of [
    { threadId: "thread-2" },
    { environmentId: "environment-2" },
    { target: "arbitrary" },
  ]) {
    expect(() => decode(input)).toThrow();
  }
  for (const input of [null, [], ["private"], "", 0, false]) {
    expect(() => decode(input)).toThrow();
  }
  expect(Context.get(tool.annotations, Tool.Readonly)).toBe(true);
  expect(Context.get(tool.annotations, Tool.Idempotent)).toBe(true);
  expect(Context.get(tool.annotations, Tool.Destructive)).toBe(false);
  expect(Context.get(tool.annotations, Tool.OpenWorld)).toBe(false);
});

it.effect("denies thread metadata before querying the shell", () =>
  Effect.gen(function* () {
    const h = yield* makeHarness();
    expect(
      yield* h.call("list_organization_thread_metadata", {}, []).pipe(Effect.flip),
    ).toMatchObject({
      _tag: "McpCapabilityUnavailableError",
      capability: "organization",
    });
    expect(h.counts().shellCalls).toBe(0);
    expect(h.unsafeReads()).toEqual({ shellByIdCalls: 0, gatewayCalls: 0 });
    expect(h.commands).toEqual([]);
    expect(h.submitted).toEqual([]);
  }),
);
it.effect(
  "projects explicit metadata fields while excluding private session and turn details",
  () =>
    Effect.gen(function* () {
      const runId = RunId.make("run-metadata-1");
      const requestedAt = DateTime.makeUnsafe("2026-08-01T01:00:00.000Z");
      const startedAt = DateTime.makeUnsafe("2026-08-01T01:00:01.000Z");
      // Extra private properties are deliberately present on the source object, never projected.
      const privateShell = {
        ...thread,
        updatedAt: DateTime.makeUnsafe("2026-08-01T02:00:00.000Z"),
        latestUserMessageAt: DateTime.makeUnsafe("2026-08-01T00:59:00.000Z"),
        latestRunId: runId,
        latestRunRequestedAt: requestedAt,
        latestRunStartedAt: startedAt,
        latestRunCompletedAt: null,
        activeRunId: runId,
        activityRunStatus: "running" as const,
        activityRunStartedAt: startedAt,
        status: "running" as const,
        pendingRequestCounts: { approval: 2, userInput: 1 },
        hasActionableProposedPlan: true,
        latestTurn: { privateBody: "PRIVATE TURN BODY" },
        session: { lastError: "PRIVATE ERROR", privateCredentials: "PRIVATE CREDENTIAL" },
        pendingBackgroundTasks: [
          { taskId: "PRIVATE TASK", kind: "monitor" as const, description: "PRIVATE DESCRIPTION" },
        ],
      };
      const h = yield* makeHarness({ threads: [privateShell], snapshotSequences: [42] });
      const result = yield* h.call("list_organization_thread_metadata", {});
      expect(result).toEqual({
        environmentId: "environment-1",
        snapshotSequence: 42,
        observedAt: "1970-01-01T00:00:00.000Z",
        nextOffset: null,
        threads: [
          {
            threadId,
            title: "Thread",
            projectId: "project-1",
            pinnedAt: null,
            pinOrderKey: null,
            activeOrderKey: null,
            snoozedUntil: null,
            settledOverride: null,
            settledAt: null,
            archivedAt: null,
            createdAt: DateTime.formatIso(thread.createdAt),
            projectionUpdatedAt: "2026-08-01T02:00:00.000Z",
            latestUserMessageAt: "2026-08-01T00:59:00.000Z",
            latestTurn: null,
            session: null,
            v2Activity: {
              latestRunId: runId,
              status: "running",
              latestRunRequestedAt: DateTime.formatIso(requestedAt),
              latestRunStartedAt: DateTime.formatIso(startedAt),
              latestRunCompletedAt: null,
              activeRunId: runId,
              activityRunStatus: "running",
              activityRunStartedAt: DateTime.formatIso(startedAt),
            },
            hasPendingApprovals: true,
            hasPendingUserInput: true,
            hasActionableProposedPlan: true,
            backgroundLiveness: "unknown",
          },
        ],
      });
      expect(
        yield* Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(result),
      ).not.toContain("PRIVATE");
      expect(h.counts().shellCalls).toBe(1);
      expect(h.counts().configReads).toEqual([]);
      expect(h.counts().addressReads).toBe(0);
      expect(h.unsafeReads()).toEqual({ shellByIdCalls: 0, gatewayCalls: 0 });
      expect(h.commands).toEqual([]);
      expect(h.submitted).toEqual([]);
    }),
);
it.effect("preserves null metadata and distinguishes absent background coverage", () =>
  Effect.gen(function* () {
    for (const backgroundLiveness of [undefined, null, "working", "monitoring"] as const) {
      const historicalSample = { ...thread, backgroundLiveness };
      const h = yield* makeHarness({ thread: historicalSample });
      const metadata = (yield* h.call("list_organization_thread_metadata", {})).threads[0]!;
      expect(metadata.latestUserMessageAt).toBeNull();
      expect(metadata.latestTurn).toBeNull();
      expect(metadata.session).toBeNull();
      expect(metadata.backgroundLiveness).toBe("unknown");
      expect(metadata.v2Activity?.latestRunCompletedAt).toBeNull();
      expect(metadata.hasPendingApprovals).toBe(false);
      expect(metadata.hasPendingUserInput).toBe(false);
      expect(metadata.hasActionableProposedPlan).toBe(false);
      expect(
        yield* Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(metadata),
      ).not.toContain("PRIVATE CONTENT");
    }
  }),
);
it.effect(
  "distinguishes absent aggregate coverage from authoritative zero without detail reads",
  () =>
    Effect.gen(function* () {
      const { pendingRequestCounts: _counts, ...historicalShell } = thread;
      const h = yield* makeHarness({ thread: historicalShell });
      const error = yield* h.call("list_organization_thread_metadata", {}).pipe(Effect.flip);
      expect(error).toMatchObject({ reason: "local-operation-failed" });
      expect(error).not.toHaveProperty("cause");
      expect(h.unsafeReads()).toEqual({ shellByIdCalls: 0, gatewayCalls: 0 });
      expect(yield* h.call("list_organization_threads", {})).toMatchObject({
        threads: [{ threadId }],
      });
      const current = yield* makeHarness();
      expect(
        (yield* current.call("list_organization_thread_metadata", {})).threads[0],
      ).toMatchObject({
        hasPendingApprovals: false,
        hasPendingUserInput: false,
      });
    }),
);
it.effect(
  "preserves independent aggregate flags and never substitutes row time for run completion",
  () =>
    Effect.gen(function* () {
      for (const counts of [
        { approval: 0, userInput: 2 },
        { approval: 3, userInput: 0 },
        { approval: 1, userInput: 1 },
      ]) {
        const h = yield* makeHarness({
          thread: {
            ...thread,
            pendingRequestCounts: counts,
            latestRunId: RunId.make("run-metadata-completed"),
            status: "completed",
            latestRunCompletedAt: null,
            updatedAt: DateTime.makeUnsafe("2026-08-01T09:00:00.000Z"),
          },
        });
        expect((yield* h.call("list_organization_thread_metadata", {})).threads[0]).toMatchObject({
          hasPendingApprovals: counts.approval > 0,
          hasPendingUserInput: counts.userInput > 0,
          latestTurn: null,
          session: null,
          backgroundLiveness: "unknown",
          projectionUpdatedAt: "2026-08-01T09:00:00.000Z",
          v2Activity: { status: "completed", latestRunCompletedAt: null },
        });
      }
    }),
);
it.effect("keeps settled and snoozed active rows but never unions archived census rows", () =>
  Effect.gen(function* () {
    const rows = [
      thread,
      { ...thread, id: ThreadId.make("settled"), settledOverride: "settled" as const },
      {
        ...thread,
        id: ThreadId.make("snoozed"),
        snoozedUntil: DateTime.makeUnsafe("2099-01-01T00:00:00Z"),
        title: "x".repeat(600),
      },
    ];
    const h = yield* makeHarness({
      threads: rows,
      archivedThreads: [{ ...thread, id: ThreadId.make("archived"), archivedAt: thread.createdAt }],
    });
    const result = yield* h.call("list_organization_thread_metadata", {});
    expect(result.threads.map((row) => row.threadId)).toEqual([threadId, "settled", "snoozed"]);
    expect(result.threads[2]!.title).toHaveLength(512);
    expect(result.nextOffset).toBeNull();
    expect(h.unsafeReads()).toEqual({ shellByIdCalls: 0, gatewayCalls: 0 });
  }),
);
it.effect("returns a bounded metadata failure without SQL details or native error causes", () =>
  Effect.gen(function* () {
    const h = yield* makeHarness({ shellFailure: true });
    const error = yield* h.call("list_organization_thread_metadata", {}).pipe(Effect.flip);
    expect(error).toMatchObject({
      _tag: "OrganizationToolError",
      reason: "local-operation-failed",
    });
    expect(error).not.toHaveProperty("cause");
    expect(
      yield* Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(error),
    ).not.toContain("PRIVATE");
    expect(h.counts().shellCalls).toBe(1);
    expect(h.unsafeReads()).toEqual({ shellByIdCalls: 0, gatewayCalls: 0 });
  }),
);
it.effect("pages metadata with a persisted watermark and rejects page drift", () =>
  Effect.gen(function* () {
    const h = yield* makeHarness({
      threads: [thread, { ...thread, id: ThreadId.make("thread-2") }],
      snapshotSequences: [42, 42, 43],
    });
    const first = yield* h.call("list_organization_thread_metadata", { limit: 1 });
    expect(first.snapshotSequence).toBe(42);
    expect(first.nextOffset).toBe(1);
    expect(first.threads.map((item) => item.threadId)).toEqual([threadId]);
    const second = yield* h.call("list_organization_thread_metadata", {
      limit: 1,
      offset: 1,
      expectedSnapshotSequence: first.snapshotSequence,
    });
    expect(second.threads.map((item) => item.threadId)).toEqual(["thread-2"]);
    expect(second.nextOffset).toBeNull();
    expect(
      yield* h
        .call("list_organization_thread_metadata", {
          offset: 1,
          expectedSnapshotSequence: first.snapshotSequence,
        })
        .pipe(Effect.flip),
    ).toMatchObject({
      reason: "snapshot-sequence-changed",
      expectedSnapshotSequence: 42,
      snapshotSequence: 43,
    });
    expect(h.counts().shellCalls).toBe(3);
    expect(h.unsafeReads()).toEqual({ shellByIdCalls: 0, gatewayCalls: 0 });
  }),
);
it("bounds metadata pagination and advertises a read-only closed-world tool", () => {
  const tool = OrganizationToolkit.tools.list_organization_thread_metadata;
  const decode = Schema.decodeUnknownSync(tool.parametersSchema);
  for (const input of [
    { limit: 0 },
    { limit: 101 },
    { offset: -1 },
    { expectedSnapshotSequence: -1 },
    { limit: 1.5 },
    { offset: 1.5 },
    { expectedSnapshotSequence: 1.5 },
    { offset: Number.POSITIVE_INFINITY },
  ]) {
    expect(() => decode(input)).toThrow();
  }
  expect(Context.get(tool.annotations, Tool.Readonly)).toBe(true);
  expect(Context.get(tool.annotations, Tool.Idempotent)).toBe(true);
  expect(Context.get(tool.annotations, Tool.Destructive)).toBe(false);
  expect(Context.get(tool.annotations, Tool.OpenWorld)).toBe(false);
});

it.effect("denies organization without reading thread metadata", () =>
  Effect.gen(function* () {
    const h = yield* makeHarness();
    const error = yield* h.call("list_organization_threads", {}, ["preview"]).pipe(Effect.flip);
    expect(error).toMatchObject({
      _tag: "McpCapabilityUnavailableError",
      capability: "organization",
    });
    expect(h.counts().shellCalls).toBe(0);
  }),
);
it.effect("organization works without preview and exposes only bounded navigation metadata", () =>
  Effect.gen(function* () {
    const h = yield* makeHarness({ thread: { ...thread, title: "x".repeat(600) } });
    const result = yield* h.call("list_organization_threads", { limit: 1 });
    expect(result.environmentId).toBe("environment-1");
    expect(result.threads).toHaveLength(1);
    expect(result.threads[0]!.title).toHaveLength(512);
    expect(result.threads[0]).not.toHaveProperty("messages");
    expect(result.threads[0]).not.toHaveProperty("session");
    expect(result.nextOffset).toBeNull();
    expect((yield* h.call("list_organization_threads", { offset: 1 })).threads).toEqual([]);
  }),
);
it.effect(
  "dispatches exact native pin, unpin and order commands with caller receipt identities",
  () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      expect(
        yield* h.call("set_thread_pinned", { commandId, threadId, pinned: true, orderKey: "a0" }),
      ).toMatchObject({ commandId, sequence: 42, readback: "pending" });
      yield* h.call("set_thread_pinned", { commandId, threadId, pinned: false });
      yield* h.call("reorder_thread", { commandId, threadId, list: "active", orderKey: "a1" });
      expect(h.commands.map((c) => c.type)).toEqual([
        "thread.pin",
        "thread.unpin",
        "thread.active.reorder",
      ]);
      expect(h.commands[0]).toEqual({ type: "thread.pin", commandId, threadId, orderKey: "a0" });
      const pinned = yield* makeHarness({ thread: { ...thread, pinnedAt: thread.createdAt } });
      yield* pinned.call("reorder_thread", { commandId, threadId, list: "pinned", orderKey: "a2" });
      expect(pinned.commands[0]!.type).toBe("thread.pin.reorder");
    }),
);
it.effect("dispatches active ordering for a pinned thread without changing its pin placement", () =>
  Effect.gen(function* () {
    const h = yield* makeHarness({
      thread: {
        ...thread,
        pinnedAt: thread.createdAt,
        pinOrderKey: "a2",
        activeOrderKey: "a0",
      },
    });
    expect(
      yield* h.call("reorder_thread", { commandId, threadId, list: "active", orderKey: "a1" }),
    ).toMatchObject({
      commandId,
      sequence: 42,
      readback: "pending",
      thread: {
        pinnedAt: DateTime.formatIso(thread.createdAt),
        pinOrderKey: "a2",
        activeOrderKey: "a0",
      },
    });
    expect(h.commands).toEqual([
      { type: "thread.active.reorder", commandId, threadId, orderKey: "a1" },
    ]);
  }),
);
it.effect("refuses missing, parked and mismatched targets without dispatch", () =>
  Effect.gen(function* () {
    for (const target of [
      null,
      { ...thread, archivedAt: thread.createdAt },
      { ...thread, deletedAt: thread.createdAt },
      { ...thread, settledOverride: "settled" as const },
      { ...thread, snoozedUntil: DateTime.makeUnsafe("2099-01-01T00:00:00.000Z") },
    ]) {
      const h = yield* makeHarness({ thread: target });
      yield* h.call("set_thread_pinned", { commandId, threadId, pinned: true }).pipe(Effect.flip);
      for (const list of ["active", "pinned"] as const) {
        yield* h
          .call("reorder_thread", { commandId, threadId, list, orderKey: "a0" })
          .pipe(Effect.flip);
      }
      expect(h.commands).toEqual([]);
    }
    const h = yield* makeHarness();
    expect(
      yield* h
        .call("reorder_thread", { commandId, threadId, list: "pinned", orderKey: "a0" })
        .pipe(Effect.flip),
    ).toMatchObject({ reason: "pin-state-mismatch" });
    expect(h.commands).toEqual([]);
  }),
);
it.effect("preserves gateway unknown effects and never retries writes or placement reads", () =>
  Effect.gen(function* () {
    const failure = new WorkstreamGatewayError({
      reason: "unknown-effect",
      detail: "exact command identity remains unresolved",
    });
    const h = yield* makeHarness({ gatewayFailure: failure });
    expect(yield* h.call("submit_workstream_command", command).pipe(Effect.flip)).toBe(failure);
    expect(h.submitted).toEqual([command]);
    expect(
      yield* h
        .call("list_thread_placements", {
          identities: [{ source_instance_id: "source-1", native_thread_id: "thread-1" }],
        })
        .pipe(Effect.flip),
    ).toBe(failure);
    expect(h.counts().placementCalls).toBe(1);
  }),
);
it.effect("passes through pending receipts and exact command lookups", () =>
  Effect.gen(function* () {
    const receipt = pendingReceipt;
    const h = yield* makeHarness({ receipt });
    expect(yield* h.call("submit_workstream_command", command)).toBe(receipt);
    expect(yield* h.call("read_workstream_command", { commandId })).toBe(receipt);
    expect(h.submitted).toHaveLength(1);
  }),
);
it.effect(
  "reorders workstreams while retaining name, lifecycle and progress and enforcing version",
  () =>
    Effect.gen(function* () {
      const workstream = {
        workstream_id: "workstream-1",
        version: 3,
        name: "Preserved",
        lifecycle: "paused",
        progress: { state: "blocked", impediment: "Awaiting owner" },
        sort_order: 5,
      } as unknown as WorkstreamDetail["workstream"];
      const h = yield* makeHarness({
        detail: {
          context: { owner_id: "owner-1", registry_version: 0, server_generation: 1 },
          workstream,
        },
        receipt: pendingReceipt,
      });
      const input = {
        commandId,
        workstreamId: "workstream-1",
        expectedVersion: 3,
        expectedServerGeneration: 1,
        expectedRegistryVersion: 0,
        sortOrder: 7,
      };
      yield* h.call("reorder_workstream", input);
      expect(h.submitted[0]!.action).toEqual({
        operation: "update_workstream",
        workstream_id: "workstream-1",
        expected_version: 3,
        name: "Preserved",
        lifecycle: "paused",
        progress: { state: "blocked", impediment: "Awaiting owner" },
        sort_order: 7,
      });
      expect(
        yield* h.call("reorder_workstream", { ...input, expectedVersion: 2 }).pipe(Effect.flip),
      ).toMatchObject({ reason: "version-conflict" });
      expect(h.submitted).toHaveLength(1);
    }),
);
it("restricts commands, order keys and listing bounds at the public input boundary", () => {
  const decode = Schema.decodeUnknownSync(OrganizationCommand);
  for (const operation of [
    "update_workstream",
    "create_workstream",
    "set_coordination_disposition",
    "register_reference",
    "verify_reference",
  ])
    expect(() => decode({ ...command, action: { ...command.action, operation } })).toThrow();
  expect(() => Schema.decodeUnknownSync(OrderKey)("a key with spaces")).toThrow();
  expect(() => Schema.decodeUnknownSync(OrderKey)("a".repeat(257))).toThrow();
  expect(() =>
    Schema.decodeUnknownSync(OrganizationToolkit.tools.list_organization_threads.parametersSchema)({
      limit: 101,
    }),
  ).toThrow();
});

it.effect("retains accepted command identity when post-dispatch readback is unavailable", () =>
  Effect.gen(function* () {
    const h = yield* makeHarness({ readbackFails: true });
    expect(yield* h.call("set_thread_pinned", { commandId, threadId, pinned: true })).toEqual({
      commandId,
      sequence: 42,
      readback: "unknown",
    });
    expect(h.commands).toHaveLength(1);
  }),
);
it.effect("reports observed native state only after a matching shell readback", () =>
  Effect.gen(function* () {
    const h = yield* makeHarness({
      thread: { ...thread, pinnedAt: thread.createdAt, pinOrderKey: "a0" },
    });
    expect(yield* h.call("set_thread_pinned", { commandId, threadId, pinned: true })).toMatchObject(
      {
        readback: "observed",
        thread: { threadId, pinnedAt: DateTime.formatIso(thread.createdAt) },
      },
    );
    expect(
      yield* h.call("reorder_thread", { commandId, threadId, list: "pinned", orderKey: "a0" }),
    ).toMatchObject({ readback: "observed" });
  }),
);

it.effect("retains command identity for unknown native dispatch effects without retry", () =>
  Effect.gen(function* () {
    const h = yield* makeHarness({ dispatchFails: true });
    expect(
      yield* h.call("set_thread_pinned", { commandId, threadId, pinned: true }).pipe(Effect.flip),
    ).toMatchObject({ reason: "dispatch-effect-unknown", commandId, threadId });
    expect(h.commands).toHaveLength(1);
  }),
);
it.effect("preserves gateway permission, version and idempotency failures", () =>
  Effect.gen(function* () {
    for (const reason of [
      "permission-denied",
      "version-conflict",
      "idempotency-conflict",
    ] as const) {
      const failure = new WorkstreamGatewayError({ reason, detail: "bounded synthetic rejection" });
      const h = yield* makeHarness({ gatewayFailure: failure });
      expect(yield* h.call("submit_workstream_command", command).pipe(Effect.flip)).toBe(failure);
      expect(h.submitted).toHaveLength(1);
    }
  }),
);
it("accepts precisely the organizational membership action shapes", () => {
  const decode = Schema.decodeUnknownSync(OrganizationCommand);
  for (const action of [
    command.action,
    { ...command.action, operation: "reattach_primary" },
    {
      operation: "remove_membership",
      workstream_id: "workstream-1",
      expected_version: 1,
      membership_id: "membership-1",
    },
    {
      operation: "move_primary",
      source_workstream_id: "workstream-1",
      expected_source_version: 1,
      source_membership_id: "membership-1",
      destination_workstream_id: "workstream-2",
      expected_destination_version: 2,
    },
  ]) {
    expect(decode({ ...command, action }).action).toEqual(action);
  }
});
