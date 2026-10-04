import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  RunId,
  RuntimeRequestId,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2ThreadShell,
  type WorkstreamCommand,
  type WorkstreamDetail,
  type WorkstreamReceipt,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type { Tool } from "effect/unstable/ai";
import { McpInvocationContext, type McpCapability } from "../../McpInvocationContext.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import { OrchestratorProjectionError } from "../../../orchestration-v2/Orchestrator.ts";
import { ServerConfig } from "../../../config.ts";
import { HttpServer } from "effect/unstable/http";
import * as NetAddress from "effect/unstable/net/NetAddress";
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
  createdBy: "user",
  creationSource: "web",
  projectId: ProjectId.make("project-1"),
  title: "Thread",
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  pullRequests: [],
  providerInstanceId: ProviderInstanceId.make("codex"),
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
  forkedFrom: null,
  activeProviderThreadId: null,
  latestRunId: null,
  activeRunId: null,
  status: "idle",
  pendingRuntimeRequest: null,
  latestVisibleMessage: null,
  pendingBackgroundTasks: [],
  providerInstanceHistory: [],
  itemCount: 0,
  visibleItemCount: 0,
  deletedAt: null,
  createdAt: DateTime.makeUnsafe("2026-08-01T00:00:00.000Z"),
  updatedAt: DateTime.makeUnsafe("2026-08-01T00:00:00.000Z"),
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  latestUserMessageAt: null,
  hasActionableProposedPlan: false,
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
    threads?: ReadonlyArray<OrchestrationV2ThreadShell>;
    archivedThreads?: ReadonlyArray<OrchestrationV2ThreadShell>;
    snapshotSequences?: ReadonlyArray<number>;
    shellFailure?: boolean;
    address?: NetAddress.SocketAddress;
    readbackFails?: boolean;
    dispatchFails?: boolean;
  } = {},
) {
  const commands: OrchestrationV2ServerCommand[] = [];
  const submitted: WorkstreamCommand[] = [];
  let placementCalls = 0;
  let shellCalls = 0;
  const target = options.thread === undefined ? thread : options.thread;
  const dependencies = Layer.mergeAll(
    Layer.mock(ServerConfig)({ baseDir: "/effective/t3-home" }),
    Layer.mock(HttpServer.HttpServer)({ address: options.address ?? NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43123) }),
    Layer.mock(ThreadManagementService)({
      getThreadShell: (id) =>
        options.readbackFails && commands.length > 0
          ? Effect.fail(new OrchestratorProjectionError({ threadId, cause: "synthetic-readback" }))
          : Effect.succeed(id === threadId ? target : null),
      getShellSnapshot: () =>
        Effect.gen(function* () {
          shellCalls++;
          if (options.shellFailure) return yield* new OrchestratorProjectionError({ threadId, cause: "PRIVATE SQL" });
          return {
            schemaVersion: 2,
            snapshotSequence: options.snapshotSequences?.[shellCalls - 1] ?? 1,
            threads: options.threads ?? (target ? [{ ...target, messages: [{ text: "PRIVATE CONTENT" }] }] : []),
            archivedThreads: options.archivedThreads ?? [],
          };
        }),
      dispatch: (input) =>
        Effect.gen(function* () {
          commands.push(input);
          if (options.dispatchFails)
            return yield* Effect.fail(new OrchestratorProjectionError({ threadId, cause: "synthetic-dispatch" }));
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
      readDetail: () => Effect.succeed(options.detail!),
      pollCommand: () => Effect.succeed(options.receipt!),
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
  ) =>
    toolkit.handle(name, input).pipe(
      Stream.unwrap,
      Stream.runCollect,
      Effect.map(
        (results) =>
          results.at(-1)!.result as Tool.Success<(typeof OrganizationToolkit.tools)[Name]>,
      ),
      Effect.provideService(McpInvocationContext, {
        environmentId: EnvironmentId.make("environment-1"),
        threadId,
        providerInstanceId: ProviderInstanceId.make("codex"),
        providerSessionId: "session-1",
        issuedAt: 1,
        capabilities: new Set(capabilities),
      }),
      Effect.provide(dependencies),
    );
  return { call, commands, submitted, counts: () => ({ placementCalls, shellCalls }) };
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
      thread: { pinnedAt: DateTime.formatIso(thread.createdAt), pinOrderKey: "a2", activeOrderKey: "a0" },
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
      { readback: "observed", thread: { threadId, pinnedAt: DateTime.formatIso(thread.createdAt) } },
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


it.effect("returns the invocation's environment and effective directory with a proved origin", () =>
  Effect.gen(function* () {
    const h = yield* makeHarness();
    expect(yield* h.call("get_invocation_context", {})).toMatchObject({
      environmentId: "environment-1", threadId, effectiveBaseDir: "/effective/t3-home",
      loopbackOrigin: "http://127.0.0.1:43123", serverGeneration: null,
    });
    expect(h.counts().shellCalls).toBe(0);
    expect(yield* h.call("get_invocation_context", {}, []).pipe(Effect.flip)).toMatchObject({
      _tag: "McpCapabilityUnavailableError",
    });
  }),
);
it.effect("reports no origin for a non-loopback binding or an unbound port", () =>
  Effect.gen(function* () {
    for (const address of [
      NetAddress.inetAddressFromIpStringUnsafe("192.0.2.1", 43123),
      NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 0),
      NetAddress.inetAddressFromStringUnsafe("[::1%2]:43123"),
      NetAddress.unixPathAddress("/synthetic/server.sock"),
    ]) {
      const h = yield* makeHarness({ address });
      expect((yield* h.call("get_invocation_context", {})).loopbackOrigin).toBeNull();
    }
  }),
);
it.effect("uses the bound address family for wildcard loopback origins", () =>
  Effect.gen(function* () {
    for (const [host, expectedHost] of [["0.0.0.0", "127.0.0.1"], ["::", "[::1]"]] as const) {
      const h = yield* makeHarness({ address: NetAddress.inetAddressFromIpStringUnsafe(host, 43123) });
      expect((yield* h.call("get_invocation_context", {})).loopbackOrigin).toBe(`http://${expectedHost}:43123`);
    }
  }),
);
it.effect("pages all V2 thread metadata with a watermark and rejects changed pages", () =>
  Effect.gen(function* () {
    const h = yield* makeHarness({
      threads: [{ ...thread, id: ThreadId.make("thread-2") }],
      archivedThreads: [{ ...thread, archivedAt: thread.createdAt }],
      snapshotSequences: [42, 42, 43],
    });
    const first = yield* h.call("list_organization_thread_metadata", { limit: 1 });
    expect(first.snapshotSequence).toBe(42);
    expect(first.nextOffset).toBe(1);
    expect(first.threads[0]?.threadId).toBe(threadId);
    expect(first.threads[0]?.archivedAt).toBe(DateTime.formatIso(thread.createdAt));
    const second = yield* h.call("list_organization_thread_metadata", { offset: 1, limit: 1, expectedSnapshotSequence: 42 });
    expect(second.nextOffset).toBeNull();
    expect(second.threads[0]?.threadId).toBe("thread-2");
    expect(yield* h.call("list_organization_thread_metadata", { offset: 1, expectedSnapshotSequence: 42 }).pipe(Effect.flip)).toMatchObject({
      reason: "snapshot-sequence-changed", expectedSnapshotSequence: 42, snapshotSequence: 43,
    });
  }),
);
it.effect("exposes V2 activity timestamps without conversation bodies, paths or errors", () =>
  Effect.gen(function* () {
    const h = yield* makeHarness({ thread: {
      ...thread, latestRunId: RunId.make("run-1"), status: "completed",
      latestRunCompletedAt: thread.createdAt, worktreePath: "/PRIVATE/path", lastError: "PRIVATE ERROR",
    } });
    const result = yield* h.call("list_organization_thread_metadata", {});
    expect(result.threads[0]).toMatchObject({
      latestRunId: "run-1", activeRunId: null, status: "completed",
      latestRunCompletedAt: DateTime.formatIso(thread.createdAt), latestRunStartedAt: null,
      latestUserMessageAt: null, hasPendingApprovals: false, hasPendingUserInput: false,
    });
    expect(JSON.stringify(result)).not.toContain("PRIVATE");
    expect(result.threads[0]).not.toHaveProperty("session");
    expect(result.threads[0]).not.toHaveProperty("latestTurn");
  }),
);
it.effect("denies metadata reads before the shell and bounds local failure details", () =>
  Effect.gen(function* () {
    const h = yield* makeHarness({ shellFailure: true });
    expect(yield* h.call("list_organization_thread_metadata", {}, []).pipe(Effect.flip)).toMatchObject({ _tag: "McpCapabilityUnavailableError" });
    expect(h.counts().shellCalls).toBe(0);
    const failure = yield* h.call("list_organization_thread_metadata", {}).pipe(Effect.flip);
    expect(failure).toMatchObject({ reason: "local-operation-failed" });
    expect(failure).not.toHaveProperty("cause");
    expect(JSON.stringify(failure)).not.toContain("PRIVATE");
  }),
);

it.effect("reports the current V2 approval or question summary", () =>
  Effect.gen(function* () {
    for (const kind of ["command", "file-read", "file-change", "mcp-elicitation", "permission", "user_input", "auth_refresh"] as const) {
      const h = yield* makeHarness({ thread: { ...thread, pendingRuntimeRequest: { id: RuntimeRequestId.make("request-1"), kind, createdAt: thread.createdAt } } });
      const metadata = (yield* h.call("list_organization_thread_metadata", {})).threads[0]!;
      expect(metadata.hasPendingUserInput).toBe(kind === "user_input");
      expect(metadata.hasPendingApprovals).toBe(kind !== "user_input" && kind !== "auth_refresh");
    }
  }),
);

it("rejects target overrides and unbounded metadata page inputs", () => {
  const context = Schema.decodeUnknownSync(OrganizationToolkit.tools.get_invocation_context.parametersSchema);
  expect(context({})).toEqual({});
  expect(() => context({ threadId: "other-thread" })).toThrow();
  expect(() => context({ environmentId: "other-environment" })).toThrow();
  const page = Schema.decodeUnknownSync(OrganizationToolkit.tools.list_organization_thread_metadata.parametersSchema);
  for (const input of [{ limit: 0 }, { limit: 101 }, { offset: -1 }, { expectedSnapshotSequence: -1 }, { expectedSnapshotSequence: 1.5 }]) {
    expect(() => page(input)).toThrow();
  }
});
