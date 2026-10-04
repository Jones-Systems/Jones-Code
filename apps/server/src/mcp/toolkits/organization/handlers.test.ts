import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
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
import {
  WorkstreamGateway,
  WorkstreamGatewayError,
} from "../../../workstreams/WorkstreamGateway.ts";
import { OrchestratorProjectionError } from "../../../orchestration-v2/Orchestrator.ts";
import { OrganizationToolkitHandlersLive } from "./handlers.ts";
import { OrganizationCommand, OrganizationToolkit, OrderKey } from "./tools.ts";

const threadId = ThreadId.make("thread-1");
const commandId = CommandId.make("organization-command-1");
const thread: OrchestrationV2ThreadShell = {
  id: threadId,
  projectId: ProjectId.make("project-1"),
  title: "Thread",
  createdBy: "user",
  creationSource: "web",
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  pullRequests: [],
  providerInstanceId: ProviderInstanceId.make("codex"),
  lineage: { relationshipToParent: null, parentThreadId: null, rootThreadId: threadId },
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
  createdAt: DateTime.makeUnsafe("2026-08-01T00:00:00.000Z"),
  updatedAt: DateTime.makeUnsafe("2026-08-01T00:00:00.000Z"),
  archivedAt: null,
  deletedAt: null,
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
    Layer.mock(ThreadManagementService)({
      getThreadShell: (id) =>
        options.readbackFails && commands.length > 0
          ? Effect.fail(new OrchestratorProjectionError({ threadId, cause: "synthetic-readback" }))
          : Effect.succeed(id === threadId ? target : null),
      getShellSnapshot: () =>
        Effect.sync(() => {
          shellCalls++;
          return {
            schemaVersion: 2 as const,
            snapshotSequence: 1,
            archivedThreads: [],
            threads: target ? [{ ...target, messages: [{ text: "PRIVATE CONTENT" }] }] : [],
          };
        }),
      dispatch: (input) =>
        Effect.gen(function* () {
          commands.push(input);
          if (options.dispatchFails)
            return yield* Effect.fail(
              new OrchestratorProjectionError({ threadId, cause: "synthetic-dispatch" }),
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
      { ...thread, deletedAt: thread.createdAt },
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
