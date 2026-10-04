import type {
  CommandId,
  OrchestrationV2ThreadShell,
  ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as OrganizationMetadata from "../../OrganizationMetadataMcpService.ts";
import { requireMcpCapability } from "../../McpInvocationContext.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import {
  WorkstreamGateway,
  WorkstreamGatewayError,
} from "../../../workstreams/WorkstreamGateway.ts";
import { OrganizationToolkit, OrganizationToolError, type OrganizationThread } from "./tools.ts";

const iso = (value: DateTime.Utc | null | undefined) => value == null ? null : DateTime.formatIso(value);
const organizationThread = (thread: OrchestrationV2ThreadShell): typeof OrganizationThread.Type => ({
  threadId: thread.id,
  title: thread.title.slice(0, 512),
  projectId: thread.projectId,
  pinnedAt: iso(thread.pinnedAt),
  pinOrderKey: thread.pinOrderKey ?? null,
  activeOrderKey: thread.activeOrderKey ?? null,
  snoozedUntil: iso(thread.snoozedUntil),
  settledOverride: thread.settledOverride,
  settledAt: iso(thread.settledAt),
  archivedAt: iso(thread.archivedAt),
});
const localFailure = (cause: unknown) =>
  new OrganizationToolError({ reason: "local-operation-failed", cause });
const pageInput = (input: {
  readonly limit?: number | undefined;
  readonly cursor?: string | undefined;
}) => ({
  ...(input.limit === undefined ? {} : { limit: input.limit }),
  ...(input.cursor === undefined ? {} : { cursor: input.cursor }),
});
const make = Effect.gen(function* () {
  const gateway = yield* WorkstreamGateway;
  const metadata = yield* OrganizationMetadata.OrganizationMetadataMcpService;
  const snapshots = yield* ThreadManagementService;
  const engine = snapshots;
  const authorized = <A, E, R>(operation: Effect.Effect<A, E, R>) =>
    requireMcpCapability("organization").pipe(Effect.andThen(operation));
  const requireThread = Effect.fn("OrganizationToolkit.requireThread")(function* (
    threadId: ThreadId,
    requireVisible: boolean,
  ) {
    yield* requireMcpCapability("organization");
    const thread = yield* snapshots
      .getThreadShell(threadId)
      .pipe(Effect.mapError(localFailure));
    if (thread === null)
      return yield* new OrganizationToolError({ reason: "thread-not-found", threadId });
    const value = thread;
    const now = yield* Clock.currentTimeMillis;
    if (
      value.archivedAt !== null ||
      (requireVisible &&
        (value.settledOverride === "settled" ||
          value.settledAt !== null ||
          (value.snoozedUntil != null && DateTime.toEpochMillis(value.snoozedUntil) > now)))
    ) {
      return yield* new OrganizationToolError({ reason: "thread-parked", threadId });
    }
    return value;
  });
  const readback = Effect.fn("OrganizationToolkit.readback")(function* (
    threadId: ThreadId,
    commandId: CommandId,
    sequence: number,
    matches: (thread: OrchestrationV2ThreadShell) => boolean,
  ) {
    return yield* snapshots.getThreadShell(threadId).pipe(
      Effect.map((thread) =>
        thread === null
          ? { commandId, sequence, readback: "unknown" as const }
          : {
              commandId,
              sequence,
              readback: matches(thread) ? ("observed" as const) : ("pending" as const),
              thread: organizationThread(thread),
            },
      ),
      Effect.catch(() => Effect.succeed({ commandId, sequence, readback: "unknown" as const })),
    );
  });
  return OrganizationToolkit.of({
    get_invocation_context: () => metadata.getInvocationContext,
    list_organization_thread_metadata: (input) => metadata.listThreadMetadata(input).pipe(
      Effect.mapError((error) => error._tag === "OrganizationMetadataError"
        ? new OrganizationToolError({ reason: error.reason,
            ...(error.expectedSnapshotSequence === undefined ? {} : { expectedSnapshotSequence: error.expectedSnapshotSequence }),
            ...(error.snapshotSequence === undefined ? {} : { snapshotSequence: error.snapshotSequence }) })
        : error),
    ),
    list_organization_threads: (input) =>
      authorized(
        Effect.gen(function* () {
          const scope = yield* requireMcpCapability("organization");
          const snapshot = yield* snapshots.getShellSnapshot().pipe(Effect.mapError(localFailure));
          const start = input.offset ?? 0;
          const end = start + (input.limit ?? 50);
          return {
            environmentId: scope.environmentId,
            threads: snapshot.threads.slice(start, end).map(organizationThread),
            nextOffset: end < snapshot.threads.length ? end : null,
          };
        }),
      ),
    list_workstreams: (input) => authorized(gateway.readMetadata(pageInput(input))),
    list_workstream_references: (input) => authorized(gateway.readReferences(pageInput(input))),
    read_workstream: (input) =>
      authorized(
        Effect.gen(function* () {
          const detail = yield* gateway.readDetail(input.workstreamId);
          const memberships = yield* gateway.readMemberships(input.workstreamId, pageInput(input));
          return { detail, memberships };
        }),
      ),
    list_thread_placements: (input) => authorized(gateway.readThreadPlacements(input)),
    submit_workstream_command: (input) => authorized(gateway.submit(input)),
    read_workstream_command: (input) => authorized(gateway.pollCommand(input.commandId)),
    reorder_workstream: (input) =>
      authorized(
        Effect.gen(function* () {
          const { workstream } = yield* gateway.readDetail(input.workstreamId);
          if (workstream.version !== input.expectedVersion)
            return yield* new WorkstreamGatewayError({
              reason: "version-conflict",
              detail: "Workstream changed before reorder.",
            });
          return yield* gateway.submit({
            command_id: input.commandId,
            expected_server_generation: input.expectedServerGeneration,
            expected_registry_version: input.expectedRegistryVersion,
            action: {
              operation: "update_workstream",
              workstream_id: input.workstreamId,
              expected_version: input.expectedVersion,
              name: workstream.name,
              lifecycle: workstream.lifecycle,
              progress: workstream.progress,
              sort_order: input.sortOrder,
            },
          });
        }),
      ),
    set_thread_pinned: (input) =>
      Effect.gen(function* () {
        yield* requireThread(input.threadId, input.pinned);
        const result = yield* engine
          .dispatch(
            input.pinned
              ? {
                  type: "thread.pin",
                  commandId: input.commandId,
                  threadId: input.threadId,
                  ...(input.orderKey === undefined ? {} : { orderKey: input.orderKey }),
                }
              : { type: "thread.unpin", commandId: input.commandId, threadId: input.threadId },
          )
          .pipe(
            Effect.mapError(
              (cause) =>
                new OrganizationToolError({
                  reason: "dispatch-effect-unknown",
                  threadId: input.threadId,
                  commandId: input.commandId,
                  cause,
                }),
            ),
          );
        return yield* readback(
          input.threadId,
          input.commandId,
          result.sequence,
          (current) => (current.pinnedAt != null) === input.pinned,
        );
      }),
    reorder_thread: (input) =>
      Effect.gen(function* () {
        const thread = yield* requireThread(input.threadId, true);
        if (input.list === "pinned" && thread.pinnedAt == null)
          return yield* new OrganizationToolError({
            reason: "pin-state-mismatch",
            threadId: input.threadId,
          });
        const result = yield* engine
          .dispatch({
            type: input.list === "pinned" ? "thread.pin.reorder" : "thread.active.reorder",
            commandId: input.commandId,
            threadId: input.threadId,
            orderKey: input.orderKey,
          })
          .pipe(
            Effect.mapError(
              (cause) =>
                new OrganizationToolError({
                  reason: "dispatch-effect-unknown",
                  threadId: input.threadId,
                  commandId: input.commandId,
                  cause,
                }),
            ),
          );
        return yield* readback(
          input.threadId,
          input.commandId,
          result.sequence,
          (current) =>
            (input.list === "pinned" ? current.pinOrderKey : current.activeOrderKey) ===
            input.orderKey,
        );
      }),
  });
});
export const OrganizationToolkitHandlersLive = OrganizationToolkit.toLayer(make).pipe(
  Layer.provide(OrganizationMetadata.layer),
);
