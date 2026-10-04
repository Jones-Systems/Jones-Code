import type {
  CommandId,
  OrchestrationV2ThreadShell,
  OrganizationThreadMetadata,
  ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import { HttpServer } from "effect/unstable/http";
import * as NetAddress from "effect/unstable/net/NetAddress";
import packageJson from "../../../../package.json" with { type: "json" };
import { ServerConfig } from "../../../config.ts";
import { requireMcpCapability } from "../../McpInvocationContext.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import {
  WorkstreamGateway,
  WorkstreamGatewayError,
} from "../../../workstreams/WorkstreamGateway.ts";
import { OrganizationToolkit, OrganizationToolError, type OrganizationThread } from "./tools.ts";

const iso = (value: DateTime.Utc | null | undefined): string | null =>
  value == null ? null : DateTime.formatIso(value);

const organizationThread = (
  thread: OrchestrationV2ThreadShell,
): typeof OrganizationThread.Type => ({
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
const organizationThreadMetadata = (
  thread: OrchestrationV2ThreadShell,
  counts: NonNullable<OrchestrationV2ThreadShell["pendingRequestCounts"]>,
): OrganizationThreadMetadata => ({
  ...organizationThread(thread),
  projectId: thread.projectId,
  createdAt: DateTime.formatIso(thread.createdAt),
  projectionUpdatedAt: DateTime.formatIso(thread.updatedAt),
  latestUserMessageAt: iso(thread.latestUserMessageAt),
  // A V2 run is not a historical provider turn or provider session.
  latestTurn: null,
  session: null,
  hasPendingApprovals: counts.approval > 0,
  hasPendingUserInput: counts.userInput > 0,
  hasActionableProposedPlan: thread.hasActionableProposedPlan,
  backgroundLiveness: "unknown",
  v2Activity: {
    latestRunId: thread.latestRunId,
    status: thread.status,
    latestRunRequestedAt: iso(thread.latestRunRequestedAt),
    latestRunStartedAt: iso(thread.latestRunStartedAt),
    latestRunCompletedAt: iso(thread.latestRunCompletedAt),
    activeRunId: thread.activeRunId,
    activityRunStatus: thread.activityRunStatus ?? null,
    activityRunStartedAt: iso(thread.activityRunStartedAt),
  },
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
  const threads = yield* ThreadManagement.ThreadManagementService;
  const authorized = <A, E, R>(operation: Effect.Effect<A, E, R>) =>
    requireMcpCapability("organization").pipe(Effect.andThen(operation));
  const requireThread = Effect.fn("OrganizationToolkit.requireThread")(function* (
    threadId: ThreadId,
    requireVisible: boolean,
  ) {
    yield* requireMcpCapability("organization");
    const thread = yield* threads.getThreadShell(threadId).pipe(Effect.mapError(localFailure));
    if (thread === null)
      return yield* new OrganizationToolError({ reason: "thread-not-found", threadId });
    const value = thread;
    const now = yield* Clock.currentTimeMillis;
    if (
      value.deletedAt !== null ||
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
    return yield* threads.getThreadShell(threadId).pipe(
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
    get_invocation_context: () =>
      Effect.gen(function* () {
        const scope = yield* requireMcpCapability("organization");
        const config = yield* ServerConfig;
        const { address } = yield* HttpServer.HttpServer;
        let loopbackOrigin: string | null = null;
        if (
          NetAddress.isInetAddress(address) &&
          address.port > 0 &&
          (!NetAddress.isInetAddressV6(address) || address.scopeId === 0) &&
          (NetAddress.isLoopback(address.address) || NetAddress.isUnspecified(address.address))
        ) {
          const host = NetAddress.isUnspecified(address.address)
            ? NetAddress.isIpv4Address(address.address)
              ? NetAddress.ipv4Loopback
              : NetAddress.ipv6Loopback
            : address.address;
          loopbackOrigin = `http://${NetAddress.formatUrlHost(host)}:${address.port}`;
        }
        return {
          environmentId: scope.environmentId,
          threadId: scope.threadId,
          effectiveBaseDir: config.baseDir,
          loopbackOrigin,
          serverVersion: packageJson.version,
          serverGeneration: null,
        };
      }),
    list_organization_thread_metadata: (input) =>
      Effect.gen(function* () {
        const scope = yield* requireMcpCapability("organization");
        const snapshot = yield* threads
          .getShellSnapshot()
          .pipe(
            Effect.mapError(() => new OrganizationToolError({ reason: "local-operation-failed" })),
          );
        if (
          input.expectedSnapshotSequence !== undefined &&
          input.expectedSnapshotSequence !== snapshot.snapshotSequence
        ) {
          return yield* new OrganizationToolError({
            reason: "snapshot-sequence-changed",
            expectedSnapshotSequence: input.expectedSnapshotSequence,
            snapshotSequence: snapshot.snapshotSequence,
          });
        }
        const start = input.offset ?? 0;
        const end = start + (input.limit ?? 50);
        const page = snapshot.threads.slice(start, end);
        const metadata: OrganizationThreadMetadata[] = [];
        for (const thread of page) {
          const counts = thread.pendingRequestCounts;
          if (counts === undefined) {
            return yield* new OrganizationToolError({ reason: "local-operation-failed" });
          }
          metadata.push(organizationThreadMetadata(thread, counts));
        }
        const observedAt = DateTime.formatIso(yield* DateTime.now);
        return {
          environmentId: scope.environmentId,
          snapshotSequence: snapshot.snapshotSequence,
          observedAt,
          threads: metadata,
          nextOffset: end < snapshot.threads.length ? end : null,
        };
      }),
    list_organization_threads: (input) =>
      authorized(
        Effect.gen(function* () {
          const scope = yield* requireMcpCapability("organization");
          const snapshot = yield* threads.getShellSnapshot().pipe(Effect.mapError(localFailure));
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
        const result = yield* threads
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
        const result = yield* threads
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
export const OrganizationToolkitHandlersLive = OrganizationToolkit.toLayer(make);
