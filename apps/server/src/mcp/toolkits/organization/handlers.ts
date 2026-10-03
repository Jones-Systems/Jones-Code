import type {
  CommandId,
  OrchestrationThreadShell,
  OrganizationThreadMetadata,
  ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { HttpServer } from "effect/unstable/http";
import * as NetAddress from "effect/unstable/net/NetAddress";
import packageJson from "../../../../package.json" with { type: "json" };
import { ServerConfig } from "../../../config.ts";
import { requireMcpCapability } from "../../McpInvocationContext.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import {
  WorkstreamGateway,
  WorkstreamGatewayError,
} from "../../../workstreams/WorkstreamGateway.ts";
import { OrganizationToolkit, OrganizationToolError, type OrganizationThread } from "./tools.ts";

const organizationThread = (thread: OrchestrationThreadShell): typeof OrganizationThread.Type => ({
  threadId: thread.id,
  title: thread.title.slice(0, 512),
  projectId: thread.projectId,
  pinnedAt: thread.pinnedAt ?? null,
  pinOrderKey: thread.pinOrderKey ?? null,
  activeOrderKey: thread.activeOrderKey ?? null,
  snoozedUntil: thread.snoozedUntil ?? null,
  settledOverride: thread.settledOverride,
  settledAt: thread.settledAt,
  archivedAt: thread.archivedAt,
});
const organizationThreadMetadata = (
  thread: OrchestrationThreadShell,
): OrganizationThreadMetadata => ({
  threadId: thread.id,
  title: thread.title.slice(0, 512),
  projectId: thread.projectId,
  pinnedAt: thread.pinnedAt ?? null,
  pinOrderKey: thread.pinOrderKey ?? null,
  activeOrderKey: thread.activeOrderKey ?? null,
  snoozedUntil: thread.snoozedUntil ?? null,
  settledOverride: thread.settledOverride,
  settledAt: thread.settledAt,
  archivedAt: thread.archivedAt,
  createdAt: thread.createdAt,
  projectionUpdatedAt: thread.updatedAt,
  latestUserMessageAt: thread.latestUserMessageAt,
  latestTurn:
    thread.latestTurn === null
      ? null
      : {
          turnId: thread.latestTurn.turnId,
          state: thread.latestTurn.state,
          requestedAt: thread.latestTurn.requestedAt,
          startedAt: thread.latestTurn.startedAt,
          completedAt: thread.latestTurn.completedAt,
        },
  session:
    thread.session === null
      ? null
      : {
          status: thread.session.status,
          activeTurnId: thread.session.activeTurnId,
          updatedAt: thread.session.updatedAt,
        },
  hasPendingApprovals: thread.hasPendingApprovals,
  hasPendingUserInput: thread.hasPendingUserInput,
  hasActionableProposedPlan: thread.hasActionableProposedPlan,
  backgroundLiveness:
    thread.backgroundLiveness === undefined ? "unknown" : thread.backgroundLiveness,
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
  const snapshots = yield* ProjectionSnapshotQuery;
  const engine = yield* OrchestrationEngineService;
  const authorized = <A, E, R>(operation: Effect.Effect<A, E, R>) =>
    requireMcpCapability("organization").pipe(Effect.andThen(operation));
  const requireThread = Effect.fn("OrganizationToolkit.requireThread")(function* (
    threadId: ThreadId,
    requireVisible: boolean,
  ) {
    yield* requireMcpCapability("organization");
    const thread = yield* snapshots
      .getThreadShellById(threadId)
      .pipe(Effect.mapError(localFailure));
    if (Option.isNone(thread))
      return yield* new OrganizationToolError({ reason: "thread-not-found", threadId });
    const value = thread.value;
    const now = yield* Clock.currentTimeMillis;
    if (
      value.archivedAt !== null ||
      (requireVisible &&
        (value.settledOverride === "settled" ||
          value.settledAt !== null ||
          (value.snoozedUntil != null && Date.parse(value.snoozedUntil) > now)))
    ) {
      return yield* new OrganizationToolError({ reason: "thread-parked", threadId });
    }
    return value;
  });
  const readback = Effect.fn("OrganizationToolkit.readback")(function* (
    threadId: ThreadId,
    commandId: CommandId,
    sequence: number,
    matches: (thread: OrchestrationThreadShell) => boolean,
  ) {
    return yield* snapshots.getThreadShellById(threadId).pipe(
      Effect.map((thread) =>
        Option.isNone(thread)
          ? { commandId, sequence, readback: "unknown" as const }
          : {
              commandId,
              sequence,
              readback: matches(thread.value) ? ("observed" as const) : ("pending" as const),
              thread: organizationThread(thread.value),
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
        const snapshot = yield* snapshots
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
        const observedAt = DateTime.formatIso(yield* DateTime.now);
        return {
          environmentId: scope.environmentId,
          snapshotSequence: snapshot.snapshotSequence,
          observedAt,
          threads: snapshot.threads.slice(start, end).map(organizationThreadMetadata),
          nextOffset: end < snapshot.threads.length ? end : null,
        };
      }),
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
export const OrganizationToolkitHandlersLive = OrganizationToolkit.toLayer(make);
