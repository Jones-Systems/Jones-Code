import { OrchestrationV2DomainEvent, ProviderRequestKind } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Hex from "effect/encoding/Hex";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { importedHistoryCanonicalJson } from "@t3tools/shared/jones/importedHistoryCanonical";
import * as Config from "../../config.ts";
import * as Projection from "../../orchestration-v2/ProjectionStore.ts";
import {
  HeldRuntimeOwner,
  bindHeldDatabaseOwner,
  assertCheckedDatabaseOwner,
  type HeldRuntimeOwnerToken,
} from "../cleanup/CheckedRuntimeOwner.ts";

export class SyntheticPresentationError extends Schema.TaggedError<SyntheticPresentationError>()(
  "SyntheticPresentationError",
  { reason: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {}
const hash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const Descriptor = Schema.Struct({
  schema: Schema.Literal("jones.private-synthetic-presentation/v1"),
  namespace: Schema.NonEmptyString,
  checkout: Schema.NonEmptyString,
  privateHome: Schema.NonEmptyString,
  database: Schema.NonEmptyString,
  projectId: Schema.NonEmptyString,
  threadIds: Schema.Array(Schema.NonEmptyString),
  eventIds: Schema.Array(Schema.NonEmptyString),
  providerIds: Schema.Array(Schema.NonEmptyString),
  source: Schema.Array(Schema.Struct({ path: Schema.NonEmptyString, sha256: hash })),
  eventsSha256: hash,
});
export type SyntheticPresentationDescriptor = typeof Descriptor.Type;
export interface SyntheticPresentationCapability {
  readonly descriptor: SyntheticPresentationDescriptor;
  readonly events: readonly OrchestrationV2DomainEvent[];
}
// There is intentionally no CLI, environment, RPC, loader or normal providing
// layer. Only the task-owned harness/test factory can supply this private value.
export class SyntheticPresentation extends Context.Reference<
  SyntheticPresentationCapability | undefined
>("t3/jones/testing/SyntheticPresentation/SyntheticPresentation", {
  defaultValue: () => undefined,
}) {}
const consumed = new WeakSet<SyntheticPresentationCapability>();
const consumedOwners = new WeakSet<HeldRuntimeOwnerToken>();
const decodeDescriptor = Schema.decodeUnknownEffect(Descriptor, { onExcessProperty: "error" });
const decodeEvent = Schema.decodeUnknownEffect(OrchestrationV2DomainEvent, {
  onExcessProperty: "error",
});
const encodeEvent = Schema.encodeEffect(OrchestrationV2DomainEvent, { onExcessProperty: "error" });
const isSyntheticPresentationError = Schema.is(SyntheticPresentationError);
const isApprovalKind = Schema.is(ProviderRequestKind);
const fail = (reason: string) => Effect.fail(new SyntheticPresentationError({ reason }));
const canonical = importedHistoryCanonicalJson;

/** Await exactly once after real recovery/readiness barriers, before activation.
 * Existing projection APIs retain their SQL fences. This installs presentation,
 * never an event log, command receipt, callback or executable obligation. */
export const installSelectedSyntheticPresentation = Effect.fn("SyntheticPresentation.install")(
  function* () {
    const selected = yield* SyntheticPresentation;
    if (selected === undefined) return;
    const heldOption = yield* Effect.serviceOption(HeldRuntimeOwner);
    if (Option.isNone(heldOption)) return yield* fail("fixture_actual_owner_or_storage_missing");
    const held = heldOption.value;
    if (consumedOwners.has(held.token)) return yield* fail("fixture_owner_already_consumed");
    consumedOwners.add(held.token);
    if (typeof selected !== "object" || selected === null)
      return yield* fail("fixture_capability_invalid");
    if (consumed.has(selected)) return yield* fail("fixture_capability_already_consumed");
    consumed.add(selected);
    const descriptor = yield* decodeDescriptor(selected.descriptor);
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const config = yield* Config.ServerConfig;
    const sqlOption = yield* Effect.serviceOption(SqlClient.SqlClient);
    const projectionOption = yield* Effect.serviceOption(Projection.ProjectionStoreV2);
    if (Option.isNone(sqlOption) || Option.isNone(projectionOption))
      return yield* fail("fixture_actual_owner_or_storage_missing");
    const sql = sqlOption.value;
    const projections = projectionOption.value;
    const crypto = yield* Digest.Digest;
    const owner = yield* bindHeldDatabaseOwner(held.token).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
    );
    const sha = (bytes: Uint8Array) =>
      crypto.digest(Digest.sha256, bytes).pipe(Effect.map(Hex.encode));
    const ids = descriptor.threadIds;
    if (
      config.devUrl === undefined ||
      config.baseDir !== descriptor.privateHome ||
      config.dbPath !== descriptor.database ||
      descriptor.database !== path.join(descriptor.privateHome, "userdata", "statev2.sqlite") ||
      ids.length < 1 ||
      ids.length > 8 ||
      new Set(ids).size !== ids.length ||
      descriptor.eventIds.length < 1 ||
      descriptor.eventIds.length > 96 ||
      new Set(descriptor.eventIds).size !== descriptor.eventIds.length ||
      descriptor.providerIds.length < 1 ||
      descriptor.providerIds.length > 8 ||
      new Set(descriptor.providerIds).size !== descriptor.providerIds.length ||
      descriptor.source.length < 1 ||
      descriptor.source.length > 256 ||
      new Set(descriptor.source.map((file) => file.path)).size !== descriptor.source.length ||
      !/^[a-z0-9-]{16,80}$/.test(descriptor.namespace) ||
      !ids.every((id) => id.startsWith(descriptor.namespace + ":"))
    )
      return yield* fail("fixture_binding_invalid");
    if (
      (yield* fs.realPath(descriptor.checkout)) !== descriptor.checkout ||
      (yield* fs.realPath(descriptor.privateHome)) !== descriptor.privateHome ||
      (yield* fs.realPath(descriptor.database)) !== descriptor.database
    )
      return yield* fail("fixture_physical_binding_changed");
    const loadedCheckout = yield* fs.realPath(
      yield* path.fromFileUrl(new URL("../../../../../", import.meta.url)),
    );
    if (
      descriptor.checkout !== loadedCheckout ||
      !descriptor.source.some(
        (file) => file.path === "apps/server/src/jones/testing/SyntheticPresentation.ts",
      ) ||
      !descriptor.source.some((file) => file.path === "apps/server/src/serverRuntimeStartup.ts")
    )
      return yield* fail("fixture_loaded_source_binding_changed");
    const homeInfo = yield* fs.stat(descriptor.privateHome);
    const markerPath = path.join(descriptor.privateHome, ".jones-synthetic-presentation.json");
    const markerInfo = yield* fs.stat(markerPath);
    if (
      homeInfo.type !== "Directory" ||
      (homeInfo.mode & 0o077) !== 0 ||
      markerInfo.type !== "File" ||
      (markerInfo.mode & 0o077) !== 0 ||
      Option.isNone(homeInfo.uid) ||
      Option.isNone(markerInfo.uid) ||
      homeInfo.uid.value !== markerInfo.uid.value ||
      (yield* fs.realPath(markerPath)) !== markerPath
    )
      return yield* fail("fixture_private_marker_invalid");
    const expectedMarker = canonical({
      schema: "jones.private-synthetic-presentation-marker/v1",
      descriptorSha256: yield* sha(new TextEncoder().encode(canonical(descriptor))),
    });
    if ((yield* fs.readFileString(markerPath)) !== expectedMarker + "\n")
      return yield* fail("fixture_private_marker_changed");
    // The harness performs the complete isolation/source preflight BEFORE server
    // construction. Recheck its exact bounded critical-source set at installation.
    for (const source of descriptor.source) {
      if (
        path.isAbsolute(source.path) ||
        source.path.split(/[\\/]/).some((part) => part === ".." || part === "." || part === "")
      )
        return yield* fail("fixture_source_path_invalid");
      const file = path.join(descriptor.checkout, source.path);
      if (
        (yield* fs.realPath(file)) !== file ||
        (yield* sha(yield* fs.readFile(file))) !== source.sha256
      )
        return yield* fail("fixture_source_changed");
    }
    const encoded = yield* Effect.forEach(selected.events, (event) => encodeEvent(event));
    const events = yield* Effect.forEach(encoded, (event) => decodeEvent(event));
    if (
      events.length !== descriptor.eventIds.length ||
      canonical(events.map((event) => event.id)) !== canonical(descriptor.eventIds) ||
      (yield* sha(new TextEncoder().encode(canonical(encoded)))) !== descriptor.eventsSha256
    )
      return yield* fail("fixture_descriptor_changed");
    const born = new Set<string>();
    const nodes = new Map<
      string,
      Extract<OrchestrationV2DomainEvent, { type: "node.updated" }>["payload"]
    >();
    const requests = new Map<
      string,
      Extract<OrchestrationV2DomainEvent, { type: "runtime-request.updated" }>["payload"]
    >();
    const items: Extract<OrchestrationV2DomainEvent, { type: "turn-item.updated" }>[] = [];
    const key = (threadId: string, id: string) => canonical([threadId, id]);
    const ownedRuns = new Map<string, Set<string>>();
    for (const event of events) {
      if (!ids.includes(event.threadId)) return yield* fail("fixture_foreign_thread");
      if (event.type === "thread.created") {
        const thread = event.payload;
        if (
          born.has(event.threadId) ||
          thread.id !== event.threadId ||
          thread.projectId !== descriptor.projectId ||
          !descriptor.providerIds.includes(thread.providerInstanceId) ||
          thread.archivedAt !== null ||
          thread.deletedAt !== null ||
          thread.worktreePath !== null ||
          thread.branch !== null ||
          thread.activeProviderThreadId !== null ||
          thread.historyOrigin !== undefined ||
          thread.selfSettlement != null ||
          thread.autoSettleDisabledAt == null ||
          !ids.includes(thread.lineage.rootThreadId) ||
          (thread.lineage.parentThreadId !== null && !born.has(thread.lineage.parentThreadId))
        )
          return yield* fail("fixture_thread_not_inert");
        born.add(thread.id);
        ownedRuns.set(thread.id, new Set());
        continue;
      }
      if (!born.has(event.threadId)) return yield* fail("fixture_birth_order_invalid");
      switch (event.type) {
        case "run.created":
          if (
            !["running", "completed"].includes(event.payload.status) ||
            event.payload.providerThreadId !== null ||
            event.payload.activeAttemptId !== null ||
            event.payload.workspacePreparation !== undefined ||
            event.payload.delegatedCompletion !== undefined ||
            event.payload.restartContinuationOfRunId !== undefined ||
            !descriptor.providerIds.includes(event.payload.providerInstanceId)
          )
            return yield* fail("fixture_run_not_inert");
          ownedRuns.get(event.threadId)!.add(event.payload.id);
          break;
        case "node.updated":
          if (
            !["user_input_request", "approval_request"].includes(event.payload.kind) ||
            event.payload.rootNodeId !== event.payload.id ||
            event.payload.parentNodeId !== null ||
            event.payload.threadId !== event.threadId ||
            event.nodeId !== event.payload.id ||
            nodes.has(key(event.threadId, event.payload.id)) ||
            (event.payload.runId !== null &&
              !ownedRuns.get(event.threadId)!.has(event.payload.runId))
          )
            return yield* fail("fixture_node_relationship_invalid");
          nodes.set(key(event.threadId, event.payload.id), event.payload);
          if (
            event.payload.countsForRun ||
            event.payload.providerThreadId !== null ||
            event.payload.providerTurnId !== null ||
            event.payload.nativeItemRef !== null ||
            event.payload.checkpointScopeId !== null ||
            !["waiting", "completed"].includes(event.payload.status)
          )
            return yield* fail("fixture_node_not_inert");
          break;
        case "runtime-request.updated":
          if (
            event.nodeId !== event.payload.nodeId ||
            requests.has(key(event.threadId, event.payload.id))
          )
            return yield* fail("fixture_request_relationship_invalid");
          requests.set(key(event.threadId, event.payload.id), event.payload);
          if (
            (event.payload.kind !== "user_input" && !isApprovalKind(event.payload.kind)) ||
            event.payload.nativeRequestRef !== null ||
            event.payload.providerTurnId !== null ||
            event.payload.responseCapability.type === "live"
          )
            return yield* fail("fixture_request_not_inert");
          break;
        case "turn-item.updated":
          items.push(event);
          if (event.payload.threadId !== event.threadId || event.nodeId !== event.payload.nodeId)
            return yield* fail("fixture_item_relationship_invalid");
          if (
            !["user_input_request", "approval_request"].includes(event.payload.type) ||
            event.payload.providerThreadId !== null ||
            event.payload.providerTurnId !== null ||
            event.payload.nativeItemRef !== null
          )
            return yield* fail("fixture_item_not_inert");
          break;
        default:
          return yield* fail("fixture_event_not_allowed");
      }
      if (event.runId !== undefined && !ownedRuns.get(event.threadId)!.has(event.runId))
        return yield* fail("fixture_run_relationship_invalid");
    }
    if (born.size !== ids.length) return yield* fail("fixture_thread_set_incomplete");
    for (const event of events) {
      if (event.type === "runtime-request.updated") {
        const node = nodes.get(key(event.threadId, event.payload.nodeId));
        if (node === undefined || node.runtimeRequestId !== event.payload.id)
          return yield* fail("fixture_request_node_missing");
      }
      if (event.type === "node.updated" && event.payload.runtimeRequestId !== null) {
        const request = requests.get(key(event.threadId, event.payload.runtimeRequestId));
        if (request === undefined || request.nodeId !== event.payload.id)
          return yield* fail("fixture_node_request_missing");
      }
    }
    const seenItems = new Set<string>();
    for (const event of items) {
      const item = event.payload;
      if (item.type !== "user_input_request" && item.type !== "approval_request")
        return yield* fail("fixture_item_not_allowed");
      const request = requests.get(key(event.threadId, item.requestId));
      if (
        seenItems.has(key(event.threadId, item.id)) ||
        request === undefined ||
        item.nodeId !== request.nodeId ||
        (item.type === "user_input_request"
          ? request.kind !== "user_input" ||
            item.responseMode !== "message" ||
            request.responseCapability.type !== "message"
          : !isApprovalKind(request.kind) || request.responseCapability.type !== "not_resumable")
      )
        return yield* fail("fixture_item_request_missing");
      seenItems.add(key(event.threadId, item.id));
    }

    const fingerprint = canonical(encoded);
    yield* Effect.try({
      try: () => assertCheckedDatabaseOwner(owner),
      catch: (cause) => new SyntheticPresentationError({ reason: "fixture_owner_changed", cause }),
    });
    yield* sql.withTransaction(
      Effect.gen(function* () {
        const projects =
          yield* sql`SELECT project_id FROM projection_projects WHERE project_id=${descriptor.projectId} AND deleted_at IS NULL`;
        if (projects.length !== 1) return yield* fail("fixture_project_missing");
        for (const table of [
          "orchestration_v2_projection_threads",
          "orchestration_v2_projection_runs",
          "orchestration_v2_projection_nodes",
          "orchestration_v2_projection_runtime_requests",
          "orchestration_v2_projection_turn_items",
          "orchestration_v2_projection_provider_threads",
          "orchestration_v2_projection_provider_turns",
          "orchestration_v2_projection_subagents",
        ]) {
          const existing = yield* sql.unsafe(
            `SELECT 1 FROM ${table} WHERE thread_id IN (${ids.map(() => "?").join(",")}) LIMIT 1`,
            ids,
          );
          if (existing.length !== 0) return yield* fail("fixture_existing_id_collision");
        }
        const history =
          yield* sql`SELECT 1 FROM orchestration_events WHERE ${sql.in("stream_id", ids)} OR ${sql.in("event_id", descriptor.eventIds)} LIMIT 1`;
        const outbox =
          yield* sql`SELECT 1 FROM orchestration_v2_effect_outbox WHERE ${sql.in("thread_id", ids)} LIMIT 1`;
        const receipts =
          yield* sql`SELECT 1 FROM orchestration_command_receipts WHERE ${sql.in("aggregate_id", ids)} LIMIT 1`;
        if (history.length || outbox.length || receipts.length)
          return yield* fail("fixture_existing_work_collision");
        for (const event of events) yield* projections.apply(event);
        if (
          canonical(yield* Effect.forEach(selected.events, (event) => encodeEvent(event))) !==
            fingerprint ||
          canonical(selected.descriptor) !== canonical(descriptor)
        )
          return yield* fail("fixture_mutated_during_installation");
        yield* Effect.try({
          try: () => assertCheckedDatabaseOwner(owner),
          catch: (cause) =>
            new SyntheticPresentationError({ reason: "fixture_owner_changed", cause }),
        });
      }),
    );
    // The transaction has committed. Validate through the actual committed
    // readers before startup may activate; a failure here leaves committed
    // presentation retained and consumes the one-shot capability.
    const expected = new Map<
      OrchestrationV2DomainEvent["threadId"],
      ReturnType<typeof Projection.emptyProjection>
    >();
    for (const event of events) {
      if (event.type === "thread.created")
        expected.set(event.threadId, Projection.emptyProjection(event));
      else {
        const prior = expected.get(event.threadId);
        if (prior === undefined) return yield* fail("fixture_committed_shape_mismatch");
        expected.set(event.threadId, Projection.applyToProjection(prior, event));
      }
    }
    const fields = [
      "runs",
      "attempts",
      "nodes",
      "subagents",
      "providerSessions",
      "providerThreads",
      "providerTurns",
      "runtimeRequests",
      "messages",
      "plans",
      "turnItems",
      "checkpointScopes",
      "checkpoints",
      "contextHandoffs",
      "contextTransfers",
    ] as const;
    const readFailure = (cause: unknown) =>
      new SyntheticPresentationError({ reason: "fixture_committed_read_failed", cause });
    for (const [threadId, projection] of expected) {
      const shell = yield* projections.getThreadShell(threadId).pipe(Effect.mapError(readFailure));
      const detail = yield* projections
        .getThreadRecords(threadId, fields)
        .pipe(Effect.mapError(readFailure));
      const expectedShell = Projection.threadShellFromProjection(projection);
      if (
        shell === null ||
        canonical({
          id: shell.id,
          projectId: shell.projectId,
          title: shell.title,
          providerInstanceId: shell.providerInstanceId,
          modelSelection: shell.modelSelection,
          runtimeMode: shell.runtimeMode,
          interactionMode: shell.interactionMode,
          lineage: shell.lineage,
          forkedFrom: shell.forkedFrom,
          branch: shell.branch,
          worktreePath: shell.worktreePath,
          activeProviderThreadId: shell.activeProviderThreadId,
          autoSettleDisabledAt: shell.autoSettleDisabledAt,
          latestRunId: shell.latestRunId,
          activeRunId: shell.activeRunId,
          status: shell.status,
          pendingRuntimeRequest: shell.pendingRuntimeRequest,
        }) !==
          canonical({
            id: expectedShell.id,
            projectId: expectedShell.projectId,
            title: expectedShell.title,
            providerInstanceId: expectedShell.providerInstanceId,
            modelSelection: expectedShell.modelSelection,
            runtimeMode: expectedShell.runtimeMode,
            interactionMode: expectedShell.interactionMode,
            lineage: expectedShell.lineage,
            forkedFrom: expectedShell.forkedFrom,
            branch: expectedShell.branch,
            worktreePath: expectedShell.worktreePath,
            activeProviderThreadId: expectedShell.activeProviderThreadId,
            autoSettleDisabledAt: expectedShell.autoSettleDisabledAt,
            latestRunId: expectedShell.latestRunId,
            activeRunId: expectedShell.activeRunId,
            status: expectedShell.status,
            pendingRuntimeRequest: expectedShell.pendingRuntimeRequest,
          }) ||
        canonical(detail.thread) !== canonical(projection.thread) ||
        fields.some(
          (field) =>
            canonical(detail[field].map(canonical).toSorted()) !==
            canonical(projection[field].map(canonical).toSorted()),
        )
      )
        return yield* fail("fixture_committed_shape_mismatch");
    }
    if (
      canonical(yield* Effect.forEach(selected.events, (event) => encodeEvent(event))) !==
        fingerprint ||
      canonical(selected.descriptor) !== canonical(descriptor)
    )
      return yield* fail("fixture_mutated_during_readback");
    yield* Effect.try({
      try: () => assertCheckedDatabaseOwner(owner),
      catch: (cause) => new SyntheticPresentationError({ reason: "fixture_owner_changed", cause }),
    });
  },
  Effect.mapError((cause) =>
    isSyntheticPresentationError(cause)
      ? cause
      : new SyntheticPresentationError({ reason: "fixture_installation_failed", cause }),
  ),
);
