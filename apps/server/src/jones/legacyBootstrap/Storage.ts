import {
  type CommandId,
  type EventId,
  type RunId,
  type ThreadId,
  OrchestrationV2DomainEventJson,
  OrchestrationV2PrivateEvent,
  QueueDispatchCommand,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { CommandReceiptV2 } from "../../orchestration-v2/CommandReceiptStore.ts";
import {
  RecordedAppThreadJson,
  RecordedRunJson,
  RecordedEventJson,
  RecordedStoredLifecycleEvent,
} from "../../orchestration-v2/RecordedTypes.ts";
import {
  legacyBootstrapCreateCommandId,
  legacyPayloadHash,
} from "../../orchestration-v2/LegacyBootstrap.ts";
import {
  makeJonesTablePresence,
  type JonesTablePresence,
} from "../persistence/JonesTablePresence.ts";
import * as M from "./RecordedMetadata.ts";
import * as S from "./StorageSchema.ts";

const names = ["threads", "runs", "event_metadata", "commands", "terminal_cleanup"].map(
  (name) => `jones_legacy_bootstrap_${name}`,
);
const error = (reason: string) => Effect.fail(new S.LegacyStorageError({ reason }));
const isLegacyStorageError = Schema.is(S.LegacyStorageError);
const normalize = (cause: unknown) =>
  isLegacyStorageError(cause)
    ? cause
    : new S.LegacyStorageError({ reason: "storage_failure", cause });
const json = <A extends Schema.Top>(schema: A) => Schema.fromJsonString(schema);
const decodeThread = Schema.decodeUnknownEffect(json(RecordedAppThreadJson));
const decodeRun = Schema.decodeUnknownEffect(json(RecordedRunJson));
const receiptFields = CommandReceiptV2.mapFields((fields) => ({
  ...fields,
  acceptedAt: Schema.DateTimeUtcFromString,
}));
const receiptJson = json(receiptFields);
const decodeTerminal = Schema.decodeUnknownEffect(S.TerminalCleanup);
const decodePrivateEvent = Schema.decodeUnknownEffect(OrchestrationV2PrivateEvent);
const decodePreflightReceipt = Schema.decodeUnknownEffect(S.LegacyPreflightReceipt);
const decodeRecord = Schema.decodeUnknownEffect(S.CommandRecord);
const decodeCommand = Schema.decodeUnknownEffect(json(S.CommandRecordJson), {
  onExcessProperty: "error",
});
const encodeCommand = Schema.encodeEffect(json(S.CommandRecordJson));
const MetadataRow = Schema.Struct({
  identity_json: Schema.String,
  metadata_json: Schema.String,
  revision: Schema.Number,
});
type MetadataRow = typeof MetadataRow.Type;
const decodeRow = Schema.decodeUnknownEffect(MetadataRow);
const EventRow = Schema.Struct({
  sequence: Schema.Number,
  event_id: Schema.String,
  command_id: Schema.NullOr(Schema.String),
  stream_id: Schema.String,
  event_type: Schema.String,
  occurred_at: Schema.String,
  payload_json: Schema.String,
  metadata_json: Schema.String,
});
type EventRow = typeof EventRow.Type;
const decodeEventRow = Schema.decodeUnknownEffect(EventRow);
const decodeBinding = Schema.decodeEffect(S.CommandBinding, { onExcessProperty: "error" });
const decodeThreadIdentity = Schema.decodeEffect(S.ThreadIdentity, { onExcessProperty: "error" });
const decodeThreadMetadata = Schema.decodeEffect(S.ThreadMetadata, { onExcessProperty: "error" });
const decodeRunIdentity = Schema.decodeEffect(S.RunIdentity, { onExcessProperty: "error" });
const decodeRunMetadata = Schema.decodeEffect(S.RunMetadata, { onExcessProperty: "error" });
const decodeEventMetadata = Schema.decodeEffect(S.EventMetadata, { onExcessProperty: "error" });
const decodeCommandId = Schema.decodeEffect(S.CommandBinding.fields.commandId);
const decodeThreadId = Schema.decodeEffect(S.ThreadIdentity.fields.threadId);
const decodeRunId = Schema.decodeEffect(S.RunIdentity.fields.runId);
const decodePrivateEventInput = Schema.decodeEffect(OrchestrationV2PrivateEvent, {
  onExcessProperty: "error",
});
const decodeThreadIdentityJson = Schema.decodeEffect(json(S.ThreadIdentity), {
  onExcessProperty: "error",
});
const decodeThreadMetadataJson = Schema.decodeEffect(json(S.ThreadMetadata), {
  onExcessProperty: "error",
});
const decodeRunIdentityJson = Schema.decodeEffect(json(S.RunIdentity), {
  onExcessProperty: "error",
});
const decodeRunMetadataJson = Schema.decodeEffect(json(S.RunMetadata), {
  onExcessProperty: "error",
});
const decodeEventMetadataJson = Schema.decodeEffect(json(S.EventMetadata), {
  onExcessProperty: "error",
});
const decodeDeleteBindingJson = Schema.decodeEffect(json(S.DeleteCommandBinding), {
  onExcessProperty: "error",
});
const decodeUnknownJson = Schema.decodeEffect(json(Schema.Unknown));
const decodeQueueCommandJson = Schema.decodeEffect(json(QueueDispatchCommand));
const encodeReceipt = Schema.encodeEffect(receiptJson);
const decodeReceiptStatus = Schema.decodeUnknownEffect(Schema.Literals(["accepted", "rejected"]));
const decodeReceipt = Schema.decodeEffect(receiptFields);

// Bind the same public payload encoded by appendAgentEvents.
const publicEvent = Schema.encodeEffect(OrchestrationV2DomainEventJson);
const eventRunId = (stored: RecordedStoredLifecycleEvent) =>
  stored.event.type === "run.created" || stored.event.type === "run.updated"
    ? stored.event.payload.id
    : stored.event.runId;
const storedEvent = Schema.decodeUnknownEffect(RecordedStoredLifecycleEvent);
const eventJson = Schema.decodeUnknownEffect(RecordedEventJson);
const parseObject = Schema.decodeUnknownEffect(json(Schema.Record(Schema.String, Schema.Unknown)));
const rowEvent = Effect.fn("LegacyStorage.rowEvent")(function* (row: EventRow) {
  const metadata = yield* parseObject(row.metadata_json);
  const event = yield* eventJson({
    ...metadata,
    id: row.event_id,
    threadId: row.stream_id,
    type: row.event_type,
    occurredAt: row.occurred_at,
    payload: yield* parseObject(row.payload_json),
  });
  return { sequence: row.sequence, commandId: row.command_id, event };
});

// Read operations are optional-schema compatible. The fixed writer is captured inside EventSink's transaction.
export const make = (
  sql: SqlClient.SqlClient,
  tables: JonesTablePresence = makeJonesTablePresence(sql),
) => {
  const available = Effect.forEach(names, tables).pipe(
    Effect.map((found) => found.every(Boolean)),
    Effect.mapError(normalize),
  );
  const requireSchema = available.pipe(
    Effect.flatMap((found) => (found ? Effect.void : error("schema_unavailable"))),
  );
  const readPublicThread = Effect.fn("LegacyStorage.readPublicThread")(function* (
    threadId: ThreadId,
  ) {
    const [row] = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId}`;
    if (row === undefined) return undefined;
    const thread = yield* decodeThread(row.payload_json);
    if (thread.id !== threadId) return yield* error("thread_row_identity_conflict");
    return thread;
  }, Effect.mapError(normalize));
  const readPublicRun = Effect.fn("LegacyStorage.readPublicRun")(function* (runId: RunId) {
    const [row] = yield* sql<{
      thread_id: string;
      payload_json: string;
    }>`SELECT thread_id, payload_json FROM orchestration_v2_projection_runs WHERE run_id = ${runId}`;
    if (row === undefined) return undefined;
    const run = yield* decodeRun(row.payload_json);
    if (run.id !== runId || run.threadId !== row.thread_id)
      return yield* error("run_row_identity_conflict");
    return run;
  }, Effect.mapError(normalize));
  const threadSidecar = Effect.fn("LegacyStorage.threadSidecar")(function* (threadId: ThreadId) {
    if (!(yield* tables(names[0]!))) return undefined;
    const [raw] =
      yield* sql`SELECT identity_json,metadata_json,revision FROM jones_legacy_bootstrap_threads WHERE thread_id = ${threadId}`;
    if (raw === undefined) return undefined;
    const row = yield* decodeRow(raw);
    const identity = yield* decodeThreadIdentityJson(row.identity_json);
    const metadata = yield* decodeThreadMetadataJson(row.metadata_json);
    if (
      identity.threadId !== threadId ||
      !Number.isSafeInteger(row.revision) ||
      row.revision < 0 ||
      !M.hasMetadata(metadata)
    )
      return yield* error("corrupt_thread_metadata");
    return { identity, metadata, revision: row.revision };
  }, Effect.mapError(normalize));
  const runSidecar = Effect.fn("LegacyStorage.runSidecar")(function* (runId: RunId) {
    if (!(yield* tables(names[1]!))) return undefined;
    const [raw] = yield* sql<
      { thread_id: string; metadata_event_id: string; metadata_sequence: number } & MetadataRow
    >`SELECT thread_id,metadata_event_id,metadata_sequence,identity_json,metadata_json,revision FROM jones_legacy_bootstrap_runs WHERE run_id = ${runId}`;
    if (raw === undefined) return undefined;
    const identity = yield* decodeRunIdentityJson(raw.identity_json);
    const metadata = yield* decodeRunMetadataJson(raw.metadata_json);
    if (
      identity.runId !== runId ||
      identity.threadId !== raw.thread_id ||
      !Number.isSafeInteger(raw.revision) ||
      raw.revision < 0 ||
      !M.hasMetadata(metadata)
    )
      return yield* error("corrupt_run_metadata");
    if (
      !Number.isSafeInteger(raw.metadata_sequence) ||
      raw.metadata_sequence < identity.birthSequence ||
      raw.metadata_event_id.length === 0
    )
      return yield* error("corrupt_run_causality");
    return {
      identity,
      metadata,
      revision: raw.revision,
      metadataEventId: raw.metadata_event_id,
      metadataSequence: raw.metadata_sequence,
    };
  }, Effect.mapError(normalize));
  const readThread = Effect.fn("LegacyStorage.readThread")(function* (threadId: ThreadId) {
    const thread = yield* readPublicThread(threadId);
    if (thread === undefined) return undefined;
    const retained = yield* threadSidecar(threadId);
    return { thread: yield* M.mergeThread(thread, retained), revision: retained?.revision ?? null };
  }, Effect.mapError(normalize));
  const readRun = Effect.fn("LegacyStorage.readRun")(function* (runId: RunId) {
    const run = yield* readPublicRun(runId);
    if (run === undefined) return undefined;
    const parent = yield* readThread(run.threadId);
    if (parent === undefined) return undefined;
    const retained = yield* runSidecar(runId);
    const merged = yield* M.mergeRun(run, retained);
    if (merged.legacyBootstrap !== undefined) {
      const { runId: _runId, ...expectedClaim } = merged.legacyBootstrap;
      if (M.canonical(parent.thread.legacyBootstrapClaim) !== M.canonical(expectedClaim))
        return yield* error("run_claim_incarnation_conflict");
    }
    return { run: merged, revision: retained?.revision ?? null };
  }, Effect.mapError(normalize));
  const actualEvent = Effect.fn("LegacyStorage.actualEvent")(function* (eventId: EventId) {
    const [raw] =
      yield* sql`SELECT sequence,event_id,command_id,stream_id,event_type,occurred_at,payload_json,metadata_json FROM orchestration_events
      WHERE event_id = ${eventId} AND aggregate_kind = 'thread' AND application_event_version = 2`;
    return raw === undefined ? undefined : yield* rowEvent(yield* decodeEventRow(raw));
  }, Effect.mapError(normalize));
  const ordinaryReceipt = Effect.fn("LegacyStorage.ordinaryReceipt")(function* (
    commandId: CommandId,
  ) {
    const [row] = yield* sql<{
      aggregate_kind: string;
      aggregate_id: string;
      command_type: string;
      accepted_at: string;
      result_sequence: number;
      status: string;
      error: string | null;
    }>`SELECT aggregate_kind,aggregate_id,command_type,accepted_at,result_sequence,status,error FROM orchestration_command_receipts WHERE command_id = ${commandId}`;
    if (row === undefined) return undefined;
    if (row.aggregate_kind !== "thread") return yield* error("foreign_command_collision");
    return yield* decodeReceipt({
      commandId,
      threadId: row.aggregate_id,
      commandType: row.command_type,
      acceptedAt: row.accepted_at,
      resultSequence: row.result_sequence,
      status: yield* decodeReceiptStatus(row.status),
      error: row.error,
    });
  }, Effect.mapError(normalize));
  const hasOrdinaryReceipt = Effect.fn("LegacyStorage.hasOrdinaryReceipt")(function* (
    commandId: CommandId,
  ) {
    return (
      (yield* sql`SELECT 1 FROM orchestration_command_receipts WHERE command_id = ${commandId}`)
        .length !== 0
    );
  }, Effect.mapError(normalize));
  const readEventMetadata = Effect.fn("LegacyStorage.readEventMetadata")(function* (
    stored: RecordedStoredLifecycleEvent,
  ) {
    if (!(yield* tables(names[2]!))) return stored;
    const [row] = yield* sql<{
      sequence: number;
      command_id: string | null;
      thread_id: string;
      run_id: string | null;
      event_type: string;
      public_digest: string;
      metadata_json: string;
    }>`SELECT sequence,command_id,thread_id,run_id,event_type,public_digest,metadata_json FROM jones_legacy_bootstrap_event_metadata WHERE event_id = ${stored.event.id}`;
    if (row === undefined) return stored;
    if (
      row.sequence !== stored.sequence ||
      row.command_id !== stored.commandId ||
      row.thread_id !== stored.event.threadId ||
      row.run_id !== (eventRunId(stored) ?? null) ||
      row.event_type !== stored.event.type ||
      row.public_digest !== M.digest(yield* publicEvent(stored.event))
    )
      return yield* error("event_identity_conflict");
    const metadata = yield* decodeEventMetadataJson(row.metadata_json);
    return yield* M.mergeEvent(stored, metadata);
  }, Effect.mapError(normalize));
  const readLifecycleByCommandId = Effect.fn("LegacyStorage.readLifecycleByCommandId")(function* (
    commandId: CommandId,
  ) {
    const rows =
      yield* sql`SELECT sequence,event_id,command_id,stream_id,event_type,occurred_at,payload_json,metadata_json FROM orchestration_events
      WHERE command_id = ${commandId} AND aggregate_kind = 'thread' AND application_event_version = 2 ORDER BY sequence`;
    const result: RecordedStoredLifecycleEvent[] = [];
    for (const raw of rows) {
      const stored = yield* rowEvent(yield* decodeEventRow(raw));
      if (stored.event.type.startsWith("legacy-bootstrap.preflight-")) continue;
      // Validate current row incarnations without using their current private values on this old event.
      const parent = yield* readThread(stored.event.threadId);
      if (parent === undefined) continue;
      const lifecycle = yield* storedEvent(stored);
      const event = lifecycle.event;
      if (
        event.type === "thread.created" ||
        event.type === "thread.metadata-updated" ||
        event.type === "thread.archived" ||
        event.type === "thread.unarchived" ||
        event.type === "thread.deleted"
      ) {
        if (
          event.payload.id !== parent.thread.id ||
          event.payload.projectId !== parent.thread.projectId ||
          DateTime.formatIso(event.payload.createdAt) !==
            DateTime.formatIso(parent.thread.createdAt)
        )
          return yield* error("historical_thread_incarnation_conflict");
      }
      const runId = eventRunId(lifecycle);
      if (event.runId !== undefined && runId !== event.runId)
        return yield* error("event_run_envelope_conflict");
      if (runId !== undefined) {
        const current = yield* readRun(runId);
        if (current === undefined) continue;
        if (
          current.run.threadId !== event.threadId ||
          ((event.type === "run.created" || event.type === "run.updated") &&
            (event.payload.userMessageId !== current.run.userMessageId ||
              DateTime.formatIso(event.payload.requestedAt) !==
                DateTime.formatIso(current.run.requestedAt)))
        )
          return yield* error("historical_run_incarnation_conflict");
      }
      result.push(yield* readEventMetadata(yield* storedEvent(stored)));
    }
    return result;
  }, Effect.mapError(normalize));
  const findCommand = Effect.fn("LegacyStorage.findCommand")(function* (commandId: CommandId) {
    if (!(yield* tables(names[3]!))) return undefined;
    const [row] = yield* sql<{
      command_id: string;
      thread_id: string;
      project_id: string;
      kind: string;
      binding_digest: string;
      sequence_domain: string;
      private_ordinal: number | null;
      record_json: string;
    }>`SELECT * FROM jones_legacy_bootstrap_commands WHERE command_id = ${commandId}`;
    if (row === undefined) return undefined;
    const decoded = yield* decodeCommand(row.record_json);
    const record = yield* decodeRecord(decoded);
    const b = record.binding;
    if (
      b.commandId !== commandId ||
      b.commandId !== row.command_id ||
      b.threadId !== row.thread_id ||
      b.projectId !== row.project_id ||
      b.kind !== row.kind ||
      M.digest(b) !== row.binding_digest ||
      row.sequence_domain !== record.sequenceDomain ||
      row.private_ordinal !== record.privateOrdinal ||
      M.canonical(yield* decodeUnknownJson(b.bindingJson)) !== b.bindingJson
    )
      return yield* error("corrupt_command_binding");
    const preflight = b.kind === "preflight-intent" || b.kind === "preflight-outcome";
    if (
      preflight
        ? record.sequenceDomain !== "jones-legacy-ordinal" ||
          record.privateOrdinal === null ||
          record.event === null ||
          record.receipt === null
        : record.sequenceDomain !== "lifecycle" ||
          record.privateOrdinal !== null ||
          record.event !== null ||
          (b.kind === "lifecycle" ? record.receipt !== null : record.receipt === null)
    )
      return yield* error("corrupt_command_domain");
    if (
      record.receipt !== null &&
      record.receiptDigest !== M.digest(yield* encodeReceipt(record.receipt))
    )
      return yield* error("private_receipt_digest_conflict");
    if (
      record.receipt !== null &&
      (record.receipt.commandId !== commandId || record.receipt.threadId !== b.threadId)
    )
      return yield* error("corrupt_private_receipt");
    if (
      preflight &&
      (record.receipt?.status !== "accepted" ||
        record.receipt.error !== null ||
        record.receipt.commandType !== record.event?.type ||
        record.event?.threadId !== b.threadId ||
        record.receipt?.resultSequence !== record.privateOrdinal ||
        record.event?.type !== `legacy-bootstrap.${b.kind}`)
    )
      return yield* error("corrupt_preflight_position");
    if (record.event !== null) {
      const actualBinding =
        record.event.type === "legacy-bootstrap.preflight-intent"
          ? record.event.payload
          : record.event.payload.binding;
      if (
        M.canonical(actualBinding) !== b.bindingJson ||
        actualBinding.policy.threadId !== b.threadId ||
        actualBinding.policy.projectId !== b.projectId
      )
        return yield* error("preflight_record_binding_conflict");
    }
    return record;
  }, Effect.mapError(normalize));
  const assertBinding = Effect.fn("LegacyStorage.assertBinding")(function* (
    input: S.CommandBinding,
  ) {
    const binding = yield* decodeBinding(input);
    if (M.canonical(yield* decodeUnknownJson(binding.bindingJson)) !== binding.bindingJson)
      return yield* error("noncanonical_binding");
    const thread = yield* readPublicThread(binding.threadId);
    if (
      binding.threadCreatedAt !== null &&
      (thread === undefined ||
        thread.projectId !== binding.projectId ||
        DateTime.formatIso(thread.createdAt) !== binding.threadCreatedAt)
    )
      return yield* error("command_incarnation_conflict");
    if (
      binding.threadCreatedAt === null &&
      binding.kind !== "preflight-intent" &&
      binding.kind !== "preflight-outcome"
    )
      return yield* error("command_birth_required");
    if (thread !== undefined && thread.projectId !== binding.projectId)
      return yield* error("command_project_conflict");
    const found = yield* findCommand(binding.commandId);
    if (found !== undefined && M.canonical(found.binding) !== M.canonical(binding))
      return yield* error("command_binding_conflict");
    return found;
  }, Effect.mapError(normalize));
  const assertUnreserved = Effect.fn("LegacyStorage.assertUnreserved")(function* (
    commandId: CommandId,
  ) {
    if ((yield* findCommand(commandId)) !== undefined)
      return yield* error("private_command_reserved");
  }, Effect.mapError(normalize));
  const readLegacyPreflight = Effect.fn("LegacyStorage.readLegacyPreflight")(function* (
    commandId: CommandId,
  ) {
    const record = yield* findCommand(commandId);
    if (record !== undefined) {
      if (
        record.binding.threadCreatedAt !== null &&
        (yield* readPublicThread(record.binding.threadId)) === undefined
      )
        return undefined;
      if (yield* hasOrdinaryReceipt(commandId))
        return yield* error("private_preflight_command_collision");
      if (record.event === null || record.receipt === null || record.privateOrdinal === null)
        return yield* error("not_preflight");
      yield* assertBinding(record.binding);
      return {
        event: record.event,
        receipt: yield* decodePreflightReceipt({
          ...record.receipt,
          sequenceDomain: "jones-legacy-ordinal",
        }),
      };
    }
    const receipt = yield* ordinaryReceipt(commandId);
    if (receipt === undefined) return undefined;
    const rows =
      yield* sql`SELECT sequence,event_id,command_id,stream_id,event_type,occurred_at,payload_json,metadata_json FROM orchestration_events
      WHERE command_id = ${commandId} AND aggregate_kind = 'thread' AND application_event_version = 2 ORDER BY sequence`;
    if (rows.length !== 1) return yield* error("historical_preflight_unavailable");
    const stored = yield* rowEvent(yield* decodeEventRow(rows[0]));
    const event = yield* decodePrivateEvent(stored.event);
    if (
      stored.sequence !== receipt.resultSequence ||
      event.threadId !== receipt.threadId ||
      event.type !== receipt.commandType
    )
      return yield* error("historical_preflight_conflict");
    return {
      event,
      receipt: yield* decodePreflightReceipt({ ...receipt, sequenceDomain: "historical-upstream" }),
    };
  }, Effect.mapError(normalize));
  const readLegacyReceipt = Effect.fn("LegacyStorage.readLegacyReceipt")(function* (
    commandId: CommandId,
  ) {
    const record = yield* findCommand(commandId);
    if (record?.event !== null && record?.event !== undefined) {
      const found = yield* readLegacyPreflight(commandId);
      return found === undefined
        ? undefined
        : { domain: "legacy-preflight" as const, receipt: found.receipt };
    }
    if (record !== undefined && (yield* readPublicThread(record.binding.threadId)) === undefined)
      return undefined;
    if (record !== undefined) yield* assertBinding(record.binding);
    const ordinary = yield* ordinaryReceipt(commandId);
    if (record?.binding.kind === "delete") {
      if (ordinary !== undefined || record.receipt === null)
        return yield* error("private_receipt_collision");
      return { domain: "legacy-delete" as const, receipt: record.receipt };
    }
    if (
      record !== undefined &&
      (ordinary === undefined || record.receiptDigest !== M.digest(yield* encodeReceipt(ordinary)))
    )
      return yield* error("lifecycle_receipt_unavailable");
    if (
      ordinary?.commandType === "legacy-bootstrap.preflight-intent" ||
      ordinary?.commandType === "legacy-bootstrap.preflight-outcome"
    ) {
      const historical = yield* readLegacyPreflight(commandId);
      return historical === undefined
        ? undefined
        : { domain: "legacy-preflight" as const, receipt: historical.receipt };
    }
    if (
      ordinary?.commandType === "legacy-bootstrap.guard-rejection-delete" ||
      ordinary?.commandType === "legacy-bootstrap.failure-delete"
    )
      return { domain: "legacy-delete" as const, receipt: ordinary };
    return ordinary === undefined ? undefined : { domain: "lifecycle" as const, receipt: ordinary };
  }, Effect.mapError(normalize));
  const readTerminalCleanup = Effect.fn("LegacyStorage.readTerminalCleanup")(function* (
    effectId: string,
  ) {
    if (!(yield* tables(names[4]!))) return undefined;
    const [row] = yield* sql<{
      effect_id: string;
      command_id: string;
      thread_id: string;
      control_json: string;
      state: string;
      attempt_id: string | null;
      revision: number;
      outcome: string | null;
    }>`SELECT * FROM jones_legacy_bootstrap_terminal_cleanup WHERE effect_id = ${effectId}`;
    if (row === undefined) return undefined;
    const entry = yield* decodeTerminal({
      effectId: row.effect_id,
      commandId: row.command_id,
      threadId: row.thread_id,
      control: yield* decodeUnknownJson(row.control_json),
      state: row.state,
      attemptId: row.attempt_id,
      revision: row.revision,
      outcome: row.outcome,
    });
    if (entry.control.threadId !== entry.threadId) return yield* error("cleanup_binding_conflict");
    const command = yield* findCommand(entry.commandId);
    if (
      command?.binding.kind !== "delete" ||
      command.binding.threadId !== entry.threadId ||
      command.receipt?.status !== "accepted"
    )
      return yield* error("cleanup_receipt_unavailable");
    yield* assertBinding(command.binding);
    const boundDelete = yield* parseObject(command.binding.bindingJson);
    if (
      M.canonical(boundDelete.legacyOwnedControl) !== M.canonical(entry.control) ||
      boundDelete.legacyNoControl !== undefined
    )
      return yield* error("cleanup_control_changed");
    return entry;
  }, Effect.mapError(normalize));
  const listPendingTerminalCleanup = Effect.fn("LegacyStorage.listPendingTerminalCleanup")(
    function* (limit: number) {
      if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000)
        return yield* error("invalid_cleanup_limit");
      if (!(yield* tables(names[4]!))) return [];
      const rows = yield* sql<{
        effect_id: string;
      }>`SELECT effect_id FROM jones_legacy_bootstrap_terminal_cleanup WHERE state IN ('pending','started','unknown') ORDER BY effect_id LIMIT ${limit}`;
      return yield* Effect.forEach(rows, (row) => readTerminalCleanup(row.effect_id));
    },
    Effect.mapError(normalize),
  );
  const forTransaction = Effect.gen(function* () {
    yield* requireSchema;
    const transaction = Option.getOrUndefined(yield* Effect.serviceOption(sql.transactionService));
    const fiber = yield* Effect.fiberId;
    const check = Effect.gen(function* () {
      if (
        transaction === undefined ||
        Option.getOrUndefined(yield* Effect.serviceOption(sql.transactionService)) !==
          transaction ||
        Option.getOrUndefined(yield* Effect.serviceOption(SqlClient.SqlClient)) !== sql ||
        (yield* Effect.fiberId) !== fiber
      )
        return yield* error("accepting_transaction_required");
    });
    yield* check;
    const verifyAnchor = Effect.fn("LegacyStorage.verifyAnchor")(function* (
      id: EventId,
      sequence: number,
      commandId: CommandId,
      threadId: ThreadId,
    ) {
      const event = yield* actualEvent(id);
      const receipt = yield* ordinaryReceipt(commandId);
      if (
        event === undefined ||
        event.sequence !== sequence ||
        event.commandId !== commandId ||
        event.event.threadId !== threadId ||
        receipt === undefined ||
        receipt.threadId !== threadId ||
        receipt.status !== "accepted" ||
        receipt.resultSequence < sequence
      )
        return yield* error("anchor_unavailable");
      return event;
    });
    const recordThread = Effect.fn("LegacyStorage.recordThread")(function* (input: {
      readonly identity: S.ThreadIdentity;
      readonly metadata: S.ThreadMetadata;
      readonly expectedRevision: number | null;
    }) {
      yield* check;
      const identity = yield* decodeThreadIdentity(input.identity);
      const metadata = yield* decodeThreadMetadata(input.metadata);
      const thread = yield* readPublicThread(identity.threadId);
      if (thread === undefined || !M.hasMetadata(metadata))
        return yield* error("thread_metadata_target_unavailable");
      if (
        (yield* sql`SELECT 1 FROM projection_projects WHERE project_id = ${identity.projectId}`)
          .length !== 1
      )
        return yield* error("metadata_project_unavailable");
      yield* M.mergeThread(thread, { identity, metadata });
      const anchor = yield* verifyAnchor(
        identity.claimEventId,
        identity.claimSequence,
        identity.claimCommandId,
        identity.threadId,
      );
      const claimReceipt = yield* ordinaryReceipt(identity.claimCommandId);
      if (
        claimReceipt?.commandType !==
        (metadata.legacyBootstrapClaim?.ownsNewThread ? "thread.create" : "thread.metadata.update")
      )
        return yield* error("claim_receipt_type_conflict");
      if (
        (anchor.event.type !== "thread.created" &&
          anchor.event.type !== "thread.metadata-updated") ||
        anchor.event.payload.id !== thread.id ||
        anchor.event.payload.projectId !== thread.projectId ||
        DateTime.formatIso(anchor.event.payload.createdAt) !== identity.createdAt ||
        metadata.legacyBootstrapClaim?.threadId !== identity.threadId ||
        metadata.legacyBootstrapClaim.projectId !== identity.projectId ||
        metadata.legacyBootstrapClaim.createCommandId !== identity.claimCommandId ||
        (metadata.legacyBootstrapClaim.ownsNewThread
          ? anchor.event.type !== "thread.created"
          : anchor.event.type !== "thread.metadata-updated")
      )
        return yield* error("claim_anchor_conflict");
      const previous = yield* threadSidecar(identity.threadId);
      if ((previous?.revision ?? null) !== input.expectedRevision)
        return yield* error("thread_revision_conflict");
      if (previous !== undefined) {
        if (
          M.canonical(previous.identity) !== M.canonical(identity) ||
          M.canonical(previous.metadata) !== M.canonical(metadata)
        )
          return yield* error("immutable_claim_conflict");
        return previous.revision;
      }
      yield* sql`INSERT INTO jones_legacy_bootstrap_threads(thread_id,identity_json,revision,metadata_json) VALUES (${identity.threadId},${M.canonical(identity)},0,${M.canonical(metadata)})`;
      return 0;
    }, Effect.mapError(normalize));
    const recordRun = Effect.fn("LegacyStorage.recordRun")(function* (input: {
      readonly identity: S.RunIdentity;
      readonly metadata: S.RunMetadata;
      readonly metadataEvent: RecordedStoredLifecycleEvent;
      readonly expectedRevision: number | null;
    }) {
      yield* check;
      const identity = yield* decodeRunIdentity(input.identity);
      const metadata = yield* decodeRunMetadata(input.metadata);
      const run = yield* readPublicRun(identity.runId);
      const parent = run === undefined ? undefined : yield* readPublicThread(run.threadId);
      if (run === undefined || parent === undefined || !M.hasMetadata(metadata))
        return yield* error("run_metadata_target_unavailable");
      if (
        (yield* sql`SELECT 1 FROM projection_projects WHERE project_id = ${parent.projectId}`)
          .length !== 1
      )
        return yield* error("metadata_project_unavailable");
      yield* M.mergeRun(run, { identity, metadata });
      const birth = yield* verifyAnchor(
        identity.birthEventId,
        identity.birthSequence,
        identity.birthCommandId,
        identity.threadId,
      );
      if ((yield* ordinaryReceipt(identity.birthCommandId))?.commandType !== "message.dispatch")
        return yield* error("birth_receipt_type_conflict");
      if (
        birth.event.type !== "run.created" ||
        birth.event.payload.id !== run.id ||
        birth.event.payload.userMessageId !== identity.userMessageId ||
        DateTime.formatIso(birth.event.payload.requestedAt) !== identity.requestedAt
      )
        return yield* error("birth_anchor_conflict");
      if (metadata.legacyBootstrap !== undefined) {
        const policy = metadata.legacyBootstrap;
        const claim = yield* readThread(identity.threadId);
        const { runId: _run, ...expectedClaim } = policy;
        if (
          policy.runId !== identity.runId ||
          policy.birthCommandId !== identity.birthCommandId ||
          policy.messageId !== identity.userMessageId ||
          policy.threadId !== identity.threadId ||
          M.canonical(claim?.thread.legacyBootstrapClaim) !== M.canonical(expectedClaim)
        )
          return yield* error("birth_policy_conflict");
      }
      const metadataEvent = input.metadataEvent;
      const actualMetadataEvent = yield* actualEvent(metadataEvent.event.id);
      if (
        (metadataEvent.event.type !== "run.created" &&
          metadataEvent.event.type !== "run.updated") ||
        metadataEvent.event.payload.id !== identity.runId ||
        metadataEvent.event.threadId !== identity.threadId ||
        metadataEvent.sequence < identity.birthSequence ||
        actualMetadataEvent === undefined ||
        actualMetadataEvent.sequence !== metadataEvent.sequence ||
        actualMetadataEvent.commandId !== metadataEvent.commandId ||
        M.canonical(M.eventMetadata(metadataEvent)) !== M.canonical({ kind: "run", metadata }) ||
        M.digest(yield* publicEvent((yield* storedEvent(actualMetadataEvent)).event)) !==
          M.digest(yield* publicEvent(metadataEvent.event))
      )
        return yield* error("run_metadata_event_conflict");
      const previous = yield* runSidecar(identity.runId);
      if ((previous?.revision ?? null) !== input.expectedRevision)
        return yield* error("run_revision_conflict");
      if (previous !== undefined) {
        if (M.canonical(previous.identity) !== M.canonical(identity))
          return yield* error("run_incarnation_conflict");
        yield* M.mergeRun({ ...run, ...previous.metadata }, { identity, metadata });
        if (M.canonical(previous.metadata) === M.canonical(metadata)) {
          yield* recordEvent(metadataEvent);
          return previous.revision;
        }
        if (metadataEvent.sequence <= previous.metadataSequence)
          return yield* error("run_metadata_out_of_order");
        yield* recordEvent(metadataEvent);
        const changed =
          yield* sql`UPDATE jones_legacy_bootstrap_runs SET metadata_event_id = ${metadataEvent.event.id}, metadata_sequence = ${metadataEvent.sequence}, metadata_json = ${M.canonical(metadata)}, revision = revision + 1
          WHERE run_id = ${identity.runId} AND revision = ${previous.revision} RETURNING revision`;
        if (changed.length !== 1) return yield* error("run_revision_conflict");
        return previous.revision + 1;
      }
      yield* recordEvent(metadataEvent);
      yield* sql`INSERT INTO jones_legacy_bootstrap_runs(run_id,thread_id,metadata_event_id,metadata_sequence,identity_json,revision,metadata_json) VALUES (${identity.runId},${identity.threadId},${metadataEvent.event.id},${metadataEvent.sequence},${M.canonical(identity)},0,${M.canonical(metadata)})`;
      return 0;
    }, Effect.mapError(normalize));
    const recordEvent = Effect.fn("LegacyStorage.recordEvent")(function* (
      stored: RecordedStoredLifecycleEvent,
    ) {
      yield* check;
      const metadata = M.eventMetadata(stored);
      if (metadata === undefined) return;
      yield* decodeEventMetadata(metadata);
      const actual = yield* actualEvent(stored.event.id);
      if (
        actual === undefined ||
        actual.sequence !== stored.sequence ||
        actual.commandId !== stored.commandId ||
        M.digest(yield* publicEvent((yield* storedEvent(actual)).event)) !==
          M.digest(yield* publicEvent(stored.event))
      )
        return yield* error("actual_event_conflict");
      yield* M.mergeEvent(yield* storedEvent(actual), metadata);
      if ((yield* readPublicThread(stored.event.threadId)) === undefined)
        return yield* error("event_target_unavailable");
      const runId = eventRunId(stored);
      if (stored.event.runId !== undefined && runId !== stored.event.runId)
        return yield* error("event_run_envelope_conflict");
      if (runId !== undefined && (yield* readPublicRun(runId)) === undefined)
        return yield* error("event_run_unavailable");
      const [previous] = yield* sql<{
        metadata_json: string;
      }>`SELECT metadata_json FROM jones_legacy_bootstrap_event_metadata WHERE event_id = ${stored.event.id}`;
      if (previous !== undefined) {
        yield* readEventMetadata(yield* storedEvent(actual));
        if (M.canonical(yield* decodeUnknownJson(previous.metadata_json)) !== M.canonical(metadata))
          return yield* error("immutable_event_metadata_conflict");
        return;
      }
      yield* sql`INSERT INTO jones_legacy_bootstrap_event_metadata(event_id,sequence,command_id,thread_id,run_id,event_type,public_digest,metadata_json)
        VALUES (${stored.event.id},${stored.sequence},${stored.commandId},${stored.event.threadId},${eventRunId(stored) ?? null},${stored.event.type},${M.digest(yield* publicEvent(stored.event))},${M.canonical(metadata)})`;
    }, Effect.mapError(normalize));
    const insertCommand = Effect.fn("LegacyStorage.insertCommand")(function* (
      record: S.CommandRecord,
    ) {
      const b = record.binding;
      yield* sql`INSERT INTO jones_legacy_bootstrap_commands(command_id,thread_id,project_id,kind,binding_digest,sequence_domain,private_ordinal,record_json)
        VALUES (${b.commandId},${b.threadId},${b.projectId},${b.kind},${M.digest(b)},${record.sequenceDomain},${record.privateOrdinal},${yield* encodeCommand(record)})`;
      return yield* findCommand(b.commandId);
    });
    const verifyStored = Effect.fn("LegacyStorage.verifyStored")(function* (
      binding: S.CommandBinding,
      receipt: CommandReceiptV2,
      events: ReadonlyArray<RecordedStoredLifecycleEvent>,
    ) {
      if (
        receipt.commandId !== binding.commandId ||
        receipt.threadId !== binding.threadId ||
        events.some(
          (entry) =>
            entry.commandId !== binding.commandId || entry.event.threadId !== binding.threadId,
        )
      )
        return yield* error("receipt_binding_conflict");
      const actualRows = yield* sql<{
        event_id: string;
        sequence: number;
      }>`SELECT event_id,sequence FROM orchestration_events
        WHERE command_id = ${binding.commandId} AND aggregate_kind = 'thread' AND application_event_version = 2 ORDER BY sequence`;
      if (
        actualRows.length !== events.length ||
        actualRows.some(
          (row, index) =>
            row.event_id !== events[index]?.event.id || row.sequence !== events[index]?.sequence,
        )
      )
        return yield* error("command_event_set_conflict");
      for (const entry of events) {
        const actual = yield* actualEvent(entry.event.id);
        if (
          actual === undefined ||
          actual.sequence !== entry.sequence ||
          actual.commandId !== binding.commandId ||
          M.digest(yield* publicEvent((yield* storedEvent(actual)).event)) !==
            M.digest(yield* publicEvent(entry.event))
        )
          return yield* error("receipt_event_conflict");
      }
      if (
        events.length > 0 &&
        Math.max(...events.map((entry) => entry.sequence)) !== receipt.resultSequence
      )
        return yield* error("receipt_position_conflict");
    });
    const recordLifecycle = Effect.fn("LegacyStorage.recordLifecycle")(function* (
      binding: S.CommandBinding,
      receipt: CommandReceiptV2,
      events: ReadonlyArray<RecordedStoredLifecycleEvent>,
    ) {
      yield* check;
      if (binding.kind !== "lifecycle") return yield* error("wrong_command_owner");
      const previous = yield* assertBinding(binding);
      const actual = yield* ordinaryReceipt(binding.commandId);
      if (
        actual === undefined ||
        M.canonical(yield* encodeReceipt(actual)) !== M.canonical(yield* encodeReceipt(receipt))
      )
        return yield* error("actual_receipt_conflict");
      yield* verifyStored(binding, receipt, events);
      if (previous !== undefined) {
        if (previous.receiptDigest !== M.digest(yield* encodeReceipt(receipt)))
          return yield* error("lifecycle_receipt_changed");
        return false;
      }
      yield* insertCommand({
        binding,
        sequenceDomain: "lifecycle",
        privateOrdinal: null,
        receiptDigest: M.digest(yield* encodeReceipt(receipt)),
        receipt: null,
        event: null,
      });
      return true;
    }, Effect.mapError(normalize));
    const recordDelete = Effect.fn("LegacyStorage.recordDelete")(function* (
      binding: S.CommandBinding,
      receipt: CommandReceiptV2,
      events: ReadonlyArray<RecordedStoredLifecycleEvent>,
    ) {
      yield* check;
      if (
        binding.kind !== "delete" ||
        receipt.status !== "accepted" ||
        receipt.error !== null ||
        (receipt.commandType !== "legacy-bootstrap.guard-rejection-delete" &&
          receipt.commandType !== "legacy-bootstrap.failure-delete")
      )
        return yield* error("wrong_delete_owner");
      const commandPayload = yield* decodeDeleteBindingJson(binding.bindingJson);
      if (
        commandPayload.commandId !== binding.commandId ||
        commandPayload.threadId !== binding.threadId ||
        commandPayload.type !== receipt.commandType ||
        commandPayload.legacyBootstrap.threadId !== binding.threadId ||
        commandPayload.legacyBootstrap.projectId !== binding.projectId ||
        !commandPayload.legacyBootstrap.ownsNewThread
      )
        return yield* error("delete_command_binding_conflict");
      const control =
        "legacyOwnedControl" in commandPayload
          ? commandPayload.legacyOwnedControl
          : commandPayload.legacyNoControl;
      if (
        control.threadId !== binding.threadId ||
        control.runId !== commandPayload.runId ||
        commandPayload.legacyBootstrap.runId !== commandPayload.runId ||
        M.canonical(control.policy) !== M.canonical(commandPayload.legacyBootstrap)
      )
        return yield* error("delete_control_identity_conflict");
      const previous = yield* assertBinding(binding);
      if (yield* hasOrdinaryReceipt(binding.commandId))
        return yield* error("ordinary_command_collision");
      if (previous !== undefined) {
        if (
          previous.receipt === null ||
          (yield* encodeReceipt(previous.receipt)) !== (yield* encodeReceipt(receipt))
        )
          return yield* error("delete_replay_conflict");
        return false;
      }
      if (events.length === 0 || !events.some((entry) => entry.event.type === "thread.deleted"))
        return yield* error("delete_tombstone_required");
      yield* verifyStored(binding, receipt, events);
      yield* insertCommand({
        binding,
        sequenceDomain: "lifecycle",
        privateOrdinal: null,
        receiptDigest: M.digest(yield* encodeReceipt(receipt)),
        receipt,
        event: null,
      });
      return true;
    }, Effect.mapError(normalize));
    const preserveInlineThread = Effect.fn("LegacyStorage.preserveInlineThread")(function* (
      threadId: ThreadId,
    ) {
      yield* check;
      const thread = yield* readPublicThread(threadId);
      if (thread === undefined || !M.hasMetadata(M.threadMetadata(thread))) return;
      const sidecar = yield* threadSidecar(threadId);
      if (sidecar !== undefined) {
        yield* M.mergeThread(thread, sidecar);
        return;
      }
      const policy = thread.legacyBootstrapClaim!;
      const candidates = yield* readLifecycleByCommandId(policy.createCommandId);
      const claims = candidates.filter(
        ({ event }) =>
          event.type === (policy.ownsNewThread ? "thread.created" : "thread.metadata-updated"),
      );
      const claim = claims[0];
      if (claims.length !== 1 || claim === undefined)
        return yield* error("historical_claim_unavailable");
      yield* recordThread({
        identity: {
          threadId,
          projectId: thread.projectId,
          createdAt: DateTime.formatIso(thread.createdAt),
          claimEventId: claim.event.id,
          claimSequence: claim.sequence,
          claimCommandId: policy.createCommandId,
        },
        metadata: M.threadMetadata(thread),
        expectedRevision: null,
      });
    }, Effect.mapError(normalize));
    const preserveInlineRun = Effect.fn("LegacyStorage.preserveInlineRun")(function* (
      runId: RunId,
    ) {
      yield* check;
      const run = yield* readPublicRun(runId);
      if (run === undefined || !M.hasMetadata(M.runMetadata(run))) return;
      const sidecar = yield* runSidecar(runId);
      if (sidecar !== undefined) {
        yield* M.mergeRun(run, sidecar);
        return;
      }
      yield* preserveInlineThread(run.threadId);
      const rows =
        yield* sql`SELECT sequence,event_id,command_id,stream_id,event_type,occurred_at,payload_json,metadata_json FROM orchestration_events
        WHERE stream_id = ${run.threadId} AND aggregate_kind = 'thread' AND application_event_version = 2
        AND event_type = 'run.created' AND json_extract(payload_json,'$.id') = ${runId} ORDER BY sequence`;
      if (rows.length !== 1) return yield* error("historical_birth_unavailable");
      const birth = yield* rowEvent(yield* decodeEventRow(rows[0]));
      if (birth.commandId === null) return yield* error("historical_birth_command_missing");
      yield* recordRun({
        identity: {
          runId,
          threadId: run.threadId,
          userMessageId: run.userMessageId,
          requestedAt: DateTime.formatIso(run.requestedAt),
          birthEventId: birth.event.id,
          birthSequence: birth.sequence,
          birthCommandId: yield* decodeCommandId(birth.commandId),
        },
        metadata: M.runMetadata(run),
        metadataEvent: yield* Effect.gen(function* () {
          const history =
            yield* sql`SELECT sequence,event_id,command_id,stream_id,event_type,occurred_at,payload_json,metadata_json FROM orchestration_events
            WHERE stream_id = ${run.threadId} AND aggregate_kind = 'thread' AND application_event_version = 2
            AND event_type IN ('run.created','run.updated') AND json_extract(payload_json,'$.id') = ${runId} ORDER BY sequence DESC`;
          for (const raw of history) {
            const entry = yield* storedEvent(yield* rowEvent(yield* decodeEventRow(raw)));
            if (
              M.canonical(M.eventMetadata(entry)) ===
              M.canonical({ kind: "run", metadata: M.runMetadata(run) })
            )
              return entry;
          }
          return yield* error("historical_metadata_anchor_unavailable");
        }),
        expectedRevision: null,
      });
    }, Effect.mapError(normalize));
    const commitPreflight = Effect.fn("LegacyStorage.commitPreflight")(function* (input: {
      readonly commandId: CommandId;
      readonly event: typeof OrchestrationV2PrivateEvent.Type;
    }) {
      yield* check;
      const event = yield* decodePrivateEventInput(input.event);
      const binding =
        event.type === "legacy-bootstrap.preflight-intent" ? event.payload : event.payload.binding;
      const p = binding.policy;
      const payload = yield* decodeQueueCommandJson(binding.canonicalPayload);
      const intentId = `${p.createCommandId}:preflight-intent`;
      const outcomeId = `${p.createCommandId}:preflight-outcome`;
      if (
        payload.type !== "thread.turn.start" ||
        payload.bootstrap === undefined ||
        p.runId !== undefined ||
        p.createCommandId !== legacyBootstrapCreateCommandId(p.threadId, p.releaseCommandId) ||
        p.birthCommandId !== `${p.createCommandId}:initial-message` ||
        event.threadId !== p.threadId ||
        binding.fetch.remote !== (binding.fetch.startFromOrigin ? "origin" : null) ||
        M.canonical(p.dispatchGuard ?? null) !== M.canonical(payload.dispatchGuard ?? null) ||
        p.payloadHash !== legacyPayloadHash(binding.canonicalPayload) ||
        binding.canonicalPayload !== M.canonical(payload) ||
        payload.threadId !== p.threadId ||
        payload.commandId !== p.releaseCommandId ||
        payload.message.messageId !== p.messageId ||
        (payload.bootstrap.createThread !== undefined &&
          payload.bootstrap.createThread.projectId !== p.projectId) ||
        payload.bootstrap.prepareWorktree?.projectCwd !== binding.fetch.cwd ||
        payload.bootstrap.prepareWorktree.baseBranch !== binding.fetch.baseRef ||
        (payload.bootstrap.prepareWorktree.startFromOrigin === true) !==
          binding.fetch.startFromOrigin ||
        (payload.bootstrap.prepareWorktree.requireWorktree === true) !==
          binding.fetch.requireWorktree ||
        input.commandId !==
          (event.type === "legacy-bootstrap.preflight-intent" ? intentId : outcomeId)
      )
        return yield* error("preflight_binding_conflict");
      const [project] = yield* sql<{
        workspace_root: string;
        deleted_at: string | null;
      }>`SELECT workspace_root,deleted_at FROM projection_projects WHERE project_id = ${p.projectId}`;
      if (
        project === undefined ||
        project.deleted_at !== null ||
        project.workspace_root !== binding.fetch.cwd
      )
        return yield* error("preflight_project_changed");
      const thread = yield* readPublicThread(p.threadId);
      if (thread !== undefined && thread.projectId !== p.projectId)
        return yield* error("preflight_target_changed");
      const descriptor: S.CommandBinding = {
        commandId: input.commandId,
        threadId: p.threadId,
        projectId: p.projectId,
        threadCreatedAt: thread === undefined ? null : DateTime.formatIso(thread.createdAt),
        kind:
          event.type === "legacy-bootstrap.preflight-intent"
            ? "preflight-intent"
            : "preflight-outcome",
        bindingJson: M.canonical(binding),
      };
      const previous = yield* findCommand(input.commandId);
      if (previous !== undefined) {
        // A prebirth record keeps its original absent-incarnation capture after a later claim.
        yield* assertBinding({ ...descriptor, threadCreatedAt: previous.binding.threadCreatedAt });
        const found = yield* readLegacyPreflight(input.commandId);
        if (
          found === undefined ||
          previous.event === null ||
          M.canonical(
            event.type === "legacy-bootstrap.preflight-outcome" ? previous.event.payload : binding,
          ) !== M.canonical(event.payload)
        )
          return yield* error("preflight_replay_conflict");
        return { committed: false, ...found };
      }
      if (yield* hasOrdinaryReceipt(input.commandId)) {
        const historical = yield* readLegacyPreflight(input.commandId);
        if (
          historical === undefined ||
          historical.event.type !== event.type ||
          historical.event.threadId !== event.threadId ||
          M.canonical(historical.event.payload) !== M.canonical(event.payload)
        )
          return yield* error("historical_preflight_replay_conflict");
        return { committed: false, ...historical };
      }
      if (
        (yield* hasOrdinaryReceipt(p.createCommandId)) ||
        (yield* hasOrdinaryReceipt(p.birthCommandId)) ||
        (yield* hasOrdinaryReceipt(p.releaseCommandId)) ||
        (yield* findCommand(p.createCommandId)) !== undefined ||
        (yield* findCommand(p.birthCommandId)) !== undefined ||
        (yield* findCommand(p.releaseCommandId)) !== undefined
      )
        return yield* error("preflight_already_born");
      if (event.type === "legacy-bootstrap.preflight-intent") {
        const rows = yield* sql<{
          command_id: string;
        }>`SELECT command_id FROM jones_legacy_bootstrap_commands WHERE thread_id = ${p.threadId} AND kind = 'preflight-intent'`;
        // A historical intent's accepted receipt survives pruning. Require the same
        // exact event and outcome readback even when only its receipt can name it.
        const historical = yield* sql<{
          command_id: string | null;
        }>`SELECT command_id FROM orchestration_events WHERE stream_id = ${p.threadId}
          AND aggregate_kind = 'thread' AND application_event_version = 2 AND event_type = 'legacy-bootstrap.preflight-intent'
          UNION
          SELECT command_id FROM orchestration_command_receipts
          WHERE aggregate_kind = 'thread' AND aggregate_id = ${p.threadId}
            AND command_type = 'legacy-bootstrap.preflight-intent' AND status = 'accepted'`;
        for (const row of [...rows, ...historical]) {
          if (row.command_id === null) return yield* error("historical_intent_identity_missing");
          const id = yield* decodeCommandId(row.command_id);
          const intent = yield* readLegacyPreflight(id);
          if (intent?.event.type !== "legacy-bootstrap.preflight-intent")
            return yield* error("previous_intent_unavailable");
          const outcomeCommandId = yield* decodeCommandId(
            `${intent.event.payload.policy.createCommandId}:preflight-outcome`,
          );
          const outcome = yield* readLegacyPreflight(outcomeCommandId);
          if (
            outcome?.event.type !== "legacy-bootstrap.preflight-outcome" ||
            outcome.event.payload.status === "unknown" ||
            outcome.event.payload.intentCommandId !== id ||
            outcome.event.payload.intentSequence !== intent.receipt.resultSequence ||
            outcome.receipt.sequenceDomain !== intent.receipt.sequenceDomain ||
            M.canonical(outcome.event.payload.binding) !== M.canonical(intent.event.payload)
          )
            return yield* error("unresolved_preflight");
        }
      } else {
        const id = yield* decodeCommandId(intentId);
        const intent = yield* readLegacyPreflight(id);
        // Historical positions are never adopted as new private ordinals.
        if (
          intent?.event.type !== "legacy-bootstrap.preflight-intent" ||
          intent.receipt.sequenceDomain !== "jones-legacy-ordinal" ||
          event.payload.intentCommandId !== id ||
          event.payload.intentSequence !== intent.receipt.resultSequence ||
          M.canonical(intent.event.payload) !== M.canonical(binding)
        )
          return yield* error("exact_private_intent_required");
      }
      const [max] = yield* sql<{
        ordinal: number;
      }>`SELECT COALESCE(MAX(private_ordinal),0) AS ordinal FROM jones_legacy_bootstrap_commands`;
      const ordinal = (max?.ordinal ?? 0) + 1;
      if (!Number.isSafeInteger(ordinal)) return yield* error("private_ordinal_exhausted");
      const receipt: CommandReceiptV2 = {
        commandId: input.commandId,
        threadId: p.threadId,
        commandType: event.type,
        acceptedAt: event.occurredAt,
        resultSequence: ordinal,
        status: "accepted",
        error: null,
      };
      yield* insertCommand({
        binding: descriptor,
        sequenceDomain: "jones-legacy-ordinal",
        privateOrdinal: ordinal,
        receiptDigest: M.digest(yield* encodeReceipt(receipt)),
        receipt,
        event,
      });
      const found = yield* readLegacyPreflight(input.commandId);
      if (found === undefined) return yield* error("preflight_readback_missing");
      return { committed: true, ...found };
    }, Effect.mapError(normalize));
    const createTerminalCleanup = Effect.fn("LegacyStorage.createTerminalCleanup")(
      function* (input: {
        readonly effectId: string;
        readonly commandId: CommandId;
        readonly threadId: ThreadId;
        readonly control: S.TerminalCleanup["control"];
      }) {
        yield* check;
        const entry = yield* decodeTerminal({
          ...input,
          state: "pending",
          attemptId: null,
          revision: 0,
          outcome: null,
        });
        const command = yield* findCommand(input.commandId);
        if (
          command?.binding.kind !== "delete" ||
          command.binding.threadId !== input.threadId ||
          command.receipt?.status !== "accepted" ||
          input.control.threadId !== input.threadId ||
          input.control.policy.threadId !== input.threadId ||
          input.control.policy.projectId !== command.binding.projectId ||
          !input.control.policy.ownsNewThread
        )
          return yield* error("cleanup_delete_binding_conflict");
        yield* assertBinding(command.binding);
        const boundDelete = yield* parseObject(command.binding.bindingJson);
        if (
          boundDelete.legacyNoControl !== undefined ||
          M.canonical(boundDelete.legacyOwnedControl) !== M.canonical(input.control)
        )
          return yield* error("exact_cleanup_control_required");
        const previous = yield* readTerminalCleanup(input.effectId);
        if (previous !== undefined) {
          if (
            previous.commandId !== input.commandId ||
            previous.threadId !== input.threadId ||
            M.canonical(previous.control) !== M.canonical(input.control)
          )
            return yield* error("cleanup_effect_collision");
          return previous;
        }
        const sameCommand =
          yield* sql`SELECT effect_id FROM jones_legacy_bootstrap_terminal_cleanup WHERE command_id = ${input.commandId}`;
        if (sameCommand.length !== 0) return yield* error("cleanup_obligation_already_exists");
        yield* sql`INSERT INTO jones_legacy_bootstrap_terminal_cleanup(effect_id,command_id,thread_id,control_json,state,attempt_id,revision,outcome)
        VALUES (${input.effectId},${input.commandId},${input.threadId},${M.canonical(input.control)},'pending',NULL,0,NULL)`;
        return entry;
      },
      Effect.mapError(normalize),
    );
    const claimTerminalCleanup = Effect.fn("LegacyStorage.claimTerminalCleanup")(function* (input: {
      readonly effectId: string;
      readonly expectedRevision: number;
      readonly attemptId: string;
    }) {
      yield* check;
      if (input.attemptId.length === 0) return yield* error("cleanup_attempt_required");
      const previous = yield* readTerminalCleanup(input.effectId);
      if (
        previous === undefined ||
        previous.state !== "pending" ||
        previous.revision !== input.expectedRevision
      )
        return undefined;
      const changed =
        yield* sql`UPDATE jones_legacy_bootstrap_terminal_cleanup SET state = 'started', attempt_id = ${input.attemptId}, revision = revision + 1
        WHERE effect_id = ${input.effectId} AND state = 'pending' AND revision = ${input.expectedRevision} RETURNING effect_id`;
      return changed.length === 1 ? yield* readTerminalCleanup(input.effectId) : undefined;
    }, Effect.mapError(normalize));
    const finishTerminalCleanup = Effect.fn("LegacyStorage.finishTerminalCleanup")(
      function* (input: {
        readonly effectId: string;
        readonly expectedRevision: number;
        readonly attemptId: string;
        readonly outcome: NonNullable<S.TerminalCleanup["outcome"]>;
      }) {
        yield* check;
        const previous = yield* readTerminalCleanup(input.effectId);
        if (
          previous === undefined ||
          previous.state !== "started" ||
          previous.revision !== input.expectedRevision ||
          previous.attemptId !== input.attemptId
        )
          return undefined;
        const state = input.outcome === "closed" ? "succeeded" : "unknown";
        const changed =
          yield* sql`UPDATE jones_legacy_bootstrap_terminal_cleanup SET state = ${state}, outcome = ${input.outcome}, revision = revision + 1
        WHERE effect_id = ${input.effectId} AND state = 'started' AND revision = ${input.expectedRevision} AND attempt_id = ${input.attemptId} RETURNING effect_id`;
        return changed.length === 1 ? yield* readTerminalCleanup(input.effectId) : undefined;
      },
      Effect.mapError(normalize),
    );
    return {
      recordThread,
      recordRun,
      recordEvent,
      recordLifecycle,
      recordDelete,
      commitPreflight,
      preserveInlineThread,
      preserveInlineRun,
      createTerminalCleanup,
      claimTerminalCleanup,
      finishTerminalCleanup,
    };
  }).pipe(Effect.mapError(normalize));
  // PK hooks for ordinary projection updates are no-ops when no historical private fields exist.
  const preserveInlineThread = Effect.fn("LegacyStorage.preserveInlineThreadBeforePublicWrite")(
    function* (threadId: ThreadId) {
      const thread = yield* readPublicThread(threadId);
      if (thread !== undefined && M.hasMetadata(M.threadMetadata(thread)))
        yield* (yield* forTransaction).preserveInlineThread(threadId);
    },
    Effect.mapError(normalize),
  );
  const preserveInlineRun = Effect.fn("LegacyStorage.preserveInlineRunBeforePublicWrite")(
    function* (runId: RunId) {
      const run = yield* readPublicRun(runId);
      if (run !== undefined && M.hasMetadata(M.runMetadata(run)))
        yield* (yield* forTransaction).preserveInlineRun(runId);
    },
    Effect.mapError(normalize),
  );
  // Projection rebuild deletes every public row. In its transaction, first move each historical
  // inline value into its sidecar; a row that cannot be anchored fails the rebuild unchanged.
  const preserveInlineProjection = Effect.fn("LegacyStorage.preserveInlineProjection")(
    function* () {
      const threads = yield* sql<{ thread_id: string }>`
        SELECT thread_id FROM orchestration_v2_projection_threads
        WHERE json_type(payload_json, '$.legacyBootstrapClaim') IS NOT NULL ORDER BY thread_id`;
      const runs = yield* sql<{ run_id: string }>`
        SELECT run_id FROM orchestration_v2_projection_runs
        WHERE json_type(payload_json, '$.legacyPreparation') IS NOT NULL
          OR json_type(payload_json, '$.legacyReleaseDecision') IS NOT NULL
          OR json_type(payload_json, '$.legacyPreparationFailureDecision') IS NOT NULL
          OR json_type(payload_json, '$.workspaceRunSetupScript') IS NOT NULL
          OR json_type(payload_json, '$.legacyBootstrap') IS NOT NULL
          OR json_type(payload_json, '$.legacyPreparationFailureKnown') IS NOT NULL
        ORDER BY run_id`;
      if (threads.length === 0 && runs.length === 0) return;
      const writer = yield* forTransaction;
      for (const row of threads)
        yield* writer.preserveInlineThread(yield* decodeThreadId(row.thread_id));
      for (const row of runs) yield* writer.preserveInlineRun(yield* decodeRunId(row.run_id));
    },
    Effect.mapError(normalize),
  );
  return {
    available,
    forTransaction,
    readThread,
    readRun,
    readLifecycleByCommandId,
    findCommand,
    assertBinding,
    assertUnreserved,
    readLegacyReceipt,
    readLegacyPreflight,
    preserveInlineThread,
    preserveInlineRun,
    preserveInlineProjection,
    readTerminalCleanup,
    listPendingTerminalCleanup,
  };
};
export type LegacyStorage = ReturnType<typeof make>;
export type LegacyStorageWriter = Effect.Success<LegacyStorage["forTransaction"]>;
