import {
  CommandId,
  MessageId,
  OrchestratorMcpThreadSendResult,
  ThreadId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import type { JonesTablePresence } from "../persistence/JonesTablePresence.ts";
import { threadSendIdentity } from "../threadResultDependencies/RemoteRequestProtocol.ts";

const digest = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/));
const Capture = Schema.Struct({
  originNamespace: Schema.NonEmptyString,
  clientRequestId: Schema.NonEmptyString,
  requestDigest: digest,
  threadId: ThreadId,
  messageId: MessageId,
  expectedThreadCreatedAt: Schema.optional(Schema.NonEmptyString),
  mode: Schema.optional(Schema.Literals(["auto", "queue", "separate_turn", "steer", "restart"])),
  freshContext: Schema.optional(Schema.Boolean),
  link: OrchestratorMcpThreadSendResult.fields.link,
  dependency: OrchestratorMcpThreadSendResult.fields.dependency,
});
/** Captured once by authenticated ingress, including the digest of final encoded SEND arguments. */
export type SendCapture = typeof Capture.Type;
export interface SendCommitContext extends SendCapture {
  readonly commandId: CommandId;
}
export const SendCommit = Context.Reference<SendCommitContext | undefined>(
  "jones/orchestration/RemoteSendCommit",
  { defaultValue: () => undefined },
);
/** A preparation callback may restore its workspace, but may never recursively dispatch. */
export const SendPreparation = Context.Reference<boolean>(
  "jones/orchestration/RemoteSendPreparation",
  { defaultValue: () => false },
);
export const assertTargetBirth = (capture: SendCommitContext, createdAt: DateTime.Utc) =>
  capture.expectedThreadCreatedAt === undefined ||
  capture.expectedThreadCreatedAt === DateTime.formatIso(createdAt)
    ? Effect.void
    : Effect.fail(new RemoteSendReceiptError({ reason: "target_generation_changed" }));
export const commandIdFor = (originNamespace: string, clientRequestId: string) =>
  threadSendIdentity(originNamespace, clientRequestId).commandId;
export const messageIdFor = (originNamespace: string, clientRequestId: string) =>
  threadSendIdentity(originNamespace, clientRequestId).messageId;

export class RemoteSendReceiptError extends Schema.TaggedError<RemoteSendReceiptError>()(
  "RemoteSendReceiptError",
  { reason: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {}
const fail = (reason: string, cause?: unknown) => new RemoteSendReceiptError({ reason, cause });
const isReceiptError = Schema.is(RemoteSendReceiptError);
const captureJson = Schema.fromJsonString(Capture);
const encodeCaptureJson = Schema.encodeEffect(captureJson);
const decodeCaptureJson = Schema.decodeUnknownEffect(captureJson);
const encodeSentJson = Schema.encodeEffect(Schema.fromJsonString(OrchestratorMcpThreadSendResult));
const decodeCapture = Schema.decodeUnknownEffect(Capture, { onExcessProperty: "error" });
const decodeSent = Schema.decodeUnknownEffect(OrchestratorMcpThreadSendResult, {
  onExcessProperty: "error",
});
const decodeSentJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestratorMcpThreadSendResult),
  { onExcessProperty: "error" },
);
const Row = Schema.Struct({
  command_id: Schema.NonEmptyString,
  origin_namespace: Schema.NonEmptyString,
  client_request_id: Schema.NonEmptyString,
  request_digest: digest,
  thread_id: ThreadId,
  expected_created_at: Schema.NullOr(Schema.NonEmptyString),
  result_json: Schema.String,
  recorded_at: Schema.NonEmptyString,
});
const decodeRow = Schema.decodeUnknownEffect(Row, { onExcessProperty: "error" });
const freeze = <A>(value: A): A => {
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
};
export interface ReceiptLookup {
  readonly originNamespace: string;
  readonly threadId: ThreadId;
  readonly clientRequestId: string;
  readonly requestDigest: string;
  readonly expectedThreadCreatedAt?: string | undefined;
}
export type ReceiptOutcome =
  | { readonly outcome: "accepted"; readonly sent: OrchestratorMcpThreadSendResult }
  | { readonly outcome: "unknown"; readonly sent: null };

/** Original acceptance only: exact message/run/item from this transaction, never latest-run inference. */
export const acceptedResult = Effect.fn("RemoteSendReceipt.acceptedResult")(function* (
  capture: SendCommitContext,
  input: {
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
    readonly runs: ReadonlyArray<OrchestrationV2Run>;
    readonly turnItems: ReadonlyArray<OrchestrationV2TurnItem>;
  },
) {
  const message = input.events.find(
    (event) =>
      event.type === "message.updated" &&
      event.threadId === capture.threadId &&
      event.payload.id === capture.messageId,
  );
  if (message?.type !== "message.updated" || message.payload.runId === null)
    return yield* fail("accepted_message_missing");
  const messageRunId = message.payload.runId;
  const run = input.runs.find(
    (run) => run.id === messageRunId && run.threadId === capture.threadId,
  );
  const item = input.turnItems.find(
    (item) => item.type === "user_message" && item.messageId === capture.messageId,
  );
  if (
    run === undefined ||
    (item === undefined && run.status !== "queued") ||
    (item !== undefined && item.runId !== run.id)
  )
    return yield* fail("accepted_run_missing");
  const mode =
    capture.freshContext && (capture.mode === "restart" || capture.mode === "steer")
      ? "queue"
      : capture.mode;
  const restartedAsNextTurn =
    mode === "restart" &&
    input.events.some(
      (event) =>
        event.type === "turn-item.updated" &&
        event.payload.type === "run_interrupt_request" &&
        event.payload.runId !== run.id,
    );
  const delivery = restartedAsNextTurn
    ? "restarted"
    : item?.type !== "user_message" || item.inputIntent === "queued_turn"
      ? "queued"
      : item.inputIntent === "turn_start"
        ? "started"
        : mode === "restart"
          ? "restarted"
          : "steered";
  return yield* decodeSent({
    threadId: capture.threadId,
    messageId: capture.messageId,
    runId: run.id,
    status: run.status,
    delivery,
    ...(run.delegatedTaskId === undefined ? {} : { taskId: run.delegatedTaskId }),
    ...(capture.link === undefined ? {} : { link: capture.link }),
    ...(capture.dependency === undefined ? {} : { dependency: capture.dependency }),
  }).pipe(Effect.mapError((cause) => fail("invalid_accepted_result", cause)));
});

/** Uses migration 157's private sparse ledger. The accepting EventSink owns its transaction. */
export const makeRemoteSendReceiptStore = (
  sql: SqlClient.SqlClient,
  tables: JonesTablePresence,
) => {
  const available = tables("jones_remote_send_receipts").pipe(
    Effect.mapError((cause) => fail("ledger_unavailable", cause)),
  );
  const read = Effect.fn("RemoteSendReceipt.read")(
    function* (commandId: CommandId) {
      if (!(yield* available)) return undefined;
      const raw =
        (yield* sql`SELECT command_id, origin_namespace, client_request_id, request_digest, thread_id,
      expected_created_at, result_json, recorded_at FROM jones_remote_send_receipts WHERE command_id = ${commandId}`)[0];
      if (raw === undefined) return undefined;
      const row = yield* decodeRow(raw);
      const recordedAt = DateTime.make(row.recorded_at);
      if (
        Option.isNone(recordedAt) ||
        DateTime.formatIso(DateTime.toUtc(recordedAt.value)) !== row.recorded_at
      )
        return yield* fail("invalid_receipt_timestamp");
      const sent = yield* decodeSentJson(row.result_json);
      if (
        row.command_id !== commandIdFor(row.origin_namespace, row.client_request_id) ||
        sent.threadId !== row.thread_id ||
        sent.messageId !== messageIdFor(row.origin_namespace, row.client_request_id)
      )
        return yield* fail("corrupt_receipt_binding");
      return { row, sent };
    },
    Effect.mapError((cause) => (isReceiptError(cause) ? cause : fail("invalid_receipt", cause))),
  );
  const matches = (row: typeof Row.Type, input: ReceiptLookup) =>
    row.origin_namespace === input.originNamespace &&
    row.thread_id === input.threadId &&
    row.client_request_id === input.clientRequestId &&
    row.request_digest === input.requestDigest &&
    (input.expectedThreadCreatedAt === undefined ||
      row.expected_created_at === input.expectedThreadCreatedAt);
  const forCommit = Effect.fn("RemoteSendReceipt.forCommit")(function* (
    capture: SendCommitContext,
  ) {
    const { commandId: _, ...identity } = capture;
    yield* decodeCapture(identity).pipe(
      Effect.mapError((cause) => fail("invalid_send_capture", cause)),
    );
    if (
      capture.commandId !== commandIdFor(capture.originNamespace, capture.clientRequestId) ||
      capture.messageId !== messageIdFor(capture.originNamespace, capture.clientRequestId)
    )
      return yield* fail("command_identity_conflict");
    if (!(yield* available)) return yield* fail("ledger_unavailable");
    const found = yield* read(capture.commandId);
    if (
      found !== undefined &&
      (!matches(found.row, capture) ||
        found.row.expected_created_at !== (capture.expectedThreadCreatedAt ?? null) ||
        found.sent.messageId !== capture.messageId)
    )
      return yield* fail("command_binding_conflict");
    return found?.sent;
  });
  const record = Effect.fn("RemoteSendReceipt.record")(
    function* (capture: SendCommitContext, sent: OrchestratorMcpThreadSendResult) {
      if (Option.isNone(yield* Effect.serviceOption(sql.transactionService)))
        return yield* fail("receipt_write_outside_accepting_transaction");
      const previous = yield* forCommit(capture);
      if (previous !== undefined) return previous;
      const validated = yield* decodeSent(sent).pipe(
        Effect.mapError((cause) => fail("invalid_accepted_result", cause)),
      );
      if (validated.threadId !== capture.threadId || validated.messageId !== capture.messageId)
        return yield* fail("accepted_result_binding_conflict");
      const recordedAt = DateTime.formatIso(yield* DateTime.now);
      const resultJson = yield* encodeSentJson(validated);
      // Plain INSERT is intentional: migration 157 rejects UPDATE and duplicate INSERT, including REPLACE.
      yield* sql`INSERT INTO jones_remote_send_receipts (command_id, origin_namespace, client_request_id,
      request_digest, thread_id, expected_created_at, result_json, recorded_at)
      VALUES (${capture.commandId}, ${capture.originNamespace}, ${capture.clientRequestId}, ${capture.requestDigest},
        ${capture.threadId}, ${capture.expectedThreadCreatedAt ?? null}, ${resultJson}, ${recordedAt})`;
      return validated;
    },
    Effect.mapError((cause) =>
      isReceiptError(cause) ? cause : fail("receipt_write_failed", cause),
    ),
  );
  const lookup = Effect.fn("RemoteSendReceipt.lookup")(function* (
    input: ReceiptLookup,
  ): Effect.fn.Return<ReceiptOutcome, RemoteSendReceiptError> {
    if (
      !/^[0-9a-f]{64}$/.test(input.requestDigest) ||
      input.originNamespace.length === 0 ||
      input.clientRequestId.length === 0
    )
      return yield* fail("invalid_lookup_binding");
    const found = yield* read(commandIdFor(input.originNamespace, input.clientRequestId));
    return found !== undefined && matches(found.row, input)
      ? { outcome: "accepted" as const, sent: found.sent }
      : { outcome: "unknown" as const, sent: null };
  });
  const withSendCommit = Effect.fn("RemoteSendReceipt.withSendCommit")(function* <A, E, R>(
    input: SendCapture,
    operation: Effect.Effect<A, E, R>,
  ) {
    if ((yield* SendCommit) !== undefined) return yield* fail("nested_send_capture");
    const capture = yield* decodeCapture(input).pipe(
      Effect.mapError((cause) => fail("invalid_send_capture", cause)),
    );
    // Copy before sharing through the fiber context, so ingress cannot alter the admitted identity.
    const copied = yield* encodeCaptureJson(capture).pipe(
      Effect.flatMap((json) => decodeCaptureJson(json)),
      Effect.mapError((cause) => fail("invalid_send_capture", cause)),
    );
    const snapshot: SendCommitContext = freeze({
      ...copied,
      commandId: commandIdFor(capture.originNamespace, capture.clientRequestId),
    });
    if (!(yield* available)) return yield* fail("ledger_unavailable");
    yield* forCommit(snapshot);
    const exit = yield* operation.pipe(Effect.provideService(SendCommit, snapshot), Effect.exit);
    const accepted = yield* forCommit(snapshot);
    if (accepted !== undefined) return accepted;
    // Only the operation's failure remains; its success value is never the send result.
    if (Exit.isFailure(exit)) return yield* Effect.failCause(exit.cause);
    return yield* fail("accepted_receipt_unavailable");
  });
  /** Cutoff is supplied by the existing retention owner; this does not schedule pruning. */
  const pruneBefore = Effect.fn("RemoteSendReceipt.pruneBefore")(
    function* (cutoff: DateTime.Utc, limit: number) {
      if (!Number.isSafeInteger(limit) || limit <= 0) return yield* fail("invalid_prune_limit");
      if (!(yield* available)) return 0;
      const rows = yield* sql`DELETE FROM jones_remote_send_receipts WHERE command_id IN
      (SELECT command_id FROM jones_remote_send_receipts WHERE recorded_at < ${DateTime.formatIso(cutoff)}
        ORDER BY recorded_at, command_id LIMIT ${limit}) RETURNING command_id`;
      return rows.length;
    },
    Effect.mapError((cause) => fail("receipt_prune_failed", cause)),
  );
  return { available, forCommit, record, lookup, withSendCommit, pruneBefore };
};
export type RemoteSendReceiptPorts = Pick<
  ReturnType<typeof makeRemoteSendReceiptStore>,
  "available" | "lookup" | "withSendCommit" | "pruneBefore"
> & {
  /** Inside withSendCommit: acquire/release the target lock for workspace preparation only. */
  readonly guardPreparation: <A, E, R>(
    commandId: CommandId,
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | RemoteSendReceiptError, R>;
};
