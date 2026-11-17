import {
  CommandId,
  EnvironmentId,
  ModelSelection,
  NodeId,
  OrchestratorMcpFailure,
  type OrchestrationV2ServerCommand,
  ProviderDriverKind,
  ProviderInstanceId,
  RemoteAdmissionDigest,
  RemoteAdmissionStatus,
  RunId,
  RuntimeMode,
  ProviderInteractionMode,
  LinkedRequestContext,
  OrchestratorMcpThreadSendResult,
  ThreadId,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as Schema from "effect/Schema";
import type * as Tool from "effect/ai/Tool";
import type { McpThreadInvocationScope } from "../../mcp/McpInvocationContext.ts";
import { ProjectToolkit } from "../../mcp/toolkits/project/tools.ts";
import { canonicalJson, PRUNE_BATCH_SIZE } from "./shared.ts";

type LaunchTool = typeof ProjectToolkit.tools.t3_thread_launch;
export type LaunchResult = Tool.Success<LaunchTool>;
type Request = Extract<
  OrchestrationV2ServerCommand,
  { readonly type: "delegated_task.remote.request" }
>;
export interface IntentEnvelope {
  readonly scope: Pick<McpThreadInvocationScope, "environmentId" | "requestNamespace" | "thread">;
  readonly environmentId: EnvironmentId;
  readonly linkedAt: string;
  readonly label: string;
  readonly launch: Tool.Parameters<LaunchTool>;
  readonly request: Omit<Request, "remoteChild">;
}
export interface Intent {
  readonly commandId: CommandId;
  readonly createdAt: string;
  readonly envelope: IntentEnvelope;
  readonly launched: LaunchResult | null;
  readonly taskId: NodeId | null;
  readonly launchStarted: boolean;
  readonly admission: OrchestratorMcpFailure["remoteAdmission"] | null;
  readonly status: "pending" | "recorded" | "stopped" | "denied";
}
interface Row {
  readonly command_id: CommandId;
  readonly created_at: string;
  readonly request_json: string;
  readonly envelope_json: string;
  readonly launched_json: string | null;
  readonly task_id: NodeId | null;
  readonly launch_started: number;
  readonly admission_json: string | null;
  readonly status: Intent["status"];
}

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

// Stored rows are validated against these schemas instead of cast, so a corrupt or
// foreign row fails as a storage failure rather than surfacing later as a mistyped value.
const StoredEnvelope = Schema.Struct({
  scope: Schema.Struct({
    environmentId: EnvironmentId,
    requestNamespace: Schema.String,
    thread: Schema.Struct({
      threadId: ThreadId,
      providerSessionId: Schema.String,
      providerInstanceId: ProviderInstanceId,
      subagent: Schema.optionalKey(Schema.Boolean),
    }),
  }),
  environmentId: EnvironmentId,
  linkedAt: Schema.String,
  label: Schema.String,
  launch: ProjectToolkit.tools.t3_thread_launch.parametersSchema,
  request: Schema.Struct({
    type: Schema.Literal("delegated_task.remote.request"),
    commandId: CommandId,
    parentThreadId: ThreadId,
    parentRunId: RunId,
    parentNodeId: NodeId,
    task: TrimmedNonEmptyString,
    title: Schema.optional(TrimmedNonEmptyString),
    driver: ProviderDriverKind,
    modelSelection: ModelSelection,
    completionWake: Schema.optional(Schema.Literals(["always", "settled_only"])),
  }),
});
const StoredAdmission = Schema.Struct({
  id: Schema.NonEmptyString,
  bindingDigest: RemoteAdmissionDigest,
  status: RemoteAdmissionStatus,
});
const decodeEnvelope = Schema.decodeUnknownSync(StoredEnvelope);
const decodeLaunched = Schema.decodeUnknownSync(
  ProjectToolkit.tools.t3_thread_launch.successSchema,
);
const decodeAdmission = Schema.decodeUnknownSync(StoredAdmission);
const storageFailure = () =>
  new OrchestratorMcpFailure({
    code: "orchestration_error",
    message: "Unable to persist or read remote delegation intent; remote execution may be pending.",
  });
const decode = (row: Row): Intent => ({
  commandId: row.command_id,
  createdAt: row.created_at,
  envelope: decodeEnvelope(decodeJson(row.envelope_json)),
  launched: row.launched_json === null ? null : decodeLaunched(decodeJson(row.launched_json)),
  taskId: row.task_id,
  launchStarted: row.launch_started === 1,
  admission: row.admission_json === null ? null : decodeAdmission(decodeJson(row.admission_json)),
  status: row.status,
});

/** The intent commits before any remote mutation. Replays retain the original scope and payload. */
const compiledIs1 = Schema.is(OrchestratorMcpFailure);

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const read = (commandId: CommandId) =>
    sql<Row>`SELECT * FROM jones_remote_delegation_intents WHERE command_id = ${commandId}`;
  const find = Effect.fn("RemoteDelegationIntentStore.find")(function* (
    commandId: CommandId,
    request: unknown,
  ) {
    const row = (yield* read(commandId).pipe(Effect.mapError(storageFailure)))[0];
    if (row === undefined) return undefined;
    if (row.request_json !== canonicalJson(request))
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message: "This remote delegation key was already used with a different request.",
      });
    return yield* Effect.try({ try: () => decode(row), catch: storageFailure });
  });
  const prepare = (commandId: CommandId, request: unknown, envelope: IntentEnvelope) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const previous = yield* find(commandId, request);
          if (previous !== undefined) return previous;
          const now = DateTime.formatIso(yield* DateTime.now);
          yield* sql`INSERT INTO jones_remote_delegation_intents
        (command_id, request_json, envelope_json, status, created_at)
        VALUES (${commandId}, ${canonicalJson(request)}, ${encodeJson(envelope)}, 'pending', ${now})`;
          return {
            commandId,
            createdAt: now,
            envelope,
            launched: null,
            taskId: null,
            launchStarted: false,
            admission: null,
            status: "pending",
          } satisfies Intent;
        }),
      )
      .pipe(Effect.mapError((error) => (compiledIs1(error) ? error : storageFailure())));
  return {
    find,
    get: (commandId: CommandId) =>
      read(commandId).pipe(
        Effect.flatMap((rows) =>
          Effect.try({
            try: () => (rows[0] === undefined ? undefined : decode(rows[0])),
            catch: storageFailure,
          }),
        ),
        Effect.mapError(storageFailure),
      ),
    prepare,
    byTask: (taskId: NodeId) =>
      sql<Row>`SELECT * FROM jones_remote_delegation_intents WHERE task_id = ${taskId}`.pipe(
        Effect.flatMap((rows) =>
          Effect.try({
            try: () => (rows[0] === undefined ? undefined : decode(rows[0])),
            catch: storageFailure,
          }),
        ),
        Effect.mapError(storageFailure),
      ),
    admission: (
      commandId: CommandId,
      admission: NonNullable<OrchestratorMcpFailure["remoteAdmission"]>,
    ) =>
      sql`UPDATE jones_remote_delegation_intents SET admission_json = ${encodeJson(admission)} WHERE command_id = ${commandId} AND status = 'pending'`.pipe(
        Effect.asVoid,
        Effect.mapError(storageFailure),
      ),
    reserved: (commandId: CommandId, taskId: NodeId) =>
      sql`UPDATE jones_remote_delegation_intents SET task_id = ${taskId} WHERE command_id = ${commandId} AND status = 'pending'`.pipe(
        Effect.asVoid,
        Effect.mapError(storageFailure),
      ),
    beginLaunch: (commandId: CommandId) =>
      sql`UPDATE jones_remote_delegation_intents SET launch_started = 1 WHERE command_id = ${commandId} AND status = 'pending'`.pipe(
        Effect.asVoid,
        Effect.mapError(storageFailure),
      ),
    // One undecodable row must not stop recovery of the others. It stays pending,
    // untouched, and is reported; reads of that exact row still fail as storage errors.
    pending:
      sql<Row>`SELECT * FROM jones_remote_delegation_intents WHERE status = 'pending' AND json_extract(envelope_json, '$.kind') IS NULL ORDER BY created_at, command_id`.pipe(
        Effect.mapError(storageFailure),
        Effect.flatMap((rows) =>
          Effect.forEach(rows, (row) =>
            Effect.try({ try: () => decode(row), catch: storageFailure }).pipe(
              Effect.tapError(() =>
                Effect.logWarning(
                  "Skipping an unreadable pending remote delegation intent; it is left untouched.",
                ).pipe(Effect.annotateLogs({ commandId: row.command_id })),
              ),
              Effect.option,
            ),
          ),
        ),
        Effect.map((decoded) =>
          decoded.flatMap((intent) => (intent._tag === "Some" ? [intent.value] : [])),
        ),
      ),
    launched: (commandId: CommandId, launched: LaunchResult) =>
      sql`UPDATE jones_remote_delegation_intents SET launched_json = ${encodeJson(launched)} WHERE command_id = ${commandId} AND status = 'pending'`.pipe(
        Effect.asVoid,
        Effect.mapError(storageFailure),
      ),
    recorded: (commandId: CommandId, taskId: NodeId) =>
      sql`UPDATE jones_remote_delegation_intents SET task_id = ${taskId}, status = 'recorded' WHERE command_id = ${commandId}`.pipe(
        Effect.asVoid,
        Effect.mapError(storageFailure),
      ),
    /**
     * Deletes intents that have left `pending` and were created before `cutoff`, in bounded
     * batches. A pending intent is never removed: it may still owe a launch or a stop.
     */
    pruneFinished: Effect.fn("RemoteDelegationIntentStore.pruneFinished")(function* (
      cutoff: string,
    ) {
      let removed = 0;
      while (true) {
        const batch = yield* sql<{ readonly command_id: string }>`
          DELETE FROM jones_remote_delegation_intents
          WHERE command_id IN (
            SELECT command_id FROM jones_remote_delegation_intents
            WHERE status != 'pending' AND created_at < ${cutoff}
            LIMIT ${PRUNE_BATCH_SIZE}
          )
          RETURNING command_id`;
        removed += batch.length;
        if (batch.length < PRUNE_BATCH_SIZE) return removed;
      }
    }, Effect.mapError(storageFailure)),
    finish: (commandId: CommandId, status: "stopped" | "denied") =>
      sql`UPDATE jones_remote_delegation_intents SET status = ${status} WHERE command_id = ${commandId} AND status = 'pending'`.pipe(
        Effect.asVoid,
        Effect.mapError(storageFailure),
      ),
  };
});

// A tagged operation in the released sparse JSON ledger. Legacy launch rows stay
// byte-for-byte untagged and continue through make's original decoder/recovery.
export const FollowUpEnvelope = Schema.Struct({
  kind: Schema.Literal("remote-followup/v1"),
  scope: StoredEnvelope.fields.scope,
  environmentId: EnvironmentId,
  linkedAt: Schema.String,
  label: Schema.String,
  request: StoredEnvelope.fields.request,
  previousTaskId: NodeId,
  previousRemoteRunId: RunId,
  linkGeneration: Schema.NonEmptyString,
  requestDigest: Schema.NonEmptyString,
  targetCreatedAt: Schema.String,
  send: Schema.Struct({
    arguments: Schema.Record(Schema.String, Schema.Unknown),
    limits: Schema.Struct({ runtimeMode: RuntimeMode, interactionMode: ProviderInteractionMode }),
    origin: Schema.Struct({
      schemaVersion: Schema.Literal("t3.linked-message-origin/v1"),
      environmentId: Schema.String,
      thread: Schema.Struct({ threadId: Schema.String, title: Schema.optionalKey(Schema.String) }),
    }),
    context: Schema.optional(LinkedRequestContext),
  }),
});
export type FollowUpEnvelope = typeof FollowUpEnvelope.Type;
export interface FollowUpIntent extends Omit<Intent, "envelope" | "launched"> {
  readonly envelope: FollowUpEnvelope;
  readonly launched: typeof OrchestratorMcpThreadSendResult.Type | null;
}
const decodeFollowUpSelectors = Schema.decodeUnknownSync(
  Schema.Struct({
    threadId: ThreadId,
    message: TrimmedNonEmptyString,
    clientRequestId: Schema.NonEmptyString,
    expectedThreadCreatedAt: Schema.String,
  }),
);
const decodeFollowUpEnvelopeShape = Schema.decodeUnknownSync(FollowUpEnvelope);
const decodeFollowUpEnvelope = (value: unknown): FollowUpEnvelope => {
  const envelope = decodeFollowUpEnvelopeShape(value);
  const selectors = decodeFollowUpSelectors(envelope.send.arguments);
  if (
    selectors.expectedThreadCreatedAt !== envelope.targetCreatedAt ||
    envelope.scope.thread.threadId !== envelope.request.parentThreadId ||
    envelope.scope.environmentId !== envelope.send.origin.environmentId ||
    envelope.scope.thread.threadId !== envelope.send.origin.thread.threadId ||
    !/^[a-f0-9]{64}$/.test(envelope.requestDigest)
  )
    throw new Error("Inconsistent follow-up envelope");
  return envelope;
};
const decodeSent = Schema.decodeUnknownSync(OrchestratorMcpThreadSendResult);
const followUpRow = (row: Row): FollowUpIntent => {
  const envelope = decodeFollowUpEnvelope(decodeJson(row.envelope_json));
  if (row.command_id !== envelope.request.commandId)
    throw new Error("Inconsistent follow-up command");
  return {
    commandId: row.command_id,
    createdAt: row.created_at,
    envelope,
    launched: row.launched_json === null ? null : decodeSent(decodeJson(row.launched_json)),
    taskId: row.task_id,
    launchStarted: row.launch_started === 1,
    admission: row.admission_json === null ? null : decodeAdmission(decodeJson(row.admission_json)),
    status: row.status,
  };
};

export const makeFollowUps = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const get = (commandId: CommandId) =>
    sql<Row>`SELECT * FROM jones_remote_delegation_intents
    WHERE command_id = ${commandId} AND json_extract(envelope_json, '$.kind') = 'remote-followup/v1'`.pipe(
      Effect.flatMap((rows) =>
        Effect.try({
          try: () => (rows[0] === undefined ? undefined : followUpRow(rows[0])),
          catch: storageFailure,
        }),
      ),
      Effect.mapError(storageFailure),
    );
  const find = Effect.fn("FollowUpIntent.find")(function* (commandId: CommandId, request: unknown) {
    const rows =
      yield* sql<Row>`SELECT * FROM jones_remote_delegation_intents WHERE command_id = ${commandId}`.pipe(
        Effect.mapError(storageFailure),
      );
    if (rows[0] === undefined) return undefined;
    if (rows[0].request_json !== canonicalJson(request))
      return yield* new OrchestratorMcpFailure({
        code: "request_conflict",
        message: "This follow-up key already has a different immutable request.",
      });
    return yield* Effect.try({ try: () => followUpRow(rows[0]!), catch: storageFailure });
  });
  return {
    get,
    find,
    pendingForTarget: (parentThreadId: ThreadId, environmentId: EnvironmentId, threadId: string) =>
      sql<Row>`SELECT * FROM jones_remote_delegation_intents WHERE status = 'pending'
        AND json_extract(envelope_json, '$.kind') = 'remote-followup/v1'
        AND json_extract(envelope_json, '$.request.parentThreadId') = ${parentThreadId}
        AND json_extract(envelope_json, '$.environmentId') = ${environmentId}
        AND json_extract(envelope_json, '$.send.arguments.threadId') = ${threadId}
        ORDER BY created_at, command_id LIMIT 1`.pipe(
        Effect.flatMap((rows) =>
          Effect.try({
            try: () => (rows[0] === undefined ? undefined : followUpRow(rows[0])),
            catch: storageFailure,
          }),
        ),
        Effect.mapError(storageFailure),
      ),
    prepare: Effect.fn("FollowUpIntent.prepare")(function* (
      commandId: CommandId,
      request: unknown,
      envelope: FollowUpEnvelope,
    ) {
      const previous = yield* find(commandId, request);
      if (previous !== undefined) return previous;
      // Caller owns the authoritative parent lock and transaction with reservation.
      yield* Effect.try({ try: () => decodeFollowUpEnvelope(envelope), catch: storageFailure });
      const now = DateTime.formatIso(yield* DateTime.now);
      yield* sql`INSERT INTO jones_remote_delegation_intents(command_id, request_json, envelope_json, status, created_at)
        VALUES (${commandId}, ${canonicalJson(request)}, ${encodeJson(envelope)}, 'pending', ${now})`.pipe(
        Effect.mapError(storageFailure),
      );
      return (yield* get(commandId))!;
    }),
    reserved: (id: CommandId, taskId: NodeId) =>
      sql`UPDATE jones_remote_delegation_intents SET task_id = ${taskId}
      WHERE command_id = ${id} AND status = 'pending' AND (task_id IS NULL OR task_id = ${taskId}) RETURNING command_id`.pipe(
        Effect.flatMap((rows) => (rows.length === 1 ? Effect.void : Effect.fail(storageFailure()))),
        Effect.mapError(storageFailure),
      ),
    beginSend: (id: CommandId, taskId: NodeId) =>
      sql`UPDATE jones_remote_delegation_intents SET launch_started = 1
      WHERE command_id = ${id} AND task_id = ${taskId} AND status = 'pending' AND launch_started = 0 RETURNING command_id`.pipe(
        Effect.map((rows) => rows.length === 1),
        Effect.mapError(storageFailure),
      ),
    admission: (id: CommandId, admission: NonNullable<OrchestratorMcpFailure["remoteAdmission"]>) =>
      sql`UPDATE jones_remote_delegation_intents SET admission_json = ${encodeJson(admission)} WHERE command_id = ${id} AND status = 'pending'`.pipe(
        Effect.asVoid,
        Effect.mapError(storageFailure),
      ),
    accepted: (id: CommandId, sent: typeof OrchestratorMcpThreadSendResult.Type) =>
      sql`UPDATE jones_remote_delegation_intents SET launched_json = ${encodeJson(sent)}
      WHERE command_id = ${id} AND status = 'pending' AND (launched_json IS NULL OR launched_json = ${encodeJson(sent)}) RETURNING command_id`.pipe(
        Effect.flatMap((rows) => (rows.length === 1 ? Effect.void : Effect.fail(storageFailure()))),
        Effect.mapError(storageFailure),
      ),
    finish: (id: CommandId, status: FollowUpIntent["status"]) =>
      sql`UPDATE jones_remote_delegation_intents SET status = ${status}
      WHERE command_id = ${id} AND status = 'pending'`.pipe(
        Effect.asVoid,
        Effect.mapError(storageFailure),
      ),
    byTask: (id: NodeId) =>
      sql<{ command_id: CommandId }>`SELECT command_id FROM jones_remote_delegation_intents
      WHERE task_id = ${id} AND json_extract(envelope_json, '$.kind') = 'remote-followup/v1'`.pipe(
        Effect.flatMap((rows) =>
          rows[0] === undefined ? Effect.undefined : get(rows[0].command_id),
        ),
        Effect.mapError(storageFailure),
      ),
    pending: sql<Row>`SELECT * FROM jones_remote_delegation_intents WHERE status = 'pending'
      AND json_extract(envelope_json, '$.kind') = 'remote-followup/v1' ORDER BY created_at, command_id`.pipe(
      Effect.flatMap((rows) =>
        Effect.forEach(rows, (row) =>
          Effect.try({ try: () => followUpRow(row), catch: storageFailure }).pipe(
            Effect.tapError(() =>
              Effect.logWarning("Unreadable follow-up intent retained", {
                commandId: row.command_id,
              }),
            ),
            Effect.option,
          ),
        ),
      ),
      Effect.map((rows) => rows.flatMap((row) => (Option.isSome(row) ? [row.value] : []))),
      Effect.mapError(storageFailure),
    ),
  };
});
