import {
  type CommandId,
  type EnvironmentId,
  type NodeId,
  OrchestratorMcpFailure,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as Schema from "effect/Schema";
import type * as Tool from "effect/ai/Tool";
import type { McpThreadInvocationScope } from "../../mcp/McpInvocationContext.ts";
import type { ProjectToolkit } from "../../mcp/toolkits/project/tools.ts";

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
  readonly envelope: IntentEnvelope;
  readonly launched: LaunchResult | null;
  readonly taskId: NodeId | null;
  readonly launchStarted: boolean;
  readonly admission: OrchestratorMcpFailure["remoteAdmission"] | null;
  readonly status: "pending" | "recorded" | "stopped" | "denied";
}
interface Row {
  readonly command_id: CommandId;
  readonly request_json: string;
  readonly envelope_json: string;
  readonly launched_json: string | null;
  readonly task_id: NodeId | null;
  readonly launch_started: number;
  readonly admission_json: string | null;
  readonly status: Intent["status"];
}

const canonicalRequest = (value: unknown): string => {
  if (Array.isArray(value)) return `[${value.map(canonicalRequest).join(",")}]`;
  if (value !== null && typeof value === "object")
    return `{${Object.entries(value)
      .filter(([, child]) => child !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([key, child]) => `${JSON.stringify(key)}:${canonicalRequest(child)}`)
      .join(",")}}`;
  return JSON.stringify(value);
};

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const storageFailure = () =>
  new OrchestratorMcpFailure({
    code: "orchestration_error",
    message: "Unable to persist or read remote delegation intent; remote execution may be pending.",
  });
const decode = (row: Row): Intent => ({
  commandId: row.command_id,
  envelope: decodeJson(row.envelope_json) as IntentEnvelope,
  launched: row.launched_json === null ? null : (decodeJson(row.launched_json) as LaunchResult),
  taskId: row.task_id,
  launchStarted: row.launch_started === 1,
  admission:
    row.admission_json === null
      ? null
      : (decodeJson(row.admission_json) as OrchestratorMcpFailure["remoteAdmission"]),
  status: row.status,
});

/** The intent commits before any remote mutation. Replays retain the original scope and payload. */
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const read = (commandId: CommandId) =>
    sql<Row>`SELECT * FROM jones_remote_delegation_intents WHERE command_id = ${commandId}`;
  const find = Effect.fn("RemoteDelegationIntentStore.find")(function*(commandId: CommandId, request: unknown) {
      const row = (yield* read(commandId).pipe(Effect.mapError(storageFailure)))[0];
      if (row === undefined) return undefined;
      if (row.request_json !== canonicalRequest(request))
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
        VALUES (${commandId}, ${canonicalRequest(request)}, ${encodeJson(envelope)}, 'pending', ${now})`;
          return {
            commandId,
            envelope,
            launched: null,
            taskId: null,
            launchStarted: false,
            admission: null,
            status: "pending",
          } satisfies Intent;
        }),
      )
      .pipe(
        Effect.mapError((error) =>
          Schema.is(OrchestratorMcpFailure)(error) ? error : storageFailure(),
        ),
      );
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
    pending:
      sql<Row>`SELECT * FROM jones_remote_delegation_intents WHERE status = 'pending' ORDER BY created_at, command_id`.pipe(
        Effect.flatMap((rows) =>
          Effect.try({ try: () => rows.map(decode), catch: storageFailure }),
        ),
        Effect.mapError(storageFailure),
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
    finish: (commandId: CommandId, status: "stopped" | "denied") =>
      sql`UPDATE jones_remote_delegation_intents SET status = ${status} WHERE command_id = ${commandId} AND status = 'pending'`.pipe(
        Effect.asVoid,
        Effect.mapError(storageFailure),
      ),
  };
});
