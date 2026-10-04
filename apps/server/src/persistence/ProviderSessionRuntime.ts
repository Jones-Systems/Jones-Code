import * as Arr from "effect/Array";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";

import {
  AgentSessionImportSource,
  IsoDateTime,
  ProviderInstanceId,
  ProviderDriverKind,
  RuntimeMode,
  ThreadId,
} from "@t3tools/contracts";

import {
  PersistenceDecodeError,
  type PersistenceErrorCorrelation,
  PersistenceSqlError,
  type ProviderSessionRuntimeRepositoryError,
} from "./Errors.ts";

/**
 * ProviderSessionRuntimeRepository - Repository interface for provider runtime sessions.
 *
 * Owns persistence operations for provider runtime metadata and resume cursors.
 *
 * @module ProviderSessionRuntimeRepository
 */

const ProviderSessionRuntimeStatus = Schema.Literals(["starting", "running", "stopped", "error"]);

export const ProviderSessionRuntime = Schema.Struct({
  threadId: ThreadId,
  providerName: Schema.String,
  /**
   * User-defined routing key for the configured provider instance that
   * owns this session. Nullable only at the storage/migration boundary:
   * rows persisted before the driver/instance split carry only
   * `providerName`. Repository consumers must materialize a concrete
   * instance id before routing.
   */
  providerInstanceId: Schema.NullOr(ProviderInstanceId),
  adapterKey: Schema.String,
  runtimeMode: RuntimeMode,
  status: ProviderSessionRuntimeStatus,
  lastSeenAt: IsoDateTime,
  resumeCursor: Schema.NullOr(Schema.Unknown),
  runtimePayload: Schema.NullOr(Schema.Unknown),
});
export type ProviderSessionRuntime = typeof ProviderSessionRuntime.Type;

export const LegacyStoppedRuntimeProofV1 = Schema.Struct({
  schema: Schema.Literal("t3.legacy-stopped-runtime-proof/v1"),
  source: Schema.Literal("persisted_runtime_row"),
  threadId: ThreadId,
  providerInstanceId: Schema.NullOr(ProviderInstanceId),
  driver: ProviderDriverKind,
  nativeThreadId: Schema.NonEmptyString,
  status: Schema.Literal("stopped"),
});
export type LegacyStoppedRuntimeProofV1 = typeof LegacyStoppedRuntimeProofV1.Type;

const LegacyContinuationAccessibilityV1 = Schema.Struct({
  providerInstanceId: ProviderInstanceId, driver: ProviderDriverKind,
  nativeThreadId: Schema.NonEmptyString, continuationKey: Schema.NonEmptyString,
  source: Schema.Literals(["historical_store", "native_read"]),
});
const LegacyHistoricalSourceIdentityV1 = Schema.Struct({
  storeIdentity: Schema.NonEmptyString, sourceHomeIdentity: Schema.NonEmptyString,
});
export const LegacyProviderContinuationEvidenceV1 = Schema.Struct({
  threadId: ThreadId,
  provenance: Schema.Literals(["legacy_row", "native_import"]),
  providerInstanceId: Schema.NullOr(ProviderInstanceId), driver: ProviderDriverKind,
  nativeThreadId: Schema.NullOr(Schema.NonEmptyString), status: ProviderSessionRuntimeStatus,
  continuationKey: Schema.NullOr(Schema.NonEmptyString),
  historicalSourceIdentity: Schema.NullOr(LegacyHistoricalSourceIdentityV1),
  stoppedProof: Schema.NullOr(LegacyStoppedRuntimeProofV1),
  accessibility: Schema.NullOr(LegacyContinuationAccessibilityV1),
});
export type LegacyProviderContinuationEvidenceV1 = typeof LegacyProviderContinuationEvidenceV1.Type;

export interface LegacyProviderContinuationInputV1 {
  readonly sourceRow?: ProviderSessionRuntime;
  readonly driver: ProviderDriverKind;
  readonly nativeThreadId: string | null;
  readonly continuationKey: string | null;
  readonly historicalSourceIdentity: typeof LegacyHistoricalSourceIdentityV1.Type | null;
  readonly accessibility: typeof LegacyContinuationAccessibilityV1.Type | null;
  readonly target: {
    readonly providerInstanceId: ProviderInstanceId;
    readonly driver: ProviderDriverKind;
    readonly continuationKey: string;
    readonly supportsNativeResume: boolean;
  };
}

export class LegacyProviderContinuationInputsV1 extends Context.Reference<
  ReadonlyMap<ThreadId, LegacyProviderContinuationInputV1>
>("t3/persistence/LegacyProviderContinuationInputsV1", { defaultValue: () => new Map() }) {}

/** Only a decoded persisted stopped row can establish historical stop evidence. */
export function makeLegacyStoppedRuntimeProofV1(input: {
  readonly sourceRow: ProviderSessionRuntime;
  readonly source: "persisted_runtime_row" | "synthetic_import";
  readonly driver: ProviderDriverKind;
  readonly nativeThreadId: string | null;
}): LegacyStoppedRuntimeProofV1 | null {
  if (input.source !== "persisted_runtime_row") return null;
  const row = Schema.decodeUnknownSync(ProviderSessionRuntime)(input.sourceRow);
  if (row.status !== "stopped" || row.providerName !== input.driver ||
      input.nativeThreadId === null || input.nativeThreadId.trim().length === 0) return null;
  const cursor = row.resumeCursor;
  if (cursor === null || typeof cursor !== "object" || Array.isArray(cursor)) return null;
  const nativeId = input.driver === "codex" && "threadId" in cursor ? cursor.threadId :
    input.driver === "claudeAgent" ? ("resume" in cursor && typeof cursor.resume === "string" ? cursor.resume :
      "sessionId" in cursor ? cursor.sessionId : undefined) : undefined;
  if (nativeId !== input.nativeThreadId || (input.driver === "claudeAgent" &&
      !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(input.nativeThreadId))) return null;
  return Schema.decodeUnknownSync(LegacyStoppedRuntimeProofV1)({
    schema: "t3.legacy-stopped-runtime-proof/v1", source: "persisted_runtime_row",
    threadId: row.threadId, providerInstanceId: row.providerInstanceId,
    driver: input.driver, nativeThreadId: input.nativeThreadId, status: row.status,
  });
}

export function makeLegacyProviderContinuationEvidenceV1(input: {
  readonly sourceRow: ProviderSessionRuntime;
  readonly provenance: "legacy_row" | "native_import";
  readonly driver: ProviderDriverKind;
  readonly nativeThreadId: string | null;
  readonly continuationKey: string | null;
  readonly historicalSourceIdentity: typeof LegacyHistoricalSourceIdentityV1.Type | null;
  readonly stoppedProof: LegacyStoppedRuntimeProofV1 | null;
  readonly accessibility: typeof LegacyContinuationAccessibilityV1.Type | null;
}): LegacyProviderContinuationEvidenceV1 {
  const row = Schema.decodeUnknownSync(ProviderSessionRuntime)(input.sourceRow);
  const proof = input.stoppedProof;
  if (proof !== null && (proof.threadId !== row.threadId || proof.providerInstanceId !== row.providerInstanceId ||
      proof.driver !== input.driver || proof.nativeThreadId !== input.nativeThreadId || proof.status !== row.status ||
      makeLegacyStoppedRuntimeProofV1({ sourceRow: row, source: "persisted_runtime_row",
        driver: input.driver, nativeThreadId: input.nativeThreadId }) === null)) {
    throw new Error("Legacy stopped runtime proof does not match its historical source row.");
  }
  return Schema.decodeUnknownSync(LegacyProviderContinuationEvidenceV1)({
    threadId: row.threadId, provenance: input.provenance, providerInstanceId: row.providerInstanceId,
    driver: input.driver, nativeThreadId: input.nativeThreadId, status: row.status,
    continuationKey: input.continuationKey, historicalSourceIdentity: input.historicalSourceIdentity,
    stoppedProof: proof, accessibility: input.accessibility,
  });
}


export const GetProviderSessionRuntimeInput = Schema.Struct({ threadId: ThreadId });
export type GetProviderSessionRuntimeInput = typeof GetProviderSessionRuntimeInput.Type;

export const DeleteProviderSessionRuntimeInput = Schema.Struct({ threadId: ThreadId });
export type DeleteProviderSessionRuntimeInput = typeof DeleteProviderSessionRuntimeInput.Type;

export const RecordImportedTranscriptInput = Schema.Struct({
  threadId: ThreadId,
  source: AgentSessionImportSource,
});
export type RecordImportedTranscriptInput = typeof RecordImportedTranscriptInput.Type;

export interface ProviderSessionRuntimeUpsertOptions {
  readonly onConflict?: "update" | "ignore";
}

/**
 * ProviderSessionRuntimeRepository - Service tag for provider runtime persistence.
 */
export class ProviderSessionRuntimeRepository extends Context.Service<
  ProviderSessionRuntimeRepository,
  {
    /**
     * Insert or replace a provider runtime row.
     *
     * Upserts by canonical `threadId`, retaining imported transcript records
     * from the current database row.
     */
    readonly upsert: (
      runtime: ProviderSessionRuntime,
      options?: ProviderSessionRuntimeUpsertOptions,
    ) => Effect.Effect<void, ProviderSessionRuntimeRepositoryError>;

    /** Record one source file without replacing the current session state. */
    readonly recordImportedTranscript: (
      input: RecordImportedTranscriptInput,
    ) => Effect.Effect<void, ProviderSessionRuntimeRepositoryError>;

    /**
     * Read provider runtime state by canonical thread id.
     */
    readonly getByThreadId: (
      input: GetProviderSessionRuntimeInput,
    ) => Effect.Effect<
      Option.Option<ProviderSessionRuntime>,
      ProviderSessionRuntimeRepositoryError
    >;

    /**
     * List provider runtime rows.
     *
     * Returned in ascending last-seen order. `excludeStopped` filters stopped
     * rows in SQL. Long-lived installs keep thousands for their resume cursors.
     */
    readonly list: (options?: {
      readonly excludeStopped?: boolean;
    }) => Effect.Effect<
      ReadonlyArray<ProviderSessionRuntime>,
      ProviderSessionRuntimeRepositoryError
    >;

    /**
     * Delete provider runtime state by canonical thread id.
     */
    readonly deleteByThreadId: (
      input: DeleteProviderSessionRuntimeInput,
    ) => Effect.Effect<void, ProviderSessionRuntimeRepositoryError>;
  }
>()("t3/persistence/ProviderSessionRuntime/ProviderSessionRuntimeRepository") {}

const ProviderSessionRuntimeDbRowSchema = ProviderSessionRuntime.mapFields(
  Struct.assign({
    resumeCursor: Schema.NullOr(Schema.fromJsonString(Schema.Unknown)),
    runtimePayload: Schema.NullOr(Schema.fromJsonString(Schema.Unknown)),
  }),
);

const ProviderSessionRuntimeRawDbRowSchema = Schema.Struct({
  threadId: Schema.String,
  providerName: Schema.Unknown,
  providerInstanceId: Schema.Unknown,
  adapterKey: Schema.Unknown,
  runtimeMode: Schema.Unknown,
  status: Schema.Unknown,
  lastSeenAt: Schema.Unknown,
  resumeCursor: Schema.Unknown,
  runtimePayload: Schema.Unknown,
});

const decodeRuntimeRow = Schema.decodeUnknownEffect(ProviderSessionRuntimeDbRowSchema);

const GetRuntimeRequestSchema = Schema.Struct({
  threadId: ThreadId,
});

const DeleteRuntimeRequestSchema = GetRuntimeRequestSchema;

const RecordImportedTranscriptRequestSchema = RecordImportedTranscriptInput.mapFields(
  Struct.assign({ source: Schema.fromJsonString(AgentSessionImportSource) }),
);

function toPersistenceSqlOrDecodeError(
  sqlOperation: string,
  decodeOperation: string,
  correlation?: PersistenceErrorCorrelation,
) {
  return (cause: unknown): ProviderSessionRuntimeRepositoryError =>
    Schema.isSchemaError(cause)
      ? PersistenceDecodeError.fromSchemaError(decodeOperation, cause, correlation)
      : new PersistenceSqlError({
          operation: sqlOperation,
          ...(correlation === undefined ? {} : { correlation }),
          cause,
        });
}

/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // Runtime writes can carry stale payloads. Only recordImportedTranscript may
  // change source records, so restore that field from the row being updated.
  const upsertRuntimeRow = SqlSchema.void({
    Request: ProviderSessionRuntimeDbRowSchema,
    execute: (runtime) =>
      sql`
        INSERT INTO provider_session_runtime (
          thread_id,
          provider_name,
          provider_instance_id,
          adapter_key,
          runtime_mode,
          status,
          last_seen_at,
          resume_cursor_json,
          runtime_payload_json
        )
        VALUES (
          ${runtime.threadId},
          ${runtime.providerName},
          ${runtime.providerInstanceId},
          ${runtime.adapterKey},
          ${runtime.runtimeMode},
          ${runtime.status},
          ${runtime.lastSeenAt},
          ${runtime.resumeCursor},
          CASE
            WHEN json_type(${runtime.runtimePayload}) = 'object'
            THEN json_remove(${runtime.runtimePayload}, '$.importedTranscripts')
            ELSE ${runtime.runtimePayload}
          END
        )
        ON CONFLICT (thread_id)
        DO UPDATE SET
          provider_name = excluded.provider_name,
          provider_instance_id = excluded.provider_instance_id,
          adapter_key = excluded.adapter_key,
          runtime_mode = excluded.runtime_mode,
          status = excluded.status,
          last_seen_at = excluded.last_seen_at,
          resume_cursor_json = excluded.resume_cursor_json,
          runtime_payload_json = CASE
            WHEN json_type(
              CASE
                WHEN json_valid(provider_session_runtime.runtime_payload_json)
                THEN provider_session_runtime.runtime_payload_json
                ELSE '{}'
              END,
              '$.importedTranscripts'
            ) IS NOT NULL
            THEN json_set(
              CASE
                WHEN json_type(excluded.runtime_payload_json) = 'object'
                THEN excluded.runtime_payload_json
                ELSE '{}'
              END,
              '$.importedTranscripts',
              json_extract(provider_session_runtime.runtime_payload_json, '$.importedTranscripts')
            )
            ELSE excluded.runtime_payload_json
          END
      `,
  });

  const insertRuntimeRow = SqlSchema.void({
    Request: ProviderSessionRuntimeDbRowSchema,
    execute: (runtime) =>
      sql`
        INSERT INTO provider_session_runtime (
          thread_id,
          provider_name,
          provider_instance_id,
          adapter_key,
          runtime_mode,
          status,
          last_seen_at,
          resume_cursor_json,
          runtime_payload_json
        )
        VALUES (
          ${runtime.threadId},
          ${runtime.providerName},
          ${runtime.providerInstanceId},
          ${runtime.adapterKey},
          ${runtime.runtimeMode},
          ${runtime.status},
          ${runtime.lastSeenAt},
          ${runtime.resumeCursor},
          CASE
            WHEN json_type(${runtime.runtimePayload}) = 'object'
            THEN json_remove(${runtime.runtimePayload}, '$.importedTranscripts')
            ELSE ${runtime.runtimePayload}
          END
        )
        ON CONFLICT (thread_id) DO NOTHING
      `,
  });

  const recordImportedTranscriptRow = SqlSchema.void({
    Request: RecordImportedTranscriptRequestSchema,
    execute: ({ threadId, source }) =>
      sql`
        WITH current_runtime AS (
          SELECT CASE
            WHEN json_valid(runtime_payload_json) THEN CASE
              WHEN json_type(runtime_payload_json) = 'object' THEN runtime_payload_json
              ELSE '{}'
            END
            ELSE '{}'
          END AS payload
          FROM provider_session_runtime
          WHERE thread_id = ${threadId}
        )
        UPDATE provider_session_runtime
        SET runtime_payload_json = (
          SELECT json_set(
            payload,
            '$.importedTranscripts',
            json((
              SELECT json_group_array(json(value))
              FROM (
                SELECT value
                FROM json_each(CASE
                  WHEN json_type(payload, '$.importedTranscripts') = 'array'
                  THEN json_extract(payload, '$.importedTranscripts')
                  ELSE '[]'
                END)
                WHERE CASE
                  WHEN type = 'object' THEN
                    json_extract(value, '$.providerInstanceId')
                      IS NOT json_extract(${source}, '$.providerInstanceId')
                    OR json_extract(value, '$.filePath') IS NOT json_extract(${source}, '$.filePath')
                  ELSE 0
                END
                UNION ALL
                SELECT ${source} AS value
              )
            ))
          )
          FROM current_runtime
        )
        WHERE thread_id = ${threadId}
      `,
  });

  const getRuntimeRowByThreadId = SqlSchema.findOneOption({
    Request: GetRuntimeRequestSchema,
    Result: ProviderSessionRuntimeRawDbRowSchema,
    execute: ({ threadId }) =>
      sql`
        SELECT
          thread_id AS "threadId",
          provider_name AS "providerName",
          provider_instance_id AS "providerInstanceId",
          adapter_key AS "adapterKey",
          runtime_mode AS "runtimeMode",
          status,
          last_seen_at AS "lastSeenAt",
          resume_cursor_json AS "resumeCursor",
          runtime_payload_json AS "runtimePayload"
        FROM provider_session_runtime
        WHERE thread_id = ${threadId}
      `,
  });

  const listRuntimeRows = SqlSchema.findAll({
    Request: Schema.Struct({ excludeStopped: Schema.Boolean }),
    Result: ProviderSessionRuntimeRawDbRowSchema,
    execute: ({ excludeStopped }) =>
      sql`
        SELECT
          thread_id AS "threadId",
          provider_name AS "providerName",
          provider_instance_id AS "providerInstanceId",
          adapter_key AS "adapterKey",
          runtime_mode AS "runtimeMode",
          status,
          last_seen_at AS "lastSeenAt",
          resume_cursor_json AS "resumeCursor",
          runtime_payload_json AS "runtimePayload"
        FROM provider_session_runtime
        ${excludeStopped ? sql`WHERE status != 'stopped'` : sql``}
        ORDER BY last_seen_at ASC, thread_id ASC
      `,
  });

  const deleteRuntimeByThreadId = SqlSchema.void({
    Request: DeleteRuntimeRequestSchema,
    execute: ({ threadId }) =>
      sql`
        DELETE FROM provider_session_runtime
        WHERE thread_id = ${threadId}
      `,
  });

  const upsert: ProviderSessionRuntimeRepository["Service"]["upsert"] = (runtime, options) =>
    (options?.onConflict === "ignore" ? insertRuntimeRow(runtime) : upsertRuntimeRow(runtime)).pipe(
      Effect.mapError(
        toPersistenceSqlOrDecodeError(
          "ProviderSessionRuntimeRepository.upsert:query",
          "ProviderSessionRuntimeRepository.upsert:encodeRequest",
          { threadId: runtime.threadId },
        ),
      ),
    );

  const recordImportedTranscript: ProviderSessionRuntimeRepository["Service"]["recordImportedTranscript"] =
    (input) =>
      recordImportedTranscriptRow(input).pipe(
        Effect.mapError(
          toPersistenceSqlOrDecodeError(
            "ProviderSessionRuntimeRepository.recordImportedTranscript:query",
            "ProviderSessionRuntimeRepository.recordImportedTranscript:encodeRequest",
            { threadId: input.threadId },
          ),
        ),
      );

  const getByThreadId: ProviderSessionRuntimeRepository["Service"]["getByThreadId"] = (input) =>
    getRuntimeRowByThreadId(input).pipe(
      Effect.mapError(
        toPersistenceSqlOrDecodeError(
          "ProviderSessionRuntimeRepository.getByThreadId:query",
          "ProviderSessionRuntimeRepository.getByThreadId:decodeRow",
          { threadId: input.threadId },
        ),
      ),
      Effect.flatMap((runtimeRowOption) =>
        Option.match(runtimeRowOption, {
          onNone: () => Effect.succeedNone,
          onSome: (row) =>
            decodeRuntimeRow(row).pipe(
              Effect.mapError((cause) =>
                PersistenceDecodeError.fromSchemaError(
                  "ProviderSessionRuntimeRepository.getByThreadId:decodeRow",
                  cause,
                  { threadId: input.threadId },
                ),
              ),
              Effect.asSome,
            ),
        }),
      ),
    );

  const list: ProviderSessionRuntimeRepository["Service"]["list"] = (options) =>
    listRuntimeRows({ excludeStopped: options?.excludeStopped === true }).pipe(
      Effect.mapError(
        toPersistenceSqlOrDecodeError(
          "ProviderSessionRuntimeRepository.list:query",
          "ProviderSessionRuntimeRepository.list:decodeRows",
        ),
      ),
      Effect.flatMap((rows) =>
        // Skip rows that no longer decode (e.g. written by an older build)
        // instead of failing the whole list — one stale row must not disable
        // every consumer that enumerates sessions, such as the reaper.
        Effect.forEach(rows, (row) =>
          decodeRuntimeRow(row).pipe(
            Effect.asSome,
            Effect.catch((cause) =>
              Effect.logWarning("provider.session.runtime.row-skipped", {
                threadId: row.threadId,
                error: PersistenceDecodeError.fromSchemaError(
                  "ProviderSessionRuntimeRepository.list:decodeRows",
                  cause,
                  { threadId: row.threadId },
                ).message,
              }).pipe(Effect.as(Option.none<ProviderSessionRuntime>())),
            ),
          ),
        ),
      ),
      Effect.map((decoded) =>
        Arr.filterMap(decoded, (row) =>
          Option.isSome(row) ? Result.succeed(row.value) : Result.failVoid,
        ),
      ),
    );

  const deleteByThreadId: ProviderSessionRuntimeRepository["Service"]["deleteByThreadId"] = (
    input,
  ) =>
    deleteRuntimeByThreadId(input).pipe(
      Effect.mapError(
        (cause) =>
          new PersistenceSqlError({
            operation: "ProviderSessionRuntimeRepository.deleteByThreadId:query",
            correlation: { threadId: input.threadId },
            cause,
          }),
      ),
    );

  return {
    upsert,
    recordImportedTranscript,
    getByThreadId,
    list,
    deleteByThreadId,
  } satisfies ProviderSessionRuntimeRepository["Service"];
});

export const layer = Layer.effect(ProviderSessionRuntimeRepository, make);
