import {
  ThreadId,
  ProviderThreadId,
  ProviderSessionId,
  ProviderInstanceId,
  ProviderDriverKind,
  RunId,
  RunAttemptId,
  type OrchestrationV2StoredEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { hasOwnJonesMigration } from "../persistence/JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "../persistence/Migrations.ts";
import {
  NativeProviderRuntimeBindingV1,
  NativeProviderRuntimeObservationV1,
  NativeProviderContinuationSourceIdentityV1,
} from "../nativeCreation/NativeCreationExecutionTypes.ts";
import {
  nativeCreationCanonicalJson,
  nativeCreationSha256,
} from "../nativeCreation/NativeCreationPreparation.ts";
import { makeCommitTransaction } from "./CommitTransaction.ts";
export class NativeProviderRuntimeEvidenceError extends Schema.TaggedError<NativeProviderRuntimeEvidenceError>()(
  "NativeProviderRuntimeEvidenceError",
  { cause: Schema.optional(Schema.Defect()) },
) {}
export interface ProviderBindingExpectationV2 {
  readonly threadId: ThreadId;
  readonly providerThreadId: ProviderThreadId;
  readonly providerSessionId: ProviderSessionId;
  readonly instanceId: ProviderInstanceId;
  readonly driver: ProviderDriverKind;
  readonly nativeThreadId: string | null;
  readonly runtimeGeneration: string | null;
}
export interface ProviderRuntimeEvidenceV2 {
  readonly binding: ProviderBindingExpectationV2;
  readonly evidenceRevision: number;
  readonly observation: NativeProviderRuntimeObservationV1 | null;
  readonly registeredAt: string;
}
export type ProviderBindingWriteResultV2 =
  | {
      readonly committed: true;
      readonly evidenceRevision: number;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
    }
  | {
      readonly committed: false;
      readonly rejection:
        | "binding_mismatch"
        | "evidence_revision_mismatch"
        | "unregistered_generation"
        | "attempt_mismatch";
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
    };

export interface NativeProviderRuntimeEvidenceShape {
  readonly readCurrentProviderRuntimeOwner: (
    threadId: ThreadId,
  ) => Effect.Effect<ProviderRuntimeEvidenceV2 | null, NativeProviderRuntimeEvidenceError>;
  readonly readProviderRuntimeEvidence: (
    threadId: ThreadId,
  ) => Effect.Effect<ProviderRuntimeEvidenceV2 | null, NativeProviderRuntimeEvidenceError>;
  readonly registerProviderRuntime: (input: {
    readonly expectedBinding: ProviderBindingExpectationV2;
    readonly expectedRegisteredBinding?: ProviderBindingExpectationV2 | null;
    readonly expectedEvidenceRevision: number;
    readonly actualBinding: NativeProviderRuntimeBindingV1;
    readonly actualContinuationSourceIdentity?: NativeProviderContinuationSourceIdentityV1;
    readonly expectedRunId?: RunId;
    readonly expectedRunAttemptId?: RunAttemptId;
  }) => Effect.Effect<ProviderBindingWriteResultV2, NativeProviderRuntimeEvidenceError>;
}
type TransactionOwner = Effect.Success<ReturnType<typeof makeCommitTransaction>>;
export const makeNativeProviderRuntimeEvidence = (transaction: TransactionOwner) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const eligibleNativeStorage = hasOwnJonesMigration(jonesMigrationEntries, [
      142,
      "V2NativeAcceptance",
    ]).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
      Effect.orElseSucceed(() => false),
    );
    const readProviderRuntimeEvidenceEffect = Effect.fnUntraced(function* (threadId: ThreadId) {
      if (!(yield* eligibleNativeStorage)) return null;
      const rows = yield* sql<{
        readonly thread_id: string;
        readonly provider_thread_id: string;
        readonly provider_session_id: string;
        readonly provider_instance_id: string;
        readonly driver: ProviderDriverKind;
        readonly native_thread_id: string | null;
        readonly runtime_generation: string;
        readonly evidence_revision: number;
        readonly observation_json: string | null;
        readonly registered_at: string;
      }>`
        SELECT * FROM orchestration_v2_provider_runtime_evidence WHERE thread_id = ${threadId}`;
      if (rows.length === 0) return null;
      const row = rows[0]!;
      const binding = yield* Schema.decodeUnknownEffect(
        Schema.Struct({
          threadId: ThreadId,
          providerThreadId: ProviderThreadId,
          providerSessionId: ProviderSessionId,
          instanceId: ProviderInstanceId,
          driver: ProviderDriverKind,
          nativeThreadId: Schema.NullOr(Schema.String),
          runtimeGeneration: Schema.NonEmptyString,
        }),
      )({
        threadId: row.thread_id,
        providerThreadId: row.provider_thread_id,
        providerSessionId: row.provider_session_id,
        instanceId: row.provider_instance_id,
        driver: row.driver,
        nativeThreadId: row.native_thread_id,
        runtimeGeneration: row.runtime_generation,
      });
      if (!Number.isSafeInteger(row.evidence_revision) || row.evidence_revision < 1)
        return yield* new NativeProviderRuntimeEvidenceError({
          cause: "Invalid runtime evidence revision",
        });
      return {
        binding,
        evidenceRevision: row.evidence_revision,
        observation:
          row.observation_json === null
            ? null
            : yield* Schema.decodeUnknownEffect(
                Schema.fromJsonString(NativeProviderRuntimeObservationV1),
              )(row.observation_json),
        registeredAt: row.registered_at,
      } satisfies ProviderRuntimeEvidenceV2;
    });
    const rejectProviderBinding = (
      rejection: Extract<ProviderBindingWriteResultV2, { committed: false }>["rejection"],
    ) => ({
      committed: false as const,
      rejection,
      storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
    });
    const continuationBindingSchema = Schema.Struct({
      threadId: ThreadId,
      providerThreadId: ProviderThreadId,
      providerSessionId: ProviderSessionId,
      instanceId: ProviderInstanceId,
      driver: ProviderDriverKind,
      nativeThreadId: Schema.NonEmptyString,
      runtimeGeneration: Schema.NonEmptyString,
    });
    const checkProviderBinding = Effect.fnUntraced(function* (input: {
      readonly expectedBinding: ProviderBindingExpectationV2;
      readonly expectedEvidenceRevision: number;
      readonly expectedRegisteredBinding?: ProviderBindingExpectationV2 | null;
      readonly expectedRunId?: RunId;
      readonly expectedRunAttemptId?: RunAttemptId;
    }) {
      const expected = input.expectedBinding;
      const rows = yield* sql<{
        readonly active_provider_thread_id: string | null;
        readonly model_instance: string | null;
        readonly thread_id: string | null;
        readonly provider_session_id: string | null;
        readonly provider_instance_id: string | null;
        readonly driver: string | null;
        readonly native_thread_id: string | null;
        readonly session_status: string;
        readonly session_instance: string | null;
        readonly session_driver: string | null;
      }>`
        SELECT json_extract(t.payload_json, '$.activeProviderThreadId') AS active_provider_thread_id,
          json_extract(t.payload_json, '$.modelSelection.instanceId') AS model_instance,
          p.thread_id, p.provider_session_id, p.provider_instance_id, p.driver,
          json_extract(p.payload_json, '$.nativeThreadRef.nativeId') AS native_thread_id,
          s.status AS session_status, s.provider_instance_id AS session_instance, s.driver AS session_driver
        FROM orchestration_v2_projection_threads t
        JOIN orchestration_v2_projection_provider_threads p ON p.provider_thread_id = ${expected.providerThreadId}
        JOIN orchestration_v2_projection_provider_sessions s ON s.provider_session_id = p.provider_session_id
        JOIN orchestration_v2_projection_provider_session_bindings b ON b.provider_session_id = s.provider_session_id AND b.thread_id = t.thread_id
        WHERE t.thread_id = ${expected.threadId}`;
      const row = rows[0];
      if (
        rows.length !== 1 ||
        row === undefined ||
        row.active_provider_thread_id !== expected.providerThreadId ||
        row.model_instance !== expected.instanceId ||
        row.thread_id !== expected.threadId ||
        row.provider_session_id !== expected.providerSessionId ||
        row.provider_instance_id !== expected.instanceId ||
        row.driver !== expected.driver ||
        row.native_thread_id !== expected.nativeThreadId ||
        row.session_instance !== expected.instanceId ||
        row.session_driver !== expected.driver ||
        row.session_status === "stopped" ||
        row.session_status === "error"
      )
        return "binding_mismatch" as const;
      const current = yield* readProviderRuntimeEvidenceEffect(expected.threadId);
      if ((current?.evidenceRevision ?? 0) !== input.expectedEvidenceRevision)
        return "evidence_revision_mismatch" as const;
      const registeredExpected =
        input.expectedRegisteredBinding === undefined ? expected : input.expectedRegisteredBinding;
      if (
        current === null
          ? registeredExpected !== null && registeredExpected.runtimeGeneration !== null
          : registeredExpected === null ||
            nativeCreationCanonicalJson(current.binding) !==
              nativeCreationCanonicalJson(registeredExpected)
      )
        return "unregistered_generation" as const;
      if (input.expectedRunId !== undefined || input.expectedRunAttemptId !== undefined) {
        if (input.expectedRunId === undefined || input.expectedRunAttemptId === undefined)
          return "attempt_mismatch" as const;
        const attempts = yield* sql`
          SELECT r.run_id FROM orchestration_v2_projection_runs r
          JOIN orchestration_v2_projection_run_attempts a ON a.attempt_id = ${input.expectedRunAttemptId} AND a.run_id = r.run_id AND a.thread_id = r.thread_id
          WHERE r.run_id = ${input.expectedRunId} AND r.thread_id = ${expected.threadId}
            AND json_extract(r.payload_json, '$.activeAttemptId') = ${input.expectedRunAttemptId}
            AND json_extract(a.payload_json, '$.providerThreadId') = ${expected.providerThreadId}`;
        if (attempts.length !== 1) return "attempt_mismatch" as const;
      }
      return null;
    });
    const registerProviderRuntimeEffect = Effect.fnUntraced(function* (
      input: Parameters<NativeProviderRuntimeEvidenceShape["registerProviderRuntime"]>[0],
    ) {
      return yield* transaction.withTransaction(
        Effect.gen(function* () {
          if (!(yield* eligibleNativeStorage))
            return rejectProviderBinding("unregistered_generation");
          const rejection = yield* checkProviderBinding(input);
          if (rejection !== null) return rejectProviderBinding(rejection);
          const actual = yield* Schema.decodeUnknownEffect(NativeProviderRuntimeBindingV1)(
            input.actualBinding,
          );
          const expected = input.expectedBinding;
          if (
            actual.threadId !== expected.threadId ||
            actual.providerThreadId !== expected.providerThreadId ||
            actual.providerSessionId !== expected.providerSessionId ||
            actual.instanceId !== expected.instanceId ||
            (actual.nativeThreadId ?? null) !== expected.nativeThreadId
          )
            return rejectProviderBinding("binding_mismatch");
          const sourceIdentity =
            input.actualContinuationSourceIdentity === undefined
              ? null
              : yield* Schema.decodeUnknownEffect(NativeProviderContinuationSourceIdentityV1)(
                  input.actualContinuationSourceIdentity,
                );
          if (
            sourceIdentity !== null &&
            (sourceIdentity.driverKind !== expected.driver ||
              sourceIdentity.runtimeGeneration !== actual.runtimeGeneration)
          )
            return rejectProviderBinding("binding_mismatch");
          const sourceBinding =
            sourceIdentity === null || actual.nativeThreadId === undefined
              ? null
              : yield* Schema.decodeUnknownEffect(continuationBindingSchema)({
                  ...actual,
                  driver: expected.driver,
                });
          const sourceId =
            sourceBinding === null
              ? null
              : nativeCreationSha256(nativeCreationCanonicalJson(sourceBinding));
          const previousSource =
            sourceId === null
              ? []
              : yield* sql<{ readonly continuation_key: string }>`
          SELECT continuation_key FROM orchestration_v2_provider_continuation_sources WHERE source_id = ${sourceId}`;
          if (
            previousSource.length > 1 ||
            (previousSource.length === 1 &&
              previousSource[0]!.continuation_key !== sourceIdentity?.continuationKey)
          )
            return rejectProviderBinding("binding_mismatch");
          const revision = input.expectedEvidenceRevision + 1;
          const now = DateTime.formatIso(yield* DateTime.now);
          if (input.expectedEvidenceRevision === 0) {
            yield* sql`INSERT INTO orchestration_v2_provider_runtime_evidence
            (thread_id, provider_thread_id, provider_session_id, provider_instance_id, driver, native_thread_id,
              runtime_generation, evidence_revision, observation_json, registered_at)
            VALUES (${actual.threadId}, ${actual.providerThreadId}, ${actual.providerSessionId}, ${actual.instanceId}, ${expected.driver},
              ${actual.nativeThreadId ?? null}, ${actual.runtimeGeneration}, ${revision}, NULL, ${now})`;
          } else {
            const rows = yield* sql`UPDATE orchestration_v2_provider_runtime_evidence SET
            provider_thread_id = ${actual.providerThreadId}, provider_session_id = ${actual.providerSessionId},
            provider_instance_id = ${actual.instanceId}, driver = ${expected.driver}, native_thread_id = ${actual.nativeThreadId ?? null},
            runtime_generation = ${actual.runtimeGeneration}, evidence_revision = ${revision}, observation_json = NULL, registered_at = ${now}
            WHERE thread_id = ${actual.threadId} AND evidence_revision = ${input.expectedEvidenceRevision}
              AND runtime_generation = ${(input.expectedRegisteredBinding ?? expected).runtimeGeneration} RETURNING thread_id`;
            if (rows.length !== 1) return rejectProviderBinding("evidence_revision_mismatch");
          }
          if (sourceBinding !== null && sourceIdentity !== null && previousSource.length === 0)
            yield* sql`INSERT INTO orchestration_v2_provider_continuation_sources
            (source_id, thread_id, provider_thread_id, provider_session_id, provider_instance_id, driver, native_thread_id, runtime_generation, continuation_key, registered_at)
            VALUES (${sourceId}, ${sourceBinding.threadId}, ${sourceBinding.providerThreadId}, ${sourceBinding.providerSessionId}, ${sourceBinding.instanceId},
              ${sourceBinding.driver}, ${sourceBinding.nativeThreadId}, ${sourceBinding.runtimeGeneration}, ${sourceIdentity.continuationKey}, ${now})`;
          return {
            committed: true as const,
            evidenceRevision: revision,
            storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
          };
        }),
      );
    });
    const readCurrentProviderRuntimeOwnerEffect = Effect.fnUntraced(function* (threadId: ThreadId) {
      const current = yield* readProviderRuntimeEvidenceEffect(threadId);
      if (current === null) return null;
      const binding = current.binding;
      const rows =
        yield* sql`SELECT provider.provider_thread_id FROM orchestration_v2_projection_threads thread
        JOIN orchestration_v2_projection_provider_threads provider ON provider.provider_thread_id = ${binding.providerThreadId}
        JOIN orchestration_v2_projection_provider_sessions session ON session.provider_session_id = provider.provider_session_id
        JOIN orchestration_v2_projection_provider_session_bindings attached
          ON attached.provider_session_id = session.provider_session_id AND attached.thread_id = thread.thread_id
        WHERE thread.thread_id = ${threadId} AND json_extract(thread.payload_json, '$.deletedAt') IS NULL
          AND json_extract(thread.payload_json, '$.activeProviderThreadId') = provider.provider_thread_id
          AND provider.thread_id = thread.thread_id AND provider.provider_session_id = ${binding.providerSessionId}
          AND provider.provider_instance_id = ${binding.instanceId} AND provider.driver = ${binding.driver}
          AND json_extract(provider.payload_json, '$.nativeThreadRef.nativeId') IS ${binding.nativeThreadId}
          AND session.provider_instance_id = ${binding.instanceId} AND session.driver = ${binding.driver}`;
      return rows.length === 1 ? current : null;
    });

    return {
      readCurrentProviderRuntimeOwner: (threadId: ThreadId) =>
        readCurrentProviderRuntimeOwnerEffect(threadId).pipe(
          Effect.mapError((cause) => new NativeProviderRuntimeEvidenceError({ cause })),
        ),
      readProviderRuntimeEvidence: (threadId: ThreadId) =>
        readProviderRuntimeEvidenceEffect(threadId).pipe(
          Effect.mapError((cause) => new NativeProviderRuntimeEvidenceError({ cause })),
        ),
      registerProviderRuntime: (
        input: Parameters<NativeProviderRuntimeEvidenceShape["registerProviderRuntime"]>[0],
      ) =>
        registerProviderRuntimeEffect(input).pipe(
          Effect.mapError((cause) => new NativeProviderRuntimeEvidenceError({ cause })),
        ),
    } satisfies NativeProviderRuntimeEvidenceShape;
  });
