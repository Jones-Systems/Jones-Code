import { hasNativeProcessCustodySchema } from "./ProcessCustodySchema.ts";
import { OrchestrationV2Command, RunId, ProviderRuntimeBinding } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import { CurrentEffectClaim } from "../workspaceLifecycle/ActiveEffectClaim.ts";
import { NativeCreationAuthorityError } from "./NativeCreationAuthority.ts";
import * as Repository from "./NativeCreationRepository.ts";
import {
  NativeCreationExecutionReferenceV2,
  NativeCreationEffectV2,
  NativeCreationWholeOperationEvidence,
} from "./NativeCreationExecutionTypes.ts";
import {
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  validateNativeCreationPreparation,
} from "./NativeCreationPreparation.ts";

const decodeProviderStartEnvelope = Schema.decodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      request: Schema.Struct({ type: Schema.Literal("provider-turn.start"), runId: RunId }),
      nativeCreationExecutionReference: NativeCreationExecutionReferenceV2,
    }),
  ),
  { onExcessProperty: "error" },
);

const unresolved = (message: string) =>
  new Repository.NativeCreationRepositoryError({ code: "unresolved_claim", message });
const isRepositoryError = Schema.is(Repository.NativeCreationRepositoryError);
const mapFailure = (cause: unknown) =>
  isRepositoryError(cause)
    ? cause
    : unresolved("Native execution storage is unavailable or inconsistent");
type Owner = Pick<
  Repository.NativeCreationRepository["Service"],
  "readHistory" | "getReservedCommand"
>;
// These methods extend the existing claim owner; they create no independent claim or grant ledger.

const decodeEffectNativeCreationExecutionReferenceV2 = Schema.decodeEffect(
  NativeCreationExecutionReferenceV2,
);
const decodeEffectFromJsonStringOrchestrationV2Command = Schema.decodeEffect(
  Schema.fromJsonString(OrchestrationV2Command),
);
const decodeEffectOrchestrationV2Command = Schema.decodeEffect(OrchestrationV2Command);
const decodeEffectNativeCreationEffectV2 = Schema.decodeEffect(NativeCreationEffectV2);
const isNativeCreationAuthorityError = Schema.is(NativeCreationAuthorityError);
const decodeEffectFromJsonStringNativeCreationWholeOperationEvidence = Schema.decodeEffect(
  Schema.fromJsonString(NativeCreationWholeOperationEvidence),
);
const decodeProviderRuntimeBinding = Schema.decodeEffect(ProviderRuntimeBinding);
const decodeProviderRuntimeBindingJson = Schema.decodeEffect(
  Schema.fromJsonString(ProviderRuntimeBinding),
);

export const makeNativeExecutionMethods = (sql: SqlClient.SqlClient, owner: Owner) => {
  const checkCurrentClaim = Effect.gen(function* () {
    const claim = yield* Effect.serviceOption(CurrentEffectClaim);
    if (Option.isSome(claim)) yield* claim.value.check;
  });
  const eligible = Effect.gen(function* () {
    const rows =
      yield* sql`SELECT migration_id FROM jones_sql_migrations WHERE migration_id=100 AND name='NativeCreationExecution'`;
    const foreign =
      yield* sql`SELECT migration_id FROM jones_sql_migrations WHERE migration_id BETWEEN 7 AND 99`;
    const processCustody =
      yield* sql`SELECT migration_id FROM jones_sql_migrations WHERE migration_id > 100 AND name='NativeExecutionProcessCustody'`;
    if (
      rows.length !== 1 ||
      processCustody.length !== 1 ||
      foreign.length !== 0 ||
      !(yield* hasNativeProcessCustodySchema(sql))
    )
      return yield* unresolved("Native execution lacks receiving migration provenance");
  });
  const requireHistory = Effect.fnUntraced(function* (claimId: string) {
    const history = yield* owner.readHistory(claimId);
    if (Option.isNone(history)) return yield* unresolved("Native execution claim is missing");
    return history.value;
  });
  const assertIdentity = Effect.fnUntraced(function* (
    claimId: string,
    commandId: string,
    threadId: string,
  ) {
    const identities =
      yield* sql`SELECT command_id FROM native_creation_reserved_command_identities WHERE claim_id=${claimId} AND command_id=${commandId} AND thread_id=${threadId}`;
    if (identities.length !== 1)
      return yield* unresolved("Native execution lacks its permanently reserved command identity");
  });
  const reserveExecutionCommandIdentities: NonNullable<
    Repository.NativeCreationRepository["Service"]["reserveExecutionCommandIdentities"]
  > = (claimId, ids) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          yield* eligible;
          const { intent } = yield* requireHistory(claimId);
          const suffixes = [
            "native:v2:create",
            "native:v2:message",
            "bootstrap-thread-create",
            "bootstrap-thread-message",
            "bootstrap-thread-preparing",
            "bootstrap-thread-meta-update",
            "bootstrap-thread-preparing-failed",
            "bootstrap-thread-delete",
            "setup-script-requested",
            "setup-script-started",
            "setup-script-failed",
            "worktree-setup-running",
            "worktree-setup-done",
            "worktree-setup-failed",
            "worktree-setup-cancelled",
          ];
          const allowed = new Set([
            intent.commandId,
            ...suffixes.map((suffix) => `${intent.commandId}:${suffix}`),
          ]);
          if (
            ids.length === 0 ||
            ids.length > allowed.size ||
            new Set(ids).size !== ids.length ||
            !ids.includes(intent.commandId) ||
            ids.some((id) => !allowed.has(id))
          )
            return yield* unresolved(
              "Native command identity inventory differs from its immutable claim",
            );
          const prior = yield* sql<{
            command_id: string;
          }>`SELECT command_id FROM native_creation_reserved_command_identities WHERE claim_id=${claimId}`;
          if (prior.length > 0) {
            if (prior.length !== ids.length || prior.some((row) => !ids.includes(row.command_id)))
              return yield* unresolved("Native identity inventory is immutable");
            for (const id of ids) yield* assertIdentity(claimId, id, intent.threadId);
            return;
          }
          for (const id of ids) {
            const receipts =
              yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id=${id}`;
            const bodies =
              yield* sql`SELECT command_id FROM native_creation_reserved_commands WHERE command_id=${id}`;
            if (receipts.length > 0 || bodies.length > 0)
              return yield* unresolved(
                "Native command body or receipt predates its new identity inventory",
              );
          }
          for (const id of ids)
            yield* sql`INSERT INTO native_creation_reserved_command_identities(command_id,claim_id,thread_id) VALUES(${id},${claimId},${intent.threadId})`;
        }),
      )
      .pipe(Effect.mapError(mapFailure));
  const resolve = Effect.fnUntraced(function* (input: NativeCreationExecutionReferenceV2) {
    yield* eligible;
    const reference = yield* decodeEffectNativeCreationExecutionReferenceV2(input);
    if (reference.stage !== "native_command")
      return yield* unresolved("Native execution requires the native command stage");
    const history = yield* requireHistory(reference.claimId);
    yield* assertIdentity(reference.claimId, reference.stageCommandId, history.intent.threadId);
    const reservation = yield* owner.getReservedCommand(reference.stageCommandId);
    if (Option.isNone(reservation) || reservation.value.claimId !== reference.claimId)
      return yield* unresolved("Native execution has no immutable command reservation");
    const command = yield* decodeEffectFromJsonStringOrchestrationV2Command(
      reservation.value.canonicalCommand,
    );
    const digest = nativeCreationSha256(nativeCreationCanonicalJson(command));
    if (
      !("threadId" in command) ||
      command.threadId !== history.intent.threadId ||
      command.commandId !== reference.stageCommandId ||
      digest !== reservation.value.commandDigest
    )
      return yield* unresolved("Native execution reservation disagrees with its claim");
    const accepted = yield* sql<{
      command_digest: string;
      binding_digest: string;
      event_id: string;
      event_sequence: number;
    }>`
      SELECT command_digest,binding_digest,event_id,event_sequence FROM jones_native_creation_execution_acceptances
      WHERE command_id=${command.commandId} AND claim_id=${reference.claimId} AND thread_id=${history.intent.threadId}`;
    if (
      accepted.length !== 1 ||
      accepted[0]!.command_digest !== digest ||
      accepted[0]!.binding_digest !== history.intent.bindingDigest
    )
      return yield* unresolved("Native execution command lacks exact native acceptance");
    const receipts =
      yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id=${command.commandId} AND aggregate_kind='thread' AND aggregate_id=${history.intent.threadId} AND command_type=${command.type} AND status='accepted'`;
    if (receipts.length !== 1)
      return yield* unresolved("Native execution command has no accepted receipt");
    const preparation = yield* validateNativeCreationPreparation(
      new TextEncoder().encode(history.intent.canonicalPreparation),
    );
    return {
      reference,
      history,
      command,
      preparation,
      nativeIdentity: { normalizedCommandDigest: digest },
    };
  });
  const recordExecutionAcceptance: NonNullable<
    Repository.NativeCreationRepository["Service"]["recordExecutionAcceptance"]
  > = Effect.fn("NativeCreationExecutionSqlite.recordExecutionAcceptance")(function* (input) {
    yield* eligible;
    if (Option.isNone(yield* Effect.serviceOption(sql.transactionService)))
      return yield* unresolved("Native acceptance must share the event and receipt transaction");
    const history = yield* requireHistory(input.claimId);
    yield* assertIdentity(input.claimId, input.command.commandId, history.intent.threadId);
    const reservation = yield* owner.getReservedCommand(input.command.commandId);
    const command = yield* decodeEffectOrchestrationV2Command(input.command, {
      onExcessProperty: "error",
    });
    const canonical = nativeCreationCanonicalJson(command);
    if (
      !("threadId" in command) ||
      command.threadId !== history.intent.threadId ||
      !["thread.create", "message.dispatch", "prepared-run.release"].includes(command.type) ||
      Option.isNone(reservation) ||
      reservation.value.claimId !== input.claimId ||
      reservation.value.canonicalCommand !== canonical
    )
      return yield* unresolved("Native acceptance has no matching reserved command");
    const rows =
      yield* sql`SELECT event.event_id FROM orchestration_events event JOIN orchestration_command_receipts receipt ON receipt.command_id=event.command_id
      WHERE event.event_id=${input.eventId} AND event.sequence=${input.sequence} AND event.command_id=${command.commandId}
      AND event.aggregate_kind='thread' AND event.stream_id=${command.threadId} AND receipt.status='accepted'
      AND receipt.aggregate_id=${command.threadId} AND receipt.command_type=${command.type} AND receipt.result_sequence=${input.sequence}`;
    if (rows.length !== 1)
      return yield* unresolved("Native acceptance has no exact committed event and receipt");
    const prior = yield* sql<{
      claim_id: string;
      command_digest: string;
      binding_digest: string;
      event_id: string;
      event_sequence: number;
    }>`SELECT claim_id,command_digest,binding_digest,event_id,event_sequence FROM jones_native_creation_execution_acceptances WHERE command_id=${command.commandId}`;
    const digest = nativeCreationSha256(canonical);
    if (prior.length > 0) {
      const value = prior[0]!;
      if (
        prior.length !== 1 ||
        value.claim_id !== input.claimId ||
        value.command_digest !== digest ||
        value.binding_digest !== history.intent.bindingDigest ||
        value.event_id !== input.eventId ||
        value.event_sequence !== input.sequence
      )
        return yield* unresolved("Native acceptance is immutable");
      return;
    }
    yield* sql`INSERT INTO jones_native_creation_execution_acceptances(command_id,claim_id,thread_id,command_digest,binding_digest,event_id,event_sequence)
      VALUES(${command.commandId},${input.claimId},${command.threadId},${digest},${history.intent.bindingDigest},${input.eventId},${input.sequence})`;
  }, Effect.mapError(mapFailure));
  const readExecutionReference: NonNullable<
    Repository.NativeCreationRepository["Service"]["readExecutionReference"]
  > = (reference) => sql.withTransaction(resolve(reference)).pipe(Effect.mapError(mapFailure));
  const startEffectV2: NonNullable<
    Repository.NativeCreationRepository["Service"]["startEffectV2"]
  > = (reference, timestamp, authorize, expectedClaim) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          yield* checkCurrentClaim;
          const resolved = yield* resolve(reference);
          if (
            resolved.command.type !== "prepared-run.release" ||
            reference.effectId !==
              `effect:${resolved.command.commandId}:provider-turn.start:${resolved.command.runId}`
          )
            return yield* unresolved(
              "Native execution has no exact final provider-start association",
            );
          const prior =
            yield* sql`SELECT effect_id FROM jones_native_creation_execution_starts WHERE effect_id=${reference.effectId}
              UNION ALL SELECT effect_id FROM jones_native_creation_execution_process_claims WHERE effect_id=${reference.effectId}
              UNION ALL SELECT effect_id FROM jones_native_creation_execution_holds WHERE effect_id=${reference.effectId}
              UNION ALL SELECT effect_id FROM jones_native_creation_execution_process_holds WHERE effect_id=${reference.effectId}`;
          if (prior.length !== 0)
            return yield* unresolved(
              "Native execution already started; reconcile its original attempt",
            );
          if (
            expectedClaim.commandId !== resolved.command.commandId ||
            expectedClaim.threadId !== resolved.history.intent.threadId ||
            expectedClaim.runId !== resolved.command.runId ||
            !Number.isSafeInteger(expectedClaim.expectedAttempt) ||
            expectedClaim.expectedAttempt < 1
          )
            return yield* unresolved("Native start differs from the executor's captured claim");
          const rows = yield* sql<{
            lease_owner: string;
            attempt_count: number;
            updated_at: string;
            payload_json: string;
          }>`SELECT lease_owner,attempt_count,updated_at,payload_json FROM orchestration_v2_effect_outbox
      WHERE effect_id=${reference.effectId} AND command_id=${reference.stageCommandId} AND thread_id=${resolved.history.intent.threadId}
      AND effect_type='provider-turn.start' AND status='running' AND lease_owner=${expectedClaim.workerId}
      AND attempt_count=${expectedClaim.expectedAttempt} AND updated_at=${expectedClaim.claimedAt}`;
          if (rows.length !== 1)
            return yield* unresolved("Native execution has no exact owned live outbox claim");
          const payload = yield* decodeProviderStartEnvelope(rows[0]!.payload_json);
          if (
            payload.request.runId !== resolved.command.runId ||
            nativeCreationSha256(nativeCreationCanonicalJson(payload.request)) !==
              expectedClaim.requestDigest ||
            nativeCreationCanonicalJson(payload.nativeCreationExecutionReference) !==
              nativeCreationCanonicalJson(reference)
          )
            return yield* unresolved("Native execution differs from the persisted envelope");
          const binding = yield* authorize;
          if (
            nativeCreationCanonicalJson(binding) !==
            nativeCreationCanonicalJson(resolved.history.intent.binding)
          )
            return yield* unresolved("Current authority differs from immutable native claim");
          const fact = yield* decodeEffectNativeCreationEffectV2({
            version: 2,
            kind: "native_command",
            phase: "started",
            effectId: reference.effectId,
            ordinal: 0,
            timestamp,
            commandId: reference.stageCommandId,
            threadId: resolved.history.intent.threadId,
            commandType: resolved.command.type,
            commandDigest: resolved.nativeIdentity.normalizedCommandDigest,
          });
          if (fact.phase !== "started")
            return yield* unresolved("Native start has an inconsistent phase");
          yield* checkCurrentClaim;
          yield* sql`INSERT INTO jones_native_creation_execution_process_claims(effect_id,claim_id,command_id,thread_id,run_id,request_digest,worker_id,expected_attempt,claimed_at,reference_json,fact_json)
      VALUES(${reference.effectId},${reference.claimId},${reference.stageCommandId},${expectedClaim.threadId},${expectedClaim.runId},${expectedClaim.requestDigest},${rows[0]!.lease_owner},${rows[0]!.attempt_count},${rows[0]!.updated_at},${nativeCreationCanonicalJson(reference)},${nativeCreationCanonicalJson(fact)})`;
          return { status: "started" as const, fact };
        }),
      )
      .pipe(
        (start) =>
          Effect.gen(function* () {
            const claim = yield* Effect.serviceOption(CurrentEffectClaim);
            return yield* Option.isNone(claim)
              ? start
              : claim.value.use((current) =>
                  current.nativeCreationExecutionReference !== undefined &&
                  nativeCreationCanonicalJson(current.nativeCreationExecutionReference) ===
                    nativeCreationCanonicalJson(reference)
                    ? start
                    : Effect.fail(unresolved("Native issue differs from its claiming worker")),
                );
          }),
        Effect.mapError((cause) =>
          isRepositoryError(cause) || isNativeCreationAuthorityError(cause)
            ? cause
            : mapFailure(cause),
        ),
      );
  const holdExecution: NonNullable<
    Repository.NativeCreationRepository["Service"]["holdExecution"]
  > = (reference, reason) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          yield* resolve(reference);
          const now = DateTime.formatIso(yield* DateTime.now);
          const prior =
            yield* sql`SELECT effect_id FROM jones_native_creation_execution_holds WHERE effect_id=${reference.effectId}
            UNION ALL SELECT effect_id FROM jones_native_creation_execution_process_holds WHERE effect_id=${reference.effectId}`;
          if (prior.length > 0) return;
          yield* sql`INSERT INTO jones_native_creation_execution_process_holds(effect_id,reason,held_at) VALUES(${reference.effectId},${reason},${now})`;
        }),
      )
      .pipe(Effect.mapError(mapFailure));
  const recordExecutionBinding: NonNullable<
    Repository.NativeCreationRepository["Service"]["recordExecutionBinding"]
  > = (input) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const resolved = yield* resolve(input.reference);
          const binding = yield* decodeProviderRuntimeBinding(input.binding);
          const expected = input.expectedClaim;
          if (
            resolved.command.type !== "prepared-run.release" ||
            binding.threadId !== expected.threadId ||
            expected.threadId !== resolved.history.intent.threadId ||
            expected.runId !== resolved.command.runId ||
            expected.commandId !== resolved.command.commandId ||
            binding.providerInstanceId !==
              resolved.history.intent.binding.providerModelSelection.instanceId
          )
            return yield* unresolved(
              "Native physical binding differs from its immutable execution",
            );
          const owned = yield* sql<{
            readonly payload_json: string;
          }>`SELECT effect.payload_json FROM orchestration_v2_effect_outbox effect
        JOIN jones_native_creation_execution_process_claims start ON start.effect_id=effect.effect_id
        WHERE effect.effect_id=${input.reference.effectId} AND effect.status='running'
        AND effect.command_id=${expected.commandId} AND effect.thread_id=${expected.threadId}
        AND effect.lease_owner=${expected.workerId} AND effect.attempt_count=${expected.expectedAttempt} AND effect.updated_at=${expected.claimedAt}
        AND start.command_id=${expected.commandId} AND start.thread_id=${expected.threadId} AND start.run_id=${expected.runId}
        AND start.worker_id=${expected.workerId} AND start.expected_attempt=${expected.expectedAttempt} AND start.claimed_at=${expected.claimedAt}
        AND start.request_digest=${expected.requestDigest} AND start.reference_json=${nativeCreationCanonicalJson(input.reference)}
        AND NOT EXISTS(SELECT 1 FROM jones_native_creation_execution_holds h WHERE h.effect_id=effect.effect_id)
        AND NOT EXISTS(SELECT 1 FROM jones_native_creation_execution_process_holds h WHERE h.effect_id=effect.effect_id)`;
          if (owned.length !== 1)
            return yield* unresolved("Native binding lost its exact process claim");
          const payload = yield* decodeProviderStartEnvelope(owned[0]!.payload_json);
          if (
            payload.request.runId !== expected.runId ||
            nativeCreationSha256(nativeCreationCanonicalJson(payload.request)) !==
              expected.requestDigest ||
            nativeCreationCanonicalJson(payload.nativeCreationExecutionReference) !==
              nativeCreationCanonicalJson(input.reference)
          )
            return yield* unresolved("Native binding differs from its captured request envelope");
          const canonical = nativeCreationCanonicalJson(binding);
          const prior = yield* sql<{
            readonly binding_json: string;
          }>`SELECT binding_json FROM jones_native_creation_execution_process_bindings WHERE effect_id=${input.reference.effectId}`;
          if (prior.length !== 0) {
            if (prior.length !== 1 || prior[0]!.binding_json !== canonical)
              return yield* unresolved("Native physical generation is immutable");
            return;
          }
          yield* sql`INSERT INTO jones_native_creation_execution_process_bindings(effect_id,binding_json,bound_at)
        VALUES(${input.reference.effectId},${canonical},${DateTime.formatIso(yield* DateTime.now)})`;
        }),
      )
      .pipe(Effect.mapError(mapFailure));
  const confirmExecution: NonNullable<
    Repository.NativeCreationRepository["Service"]["confirmExecution"]
  > = (input) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          yield* checkCurrentClaim;
          const resolved = yield* resolve(input.reference);
          const evidence = Object.freeze(
            yield* decodeEffectFromJsonStringNativeCreationWholeOperationEvidence(
              nativeCreationCanonicalJson(input.evidence),
            ),
          );
          if (
            resolved.command.type !== "prepared-run.release" ||
            evidence.effectId !== input.reference.effectId ||
            evidence.commandId !== input.reference.stageCommandId ||
            evidence.threadId !== resolved.history.intent.threadId ||
            evidence.runId !== resolved.command.runId
          )
            return yield* unresolved(
              "Native confirmation lacks complete correlated whole-operation evidence",
            );
          const now = DateTime.formatIso(yield* DateTime.now);
          const owned = yield* sql<{
            payload_json: string;
            reference_json: string;
            request_digest: string;
            binding_json: string;
          }>`SELECT effect.payload_json, start.reference_json, start.request_digest, physical.binding_json FROM orchestration_v2_effect_outbox effect
      JOIN jones_native_creation_execution_process_claims start ON start.effect_id=effect.effect_id
      JOIN jones_native_creation_execution_process_bindings physical ON physical.effect_id=start.effect_id
      WHERE effect.effect_id=${input.reference.effectId} AND effect.status='running'
      AND effect.command_id=${input.reference.stageCommandId} AND effect.thread_id=${resolved.history.intent.threadId} AND effect.effect_type='provider-turn.start'
      AND effect.lease_owner=${input.workerId} AND effect.attempt_count=${input.expectedAttempt}
      AND effect.updated_at=${input.claimedAt}
      AND start.claim_id=${input.reference.claimId} AND start.command_id=${input.reference.stageCommandId}
      AND start.reference_json=${nativeCreationCanonicalJson(input.reference)}
      AND start.run_id=${evidence.runId} AND start.thread_id=${evidence.threadId}
      AND start.worker_id=${input.workerId} AND start.expected_attempt=${input.expectedAttempt} AND start.claimed_at=${input.claimedAt}
      AND NOT EXISTS(SELECT 1 FROM jones_native_creation_execution_holds hold WHERE hold.effect_id=effect.effect_id)
      AND NOT EXISTS(SELECT 1 FROM jones_native_creation_execution_process_holds hold WHERE hold.effect_id=effect.effect_id)`;
          if (owned.length !== 1)
            return yield* unresolved(
              "Native confirmation lost its exact captured claim or has an unknown hold",
            );
          const physical = yield* decodeProviderRuntimeBindingJson(owned[0]!.binding_json);
          if (
            physical.threadId !== evidence.threadId ||
            physical.providerSessionId !== evidence.providerSessionId ||
            physical.providerThreadId !== evidence.providerThreadId ||
            physical.runtimeGeneration !== evidence.runtimeGeneration
          )
            return yield* unresolved(
              "Native confirmation differs from its captured physical generation",
            );
          const payload = yield* decodeProviderStartEnvelope(owned[0]!.payload_json);
          const canonicalReference = nativeCreationCanonicalJson(input.reference);
          if (
            payload.request.runId !== evidence.runId ||
            nativeCreationSha256(nativeCreationCanonicalJson(payload.request)) !==
              owned[0]!.request_digest ||
            nativeCreationCanonicalJson(payload.nativeCreationExecutionReference) !==
              canonicalReference ||
            owned[0]!.reference_json !== canonicalReference
          )
            return yield* unresolved(
              "Native confirmation differs from the immutable captured envelope",
            );
          const runtime = yield* sql`SELECT run.run_id FROM orchestration_v2_projection_runs run
      JOIN orchestration_v2_projection_run_attempts attempt ON attempt.run_id=run.run_id
      JOIN orchestration_v2_projection_provider_threads provider ON provider.provider_thread_id=attempt.provider_thread_id
      JOIN orchestration_v2_projection_provider_session_bindings binding ON binding.thread_id=run.thread_id AND binding.provider_session_id=provider.provider_session_id
      JOIN orchestration_v2_projection_threads thread ON thread.thread_id=run.thread_id
      WHERE run.run_id=${evidence.runId} AND run.thread_id=${evidence.threadId} AND attempt.attempt_id=${evidence.attemptId}
      AND attempt.thread_id=run.thread_id AND run.provider_thread_id=provider.provider_thread_id
      AND provider.thread_id=run.thread_id AND provider.provider_thread_id=${evidence.providerThreadId} AND binding.provider_session_id=${evidence.providerSessionId}
      AND thread.active_provider_thread_id=provider.provider_thread_id
      AND json_extract(provider.payload_json,'$.runtimeIdentity.runtimeGeneration')=${evidence.runtimeGeneration}`;
          if (runtime.length !== 1)
            return yield* unresolved(
              "Native confirmation lacks current run and provider correlation",
            );
          const prior = yield* sql<{
            evidence_json: string;
          }>`SELECT evidence_json FROM jones_native_creation_execution_process_confirmations WHERE effect_id=${input.reference.effectId}`;
          const canonical = nativeCreationCanonicalJson(evidence);
          yield* checkCurrentClaim;
          if (prior.length !== 0) {
            if (prior.length !== 1 || prior[0]!.evidence_json !== canonical)
              return yield* unresolved("Native confirmation is immutable");
            return;
          }
          yield* sql`INSERT INTO jones_native_creation_execution_process_confirmations(effect_id,worker_id,expected_attempt,evidence_json,confirmed_at)
      VALUES(${input.reference.effectId},${input.workerId},${input.expectedAttempt},${canonical},${now})`;
        }),
      )
      .pipe(Effect.mapError(mapFailure));
  return {
    assertExecutionCapability: eligible.pipe(Effect.mapError(mapFailure)),
    reserveExecutionCommandIdentities,
    recordExecutionAcceptance,
    readExecutionReference,
    startEffectV2,
    confirmExecution,
    recordExecutionBinding,
    holdExecution,
  };
};
