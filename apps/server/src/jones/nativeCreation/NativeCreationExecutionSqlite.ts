import { OrchestrationV2Command, RunId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
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
export const makeNativeExecutionMethods = (sql: SqlClient.SqlClient, owner: Owner) => {
  const eligible = Effect.gen(function* () {
    const rows =
      yield* sql`SELECT migration_id FROM jones_sql_migrations WHERE migration_id=100 AND name='NativeCreationExecution'`;
    const foreign =
      yield* sql`SELECT migration_id FROM jones_sql_migrations WHERE migration_id BETWEEN 7 AND 99`;
    if (rows.length !== 1 || foreign.length !== 0)
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
    const reference = yield* Schema.decodeUnknownEffect(NativeCreationExecutionReferenceV2)(input);
    if (reference.stage !== "native_command")
      return yield* unresolved("Native execution requires the native command stage");
    const history = yield* requireHistory(reference.claimId);
    yield* assertIdentity(reference.claimId, reference.stageCommandId, history.intent.threadId);
    const reservation = yield* owner.getReservedCommand(reference.stageCommandId);
    if (Option.isNone(reservation) || reservation.value.claimId !== reference.claimId)
      return yield* unresolved("Native execution has no immutable command reservation");
    const command = yield* Schema.decodeUnknownEffect(
      Schema.fromJsonString(OrchestrationV2Command),
    )(reservation.value.canonicalCommand);
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
  > = (input) =>
    Effect.gen(function* () {
      yield* eligible;
      if (Option.isNone(yield* Effect.serviceOption(sql.transactionService)))
        return yield* unresolved("Native acceptance must share the event and receipt transaction");
      const history = yield* requireHistory(input.claimId);
      yield* assertIdentity(input.claimId, input.command.commandId, history.intent.threadId);
      const reservation = yield* owner.getReservedCommand(input.command.commandId);
      const command = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)(input.command, {
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
    }).pipe(Effect.mapError(mapFailure));
  const readExecutionReference: NonNullable<
    Repository.NativeCreationRepository["Service"]["readExecutionReference"]
  > = (reference) => sql.withTransaction(resolve(reference)).pipe(Effect.mapError(mapFailure));
  const startEffectV2: NonNullable<
    Repository.NativeCreationRepository["Service"]["startEffectV2"]
  > = (reference, timestamp, authorize) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
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
            yield* sql`SELECT effect_id FROM jones_native_creation_execution_starts WHERE effect_id=${reference.effectId}`;
          if (prior.length !== 0)
            return yield* unresolved(
              "Native execution already started; reconcile its original attempt",
            );
          const now = DateTime.formatIso(yield* DateTime.now);
          const rows = yield* sql<{
            lease_owner: string;
            attempt_count: number;
            lease_expires_at: string;
            payload_json: string;
          }>`SELECT lease_owner,attempt_count,lease_expires_at,payload_json FROM orchestration_v2_effect_outbox
      WHERE effect_id=${reference.effectId} AND command_id=${reference.stageCommandId} AND thread_id=${resolved.history.intent.threadId}
      AND effect_type='provider-turn.start' AND status='running' AND lease_owner IS NOT NULL AND attempt_count>0 AND lease_expires_at>${now}`;
          if (rows.length !== 1)
            return yield* unresolved("Native execution has no exact owned live outbox claim");
          const payload = yield* Schema.decodeUnknownEffect(
            Schema.fromJsonString(
              Schema.Struct({
                request: Schema.Struct({
                  type: Schema.Literal("provider-turn.start"),
                  runId: RunId,
                }),
                nativeCreationExecutionReference: NativeCreationExecutionReferenceV2,
              }),
            ),
            { onExcessProperty: "error" },
          )(rows[0]!.payload_json);
          if (
            payload.request.runId !== resolved.command.runId ||
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
          const fact = yield* Schema.decodeUnknownEffect(NativeCreationEffectV2)({
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
          yield* sql`INSERT INTO jones_native_creation_execution_starts(effect_id,claim_id,command_id,worker_id,expected_attempt,lease_expires_at,reference_json,fact_json)
      VALUES(${reference.effectId},${reference.claimId},${reference.stageCommandId},${rows[0]!.lease_owner},${rows[0]!.attempt_count},${rows[0]!.lease_expires_at},${nativeCreationCanonicalJson(reference)},${nativeCreationCanonicalJson(fact)})`;
          return { status: "started" as const, fact };
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          isRepositoryError(cause) || Schema.is(NativeCreationAuthorityError)(cause)
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
            yield* sql`SELECT effect_id FROM jones_native_creation_execution_holds WHERE effect_id=${reference.effectId}`;
          if (prior.length > 0) return;
          yield* sql`INSERT INTO jones_native_creation_execution_holds(effect_id,reason,held_at) VALUES(${reference.effectId},${reason},${now})`;
        }),
      )
      .pipe(Effect.mapError(mapFailure));
  const confirmExecution: NonNullable<
    Repository.NativeCreationRepository["Service"]["confirmExecution"]
  > = (input) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const resolved = yield* resolve(input.reference);
          const evidence = Object.freeze(
            yield* Schema.decodeUnknownEffect(
              Schema.fromJsonString(NativeCreationWholeOperationEvidence),
            )(nativeCreationCanonicalJson(input.evidence)),
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
          const owned =
            yield* sql`SELECT effect.effect_id FROM orchestration_v2_effect_outbox effect
      JOIN jones_native_creation_execution_starts start ON start.effect_id=effect.effect_id
      WHERE effect.effect_id=${input.reference.effectId} AND effect.status='running'
      AND effect.lease_owner=${input.workerId} AND effect.attempt_count=${input.expectedAttempt}
      AND effect.lease_expires_at=${input.leaseExpiresAt} AND effect.lease_expires_at>${now}
      AND start.worker_id=${input.workerId} AND start.expected_attempt=${input.expectedAttempt} AND start.lease_expires_at=${input.leaseExpiresAt}
      AND NOT EXISTS(SELECT 1 FROM jones_native_creation_execution_holds hold WHERE hold.effect_id=effect.effect_id)`;
          if (owned.length !== 1)
            return yield* unresolved(
              "Native confirmation lost its exact captured claim or has an unknown hold",
            );
          const runtime = yield* sql`SELECT run.run_id FROM orchestration_v2_projection_runs run
      JOIN orchestration_v2_projection_run_attempts attempt ON attempt.run_id=run.run_id
      JOIN orchestration_v2_projection_provider_threads provider ON provider.provider_thread_id=attempt.provider_thread_id
      JOIN orchestration_v2_projection_provider_session_bindings binding ON binding.thread_id=run.thread_id AND binding.provider_session_id=provider.provider_session_id
      JOIN orchestration_v2_projection_threads thread ON thread.thread_id=run.thread_id
      WHERE run.run_id=${evidence.runId} AND run.thread_id=${evidence.threadId} AND attempt.attempt_id=${evidence.attemptId}
      AND provider.provider_thread_id=${evidence.providerThreadId} AND binding.provider_session_id=${evidence.providerSessionId}
      AND thread.active_provider_thread_id=provider.provider_thread_id
      AND json_extract(provider.payload_json,'$.runtimeIdentity.runtimeGeneration')=${evidence.runtimeGeneration}`;
          if (runtime.length !== 1)
            return yield* unresolved(
              "Native confirmation lacks current run and provider correlation",
            );
          const prior = yield* sql<{
            evidence_json: string;
          }>`SELECT evidence_json FROM jones_native_creation_execution_confirmations WHERE effect_id=${input.reference.effectId}`;
          const canonical = nativeCreationCanonicalJson(evidence);
          if (prior.length !== 0) {
            if (prior.length !== 1 || prior[0]!.evidence_json !== canonical)
              return yield* unresolved("Native confirmation is immutable");
            return;
          }
          yield* sql`INSERT INTO jones_native_creation_execution_confirmations(effect_id,worker_id,expected_attempt,evidence_json,confirmed_at)
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
    holdExecution,
  };
};
