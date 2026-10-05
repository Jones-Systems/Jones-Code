import {
  CommandId,
  EventId,
  ThreadId,
  RunId,
  RunAttemptId,
  MessageId,
  NativeCreationEffect,
  NativeCreationHistoricalBinding,
  OrchestrationV2Command,
  OrchestrationV2RunJson,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { hasOwnJonesMigration } from "../persistence/JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "../persistence/Migrations.ts";
import {
  NativeCommandIdentityV2,
  NativeCreationEffectV2,
  NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS,
  NativeCreationExecutionReferenceV2,
  OrchestrationV2StartWithImportedHistoryCommand,
  NativeProviderRuntimeBindingV1 as ProviderRuntimeBinding,
  NativeCreationObservationV2,
  NativeCommandReceiptObservationV2Json,
} from "./NativeCreationExecutionTypes.ts";
import { NativeCreationAuthorityError } from "./NativeCreationAuthority.ts";
import {
  NativeCreationRepositoryError,
  NativeCreationStoredIntent,
} from "./NativeCreationRepository.ts";
import type { NativeCreationHistory as LegacyNativeCreationHistory } from "./NativeCreationRepository.ts";
import {
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  validateNativeCreationPreparation,
} from "./NativeCreationPreparation.ts";
import type { ValidatedNativeCreationPreparation } from "./NativeCreationPreparation.ts";
import { ProviderNativeEffectEvidence } from "../orchestration-v2/ProviderAdapter.ts";
import { decodeOrchestrationEffectPayloadV2 } from "../orchestration-v2/EffectOutbox.ts";
const nativeCreationV2CommandDigest = (command: OrchestrationV2Command) =>
  nativeCreationSha256(
    nativeCreationCanonicalJson(Schema.encodeSync(OrchestrationV2Command)(command)),
  );
declare const nativeEffectConfirmationProof: unique symbol;
export interface NativeEffectConfirmationV1 {
  readonly [nativeEffectConfirmationProof]: true;
  readonly version: 1;
  readonly effectId: string;
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly workerId: string;
  readonly expectedAttempt: number;
  readonly runId: RunId;
  readonly attemptId: RunAttemptId;
  readonly binding: ProviderRuntimeBinding;
  readonly evidenceRevision: number;
  readonly evidence: ProviderNativeEffectEvidence;
  readonly nativeExecutionReference: NativeCreationExecutionReferenceV2 | null;
  /** Correlates the accepted command; the complete native evidence proves the ACK. */
  readonly commandEvent: { readonly eventId: EventId; readonly sequence: number };
  readonly confirmedAt: string;
}

export type NativeCreationBoundedHistoryV2 = Omit<
  NativeCreationObservationV2,
  "version" | "schema" | "incarnation" | "outcome"
> & {
  readonly originalCommandId: CommandId;
  readonly threadId: ThreadId;
  readonly messageId: MessageId;
};
export interface NativeCreationHistory extends LegacyNativeCreationHistory {
  readonly effectsV2: ReadonlyArray<NativeCreationEffectV2>;
  readonly effectOverflow: boolean;
}
export interface NativeCreationReservedCommandIdentity {
  readonly claimId: string;
  readonly commandId: string;
  readonly threadId: string;
}
export interface NativeCreationReservedCommand extends NativeCreationReservedCommandIdentity {
  readonly commandType: string;
  readonly commandDigest: string;
  readonly canonicalCommand: string;
}
export interface NativeCreationResolvedExecutionV2 {
  readonly reference: NativeCreationExecutionReferenceV2;
  readonly history: NativeCreationHistory;
  readonly preparation: ValidatedNativeCreationPreparation;
  readonly command: OrchestrationV2Command;
  readonly nativeIdentity: NativeCommandIdentityV2;
}
export type NativeCreationStartedFactV2 = Extract<NativeCreationEffectV2, { phase: "started" }>;
export type NativeCreationCompletedFactV2 = Extract<NativeCreationEffectV2, { phase: "completed" }>;
export class NativeCreationExecutionRepository extends Context.Service<
  NativeCreationExecutionRepository,
  {
    readonly reserveCommand: (
      claimId: string,
      command: OrchestrationV2Command,
    ) => Effect.Effect<void, NativeCreationRepositoryError>;
    readonly recordNormalizedCommand: (
      claimId: string,
      command: OrchestrationV2Command,
    ) => Effect.Effect<void, NativeCreationRepositoryError>;
    readonly recordNativeEffectConfirmation: (input: {
      readonly effectId: string;
      readonly workerId: string;
      readonly expectedAttempt: number;
      readonly runId: RunId;
      readonly attemptId: RunAttemptId;
      readonly binding: ProviderRuntimeBinding;
      readonly expectedEvidenceRevision: number;
      readonly evidence: ProviderNativeEffectEvidence;
    }) => Effect.Effect<NativeEffectConfirmationV1, NativeCreationRepositoryError>;
    readonly readNativeEffectConfirmation: (
      effectId: string,
    ) => Effect.Effect<NativeEffectConfirmationV1 | null, NativeCreationRepositoryError>;
    readonly readBoundedHistoryByThread: (
      threadId: ThreadId,
    ) => Effect.Effect<NativeCreationBoundedHistoryV2 | null, NativeCreationRepositoryError>;
    readonly readHistoryByClaim: (
      claimId: string,
    ) => Effect.Effect<NativeCreationHistory, NativeCreationRepositoryError>;
    readonly readExecutionReference: (
      reference: NativeCreationExecutionReferenceV2,
    ) => Effect.Effect<NativeCreationResolvedExecutionV2, NativeCreationRepositoryError>;
    readonly validateCommandAcceptanceV2: (input: {
      readonly commandId: CommandId;
      readonly threadId: ThreadId;
      readonly commandType: "thread.create" | "message.dispatch";
      readonly commandDigest: string;
      readonly bindingDigest: string;
      readonly eventId: EventId;
      readonly sequence: number;
    }) => Effect.Effect<void, NativeCreationRepositoryError>;
    readonly startEffectV2: (
      reference: NativeCreationExecutionReferenceV2,
      timestamp: string,
      authorize: Effect.Effect<NativeCreationHistoricalBinding, NativeCreationAuthorityError>,
    ) => Effect.Effect<
      { readonly status: "started"; readonly fact: NativeCreationStartedFactV2 },
      NativeCreationRepositoryError | NativeCreationAuthorityError
    >;
    readonly completeEffectV2: (
      reference: NativeCreationExecutionReferenceV2,
      completion: {
        readonly timestamp: string;
        readonly eventId: EventId;
        readonly sequence: number;
      },
    ) => Effect.Effect<NativeCreationCompletedFactV2, NativeCreationRepositoryError>;
    readonly reserveCommandIdentities: (
      claimId: string,
      commandIds: ReadonlyArray<string>,
    ) => Effect.Effect<void, NativeCreationRepositoryError>;
    readonly getReservedCommand: (
      commandId: string,
    ) => Effect.Effect<Option.Option<NativeCreationReservedCommand>, NativeCreationRepositoryError>;
    readonly getReservedCommandIdentity: (
      commandId: string,
    ) => Effect.Effect<
      Option.Option<NativeCreationReservedCommandIdentity>,
      NativeCreationRepositoryError
    >;
  }
>()("t3/nativeCreation/NativeCreationExecutionRepository") {}
const fail = (message: string) => new NativeCreationRepositoryError({ code: "conflict", message });
const unresolved = (message: string) =>
  new NativeCreationRepositoryError({ code: "unresolved_claim", message });
const isRepositoryError = Schema.is(NativeCreationRepositoryError);
const mapIdentityError = (cause: unknown) =>
  isRepositoryError(cause)
    ? cause
    : unresolved("Native creation command identity is unavailable or malformed");
const commandIdentitySuffixes = [
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
] as const;
const isAuthorityError = Schema.is(NativeCreationAuthorityError);
const decodeIntentJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(NativeCreationStoredIntent),
);
const decodeStoredEffectJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Union([NativeCreationEffect, NativeCreationEffectV2])),
);
const decodeCommand = Schema.decodeUnknownEffect(OrchestrationV2Command);
const decodeCommandId = Schema.decodeUnknownEffect(CommandId);
const decodeCommandIdentity = Schema.decodeUnknownEffect(
  Schema.Struct({ claimId: Schema.NonEmptyString, commandId: CommandId, threadId: ThreadId }),
);
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const readByClaim = Effect.fnUntraced(function* (claimId: string) {
    const rows = yield* sql<{
      claim_id: string;
      command_id: string;
      thread_id: string;
      intent_json: string;
    }>`SELECT claim_id, command_id, thread_id, intent_json FROM native_creation_intents WHERE claim_id = ${claimId}`;
    if (rows.length !== 1)
      return yield* unresolved("Native creation claim is missing or inconsistent");
    const intent = yield* decodeIntentJson(rows[0]!.intent_json);
    if (
      intent.claimId !== claimId ||
      intent.claimId !== rows[0]!.claim_id ||
      intent.commandId !== rows[0]!.command_id ||
      intent.threadId !== rows[0]!.thread_id
    )
      return yield* unresolved("Stored creation claim identity disagrees");
    const normalized = yield* sql<{
      command_digest: string;
    }>`SELECT command_digest FROM native_creation_normalized_commands WHERE claim_id = ${claimId}`;
    if (
      normalized.length > 1 ||
      (normalized.length === 1 &&
        (typeof normalized[0]!.command_digest !== "string" ||
          !/^[a-f0-9]{64}$/.test(normalized[0]!.command_digest)))
    )
      return yield* unresolved("Stored normalized creation digest is malformed");
    const facts = yield* sql<{
      fact_json: string;
      effect_id: string;
      phase: string;
      ordinal: number;
    }>`SELECT fact_json, effect_id, phase, ordinal FROM native_creation_effect_facts WHERE claim_id = ${claimId} ORDER BY ordinal`;
    const allEffects = yield* Effect.forEach(facts, (row, index) =>
      Effect.gen(function* () {
        const fact = yield* decodeStoredEffectJson(row.fact_json);
        if (
          fact.effectId !== row.effect_id ||
          fact.phase !== row.phase ||
          fact.ordinal !== row.ordinal ||
          fact.ordinal !== index
        )
          return yield* unresolved("Stored creation fact attribution is inconsistent");
        return fact;
      }),
    );
    const effects = allEffects.filter((fact): fact is NativeCreationEffect => !("version" in fact));
    const effectsV2 = allEffects.filter(
      (fact): fact is NativeCreationEffectV2 => "version" in fact && fact.version === 2,
    );
    return {
      intent,
      normalizedCommandDigest: normalized[0]?.command_digest ?? null,
      effects,
      effectsV2,
      effectOverflow:
        effects.length > NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS ||
        effectsV2.length > NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS,
    } satisfies NativeCreationHistory;
  }, Effect.mapError(mapIdentityError));

  const getReserved = Effect.fnUntraced(function* (commandId: string) {
    const rows = yield* sql<{
      claim_id: string;
      command_id: string;
      thread_id: string;
      command_type: NativeCreationReservedCommand["commandType"];
      command_digest: string;
      canonical_command: string;
    }>`SELECT claim_id, command_id, thread_id, command_type, command_digest, canonical_command
       FROM native_creation_reserved_commands WHERE command_id = ${commandId}`;
    const row = rows[0];
    if (row === undefined) return Option.none<NativeCreationReservedCommand>();
    if (rows.length !== 1) return yield* unresolved("Reserved command body is inconsistent");
    const identity = yield* decodeCommandIdentity({
      claimId: row.claim_id,
      commandId: row.command_id,
      threadId: row.thread_id,
    });
    const historicalBody = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(
      row.canonical_command,
    );
    const command = yield* Schema.decodeUnknownEffect(
      Schema.Struct({
        commandId: CommandId,
        threadId: ThreadId,
        type: Schema.NonEmptyString,
      }),
    )(historicalBody);
    if (
      identity.commandId !== commandId ||
      command.commandId !== commandId ||
      !("threadId" in command) ||
      command.threadId !== identity.threadId ||
      command.type !== row.command_type ||
      nativeCreationSha256(row.canonical_command) !== row.command_digest ||
      nativeCreationCanonicalJson(historicalBody) !== row.canonical_command
    )
      return yield* unresolved("Reserved command body disagrees with its identity or digest");
    return Option.some({
      ...identity,
      commandType: row.command_type,
      commandDigest: row.command_digest,
      canonicalCommand: row.canonical_command,
    });
  }, Effect.mapError(mapIdentityError));

  const rejectReceipt = Effect.fnUntraced(function* (commandId: string) {
    const rows =
      yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id = ${commandId}`;
    if (rows.length !== 0)
      return yield* fail("Native creation command already has a native receipt");
  }, Effect.mapError(mapIdentityError));

  const reserve = Effect.fnUntraced(function* (claimId: string, input: OrchestrationV2Command) {
    const command = yield* decodeCommand(input, {
      onExcessProperty: "error",
    });
    const { intent } = yield* readByClaim(claimId);
    if (
      !("threadId" in command) ||
      command.threadId !== intent.threadId ||
      ![
        "thread.create",
        "thread.metadata.update",
        "message.dispatch",
        "prepared-run.release",
        "prepared-run.progress",
        "prepared-run.fail",
        "provider-session.detach",
        "thread.delete",
      ].includes(command.type)
    ) {
      return yield* fail("Creation command does not address the claimed thread");
    }
    const owners = yield* sql<{
      claim_id: string;
    }>`SELECT claim_id FROM native_creation_intents WHERE command_id = ${command.commandId}`;
    if (owners.some((row) => row.claim_id !== claimId))
      return yield* fail("Creation command is claimed by another intent");
    const identity = yield* getIdentity(command.commandId);
    if (
      Option.isNone(identity) ||
      identity.value.claimId !== claimId ||
      identity.value.threadId !== command.threadId
    )
      return yield* fail("Creation command body has no matching reserved identity");
    const canonicalCommand = nativeCreationCanonicalJson(command);
    const existing = yield* getReserved(command.commandId);
    if (Option.isSome(existing)) {
      if (
        existing.value.claimId !== claimId ||
        existing.value.canonicalCommand !== canonicalCommand
      )
        return yield* fail("Reserved creation command is immutable");
      return;
    }
    yield* rejectReceipt(command.commandId);
    yield* sql`INSERT INTO native_creation_reserved_commands (command_id, claim_id, thread_id, command_type, command_digest, canonical_command)
      VALUES (${command.commandId}, ${claimId}, ${command.threadId}, ${command.type}, ${nativeCreationV2CommandDigest(command)}, ${canonicalCommand})`;
  });

  const reserveCommand: NativeCreationExecutionRepository["Service"]["reserveCommand"] = (
    claimId,
    command,
  ) => sql.withTransaction(reserve(claimId, command)).pipe(Effect.mapError(mapIdentityError));
  const recordNormalizedCommand: NativeCreationExecutionRepository["Service"]["recordNormalizedCommand"] =
    (claimId, command) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const { intent } = yield* readByClaim(claimId);
            if (command.commandId !== intent.commandId || command.type !== "prepared-run.release")
              return yield* fail("Normalized command identity differs from intent");
            yield* reserve(claimId, command);
            const canonicalCommand = nativeCreationCanonicalJson(command);
            const existing = yield* sql<{
              canonical_command: string;
            }>`SELECT canonical_command FROM native_creation_normalized_commands WHERE claim_id = ${claimId}`;
            if (existing.length > 0) {
              if (existing[0]!.canonical_command !== canonicalCommand)
                return yield* fail("Normalized creation command is immutable");
              return;
            }
            yield* sql`INSERT INTO native_creation_normalized_commands (claim_id, command_digest, canonical_command)
      VALUES (${claimId}, ${nativeCreationV2CommandDigest(command)}, ${canonicalCommand})`;
          }),
        )
        .pipe(Effect.mapError(mapIdentityError));

  const resolveAcceptedStage = Effect.fnUntraced(function* (
    claimId: string,
    stageCommandId: CommandId,
  ) {
    const history = yield* readByClaim(claimId);
    const intent = history.intent;
    const claims =
      yield* sql`SELECT operation_id, preparation_id, canonical_preparation, message_id, project_cwd, branch, worktree_path
      FROM native_creation_intents WHERE claim_id = ${claimId}`;
    const claim = claims[0];
    if (
      claims.length !== 1 ||
      claim === undefined ||
      claim.operation_id !== intent.operationId ||
      claim.preparation_id !== intent.preparationId ||
      claim.canonical_preparation !== intent.canonicalPreparation ||
      claim.message_id !== intent.messageId ||
      claim.project_cwd !== intent.resources.projectCwd ||
      claim.branch !== intent.resources.branch ||
      claim.worktree_path !== intent.resources.worktreePath
    )
      return yield* unresolved("Execution claim columns disagree with their immutable intent");
    const preparation = yield* validateNativeCreationPreparation(
      new TextEncoder().encode(intent.canonicalPreparation),
    );
    if (
      preparation.preparationSha256 !== intent.preparationSha256 ||
      preparation.commandDigest !== intent.commandDigest ||
      preparation.bindingDigest !== intent.bindingDigest ||
      preparation.command.commandId !== intent.commandId ||
      preparation.command.threadId !== intent.threadId ||
      preparation.operationId !== intent.operationId ||
      preparation.preparationId !== intent.preparationId ||
      preparation.promptDigest !== intent.promptDigest ||
      preparation.command.message.messageId !== intent.messageId
    )
      return yield* unresolved("Execution preparation disagrees with its immutable claim");
    const reservation = yield* getReserved(stageCommandId);
    const reservedIdentity = yield* getIdentity(stageCommandId);
    if (
      Option.isNone(reservation) ||
      Option.isNone(reservedIdentity) ||
      reservation.value.claimId !== claimId ||
      reservedIdentity.value.claimId !== claimId ||
      reservation.value.threadId !== intent.threadId ||
      reservedIdentity.value.threadId !== intent.threadId
    )
      return yield* unresolved("Execution command is not reserved by this claim");
    const command = yield* Schema.decodeUnknownEffect(
      Schema.fromJsonString(OrchestrationV2Command),
    )(reservation.value.canonicalCommand, { onExcessProperty: "error" });
    const expectedType =
      stageCommandId === `${intent.commandId}:native:v2:create`
        ? "thread.create"
        : stageCommandId === `${intent.commandId}:native:v2:message`
          ? "message.dispatch"
          : stageCommandId === intent.commandId
            ? "prepared-run.release"
            : null;
    if (
      expectedType === null ||
      command.type !== expectedType ||
      !("threadId" in command) ||
      command.threadId !== intent.threadId ||
      command.commandId !== stageCommandId ||
      nativeCreationV2CommandDigest(command) !== reservation.value.commandDigest ||
      (command.type === "prepared-run.release" &&
        history.normalizedCommandDigest !== reservation.value.commandDigest)
    )
      return yield* unresolved("Execution command does not match its exact V2 stage");
    const rows =
      yield* sql`SELECT command_id AS "commandId", kind, version, command_type AS "commandType",
      aggregate_kind AS "aggregateKind", aggregate_id AS "aggregateId", normalized_command_digest AS "normalizedCommandDigest", binding_digest AS "bindingDigest"
      FROM orchestration_v2_native_command_identities WHERE command_id = ${stageCommandId}`;
    if (rows.length !== 1)
      return yield* unresolved("Execution command has no accepted native identity");
    const nativeIdentity = yield* Schema.decodeUnknownEffect(NativeCommandIdentityV2)(rows[0]);
    const receipts = yield* sql`SELECT command_id FROM orchestration_command_receipts
      WHERE command_id = ${stageCommandId} AND aggregate_kind = 'thread' AND aggregate_id = ${intent.threadId}
        AND command_type = ${command.type} AND status = 'accepted'`;
    if (
      receipts.length !== 1 ||
      nativeIdentity.kind !== "native_creation_stage" ||
      nativeIdentity.commandId !== command.commandId ||
      nativeIdentity.aggregateId !== intent.threadId ||
      nativeIdentity.commandType !== command.type ||
      nativeIdentity.normalizedCommandDigest !== reservation.value.commandDigest ||
      nativeIdentity.bindingDigest !== intent.bindingDigest
    )
      return yield* unresolved("Execution native acceptance disagrees with the immutable stage");
    return { history, preparation, command, nativeIdentity };
  }, Effect.mapError(mapIdentityError));

  const resolveExecutionReference = Effect.fnUntraced(function* (
    input: NativeCreationExecutionReferenceV2,
  ) {
    const reference = yield* Schema.decodeUnknownEffect(NativeCreationExecutionReferenceV2)(input);
    if (reference.stage !== "native_command")
      return yield* unresolved("Execution reference does not identify a native command stage");
    return {
      reference,
      ...(yield* resolveAcceptedStage(reference.claimId, reference.stageCommandId)),
    };
  }, Effect.mapError(mapIdentityError));

  const validateCommandAcceptanceV2: NativeCreationExecutionRepository["Service"]["validateCommandAcceptanceV2"] =
    (input) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const reservation = yield* getReserved(input.commandId);
            if (Option.isNone(reservation))
              return yield* unresolved("Native command acceptance has no immutable reservation");
            const resolved = yield* resolveAcceptedStage(
              reservation.value.claimId,
              input.commandId,
            );
            if (
              resolved.command.type !== input.commandType ||
              resolved.history.intent.threadId !== input.threadId ||
              resolved.nativeIdentity.normalizedCommandDigest !== input.commandDigest ||
              resolved.nativeIdentity.bindingDigest !== input.bindingDigest
            )
              return yield* unresolved(
                "Native command acceptance differs from its immutable stage",
              );
            const events = yield* sql<{
              readonly event_type: string;
            }>`SELECT event.event_type FROM orchestration_events event
        JOIN orchestration_command_receipts receipt ON receipt.command_id = event.command_id
        WHERE event.event_id = ${input.eventId} AND event.sequence = ${input.sequence}
          AND event.command_id = ${input.commandId} AND event.application_event_version = 2
          AND event.aggregate_kind = 'thread' AND event.stream_id = ${input.threadId}
          AND receipt.result_sequence = ${input.sequence} AND receipt.status = 'accepted'`;
            if (
              events.length !== 1 ||
              (input.commandType === "thread.create" && events[0]!.event_type !== "thread.created")
            )
              return yield* unresolved(
                "Native command acceptance has no exact committed stage event",
              );
          }),
        )
        .pipe(Effect.mapError(mapIdentityError));

  const getIdentity = Effect.fnUntraced(function* (commandId: string) {
    const id = yield* decodeCommandId(commandId);
    const rows = yield* sql<{ claimId: string; commandId: string; threadId: string }>`
      SELECT claim_id AS "claimId", command_id AS "commandId", thread_id AS "threadId"
      FROM native_creation_reserved_command_identities WHERE command_id = ${id}`;
    if (rows.length === 0) return Option.none<NativeCreationReservedCommandIdentity>();
    if (rows.length !== 1) return yield* unresolved("Reserved command identity is inconsistent");
    const identity = yield* decodeCommandIdentity(rows[0]);
    const { intent } = yield* readByClaim(identity.claimId);
    if (
      identity.commandId !== id ||
      identity.threadId !== intent.threadId ||
      ![
        intent.commandId,
        ...commandIdentitySuffixes.map((suffix) => `${intent.commandId}:${suffix}`),
      ].includes(id)
    )
      return yield* unresolved("Reserved command identity disagrees with its claim");
    return Option.some(identity);
  }, Effect.mapError(mapIdentityError));

  const reserveCommandIdentities: NativeCreationExecutionRepository["Service"]["reserveCommandIdentities"] =
    (claimId, commandIds) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const { intent } = yield* readByClaim(claimId);
            if (commandIds.length === 0 || new Set(commandIds).size !== commandIds.length)
              return yield* fail("Creation command identities must be nonempty and unique");
            const ids = yield* Effect.forEach(commandIds, (id) => decodeCommandId(id));
            if (
              new Set(ids).size !== ids.length ||
              ids.some((id, index) => id !== commandIds[index])
            )
              return yield* fail(
                "Creation command identities must retain their exact input values",
              );
            const allowed = new Set([
              intent.commandId,
              ...commandIdentitySuffixes.map((suffix) => `${intent.commandId}:${suffix}`),
            ]);
            if (!ids.includes(intent.commandId as CommandId) || ids.some((id) => !allowed.has(id)))
              return yield* fail(
                "Creation command identity does not belong to the claimed inventory",
              );
            const owned = yield* sql<{ commandId: string }>`SELECT command_id AS "commandId"
        FROM native_creation_reserved_command_identities WHERE claim_id = ${claimId}`;
            if (
              owned.length > 0 &&
              (owned.length !== ids.length ||
                owned.some((row) => !ids.includes(row.commandId as CommandId)))
            )
              return yield* unresolved(
                "Creation claim has an incomplete or different reserved identity inventory",
              );
            for (const id of ids) {
              if (
                (yield* sql`SELECT command_id FROM native_creation_thread_recovery_commands WHERE command_id = ${id}`)
                  .length !== 0
              )
                return yield* fail(
                  "Creation inventory cannot include a separately reserved recovery command",
                );
              const identity = yield* getIdentity(id);
              if (Option.isSome(identity)) {
                if (
                  identity.value.claimId !== claimId ||
                  identity.value.threadId !== intent.threadId
                )
                  return yield* fail("Creation command identity belongs to another claim");
              } else {
                const owners = yield* sql<{
                  claim_id: string;
                }>`SELECT claim_id FROM native_creation_intents WHERE command_id = ${id}`;
                if (owners.some((row) => row.claim_id !== claimId))
                  return yield* fail("Creation command identity is claimed by another intent");
                if (Option.isSome(yield* getReserved(id)))
                  return yield* fail("Creation command body predates its reserved identity");
                yield* rejectReceipt(id);
              }
            }
            for (const id of ids) {
              if (owned.length === 0)
                yield* sql`INSERT INTO native_creation_reserved_command_identities (command_id, claim_id, thread_id)
          VALUES (${id}, ${claimId}, ${intent.threadId})`;
            }
          }),
        )
        .pipe(Effect.mapError(mapIdentityError));

  const startEffectV2: NativeCreationExecutionRepository["Service"]["startEffectV2"] = (
    reference,
    timestamp,
    authorize,
  ) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const resolved = yield* resolveExecutionReference(reference);
          const { history, command } = resolved;
          if (
            history.effects.some((fact) => fact.effectId === reference.effectId) ||
            history.effectsV2.some(
              (fact) =>
                fact.effectId === reference.effectId ||
                (fact.phase === "started" && fact.commandId === reference.stageCommandId),
            )
          )
            return yield* unresolved(
              "Native V2 stage already has an execution start; reconcile its original reference",
            );
          if (
            command.type !== "prepared-run.release" ||
            reference.effectId !==
              `effect:${command.commandId}:provider-turn.start:${command.runId}`
          )
            return yield* unresolved(
              "Native execution has no supported final provider-start association",
            );
          const now = DateTime.formatIso(yield* DateTime.now);
          const outboxRows = yield* sql<{ readonly payload_json: string }>`
        SELECT payload_json FROM orchestration_v2_effect_outbox
        WHERE effect_id = ${reference.effectId} AND command_id = ${reference.stageCommandId}
          AND thread_id = ${history.intent.threadId} AND effect_type = 'provider-turn.start'
          AND status = 'running' AND lease_owner IS NOT NULL AND attempt_count > 0 AND lease_expires_at > ${now}
          AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold
            WHERE hold.effect_id = orchestration_v2_effect_outbox.effect_id)
      `;
          if (outboxRows.length !== 1)
            return yield* unresolved("Native execution has no current claimed outbox effect");
          const payload = yield* decodeOrchestrationEffectPayloadV2(outboxRows[0]!.payload_json);
          if (
            !("nativeCreationExecutionReference" in payload) ||
            nativeCreationCanonicalJson(payload.nativeCreationExecutionReference) !==
              nativeCreationCanonicalJson(reference) ||
            payload.request.type !== "provider-turn.start" ||
            payload.request.runId !== command.runId
          )
            return yield* unresolved(
              "Native execution reference differs from its persisted effect",
            );
          const binding = yield* authorize;
          if (
            nativeCreationCanonicalJson(binding) !==
            nativeCreationCanonicalJson(history.intent.binding)
          )
            return yield* fail(
              "Current authority binding differs from the immutable execution claim",
            );
          const fact = yield* Schema.decodeUnknownEffect(NativeCreationEffectV2)({
            version: 2,
            kind: "native_command",
            phase: "started",
            effectId: reference.effectId,
            ordinal: history.effects.length + history.effectsV2.length,
            timestamp,
            commandId: command.commandId,
            threadId: history.intent.threadId,
            commandType: command.type,
            commandDigest: resolved.nativeIdentity.normalizedCommandDigest,
          });
          if (fact.phase !== "started")
            return yield* unresolved("V2 execution start phase is inconsistent");
          yield* sql`INSERT INTO native_creation_effect_facts (claim_id, effect_id, phase, ordinal, fact_json)
        VALUES (${reference.claimId}, ${fact.effectId}, 'started', ${fact.ordinal}, ${nativeCreationCanonicalJson(fact)})`;
          return { status: "started" as const, fact };
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          isRepositoryError(cause) || isAuthorityError(cause)
            ? cause
            : unresolved(
                "Native V2 execution start is unavailable; reconcile its original reference",
              ),
        ),
      );

  const completeEffectV2: NativeCreationExecutionRepository["Service"]["completeEffectV2"] = (
    reference,
    completion,
  ) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const resolved = yield* resolveExecutionReference(reference);
          const started = resolved.history.effectsV2.find(
            (fact) => fact.phase === "started" && fact.effectId === reference.effectId,
          );
          if (
            started === undefined ||
            started.commandId !== reference.stageCommandId ||
            started.threadId !== resolved.history.intent.threadId ||
            started.commandDigest !== resolved.nativeIdentity.normalizedCommandDigest ||
            resolved.history.effectsV2.some(
              (fact) => fact.phase === "completed" && fact.effectId === reference.effectId,
            )
          )
            return yield* unresolved("Native V2 completion has no unique matching execution start");
          const events = yield* sql`SELECT event_id FROM orchestration_events
        WHERE event_id = ${completion.eventId} AND sequence = ${completion.sequence} AND command_id = ${reference.stageCommandId}
          AND application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${started.threadId}`;
          if (events.length !== 1)
            return yield* unresolved("Native V2 completion has no exact committed command event");
          const fact = yield* Schema.decodeUnknownEffect(NativeCreationEffectV2)({
            ...started,
            phase: "completed",
            ordinal: resolved.history.effects.length + resolved.history.effectsV2.length,
            timestamp: completion.timestamp,
            eventId: completion.eventId,
            sequence: completion.sequence,
          });
          if (fact.phase !== "completed")
            return yield* unresolved("V2 execution completion phase is inconsistent");
          yield* sql`INSERT INTO native_creation_effect_facts (claim_id, effect_id, phase, ordinal, fact_json)
        VALUES (${reference.claimId}, ${fact.effectId}, 'completed', ${fact.ordinal}, ${nativeCreationCanonicalJson(fact)})`;
          return fact;
        }),
      )
      .pipe(Effect.mapError(mapIdentityError));

  interface ConfirmationRow {
    readonly effect_id: string;
    readonly command_id: string;
    readonly thread_id: string;
    readonly worker_id: string;
    readonly operation_id: string;
    readonly run_id: string;
    readonly run_attempt_id: string;
    readonly expected_attempt: number;
    readonly binding_json: string;
    readonly evidence_json: string;
    readonly evidence_revision: number;
    readonly native_execution_reference_json: string | null;
    readonly command_event_id: string;
    readonly command_event_sequence: number;
    readonly confirmed_at: string;
  }
  const confirmationFromRow = Effect.fnUntraced(function* (row: ConfirmationRow) {
    const binding = yield* Schema.decodeUnknownEffect(
      Schema.fromJsonString(ProviderRuntimeBinding),
    )(row.binding_json);
    const evidence = yield* Schema.decodeUnknownEffect(
      Schema.fromJsonString(ProviderNativeEffectEvidence),
    )(row.evidence_json);
    const reference =
      row.native_execution_reference_json === null
        ? null
        : yield* Schema.decodeUnknownEffect(
            Schema.fromJsonString(NativeCreationExecutionReferenceV2),
          )(row.native_execution_reference_json);
    if (
      binding.nativeThreadId === undefined ||
      binding.threadId !== row.thread_id ||
      evidence.outcome !== "confirmed_success" ||
      evidence.operationId !== row.operation_id ||
      row.operation_id !== row.effect_id ||
      (evidence.operation !== "start_turn" && evidence.operation !== "compact_thread") ||
      evidence.threadId !== binding.threadId ||
      evidence.instanceId !== binding.instanceId ||
      evidence.providerThreadId !== binding.providerThreadId ||
      evidence.providerSessionId !== binding.providerSessionId ||
      evidence.runtimeGeneration !== binding.runtimeGeneration ||
      evidence.attemptId !== row.run_attempt_id ||
      row.expected_attempt < 1 ||
      row.evidence_revision < 1 ||
      (reference !== null &&
        (reference.effectId !== row.effect_id || reference.stageCommandId !== row.command_id))
    )
      return yield* unresolved(
        "Persisted native confirmation has mismatched complete-operation subjects",
      );
    return Object.freeze({
      version: 1,
      effectId: row.effect_id,
      commandId: CommandId.make(row.command_id),
      threadId: ThreadId.make(row.thread_id),
      workerId: row.worker_id,
      expectedAttempt: row.expected_attempt,
      runId: RunId.make(row.run_id),
      attemptId: RunAttemptId.make(row.run_attempt_id),
      binding: Object.freeze(binding),
      evidenceRevision: row.evidence_revision,
      evidence: Object.freeze(evidence),
      nativeExecutionReference: reference,
      commandEvent: Object.freeze({
        eventId: EventId.make(row.command_event_id),
        sequence: row.command_event_sequence,
      }),
      confirmedAt: row.confirmed_at,
    }) as NativeEffectConfirmationV1;
  });
  const eligibleNativeStorage = hasOwnJonesMigration(jonesMigrationEntries, [
    142,
    "V2NativeAcceptance",
  ]).pipe(
    Effect.provideService(SqlClient.SqlClient, sql),
    Effect.orElseSucceed(() => false),
  );
  const readNativeEffectConfirmation: NativeCreationExecutionRepository["Service"]["readNativeEffectConfirmation"] =
    (effectId) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            if (!(yield* eligibleNativeStorage)) return null;
            const rows =
              yield* sql<ConfirmationRow>`SELECT * FROM orchestration_v2_native_effect_confirmations WHERE effect_id = ${effectId}`;
            if (rows.length === 0) return null;
            const proof = yield* confirmationFromRow(rows[0]!);
            const effects = yield* sql<{
              readonly payload_json: string;
            }>`SELECT payload_json FROM orchestration_v2_effect_outbox
        WHERE effect_id = ${proof.effectId} AND command_id = ${proof.commandId} AND thread_id = ${proof.threadId}
          AND status = 'succeeded' AND attempt_count = ${proof.expectedAttempt} AND effect_type = 'provider-turn.start'`;
            const events =
              yield* sql`SELECT event_id FROM orchestration_events WHERE event_id = ${proof.commandEvent.eventId}
        AND sequence = ${proof.commandEvent.sequence} AND command_id = ${proof.commandId} AND stream_id = ${proof.threadId}
        AND application_event_version = 2 AND aggregate_kind = 'thread'`;
            if (effects.length !== 1 || events.length !== 1)
              return yield* unresolved(
                "Native confirmation has no matching terminal effect or command attribution",
              );
            const payload = yield* decodeOrchestrationEffectPayloadV2(effects[0]!.payload_json);
            if (
              payload.request.type !== "provider-turn.start" ||
              payload.request.runId !== proof.runId ||
              nativeCreationCanonicalJson(
                "nativeCreationExecutionReference" in payload
                  ? payload.nativeCreationExecutionReference
                  : null,
              ) !== nativeCreationCanonicalJson(proof.nativeExecutionReference)
            )
              return yield* unresolved(
                "Native confirmation differs from its durable effect/reference",
              );
            if (proof.nativeExecutionReference !== null) {
              const resolved = yield* resolveExecutionReference(proof.nativeExecutionReference);
              if (
                !resolved.history.effectsV2.some(
                  (fact) =>
                    fact.phase === "completed" &&
                    fact.effectId === proof.effectId &&
                    fact.eventId === proof.commandEvent.eventId &&
                    fact.sequence === proof.commandEvent.sequence,
                )
              )
                return yield* unresolved(
                  "Native confirmation has no matching completed external ledger fact",
                );
            } else {
              const choices = yield* sql<{
                readonly canonical_command_json: string;
                readonly command_digest: string;
                readonly reviewed_basis: string;
                readonly choice_basis_json: string;
                readonly reservation_basis_json: string;
              }>`
          SELECT choice.canonical_command_json, choice.command_digest, choice.reviewed_basis,
            choice.basis_json AS choice_basis_json, reservation.basis_json AS reservation_basis_json
          FROM orchestration_v2_imported_history_start_choices choice
          JOIN orchestration_v2_imported_history_start_outcomes outcome ON outcome.command_id = choice.command_id
          JOIN orchestration_v2_queued_start_reservations reservation ON reservation.command_id = choice.command_id
          WHERE choice.command_id = ${proof.commandId} AND choice.thread_id = ${proof.threadId}
            AND outcome.intent_status = 'accepted' AND outcome.run_id = ${proof.runId}
            AND (outcome.effect_id IS NULL OR outcome.effect_id = ${proof.effectId})
            AND reservation.effect_id = ${proof.effectId} AND reservation.thread_id = choice.thread_id
            AND reservation.run_id = outcome.run_id AND reservation.run_attempt_id = ${proof.attemptId}
            AND json_extract(reservation.execution_intent_json, '$.kind') = 'imported_history_choice'
            AND json_extract(reservation.execution_intent_json, '$.reviewedBasis') = choice.reviewed_basis
            AND json_extract(reservation.basis_json, '$.sourceMode') = 'new_context'`;
              if (choices.length !== 1)
                return yield* unresolved(
                  "Imported confirmation lost its accepted exact reserved intent",
                );
              const command = yield* Schema.decodeUnknownEffect(
                Schema.fromJsonString(OrchestrationV2StartWithImportedHistoryCommand),
              )(choices[0]!.canonical_command_json, { onExcessProperty: "error" });
              const canonical = yield* Schema.encodeEffect(
                OrchestrationV2StartWithImportedHistoryCommand,
              )(command);
              if (
                command.commandId !== proof.commandId ||
                command.threadId !== proof.threadId ||
                command.reviewedBasis !== choices[0]!.reviewed_basis ||
                nativeCreationSha256(nativeCreationCanonicalJson(canonical)) !==
                  choices[0]!.command_digest ||
                (command.delivery.type === "queued_run" && command.delivery.runId !== proof.runId)
              )
                return yield* unresolved(
                  "Imported confirmation differs from its canonical full command",
                );
              const reservationBasis = yield* Schema.decodeUnknownEffect(
                Schema.fromJsonString(
                  Schema.Struct({ queuedProviderThreadId: Schema.NullOr(Schema.String) }),
                ),
              )(choices[0]!.reservation_basis_json);
              let originalProviderId = proof.binding.providerThreadId;
              let originalRootNodeId: string | null = null;
              if (command.delivery.type === "queued_run") {
                const originalBasis = yield* Schema.decodeUnknownEffect(
                  Schema.fromJsonString(
                    Schema.Struct({
                      snapshot: Schema.Struct({
                        records: Schema.Struct({
                          runs: Schema.Array(
                            Schema.Struct({ run_id: Schema.String, payload_json: Schema.String }),
                          ),
                        }),
                      }),
                    }),
                  ),
                )(choices[0]!.choice_basis_json);
                const originalRows = originalBasis.snapshot.records.runs.filter(
                  (row) => row.run_id === proof.runId,
                );
                if (originalRows.length !== 1)
                  return yield* unresolved(
                    "Imported confirmation has no exact original queued run",
                  );
                const originalRun = yield* Schema.decodeUnknownEffect(
                  Schema.fromJsonString(OrchestrationV2RunJson),
                )(originalRows[0]!.payload_json);
                if (
                  originalRun.providerThreadId === null ||
                  originalRun.rootNodeId === null ||
                  originalRun.activeAttemptId !== proof.attemptId ||
                  originalRun.providerThreadId === proof.binding.providerThreadId
                )
                  return yield* unresolved(
                    "Imported confirmation has no exact original-to-fresh queued mapping",
                  );
                originalProviderId = originalRun.providerThreadId;
                originalRootNodeId = originalRun.rootNodeId;
              }
              if (reservationBasis.queuedProviderThreadId !== originalProviderId)
                return yield* unresolved(
                  "Imported confirmation lost its immutable original provider basis",
                );
              const prepared =
                yield* sql`SELECT DISTINCT scope_event.event_id, provider_event.event_id, run_event.event_id, attempt_event.event_id
          FROM orchestration_command_receipts receipt
          JOIN orchestration_events scope_event ON scope_event.command_id = receipt.command_id
            AND scope_event.application_event_version = 2 AND scope_event.aggregate_kind = 'thread' AND scope_event.stream_id = receipt.aggregate_id
            AND scope_event.event_type = 'checkpoint-scope.created' AND scope_event.sequence > receipt.result_sequence
            AND json_extract(scope_event.payload_json, '$.runId') = ${proof.runId}
            AND json_extract(scope_event.payload_json, '$.providerThreadId') = ${proof.binding.providerThreadId}
          JOIN orchestration_events provider_event ON provider_event.command_id = receipt.command_id
            AND provider_event.application_event_version = 2 AND provider_event.aggregate_kind = 'thread' AND provider_event.stream_id = receipt.aggregate_id
            AND provider_event.event_type = 'provider-thread.updated' AND provider_event.sequence > receipt.result_sequence
            AND json_extract(provider_event.payload_json, '$.id') = ${proof.binding.providerThreadId}
            AND json_extract(provider_event.payload_json, '$.providerSessionId') = ${proof.binding.providerSessionId}
            AND json_extract(provider_event.payload_json, '$.nativeThreadRef') IS NULL
            AND json_extract(provider_event.payload_json, '$.nativeConversationHeadRef') IS NULL
          JOIN orchestration_events run_event ON run_event.command_id = receipt.command_id
            AND run_event.application_event_version = 2 AND run_event.aggregate_kind = 'thread' AND run_event.stream_id = receipt.aggregate_id
            AND run_event.event_type = ${command.delivery.type === "queued_run" ? "run.updated" : "run.created"}
            AND json_extract(run_event.payload_json, '$.id') = ${proof.runId}
            AND json_extract(run_event.payload_json, '$.providerThreadId') = ${proof.binding.providerThreadId}
            AND json_extract(run_event.payload_json, '$.activeAttemptId') = ${proof.attemptId}
            AND json_extract(run_event.payload_json, '$.rootNodeId') = json_extract(scope_event.payload_json, '$.nodeId')
            AND json_extract(run_event.payload_json, '$.status') = 'starting'
          JOIN orchestration_events attempt_event ON attempt_event.command_id = receipt.command_id
            AND attempt_event.application_event_version = 2 AND attempt_event.aggregate_kind = 'thread' AND attempt_event.stream_id = receipt.aggregate_id
            AND attempt_event.event_type = ${command.delivery.type === "queued_run" ? "run-attempt.updated" : "run-attempt.created"}
            AND json_extract(attempt_event.payload_json, '$.id') = ${proof.attemptId}
            AND json_extract(attempt_event.payload_json, '$.runId') = ${proof.runId}
            AND json_extract(attempt_event.payload_json, '$.providerThreadId') = ${proof.binding.providerThreadId}
            AND json_extract(attempt_event.payload_json, '$.rootNodeId') = json_extract(scope_event.payload_json, '$.nodeId')
            AND json_extract(attempt_event.payload_json, '$.status') = 'pending'
          WHERE receipt.command_id = ${proof.commandId} AND receipt.aggregate_id = ${proof.threadId} AND receipt.status = 'accepted'
            AND (${originalRootNodeId} IS NULL OR json_extract(scope_event.payload_json, '$.nodeId') = ${originalRootNodeId})
            ${command.delivery.type === "queued_run" ? sql`AND run_event.sequence > receipt.result_sequence AND attempt_event.sequence > receipt.result_sequence` : sql``}`;
              if (prepared.length !== 1)
                return yield* unresolved(
                  "Imported confirmation lacks its unique committed fresh preparation mapping",
                );
            }
            return proof;
          }),
        )
        .pipe(Effect.mapError(mapIdentityError));

  const recordNativeEffectConfirmation: NativeCreationExecutionRepository["Service"]["recordNativeEffectConfirmation"] =
    (input) =>
      Effect.gen(function* () {
        if (!(yield* eligibleNativeStorage))
          return yield* unresolved(
            "Native confirmation storage lacks eligible own142 migration provenance",
          );
        if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
          return yield* unresolved(
            "Native confirmation owns its durable transaction; publish its proof after commit",
          );
        return yield* sql.withTransaction(
          Effect.gen(function* () {
            if (!(yield* eligibleNativeStorage))
              return yield* unresolved(
                "Native confirmation storage lost eligible own142 migration provenance",
              );
            const binding = yield* Schema.decodeUnknownEffect(ProviderRuntimeBinding)(
              input.binding,
              { onExcessProperty: "error" },
            );
            const evidence = yield* Schema.decodeUnknownEffect(ProviderNativeEffectEvidence)(
              input.evidence,
              { onExcessProperty: "error" },
            );
            if (
              binding.nativeThreadId === undefined ||
              evidence.outcome !== "confirmed_success" ||
              evidence.operationId !== input.effectId ||
              evidence.threadId !== binding.threadId ||
              evidence.instanceId !== binding.instanceId ||
              evidence.providerThreadId !== binding.providerThreadId ||
              evidence.providerSessionId !== binding.providerSessionId ||
              evidence.runtimeGeneration !== binding.runtimeGeneration ||
              evidence.attemptId !== input.attemptId ||
              !Number.isSafeInteger(input.expectedAttempt) ||
              input.expectedAttempt < 1 ||
              !Number.isSafeInteger(input.expectedEvidenceRevision) ||
              input.expectedEvidenceRevision < 1
            )
              return yield* unresolved(
                "Native confirmation lacks complete correlated operation/binding evidence",
              );
            const now = DateTime.formatIso(yield* DateTime.now);
            const effects = yield* sql<{
              readonly command_id: string;
              readonly payload_json: string;
            }>`
        SELECT command_id, payload_json FROM orchestration_v2_effect_outbox WHERE effect_id = ${input.effectId}
          AND thread_id = ${binding.threadId} AND effect_type = 'provider-turn.start' AND status = 'running'
          AND lease_owner = ${input.workerId} AND attempt_count = ${input.expectedAttempt} AND lease_expires_at > ${now}
          AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold WHERE hold.effect_id = ${input.effectId})`;
            if (effects.length !== 1)
              return yield* unresolved(
                "Native confirmation lost its exact owned unexpired effect claim",
              );
            const effect = effects[0]!;
            const payload = yield* decodeOrchestrationEffectPayloadV2(effect.payload_json);
            if (
              payload.request.type !== "provider-turn.start" ||
              payload.request.runId !== input.runId ||
              input.effectId !== `effect:${effect.command_id}:provider-turn.start:${input.runId}`
            )
              return yield* unresolved(
                "Native confirmation differs from the supported provider-start effect",
              );
            const reference =
              "nativeCreationExecutionReference" in payload
                ? payload.nativeCreationExecutionReference
                : null;
            if (
              reference !== null &&
              (reference.effectId !== input.effectId ||
                reference.stageCommandId !== effect.command_id)
            )
              return yield* unresolved("Native confirmation differs from its execution reference");
            if (reference === null) {
              const choices =
                yield* sql`SELECT choice.command_id FROM orchestration_v2_imported_history_start_choices choice
          JOIN orchestration_v2_imported_history_start_outcomes outcome ON outcome.command_id = choice.command_id
          WHERE choice.command_id = ${effect.command_id} AND choice.thread_id = ${binding.threadId}
            AND outcome.intent_status = 'accepted' AND outcome.run_id = ${input.runId}
            AND (outcome.effect_id = ${input.effectId} OR (outcome.effect_id IS NULL AND EXISTS (
              SELECT 1 FROM orchestration_v2_queued_start_reservations reservation WHERE reservation.effect_id = ${input.effectId}
                AND reservation.command_id = choice.command_id AND reservation.thread_id = choice.thread_id
                AND reservation.run_id = outcome.run_id AND reservation.run_attempt_id = ${input.attemptId}
                AND json_extract(reservation.execution_intent_json, '$.kind') = 'imported_history_choice')))`;
              if (choices.length !== 1)
                return yield* unresolved(
                  "Native confirmation has no accepted explicit imported-context intent",
                );
            }
            if (reference === null) {
              const reservations =
                yield* sql`SELECT effect_id FROM orchestration_v2_queued_start_reservations
          WHERE effect_id = ${input.effectId} AND command_id = ${effect.command_id} AND thread_id = ${binding.threadId}
            AND run_id = ${input.runId} AND run_attempt_id = ${input.attemptId}
            AND json_extract(execution_intent_json, '$.kind') = 'imported_history_choice'
            AND json_extract(basis_json, '$.sourceMode') = 'new_context'`;
              if (reservations.length !== 1)
                return yield* unresolved(
                  "Imported confirmation lacks its exact reserved fresh-context execution intent",
                );
            }
            const current = yield* sql<{ readonly message_payload: string }>`
        SELECT message.payload_json AS message_payload FROM orchestration_v2_projection_threads thread
        JOIN orchestration_v2_projection_provider_threads provider ON provider.provider_thread_id = ${binding.providerThreadId}
        JOIN orchestration_v2_projection_provider_sessions session ON session.provider_session_id = ${binding.providerSessionId}
        JOIN orchestration_v2_projection_provider_session_bindings attached ON attached.provider_session_id = session.provider_session_id
          AND attached.thread_id = thread.thread_id
        JOIN orchestration_v2_provider_runtime_evidence registered ON registered.thread_id = thread.thread_id
        JOIN orchestration_v2_projection_runs run ON run.thread_id = thread.thread_id AND run.run_id = ${input.runId}
        JOIN orchestration_v2_projection_run_attempts attempt ON attempt.thread_id = run.thread_id
          AND attempt.run_id = run.run_id AND attempt.attempt_id = ${input.attemptId}
        JOIN orchestration_v2_projection_messages message ON message.thread_id = run.thread_id
          AND message.message_id = json_extract(run.payload_json, '$.userMessageId')
        WHERE thread.thread_id = ${binding.threadId} AND json_extract(thread.payload_json, '$.deletedAt') IS NULL
          AND json_extract(thread.payload_json, '$.activeProviderThreadId') = ${binding.providerThreadId}
          AND json_extract(run.payload_json, '$.modelSelection.instanceId') = ${binding.instanceId}
          AND provider.thread_id = thread.thread_id AND provider.provider_instance_id = ${binding.instanceId}
          AND provider.provider_session_id = ${binding.providerSessionId} AND provider.driver = registered.driver
          AND json_extract(provider.payload_json, '$.nativeThreadRef.nativeId') = ${binding.nativeThreadId}
          AND session.provider_instance_id = ${binding.instanceId} AND session.driver = provider.driver
          AND session.status NOT IN ('stopped', 'error')
          AND registered.provider_thread_id = ${binding.providerThreadId} AND registered.provider_session_id = ${binding.providerSessionId}
          AND registered.provider_instance_id = ${binding.instanceId} AND registered.native_thread_id = ${binding.nativeThreadId}
          AND registered.runtime_generation = ${binding.runtimeGeneration} AND registered.evidence_revision = ${input.expectedEvidenceRevision}
          AND json_extract(run.payload_json, '$.activeAttemptId') = ${input.attemptId}
          AND json_extract(run.payload_json, '$.providerThreadId') = ${binding.providerThreadId}
          AND json_extract(attempt.payload_json, '$.providerThreadId') = ${binding.providerThreadId}
          AND run.status = 'running' AND attempt.status = 'running'`;
            if (current.length !== 1)
              return yield* unresolved(
                "Native confirmation lost its current run/attempt/registered binding revision",
              );
            const message = yield* Schema.decodeUnknownEffect(
              Schema.fromJsonString(
                Schema.Struct({ text: Schema.String, attachments: Schema.Array(Schema.Unknown) }),
              ),
            )(current[0]!.message_payload);
            const operation =
              message.attachments.length === 0 && message.text.trim().toLowerCase() === "/compact"
                ? "compact_thread"
                : "start_turn";
            if (evidence.operation !== operation)
              return yield* unresolved(
                "Native confirmation operation differs from the actual accepted run message",
              );
            const attributed = yield* sql<{ readonly event_id: string; readonly sequence: number }>`
        SELECT event.event_id, event.sequence FROM orchestration_command_receipts receipt JOIN orchestration_events event
          ON event.sequence = receipt.result_sequence AND event.command_id = receipt.command_id
        WHERE receipt.command_id = ${effect.command_id} AND receipt.status = 'accepted' AND receipt.aggregate_id = ${binding.threadId}
          AND event.application_event_version = 2 AND event.aggregate_kind = 'thread' AND event.stream_id = ${binding.threadId}`;
            if (attributed.length !== 1)
              return yield* unresolved(
                "Native confirmation lacks accepted exact command attribution",
              );
            const commandEvent = attributed[0]!;
            yield* sql`INSERT INTO orchestration_v2_native_effect_confirmations
        (effect_id, command_id, thread_id, worker_id, operation_id, run_id, run_attempt_id, expected_attempt,
          binding_json, evidence_json, evidence_revision, native_execution_reference_json, command_event_id, command_event_sequence, confirmed_at)
        VALUES (${input.effectId}, ${effect.command_id}, ${binding.threadId}, ${input.workerId}, ${evidence.operationId},
          ${input.runId}, ${input.attemptId}, ${input.expectedAttempt}, ${nativeCreationCanonicalJson(binding)},
          ${nativeCreationCanonicalJson(evidence)}, ${input.expectedEvidenceRevision},
          ${reference === null ? null : nativeCreationCanonicalJson(reference)}, ${commandEvent.event_id}, ${commandEvent.sequence}, ${now})`;
            if (reference !== null)
              yield* completeEffectV2(reference, {
                timestamp: now,
                eventId: EventId.make(commandEvent.event_id),
                sequence: commandEvent.sequence,
              });
            const terminal =
              yield* sql`UPDATE orchestration_v2_effect_outbox SET status = 'succeeded', lease_owner = NULL,
        lease_expires_at = NULL, completed_at = ${now}, updated_at = ${now}, last_error = NULL
        WHERE effect_id = ${input.effectId} AND status = 'running' AND lease_owner = ${input.workerId}
          AND attempt_count = ${input.expectedAttempt} AND lease_expires_at > ${now}
          AND NOT EXISTS (SELECT 1 FROM orchestration_v2_unknown_effect_holds hold WHERE hold.effect_id = ${input.effectId}) RETURNING effect_id`;
            if (terminal.length !== 1)
              return yield* unresolved(
                "Native confirmation could not atomically settle its owned effect",
              );
            const proof = yield* readNativeEffectConfirmation(input.effectId);
            if (proof === null) return yield* unresolved("Native confirmation readback is missing");
            return proof;
          }),
        );
      }).pipe(Effect.mapError(mapIdentityError));

  const readBoundedHistoryByThread: NativeCreationExecutionRepository["Service"]["readBoundedHistoryByThread"] =
    (threadId) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const claims = yield* sql<{
              readonly claim_id: string;
            }>`SELECT claim_id FROM native_creation_intents WHERE thread_id = ${threadId}`;
            if (claims.length === 0) return null;
            if (claims.length !== 1)
              return yield* unresolved("Native creation history has ambiguous thread ownership");
            const history = yield* readByClaim(claims[0]!.claim_id);
            const intent = history.intent;
            if (intent.threadId !== threadId)
              return yield* unresolved("Native creation history targets another thread");
            const readReceipt = Effect.fnUntraced(function* (id: CommandId) {
              const rows =
                yield* sql`SELECT command_id AS "commandId", aggregate_id AS "threadId", command_type AS "commandType",
          accepted_at AS "acceptedAt", result_sequence AS "resultSequence", status, error
          FROM orchestration_command_receipts WHERE command_id = ${id} AND aggregate_kind = 'thread'`;
              if (rows.length === 0) return null;
              if (rows.length !== 1)
                return yield* unresolved("Native creation receipt is ambiguous");
              const receipt = yield* Schema.decodeUnknownEffect(
                NativeCommandReceiptObservationV2Json,
              )(rows[0]);
              if (receipt.threadId !== threadId)
                return yield* unresolved("Native creation receipt targets another thread");
              return receipt;
            });
            let overflow = history.effectOverflow;
            const stageCommands: Array<NativeCreationBoundedHistoryV2["stageCommands"][number]> =
              [];
            let finalExecution: {
              readonly commandId: CommandId;
              readonly commandDigest: string;
              readonly effectId: string;
            } | null = null;
            for (const [id, type] of [
              [`${intent.commandId}:native:v2:create`, "thread.create"],
              [`${intent.commandId}:native:v2:message`, "message.dispatch"],
              [intent.commandId, "prepared-run.release"],
            ] as const) {
              const commandId = CommandId.make(id);
              const reserved = yield* getReserved(commandId);
              if (Option.isNone(reserved)) continue;
              if (reserved.value.claimId !== intent.claimId || reserved.value.threadId !== threadId)
                return yield* unresolved("Native creation stage is owned by another claim");
              if (reserved.value.commandType !== type) {
                if (
                  id === intent.commandId &&
                  !["thread.create", "message.dispatch", "prepared-run.release"].includes(
                    reserved.value.commandType,
                  )
                )
                  continue;
                return yield* unresolved("Native creation stage command type is inconsistent");
              }
              const command = yield* Schema.decodeUnknownEffect(
                Schema.fromJsonString(OrchestrationV2Command),
              )(reserved.value.canonicalCommand, { onExcessProperty: "error" });
              if (
                command.commandId !== commandId ||
                !("threadId" in command) ||
                command.threadId !== threadId ||
                command.type !== type ||
                nativeCreationV2CommandDigest(command) !== reserved.value.commandDigest
              )
                return yield* unresolved("Native creation stage body is inconsistent");
              const receipt = yield* readReceipt(commandId);
              const identities =
                yield* sql`SELECT command_id AS "commandId", kind, version, command_type AS "commandType",
          aggregate_kind AS "aggregateKind", aggregate_id AS "aggregateId", normalized_command_digest AS "normalizedCommandDigest", binding_digest AS "bindingDigest"
          FROM orchestration_v2_native_command_identities WHERE command_id = ${commandId}`;
              if (receipt !== null) {
                if (identities.length !== 1 || receipt.commandType !== type)
                  return yield* unresolved(
                    "Native creation stage receipt is not bound to one native identity",
                  );
                const identity = yield* Schema.decodeUnknownEffect(NativeCommandIdentityV2)(
                  identities[0],
                );
                if (
                  identity.kind !== "native_creation_stage" ||
                  identity.aggregateId !== threadId ||
                  identity.commandType !== type ||
                  identity.normalizedCommandDigest !== reserved.value.commandDigest ||
                  identity.bindingDigest !== intent.bindingDigest
                )
                  return yield* unresolved(
                    "Native creation stage identity disagrees with its immutable reservation",
                  );
              } else if (identities.length !== 0)
                return yield* unresolved("Native creation identity has no receipt");
              const events = yield* sql<{
                readonly eventId: string;
                readonly sequence: number;
                readonly aggregate_kind: string;
                readonly stream_id: string;
                readonly application_event_version: number;
              }>`
          SELECT event_id AS "eventId", sequence, aggregate_kind, stream_id, application_event_version
          FROM orchestration_events WHERE command_id = ${commandId} ORDER BY sequence LIMIT 257`;
              if (events.length > NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS) overflow = true;
              if (
                events.some(
                  (event, index) =>
                    event.aggregate_kind !== "thread" ||
                    event.stream_id !== threadId ||
                    event.application_event_version !== 2 ||
                    (index > 0 && event.sequence !== events[index - 1]!.sequence + 1),
                )
              )
                return yield* unresolved(
                  "Native creation command events have ambiguous attribution",
                );
              const event =
                receipt === null || receipt.status !== "accepted"
                  ? null
                  : (events.find((candidate) => candidate.sequence === receipt.resultSequence) ??
                    null);
              stageCommands.push({
                claimId: intent.claimId,
                commandId,
                threadId,
                commandType: type,
                commandDigest: reserved.value.commandDigest,
                receipt,
                event:
                  event === null
                    ? null
                    : { eventId: EventId.make(event.eventId), sequence: event.sequence },
              });
              if (command.type === "prepared-run.release" && receipt?.status === "accepted") {
                finalExecution = {
                  commandId,
                  commandDigest: reserved.value.commandDigest,
                  effectId: `effect:${commandId}:provider-turn.start:${command.runId}`,
                };
              }
            }
            for (const fact of history.effectsV2) {
              if (
                finalExecution === null ||
                fact.commandType !== "prepared-run.release" ||
                fact.commandId !== finalExecution.commandId ||
                fact.threadId !== threadId ||
                fact.commandDigest !== finalExecution.commandDigest ||
                fact.effectId !== finalExecution.effectId
              )
                return yield* unresolved(
                  "Native V2 execution fact has no supported final provider-start association",
                );
              if (fact.phase === "completed") {
                const started = history.effectsV2.find(
                  (candidate) =>
                    candidate.phase === "started" && candidate.effectId === fact.effectId,
                );
                if (
                  started === undefined ||
                  started.commandId !== fact.commandId ||
                  started.threadId !== fact.threadId ||
                  started.commandType !== fact.commandType ||
                  started.commandDigest !== fact.commandDigest ||
                  started.ordinal >= fact.ordinal
                )
                  return yield* unresolved(
                    "Native V2 completion has no preceding matching execution start",
                  );
                const events = yield* sql`SELECT event_id FROM orchestration_events
            WHERE event_id = ${fact.eventId} AND sequence = ${fact.sequence} AND command_id = ${fact.commandId}
              AND aggregate_kind = 'thread' AND stream_id = ${threadId} AND application_event_version = 2`;
                if (events.length !== 1)
                  return yield* unresolved(
                    "Native V2 execution completion has no exact committed command event",
                  );
                const confirmation = yield* readNativeEffectConfirmation(fact.effectId);
                if (
                  confirmation === null ||
                  confirmation.nativeExecutionReference === null ||
                  confirmation.nativeExecutionReference.claimId !== intent.claimId ||
                  confirmation.nativeExecutionReference.stage !== "native_command" ||
                  confirmation.commandId !== fact.commandId ||
                  confirmation.threadId !== fact.threadId ||
                  confirmation.commandEvent.eventId !== fact.eventId ||
                  confirmation.commandEvent.sequence !== fact.sequence
                )
                  return yield* unresolved(
                    "Native V2 completion lacks its immutable whole-operation ACK and terminal attempt proof",
                  );
              }
            }
            const effects = [...history.effects, ...history.effectsV2];
            const unresolvedEffects = effects
              .filter(
                (fact) =>
                  fact.phase === "started" &&
                  !effects.some(
                    (completion) =>
                      completion.effectId === fact.effectId &&
                      completion.phase === "completed" &&
                      (!("result" in completion) || completion.result !== "unknown"),
                  ),
              )
              .map((fact) => fact.effectId);
            if (unresolvedEffects.length > NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS)
              overflow = true;
            const observed = yield* Schema.decodeUnknownEffect(NativeCreationObservationV2)({
              version: 2,
              schema: "t3.native-creation-observation/v2",
              preparationId: intent.preparationId,
              operationId: intent.operationId,
              preparationSha256: intent.preparationSha256,
              bindingDigest: intent.bindingDigest,
              promptDigest: intent.promptDigest,
              commandDigest: intent.commandDigest,
              normalizedCommandDigest: history.normalizedCommandDigest,
              claimId: intent.claimId,
              claimedBootId: intent.claimedBootId,
              claimedAt: intent.claimedAt,
              actorSessionId: intent.actorSessionId,
              grantId: intent.grantId,
              grantRevision: intent.grantRevision,
              binding: intent.binding,
              incarnation: null,
              outcome: "unknown",
              overflow,
              stageCommands,
              finalReceipt: yield* readReceipt(CommandId.make(intent.commandId)),
              effectsV1: history.effects.slice(0, NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS),
              effectsV2: history.effectsV2.slice(0, NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS),
              unresolvedEffects: unresolvedEffects.slice(
                0,
                NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS,
              ),
            });
            const {
              version: _version,
              schema: _schema,
              incarnation: _incarnation,
              outcome: _outcome,
              ...safe
            } = observed;
            return {
              ...safe,
              originalCommandId: CommandId.make(intent.commandId),
              threadId,
              messageId: MessageId.make(intent.messageId),
            };
          }),
        )
        .pipe(Effect.mapError(mapIdentityError));

  return NativeCreationExecutionRepository.of({
    readBoundedHistoryByThread,
    reserveCommand,
    recordNormalizedCommand,
    recordNativeEffectConfirmation,
    readNativeEffectConfirmation,
    readHistoryByClaim: (claimId) => readByClaim(claimId).pipe(Effect.mapError(mapIdentityError)),
    readExecutionReference: resolveExecutionReference,
    validateCommandAcceptanceV2,
    startEffectV2,
    completeEffectV2,
    reserveCommandIdentities,
    getReservedCommand: getReserved,
    getReservedCommandIdentity: getIdentity,
  });
});
export const layer = Layer.effect(NativeCreationExecutionRepository, make);
