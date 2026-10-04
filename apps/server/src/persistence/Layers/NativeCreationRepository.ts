import {
  AuthSessionId,
  CommandId,
  ThreadId,
  NativeCreationEffect,
  OrchestrationV2Command, NativeCreationEffectV2, NativeCommandIdentityV2,
  NativeCommandReceiptObservationV2Json, NativeThreadIncarnationV2,
  NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS, NATIVE_CREATION_OBSERVATION_V2_MAX_STAGES,
  EventId, RunId, RunAttemptId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { decodeOrchestrationEffectPayloadV2 } from "../../orchestration-v2/EffectOutbox.ts";
import { ProviderRuntimeBinding, ProviderNativeEffectEvidence } from "../../orchestration-v2/ProviderAdapter.ts";
import { NativeCreationAuthorityError, NativeCreationExecutionReferenceV2 } from "../../orchestration-v2/NativeCreationAuthority.ts";
import {
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  validateNativeCreationPreparation,
} from "../../orchestration-v2/NativeCreationPreparation.ts";
import {
  NativeCreationRepository,
  NativeCreationRepositoryError,
  NativeCreationStoredIntent, NativeCreationCommand, NativeCreationThreadRecoveryCommandV2,
  type NativeCreationBoundedHistoryV2, type NativeCreationResolvedExecutionV2,
  type NativeCreationStageCommandV2, type NativeEffectConfirmationV1,
  type NativeCreationHistory,
  type NativeCreationReservedCommand,
  type NativeCreationReservedCommandIdentity,
} from "../Services/NativeCreationRepository.ts";

const fail = (message: string) => new NativeCreationRepositoryError({ code: "conflict", message });
const unresolved = (message: string) =>
  new NativeCreationRepositoryError({ code: "unresolved_claim", message });
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
const isRepositoryError = Schema.is(NativeCreationRepositoryError);
const isAuthorityError = Schema.is(NativeCreationAuthorityError);
const mapError = (cause: unknown) =>
  isRepositoryError(cause) || isAuthorityError(cause)
    ? cause
    : fail("Native creation persistence rejected the operation");
const mapRepositoryError = (cause: unknown) =>
  isRepositoryError(cause) ? cause : fail("Native creation persistence rejected the operation");
const decodeIntentJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(NativeCreationStoredIntent),
);
const decodeEffectJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Union([NativeCreationEffect, NativeCreationEffectV2])));
const decodeIntent = Schema.decodeUnknownEffect(NativeCreationStoredIntent);
const decodeCommandId = Schema.decodeUnknownEffect(CommandId);
const decodeCommandIdentity = Schema.decodeUnknownEffect(
  Schema.Struct({ claimId: Schema.NonEmptyString, commandId: CommandId, threadId: ThreadId }),
);
const decodeCommand = Schema.decodeUnknownEffect(NativeCreationCommand);
const decodeEffect = Schema.decodeUnknownEffect(NativeCreationEffect);
const decodeEnrollmentSessionId = Schema.decodeUnknownEffect(AuthSessionId);
const decodeEnrollmentRow = Schema.decodeUnknownEffect(
  Schema.Struct({
    sessionId: AuthSessionId,
    enrolledAt: Schema.DateTimeUtcFromString,
  }),
);

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const hasAutomationEnrollment: NativeCreationRepository["Service"]["hasAutomationEnrollment"] =
    Effect.fn("NativeCreationRepository.hasAutomationEnrollment")(
      function* (actorSessionId: AuthSessionId) {
        const sessionId = yield* decodeEnrollmentSessionId(actorSessionId);
        const rows = yield* sql<{ sessionId: string; enrolledAt: string }>`
      SELECT session_id AS "sessionId", enrolled_at AS "enrolledAt"
      FROM native_creation_automation_enrollments WHERE session_id = ${sessionId}
    `;
        if (rows.length === 0) return false;
        if (rows.length !== 1)
          return yield* fail("Native automation enrollment membership is inconsistent");
        const row = yield* decodeEnrollmentRow(rows[0]);
        if (row.sessionId !== sessionId)
          return yield* fail("Native automation enrollment session disagrees");
        return true;
      },
      Effect.mapError(
        () =>
          new NativeCreationRepositoryError({
            code: "unresolved_claim",
            message: "Native automation enrollment membership is unavailable or malformed",
          }),
      ),
    );
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
      fact_json: string; effect_id: string; phase: string; ordinal: number;
    }>`SELECT fact_json, effect_id, phase, ordinal FROM native_creation_effect_facts WHERE claim_id = ${claimId}
      ORDER BY ordinal LIMIT ${NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS * 2 + 1}`;
    const decoded = yield* Effect.forEach(facts.slice(0, NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS * 2), (row) => Effect.gen(function* () {
      const fact = yield* decodeEffectJson(row.fact_json);
      if (fact.effectId !== row.effect_id || fact.phase !== row.phase || fact.ordinal !== row.ordinal)
        return yield* unresolved("Stored creation fact identity disagrees");
      return fact;
    }));
    const effects = decoded.filter((fact): fact is NativeCreationEffect => !("version" in fact));
    const effectsV2 = decoded.filter((fact): fact is NativeCreationEffectV2 => "version" in fact);
    return {
      intent,
      normalizedCommandDigest: normalized[0]?.command_digest ?? null,
      effects: effects.slice(0, NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS),
      effectsV2: effectsV2.slice(0, NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS),
      effectOverflow: facts.length > NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS * 2 || effects.length > NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS || effectsV2.length > NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS,
    } satisfies NativeCreationHistory;
  }, Effect.mapError(mapIdentityError));

  const claim: NativeCreationRepository["Service"]["claim"] = (input, authorize) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const preparation = yield* validateNativeCreationPreparation(
            new TextEncoder().encode(input.preparation.canonicalText),
          );
          const binding = yield* authorize;
          const expectedBinding = {
            backendInstance: preparation.binding.backend_instance,
            environmentId: preparation.binding.environment_id,
            projectId: preparation.binding.project_id,
            projectCwd: preparation.binding.project_cwd,
            accountRef: preparation.binding.account_ref,
            accountBindingId: binding.accountBindingId,
            accountBindingRevision: binding.accountBindingRevision,
            providerModelSelection: preparation.binding.provider_model_selection,
            runtimeMode: preparation.binding.runtime_mode,
            interactionMode: preparation.binding.interaction_mode,
            baseBranch: preparation.binding.base_branch,
            startFromOrigin: preparation.binding.start_from_origin,
            runSetupScript: preparation.binding.run_setup_script,
            requestedBranch: preparation.command.bootstrap.prepareWorktree.branch,
          };
          if (
            !input.claimId ||
            !input.claimedBootId ||
            !input.actorSessionId ||
            !input.grantId ||
            !Number.isSafeInteger(input.grantRevision) ||
            input.grantRevision < 1 ||
            !Number.isFinite(Date.parse(input.claimedAt)) ||
            input.resources.projectCwd !== preparation.binding.project_cwd ||
            input.resources.branch !== preparation.command.bootstrap.prepareWorktree.branch ||
            !input.resources.worktreePath.startsWith("/") ||
            preparation.preparationSha256 !== input.preparation.preparationSha256 ||
            nativeCreationCanonicalJson(binding) !== nativeCreationCanonicalJson(expectedBinding)
          ) {
            return yield* fail("Native creation claim input disagrees with immutable intent");
          }
          const existing = yield* sql<{
            claim_id: string;
          }>`SELECT claim_id FROM native_creation_intents WHERE operation_id = ${preparation.operationId}`;
          if (existing.length > 0) {
            const history = yield* readByClaim(existing[0]!.claim_id);
            if (
              history.intent.canonicalPreparation !== preparation.canonicalText ||
              history.intent.actorSessionId !== input.actorSessionId ||
              nativeCreationCanonicalJson(history.intent.resources) !==
                nativeCreationCanonicalJson(input.resources) ||
              nativeCreationCanonicalJson(history.intent.binding) !==
                nativeCreationCanonicalJson(binding)
            ) {
              return yield* fail(
                "Native creation operation already has a different immutable intent",
              );
            }
            return { status: "duplicate" as const, history };
          }
          const identity = yield* getIdentity(preparation.command.commandId);
          if (Option.isSome(identity))
            return yield* fail(
              "Native creation command identity is already reserved by another intent",
            );
          yield* rejectReceipt(preparation.command.commandId);
          const reservations =
            yield* sql`SELECT command_id FROM native_creation_reserved_commands WHERE command_id = ${preparation.command.commandId}`;
          if (reservations.length !== 0)
            return yield* fail("Native creation command is already reserved by another intent");
          const intent: NativeCreationStoredIntent = {
            claimId: input.claimId,
            claimedBootId: input.claimedBootId,
            claimedAt: input.claimedAt,
            actorSessionId: input.actorSessionId,
            grantId: input.grantId,
            grantRevision: input.grantRevision,
            preparationId: preparation.preparationId,
            operationId: preparation.operationId,
            preparationSha256: preparation.preparationSha256,
            bindingDigest: preparation.bindingDigest,
            promptDigest: preparation.promptDigest,
            commandDigest: preparation.commandDigest,
            commandId: preparation.command.commandId,
            threadId: preparation.command.threadId,
            messageId: preparation.command.message.messageId,
            canonicalPreparation: preparation.canonicalText,
            binding,
            resources: input.resources,
          };
          yield* decodeIntent(intent);
          yield* sql`INSERT INTO native_creation_intents
      (claim_id, operation_id, preparation_id, command_id, thread_id, message_id, project_cwd, branch, worktree_path, canonical_preparation, intent_json)
      VALUES (${intent.claimId}, ${intent.operationId}, ${intent.preparationId}, ${intent.commandId}, ${intent.threadId}, ${intent.messageId},
        ${intent.resources.projectCwd}, ${intent.resources.branch}, ${intent.resources.worktreePath}, ${intent.canonicalPreparation}, ${nativeCreationCanonicalJson(intent)})`;
          return {
            status: "claimed" as const,
            history: { intent, normalizedCommandDigest: null, effects: [], effectsV2: [], effectOverflow: false },
          };
        }),
      )
      .pipe(Effect.mapError(mapError));

  const readHistory: NativeCreationRepository["Service"]["readHistory"] = (commandId) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const rows = yield* sql<{
            claim_id: string;
          }>`SELECT claim_id FROM native_creation_intents WHERE command_id = ${commandId}`;
          if (rows.length === 0) return Option.none();
          return Option.some(yield* readByClaim(rows[0]!.claim_id));
        }),
      )
      .pipe(Effect.mapError(mapRepositoryError));

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
    const command = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)))(
      row.canonical_command,
    );
    if (
      identity.commandId !== commandId ||
      command.commandId !== commandId ||
      !("threadId" in command) ||
      command.threadId !== identity.threadId ||
      typeof command.type !== "string" || command.type.length === 0 || command.type !== row.command_type ||
      nativeCreationSha256(row.canonical_command) !== row.command_digest ||
      nativeCreationCanonicalJson(command) !== row.canonical_command
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

  const reserveCommandIdentities: NativeCreationRepository["Service"]["reserveCommandIdentities"] =
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

  const reserve = Effect.fnUntraced(function* (claimId: string, input: NativeCreationCommand) {
    const command = yield* decodeCommand(input, {
      onExcessProperty: "error",
    });
    const { intent } = yield* readByClaim(claimId);
    if (
      !("threadId" in command) ||
      command.threadId !== intent.threadId ||
      ![
        "thread.create",
        "thread.meta.update",
        "thread.message.user.append",
        "thread.session.set",
        "thread.activity.append",
        "thread.turn.start",
        "message.dispatch",
        "prepared-run.release",
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
    if ((command.type === "thread.create" && command.commandId !== `${intent.commandId}:native:v2:create`) ||
      (command.type === "message.dispatch" && command.commandId !== `${intent.commandId}:native:v2:message`) ||
      (command.type === "prepared-run.release" && command.commandId !== intent.commandId))
      return yield* fail("Native V2 stage identity differs from its claimed inventory");
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
      VALUES (${command.commandId}, ${claimId}, ${command.threadId}, ${command.type}, ${nativeCreationSha256(canonicalCommand)}, ${canonicalCommand})`;
  });

  const reserveCommand: NativeCreationRepository["Service"]["reserveCommand"] = (
    claimId,
    command,
  ) => sql.withTransaction(reserve(claimId, command)).pipe(Effect.mapError(mapRepositoryError));
  const recordNormalizedCommand: NativeCreationRepository["Service"]["recordNormalizedCommand"] = (
    claimId,
    command,
  ) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const { intent } = yield* readByClaim(claimId);
          if (command.commandId !== intent.commandId || !["thread.turn.start", "prepared-run.release"].includes(command.type))
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
      VALUES (${claimId}, ${nativeCreationSha256(canonicalCommand)}, ${canonicalCommand})`;
        }),
      )
      .pipe(Effect.mapError(mapRepositoryError));

  const append = Effect.fnUntraced(function* (
    claimId: string,
    input:
      | Parameters<NativeCreationRepository["Service"]["startEffect"]>[1]
      | Parameters<NativeCreationRepository["Service"]["completeEffect"]>[1],
  ) {
    const history = yield* readByClaim(claimId);
    const fact = yield* decodeEffect({
      ...input,
      ordinal: history.effects.length + history.effectsV2.length,
    });
    if (history.effectOverflow) return yield* unresolved("Creation fact inventory exceeds the bounded writer limit");
    const resources = history.intent.resources;
    if (
      ("threadId" in fact && fact.threadId !== history.intent.threadId) ||
      ("projectCwd" in fact && fact.projectCwd !== resources.projectCwd) ||
      ("worktreePath" in fact && fact.worktreePath !== resources.worktreePath) ||
      ("branch" in fact && fact.branch !== resources.branch)
    )
      return yield* fail("Creation effect addresses unclaimed resources");
    if (fact.kind === "native_command") {
      const identity = yield* getIdentity(fact.commandId);
      const reserved = yield* getReserved(fact.commandId);
      if (
        Option.isNone(identity) ||
        identity.value.claimId !== claimId ||
        identity.value.threadId !== fact.threadId ||
        Option.isNone(reserved) ||
        reserved.value.claimId !== claimId ||
        reserved.value.commandDigest !== fact.commandDigest ||
        reserved.value.threadId !== fact.threadId ||
        reserved.value.commandType !== fact.commandType
      )
        return yield* fail("Creation command effect is not reserved");
    }
    if (fact.kind === "cleanup") {
      const resource = fact.resource;
      if (
        (resource.kind === "thread" && resource.threadId !== history.intent.threadId) ||
        (resource.kind !== "thread" && resource.worktreePath !== resources.worktreePath) ||
        (resource.kind === "worktree" &&
          (resource.projectCwd !== resources.projectCwd || resource.branch !== resources.branch))
      )
        return yield* fail("Creation cleanup addresses unclaimed resources");
    }
    if (fact.phase === "completed") {
      const started = history.effects.find(
        (entry) => entry.effectId === fact.effectId && entry.phase === "started",
      );
      if (started === undefined || started.kind !== fact.kind)
        return yield* fail("Creation effect completion has no matching start");
      const ignored = new Set([
        "timestamp",
        "ordinal",
        "phase",
        "result",
        "eventId",
        "sequence",
        "exitCode",
        "terminalId",
        "ownership",
      ]);
      for (const [key, value] of Object.entries(started)) {
        if (
          !ignored.has(key) &&
          nativeCreationCanonicalJson(value) !== nativeCreationCanonicalJson(Reflect.get(fact, key))
        )
          return yield* fail("Creation effect completion disagrees with start");
      }
      if (
        started.kind === "setup" &&
        fact.kind === "setup" &&
        started.terminalId !== null &&
        started.terminalId !== fact.terminalId
      ) {
        return yield* fail("Setup completion differs from the known started terminal");
      }
    }
    yield* sql`INSERT INTO native_creation_effect_facts (claim_id, effect_id, phase, ordinal, fact_json)
      VALUES (${claimId}, ${fact.effectId}, ${fact.phase}, ${fact.ordinal}, ${nativeCreationCanonicalJson(fact)})`;
    return fact;
  });

  const startEffect: NativeCreationRepository["Service"]["startEffect"] = (
    claimId,
    fact,
    authorize,
  ) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const binding = yield* authorize;
          const { intent } = yield* readByClaim(claimId);
          if (nativeCreationCanonicalJson(binding) !== nativeCreationCanonicalJson(intent.binding))
            return yield* fail("Current authority binding differs from claimed binding");
          if (fact.phase !== "started")
            return yield* fail("Start requires a started creation effect");
          // The committed start authorizes the subsequent external action; SQL and external effects are not atomic.
          const persisted = yield* append(claimId, fact);
          if (persisted.phase !== "started")
            return yield* fail("Persisted fact phase differs from start");
          return persisted;
        }),
      )
      .pipe(Effect.mapError(mapError));
  const completeEffect: NativeCreationRepository["Service"]["completeEffect"] = (claimId, fact) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          if (fact.phase !== "completed")
            return yield* fail("Completion requires a completed creation effect");
          const persisted = yield* append(claimId, fact);
          if (persisted.phase !== "completed")
            return yield* fail("Persisted fact phase differs from completion");
          return persisted;
        }),
      )
      .pipe(Effect.mapError(mapRepositoryError));

  const readReceipt = Effect.fnUntraced(function* (commandId: string) {
    const rows = yield* sql<{ commandId: string; threadId: string; commandType: string; acceptedAt: string;
      resultSequence: number; status: string; error: string | null }>`SELECT command_id AS "commandId",
      aggregate_id AS "threadId", command_type AS "commandType", accepted_at AS "acceptedAt",
      result_sequence AS "resultSequence", status, error FROM orchestration_command_receipts
      WHERE command_id = ${commandId} AND aggregate_kind = 'thread'`;
    if (rows.length === 0) return null;
    if (rows.length !== 1) return yield* unresolved("Creation receipt is ambiguous");
    return yield* Schema.decodeUnknownEffect(NativeCommandReceiptObservationV2Json)(rows[0]);
  });
  const readNativeIdentity = Effect.fnUntraced(function* (commandId: string) {
    const rows = yield* sql`SELECT command_id AS "commandId", kind, version, command_type AS "commandType",
      aggregate_kind AS "aggregateKind", aggregate_id AS "aggregateId", normalized_command_digest AS "normalizedCommandDigest",
      binding_digest AS "bindingDigest" FROM orchestration_v2_native_command_identities WHERE command_id = ${commandId}`;
    if (rows.length !== 1) return yield* unresolved("Native V2 accepted identity is missing or ambiguous");
    return yield* Schema.decodeUnknownEffect(NativeCommandIdentityV2)(rows[0]);
  });
  const validateAcceptance = Effect.fnUntraced(function* (
    input: Parameters<NativeCreationRepository["Service"]["validateCommandAcceptanceV2"]>[0],
  ) {
    const reservation = yield* getReserved(input.commandId);
    const ownership = yield* getIdentity(input.commandId);
    if (Option.isNone(reservation) || Option.isNone(ownership) || reservation.value.claimId !== ownership.value.claimId ||
      reservation.value.threadId !== input.threadId || reservation.value.commandType !== input.commandType ||
      reservation.value.commandDigest !== input.commandDigest)
      return yield* unresolved("Native V2 acceptance has no matching immutable reservation");
    const history = yield* readByClaim(reservation.value.claimId);
    const identity = yield* readNativeIdentity(input.commandId);
    const receipt = yield* readReceipt(input.commandId);
    if (history.effectOverflow || history.intent.bindingDigest !== input.bindingDigest || identity.kind !== "native_creation_stage" ||
      identity.version !== 2 || identity.commandId !== input.commandId || identity.commandType !== input.commandType ||
      identity.aggregateKind !== "thread" || identity.aggregateId !== input.threadId ||
      identity.normalizedCommandDigest !== input.commandDigest || identity.bindingDigest !== input.bindingDigest ||
      receipt?.status !== "accepted" || receipt.threadId !== input.threadId || receipt.commandType !== input.commandType ||
      receipt.resultSequence !== input.sequence)
      return yield* unresolved("Native V2 acceptance attribution disagrees");
    const event = yield* sql`SELECT event_id FROM orchestration_events WHERE event_id = ${input.eventId}
      AND sequence = ${input.sequence} AND command_id = ${input.commandId} AND aggregate_kind = 'thread'
      AND stream_id = ${input.threadId} AND application_event_version = 2`;
    if (event.length !== 1) return yield* unresolved("Native V2 acceptance event is missing or mismatched");
  });
  const validateCommandAcceptanceV2: NativeCreationRepository["Service"]["validateCommandAcceptanceV2"] = (input) =>
    sql.withTransaction(validateAcceptance(input)).pipe(Effect.mapError(mapIdentityError));

  const readBoundedHistoryByThread: NativeCreationRepository["Service"]["readBoundedHistoryByThread"] = (threadId) =>
    sql.withTransaction(Effect.gen(function* () {
      const claims = yield* sql<{ claim_id: string }>`SELECT claim_id FROM native_creation_intents WHERE thread_id = ${threadId} LIMIT 2`;
      if (claims.length === 0) return null;
      if (claims.length !== 1) return yield* unresolved("Native creation thread has ambiguous claims");
      const history = yield* readByClaim(claims[0]!.claim_id);
      const { intent } = history;
      const rows = yield* sql<{ command_id: string }>`SELECT command_id FROM native_creation_reserved_commands
        WHERE claim_id = ${intent.claimId} AND command_type IN ('thread.create', 'message.dispatch', 'prepared-run.release')
        ORDER BY command_id LIMIT ${NATIVE_CREATION_OBSERVATION_V2_MAX_STAGES + 1}`;
      const stageCommands = yield* Effect.forEach(rows.slice(0, NATIVE_CREATION_OBSERVATION_V2_MAX_STAGES), (row) => Effect.gen(function* () {
        const reserved = Option.getOrThrow(yield* getReserved(row.command_id));
        if (!["thread.create", "message.dispatch", "prepared-run.release"].includes(reserved.commandType))
          return yield* unresolved("Unknown native V2 stage type");
        const receipt = yield* readReceipt(row.command_id);
        const events = receipt?.status !== "accepted" ? [] : yield* sql<{ eventId: string; sequence: number }>`
          SELECT event_id AS "eventId", sequence FROM orchestration_events WHERE command_id = ${row.command_id}
          AND sequence = ${receipt.resultSequence} AND stream_id = ${threadId} AND aggregate_kind = 'thread' AND application_event_version = 2`;
        return { claimId: intent.claimId, commandId: CommandId.make(row.command_id), threadId,
          commandType: reserved.commandType as NativeCreationStageCommandV2["type"], commandDigest: reserved.commandDigest,
          receipt, event: events.length === 1 ? yield* Schema.decodeUnknownEffect(NativeThreadIncarnationV2)(events[0]) : null };
      }));
      const all = [...history.effects, ...history.effectsV2];
      const unresolvedEffects = all.filter((fact) => fact.phase === "started" && !all.some((end) =>
        end.effectId === fact.effectId && end.phase === "completed" && end.kind === fact.kind &&
        (!("result" in end) || end.result !== "unknown"))).map((fact) => fact.effectId);
      return { preparationId: intent.preparationId, operationId: intent.operationId, preparationSha256: intent.preparationSha256,
        bindingDigest: intent.bindingDigest, promptDigest: intent.promptDigest, commandDigest: intent.commandDigest,
        normalizedCommandDigest: history.normalizedCommandDigest, claimId: intent.claimId, claimedBootId: intent.claimedBootId,
        claimedAt: intent.claimedAt, actorSessionId: intent.actorSessionId, grantId: intent.grantId, grantRevision: intent.grantRevision,
        binding: intent.binding, originalCommandId: CommandId.make(intent.commandId), threadId, messageId: intent.messageId,
        effectsV1: history.effects, effectsV2: history.effectsV2, stageCommands,
        finalReceipt: stageCommands.find((stage) => stage.commandType === "prepared-run.release")?.receipt ?? null,
        unresolvedEffects: unresolvedEffects.slice(0, NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS),
        overflow: history.effectOverflow || rows.length > NATIVE_CREATION_OBSERVATION_V2_MAX_STAGES || unresolvedEffects.length > NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS,
      } satisfies NativeCreationBoundedHistoryV2;
    })).pipe(Effect.mapError(mapIdentityError));

  const readExecution = Effect.fnUntraced(function* (input: NativeCreationExecutionReferenceV2) {
    const reference = yield* Schema.decodeUnknownEffect(NativeCreationExecutionReferenceV2)(input, { onExcessProperty: "error" });
    const history = yield* readByClaim(reference.claimId);
    const reserved = yield* getReserved(reference.stageCommandId);
    if (reference.stage !== "native_command" || history.effectOverflow || Option.isNone(reserved) ||
      reserved.value.claimId !== reference.claimId || reserved.value.threadId !== history.intent.threadId)
      return yield* unresolved("Native execution reference has no exact reservation");
    const command = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2Command))(reserved.value.canonicalCommand);
    if (command.type !== "thread.create" && command.type !== "message.dispatch" && command.type !== "prepared-run.release")
      return yield* unresolved("Native execution reference is not a V2 creation stage");
    const outbox = yield* sql<{ command_id: string; thread_id: string; effect_type: string; payload_json: string }>`
      SELECT command_id, thread_id, effect_type, payload_json FROM orchestration_v2_effect_outbox WHERE effect_id = ${reference.effectId}`;
    if (outbox.length !== 1 || outbox[0]!.command_id !== reference.stageCommandId || outbox[0]!.thread_id !== command.threadId)
      return yield* unresolved("Native execution reference has no immutable outbox association");
    const payload = yield* decodeOrchestrationEffectPayloadV2(outbox[0]!.payload_json);
    if (!("nativeCreationExecutionReference" in payload) ||
      nativeCreationCanonicalJson(payload.nativeCreationExecutionReference) !== nativeCreationCanonicalJson(reference) ||
      payload.request.type !== outbox[0]!.effect_type || command.type !== "prepared-run.release" ||
      payload.request.type !== "provider-turn.start" || payload.request.runId !== command.runId ||
      reference.effectId !== `effect:${command.commandId}:provider-turn.start:${command.runId}`)
      return yield* unresolved("Native execution reference differs from its reserved release effect");
    const nativeIdentity = yield* readNativeIdentity(reference.stageCommandId);
    const receipt = yield* readReceipt(reference.stageCommandId);
    if (receipt?.status !== "accepted") return yield* unresolved("Native execution has no accepted command receipt");
    const events = yield* sql<{ event_id: string }>`SELECT event_id FROM orchestration_events WHERE sequence = ${receipt.resultSequence}`;
    if (events.length !== 1) return yield* unresolved("Native execution receipt event is missing");
    yield* validateAcceptance({ commandId: command.commandId, threadId: command.threadId, commandType: command.type,
      commandDigest: reserved.value.commandDigest, bindingDigest: history.intent.bindingDigest,
      eventId: EventId.make(events[0]!.event_id), sequence: receipt.resultSequence });
    const preparation = yield* validateNativeCreationPreparation(new TextEncoder().encode(history.intent.canonicalPreparation));
    if (preparation.preparationSha256 !== history.intent.preparationSha256 || preparation.bindingDigest !== history.intent.bindingDigest ||
      preparation.command.commandId !== history.intent.commandId || preparation.command.threadId !== history.intent.threadId)
      return yield* unresolved("Native execution preparation disagrees with its claim");
    return { reference, history, command, preparation, nativeIdentity } satisfies NativeCreationResolvedExecutionV2;
  });
  const readExecutionReference: NativeCreationRepository["Service"]["readExecutionReference"] = (reference) =>
    sql.withTransaction(readExecution(reference)).pipe(Effect.mapError(mapIdentityError));
  const startEffectV2: NativeCreationRepository["Service"]["startEffectV2"] = (reference, timestamp, authorize) =>
    sql.withTransaction(Effect.gen(function* () {
      const binding = yield* authorize;
      const resolved = yield* readExecution(reference);
      if (nativeCreationCanonicalJson(binding) !== nativeCreationCanonicalJson(resolved.history.intent.binding))
        return yield* fail("Current authority differs from the native execution claim");
      const facts = [...resolved.history.effects, ...resolved.history.effectsV2];
      if (facts.some((fact) => fact.effectId === reference.effectId ||
        (fact.kind === "native_command" && fact.commandId === reference.stageCommandId)))
        return yield* unresolved("Native execution already has an effect start or unknown prior effect");
      const fact = yield* Schema.decodeUnknownEffect(NativeCreationEffectV2)({ version: 2, kind: "native_command", phase: "started",
        effectId: reference.effectId, timestamp, ordinal: facts.length, commandId: reference.stageCommandId,
        threadId: resolved.command.threadId, commandType: resolved.command.type,
        commandDigest: resolved.nativeIdentity.normalizedCommandDigest });
      if (fact.phase !== "started") return yield* fail("Native V2 start phase disagrees");
      yield* sql`INSERT INTO native_creation_effect_facts (claim_id, effect_id, phase, ordinal, fact_json)
        VALUES (${reference.claimId}, ${fact.effectId}, ${fact.phase}, ${fact.ordinal}, ${nativeCreationCanonicalJson(fact)})`;
      return { status: "started" as const, fact };
    })).pipe(Effect.mapError(mapError));
  const completeV2 = Effect.fnUntraced(function* (reference: NativeCreationExecutionReferenceV2,
    input: Parameters<NativeCreationRepository["Service"]["completeEffectV2"]>[1]) {
    const resolved = yield* readExecution(reference);
    const starts = resolved.history.effectsV2.filter((fact) => fact.effectId === reference.effectId && fact.phase === "started");
    const start = starts[0];
    if (starts.length !== 1 || start === undefined || start.commandId !== reference.stageCommandId ||
      resolved.history.effectsV2.some((fact) => fact.effectId === reference.effectId && fact.phase === "completed"))
      return yield* unresolved("Native V2 completion has no unique unfinished start");
    yield* validateAcceptance({ commandId: start.commandId, threadId: start.threadId, commandType: start.commandType,
      commandDigest: start.commandDigest, bindingDigest: resolved.history.intent.bindingDigest, eventId: input.eventId, sequence: input.sequence });
    const fact = yield* Schema.decodeUnknownEffect(NativeCreationEffectV2)({ ...start, ...input, phase: "completed",
      ordinal: resolved.history.effects.length + resolved.history.effectsV2.length });
    if (fact.phase !== "completed") return yield* fail("Native V2 completion phase disagrees");
    yield* sql`INSERT INTO native_creation_effect_facts (claim_id, effect_id, phase, ordinal, fact_json)
      VALUES (${reference.claimId}, ${fact.effectId}, ${fact.phase}, ${fact.ordinal}, ${nativeCreationCanonicalJson(fact)})`;
    return fact;
  });
  const completeEffectV2: NativeCreationRepository["Service"]["completeEffectV2"] = (reference, input) =>
    sql.withTransaction(completeV2(reference, input)).pipe(Effect.mapError(mapIdentityError));

  const readRecovery = Effect.fnUntraced(function* (commandId: string) {
    const rows = yield* sql<{ claim_id: string; thread_id: string; command_digest: string; recovery_json: string }>`
      SELECT claim_id, thread_id, command_digest, recovery_json FROM native_creation_thread_recovery_commands WHERE command_id = ${commandId}`;
    if (rows.length === 0) return null;
    if (rows.length !== 1) return yield* unresolved("Native thread recovery is ambiguous");
    const row = rows[0]!;
    const recovery = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(NativeCreationThreadRecoveryCommandV2))(row.recovery_json, { onExcessProperty: "error" });
    const history = yield* readByClaim(row.claim_id);
    if (recovery.commandId !== commandId || recovery.claimId !== row.claim_id || recovery.threadId !== row.thread_id ||
      history.intent.threadId !== row.thread_id || recovery.commandDigest !== row.command_digest ||
      recovery.canonicalCommand.commandId !== commandId || recovery.canonicalCommand.threadId !== row.thread_id ||
      recovery.resource.threadId !== row.thread_id || commandId !== `${history.intent.commandId}:bootstrap-thread-delete` ||
      nativeCreationSha256(nativeCreationCanonicalJson(recovery.canonicalCommand)) !== row.command_digest)
      return yield* unresolved("Native thread recovery identity disagrees");
    return recovery;
  });
  const readThreadRecoveryCommand: NativeCreationRepository["Service"]["readThreadRecoveryCommand"] = (commandId) =>
    sql.withTransaction(readRecovery(commandId)).pipe(Effect.mapError(mapIdentityError));
  const reserveThreadRecoveryCommand: NativeCreationRepository["Service"]["reserveThreadRecoveryCommand"] = (input) =>
    sql.withTransaction(Effect.gen(function* () {
      const recovery = yield* Schema.decodeUnknownEffect(NativeCreationThreadRecoveryCommandV2)(input, { onExcessProperty: "error" });
      const history = yield* readByClaim(recovery.claimId);
      const prior = yield* readRecovery(recovery.commandId);
      if (prior !== null) {
        if (nativeCreationCanonicalJson(prior) !== nativeCreationCanonicalJson(recovery)) return yield* fail("Native recovery is immutable");
        return;
      }
      const cleanup = history.effects.find((fact) => fact.effectId === recovery.cleanupStartEffectId && fact.phase === "started");
      if (history.effectOverflow || recovery.threadId !== history.intent.threadId ||
        recovery.commandId !== `${history.intent.commandId}:bootstrap-thread-delete` ||
        recovery.canonicalCommand.commandId !== recovery.commandId || recovery.canonicalCommand.threadId !== recovery.threadId ||
        nativeCreationSha256(nativeCreationCanonicalJson(recovery.canonicalCommand)) !== recovery.commandDigest ||
        cleanup?.kind !== "cleanup" || cleanup.ordinal !== recovery.cleanupStartOrdinal || cleanup.recoveryScopeId !== recovery.recoveryScopeId ||
        nativeCreationCanonicalJson(cleanup.resource) !== nativeCreationCanonicalJson(recovery.resource) ||
        history.effects.some((fact) => fact.effectId === recovery.cleanupStartEffectId && fact.phase === "completed"))
        return yield* fail("Native recovery has no matching unfinished authorized cleanup");
      yield* rejectReceipt(recovery.commandId);
      const identity = yield* getIdentity(recovery.commandId);
      if (Option.isSome(identity) && identity.value.claimId !== recovery.claimId) return yield* fail("Recovery identity has a different owner");
      if (Option.isNone(identity)) yield* sql`INSERT INTO native_creation_reserved_command_identities (command_id, claim_id, thread_id)
        VALUES (${recovery.commandId}, ${recovery.claimId}, ${recovery.threadId})`;
      yield* reserve(recovery.claimId, recovery.canonicalCommand);
      yield* sql`INSERT INTO native_creation_thread_recovery_commands (command_id, claim_id, thread_id, command_digest, recovery_json, reserved_at)
        VALUES (${recovery.commandId}, ${recovery.claimId}, ${recovery.threadId}, ${recovery.commandDigest}, ${nativeCreationCanonicalJson(recovery)}, ${cleanup.timestamp})`;
    })).pipe(Effect.mapError(mapIdentityError));

  const confirmationSchema = Schema.Struct({
    version: Schema.Literal(1), effectId: Schema.NonEmptyString, commandId: CommandId, threadId: ThreadId,
    workerId: Schema.NonEmptyString, operationId: Schema.NonEmptyString, runId: RunId, attemptId: RunAttemptId,
    expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)), binding: ProviderRuntimeBinding,
    evidence: ProviderNativeEffectEvidence, evidenceRevision: Schema.Int.check(Schema.isGreaterThan(0)),
    nativeExecutionReference: Schema.NullOr(NativeCreationExecutionReferenceV2), commandEventId: EventId,
    commandEventSequence: Schema.Int.check(Schema.isGreaterThan(0)), confirmedAt: Schema.String,
  });
  const readConfirmation = Effect.fnUntraced(function* (effectId: string) {
    const rows = yield* sql<{ effect_id: string; command_id: string; thread_id: string; worker_id: string; operation_id: string;
      run_id: string; run_attempt_id: string; expected_attempt: number; binding_json: string; evidence_json: string;
      evidence_revision: number; native_execution_reference_json: string | null; command_event_id: string;
      command_event_sequence: number; confirmed_at: string }>`SELECT * FROM orchestration_v2_native_effect_confirmations WHERE effect_id = ${effectId}`;
    if (rows.length === 0) return null;
    if (rows.length !== 1) return yield* unresolved("Native confirmation is ambiguous");
    const row = rows[0]!;
    const binding = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ProviderRuntimeBinding))(row.binding_json);
    const evidence = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ProviderNativeEffectEvidence))(row.evidence_json);
    const reference = row.native_execution_reference_json === null ? null :
      yield* Schema.decodeUnknownEffect(Schema.fromJsonString(NativeCreationExecutionReferenceV2))(row.native_execution_reference_json);
    const proof = yield* Schema.decodeUnknownEffect(confirmationSchema)({ version: 1, effectId: row.effect_id,
      commandId: row.command_id, threadId: row.thread_id, workerId: row.worker_id, operationId: row.operation_id,
      runId: row.run_id, attemptId: row.run_attempt_id, expectedAttempt: row.expected_attempt, binding, evidence,
      evidenceRevision: row.evidence_revision, nativeExecutionReference: reference, commandEventId: row.command_event_id,
      commandEventSequence: row.command_event_sequence, confirmedAt: row.confirmed_at });
    if (proof.effectId !== effectId || proof.operationId !== effectId || proof.threadId !== binding.threadId ||
      evidence.outcome !== "confirmed_success" || evidence.operationId !== effectId || evidence.attemptId !== proof.attemptId ||
      evidence.threadId !== binding.threadId || evidence.providerThreadId !== binding.providerThreadId ||
      evidence.providerSessionId !== binding.providerSessionId || evidence.instanceId !== binding.instanceId ||
      evidence.runtimeGeneration !== binding.runtimeGeneration || !Number.isFinite(Date.parse(proof.confirmedAt)) ||
      (reference !== null && (reference.effectId !== effectId || reference.stageCommandId !== proof.commandId)))
      return yield* unresolved("Native confirmation correlation disagrees");
    const attributed = yield* sql`SELECT event_id FROM orchestration_events WHERE event_id = ${proof.commandEventId}
      AND sequence = ${proof.commandEventSequence} AND command_id = ${proof.commandId} AND stream_id = ${proof.threadId}
      AND aggregate_kind = 'thread' AND application_event_version = 2`;
    if (attributed.length !== 1) return yield* unresolved("Native confirmation lost its command event");
    return proof satisfies NativeEffectConfirmationV1;
  });
  const readNativeEffectConfirmation: NativeCreationRepository["Service"]["readNativeEffectConfirmation"] = (effectId) =>
    sql.withTransaction(readConfirmation(effectId)).pipe(Effect.mapError(mapIdentityError));
  const recordNativeEffectConfirmation: NativeCreationRepository["Service"]["recordNativeEffectConfirmation"] = (input) =>
    sql.withTransaction(Effect.gen(function* () {
      const binding = yield* Schema.decodeUnknownEffect(ProviderRuntimeBinding)(input.binding, { onExcessProperty: "error" });
      const evidence = yield* Schema.decodeUnknownEffect(ProviderNativeEffectEvidence)(input.evidence, { onExcessProperty: "error" });
      const prior = yield* readConfirmation(input.effectId);
      if (prior !== null) {
        if (prior.workerId !== input.workerId || prior.expectedAttempt !== input.expectedAttempt || prior.runId !== input.runId ||
          prior.attemptId !== input.attemptId || prior.evidenceRevision !== input.expectedEvidenceRevision ||
          nativeCreationCanonicalJson(prior.binding) !== nativeCreationCanonicalJson(binding) ||
          nativeCreationCanonicalJson(prior.evidence) !== nativeCreationCanonicalJson(evidence))
          return yield* unresolved("Native confirmation retry differs from committed evidence");
        return prior;
      }
      const now = DateTime.formatIso(yield* DateTime.now);
      const rows = yield* sql<{ command_id: string; thread_id: string; effect_type: string; payload_json: string;
        status: string; attempt_count: number; lease_owner: string | null; lease_expires_at: string | null }>`
        SELECT command_id, thread_id, effect_type, payload_json, status, attempt_count, lease_owner, lease_expires_at
        FROM orchestration_v2_effect_outbox WHERE effect_id = ${input.effectId}`;
      const row = rows[0];
      if (rows.length !== 1 || row === undefined || row.status !== "running" || row.lease_owner !== input.workerId ||
        row.attempt_count !== input.expectedAttempt || row.lease_expires_at === null ||
        !Number.isFinite(Date.parse(row.lease_expires_at)) || Date.parse(row.lease_expires_at) <= Date.parse(now) ||
        row.thread_id !== binding.threadId || !binding.nativeThreadId || evidence.outcome !== "confirmed_success" ||
        evidence.operationId !== input.effectId || evidence.attemptId !== input.attemptId ||
        evidence.threadId !== binding.threadId || evidence.providerThreadId !== binding.providerThreadId ||
        evidence.providerSessionId !== binding.providerSessionId || evidence.instanceId !== binding.instanceId ||
        evidence.runtimeGeneration !== binding.runtimeGeneration ||
        !((row.effect_type === "provider-turn.start" || row.effect_type === "provider-turn.restart") &&
          (evidence.operation === "start_turn" || evidence.operation === "compact_thread")))
        return yield* unresolved("Native confirmation does not own a live matching execution claim");
      const holds = yield* sql`SELECT effect_id FROM orchestration_v2_unknown_effect_holds WHERE effect_id = ${input.effectId}`;
      if (holds.length !== 0) return yield* unresolved("An unknown native effect cannot be acknowledged as success");
      const payload = yield* decodeOrchestrationEffectPayloadV2(row.payload_json);
      const reference = "nativeCreationExecutionReference" in payload ? payload.nativeCreationExecutionReference : null;
      const request = payload.request;
      if (!("runId" in request) || request.type !== row.effect_type || request.runId !== input.runId ||
        (reference !== null && (reference.effectId !== input.effectId || reference.stageCommandId !== row.command_id)))
        return yield* unresolved("Native confirmation payload differs from its immutable outbox request");
      const owners = yield* sql<{ provider_thread_id: string; provider_session_id: string; provider_instance_id: string;
        native_thread_id: string | null; runtime_generation: string; evidence_revision: number; driver: string }>`
        SELECT * FROM orchestration_v2_provider_runtime_evidence WHERE thread_id = ${binding.threadId}`;
      const owner = owners[0];
      if (owners.length !== 1 || owner === undefined || owner.provider_thread_id !== binding.providerThreadId ||
        owner.provider_session_id !== binding.providerSessionId || owner.provider_instance_id !== binding.instanceId ||
        owner.native_thread_id !== binding.nativeThreadId || owner.runtime_generation !== binding.runtimeGeneration ||
        owner.evidence_revision !== input.expectedEvidenceRevision)
        return yield* unresolved("Native confirmation runtime owner changed");
      const projections = yield* sql`SELECT run.run_id FROM orchestration_v2_projection_runs run
        JOIN orchestration_v2_projection_run_attempts attempt ON attempt.run_id = run.run_id AND attempt.thread_id = run.thread_id
        JOIN orchestration_v2_projection_threads thread ON thread.thread_id = run.thread_id
        JOIN orchestration_v2_projection_provider_threads provider ON provider.thread_id = run.thread_id AND provider.provider_thread_id = run.provider_thread_id
        JOIN orchestration_v2_projection_provider_sessions session ON session.provider_session_id = provider.provider_session_id
        JOIN orchestration_v2_projection_provider_session_bindings binding ON binding.provider_session_id = session.provider_session_id AND binding.thread_id = run.thread_id
        WHERE run.run_id = ${input.runId} AND run.thread_id = ${input.binding.threadId}
          AND attempt.attempt_id = ${input.attemptId} AND attempt.provider_thread_id = ${input.binding.providerThreadId}
          AND run.provider_thread_id = ${input.binding.providerThreadId} AND provider.provider_session_id = ${input.binding.providerSessionId}
          AND provider.provider_instance_id = ${input.binding.instanceId} AND session.provider_instance_id = ${input.binding.instanceId}
          AND json_extract(run.payload_json, '$.activeAttemptId') = ${input.attemptId}
          AND json_extract(thread.payload_json, '$.activeProviderThreadId') = ${input.binding.providerThreadId}
          AND json_extract(thread.payload_json, '$.modelSelection.instanceId') = ${input.binding.instanceId}
          AND json_extract(provider.payload_json, '$.nativeThreadRef.nativeId') = ${input.binding.nativeThreadId}
          AND run.provider = ${input.binding.instanceId} AND attempt.provider = ${input.binding.instanceId}
          AND provider.driver = ${owner.driver} AND session.driver = ${owner.driver}
          AND json_extract(thread.payload_json, '$.deletedAt') IS NULL
          AND run.status = 'running' AND attempt.status = 'running' AND session.status NOT IN ('stopped', 'error')`;
      if (projections.length !== 1) return yield* unresolved("Native confirmation is not bound to the active projected run");
      const receipt = yield* readReceipt(row.command_id);
      if (receipt?.status !== "accepted" || receipt.threadId !== binding.threadId)
        return yield* unresolved("Native confirmation has no matching accepted command");
      const events = yield* sql<{ event_id: string }>`SELECT event_id FROM orchestration_events
        WHERE sequence = ${receipt.resultSequence} AND command_id = ${row.command_id} AND stream_id = ${binding.threadId}
          AND aggregate_kind = 'thread' AND application_event_version = 2`;
      if (events.length !== 1) return yield* unresolved("Native confirmation has no attributed command event");
      const proof: NativeEffectConfirmationV1 = { version: 1, effectId: input.effectId, commandId: CommandId.make(row.command_id),
        threadId: binding.threadId, workerId: input.workerId, operationId: evidence.operationId, runId: input.runId,
        attemptId: input.attemptId, expectedAttempt: input.expectedAttempt, binding, evidence,
        evidenceRevision: input.expectedEvidenceRevision, nativeExecutionReference: reference,
        commandEventId: EventId.make(events[0]!.event_id), commandEventSequence: receipt.resultSequence, confirmedAt: now };
      yield* Schema.decodeUnknownEffect(confirmationSchema)(proof);
      if (reference !== null) yield* completeV2(reference, { timestamp: now, eventId: proof.commandEventId, sequence: proof.commandEventSequence });
      yield* sql`INSERT INTO orchestration_v2_native_effect_confirmations (effect_id, command_id, thread_id, worker_id, operation_id,
        run_id, run_attempt_id, expected_attempt, binding_json, evidence_json, evidence_revision, native_execution_reference_json,
        command_event_id, command_event_sequence, confirmed_at)
        VALUES (${proof.effectId}, ${proof.commandId}, ${proof.threadId}, ${proof.workerId}, ${proof.operationId}, ${proof.runId},
          ${proof.attemptId}, ${proof.expectedAttempt}, ${nativeCreationCanonicalJson(binding)}, ${nativeCreationCanonicalJson(evidence)},
          ${proof.evidenceRevision}, ${reference === null ? null : nativeCreationCanonicalJson(reference)}, ${proof.commandEventId},
          ${proof.commandEventSequence}, ${proof.confirmedAt})`;
      const changed = yield* sql`UPDATE orchestration_v2_effect_outbox SET status = 'succeeded', completed_at = ${now}, updated_at = ${now},
        lease_owner = NULL, lease_expires_at = NULL, last_error = NULL WHERE effect_id = ${proof.effectId}
        AND status = 'running' AND lease_owner = ${proof.workerId} AND attempt_count = ${proof.expectedAttempt} RETURNING effect_id`;
      if (changed.length !== 1) return yield* unresolved("Native effect lost ownership during confirmation");
      return proof;
    })).pipe(Effect.mapError(mapIdentityError));

  return NativeCreationRepository.of({
    readBoundedHistoryByThread, validateCommandAcceptanceV2, readExecutionReference, startEffectV2, completeEffectV2,
    readThreadRecoveryCommand, reserveThreadRecoveryCommand, readNativeEffectConfirmation, recordNativeEffectConfirmation,
    hasAutomationEnrollment,
    claim,
    readHistory,
    readHistoryByClaim: (claimId) => readByClaim(claimId).pipe(Effect.mapError(mapIdentityError)),
    reserveCommandIdentities,
    getReservedCommandIdentity: getIdentity,
    reserveCommand,
    recordNormalizedCommand,
    getReservedCommand: (commandId) =>
      getReserved(commandId).pipe(Effect.mapError(mapRepositoryError)),
    startEffect,
    completeEffect,
  });
});

export const layer = Layer.effect(NativeCreationRepository, make);
