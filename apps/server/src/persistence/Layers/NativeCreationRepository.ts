import {
  AuthSessionId,
  CommandId,
  ThreadId,
  NativeCreationEffect,
  OrchestrationCommand,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { NativeCreationAuthorityError } from "../../orchestration-v2/NativeCreationAuthority.ts";
import {
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  validateNativeCreationPreparation,
} from "../../orchestration-v2/NativeCreationPreparation.ts";
import {
  NativeCreationRepository,
  NativeCreationRepositoryError,
  NativeCreationStoredIntent,
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
const decodeEffectJson = Schema.decodeUnknownEffect(Schema.fromJsonString(NativeCreationEffect));
const decodeIntent = Schema.decodeUnknownEffect(NativeCreationStoredIntent);
const decodeCommandId = Schema.decodeUnknownEffect(CommandId);
const decodeCommandIdentity = Schema.decodeUnknownEffect(
  Schema.Struct({ claimId: Schema.NonEmptyString, commandId: CommandId, threadId: ThreadId }),
);
const decodeCommand = Schema.decodeUnknownEffect(OrchestrationCommand);
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
      fact_json: string;
    }>`SELECT fact_json FROM native_creation_effect_facts WHERE claim_id = ${claimId} ORDER BY ordinal`;
    const effects = yield* Effect.forEach(facts, (row) => decodeEffectJson(row.fact_json));
    return {
      intent,
      normalizedCommandDigest: normalized[0]?.command_digest ?? null,
      effects,
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
            history: { intent, normalizedCommandDigest: null, effects: [] },
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
    const command = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationCommand))(
      row.canonical_command,
    );
    if (
      identity.commandId !== commandId ||
      command.commandId !== commandId ||
      !("threadId" in command) ||
      command.threadId !== identity.threadId ||
      command.type !== row.command_type ||
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

  const reserve = Effect.fnUntraced(function* (claimId: string, input: OrchestrationCommand) {
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
          if (command.commandId !== intent.commandId || command.type !== "thread.turn.start")
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
      ordinal: history.effects.length,
    });
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

  return NativeCreationRepository.of({
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
