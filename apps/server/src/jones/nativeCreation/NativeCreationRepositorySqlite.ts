import * as Workspace from "./NativeCreationWorkspaceTypes.ts";
import { AuthSessionId, NativeCreationEffect, OrchestrationV2Command } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { makeNativeExecutionMethods } from "./NativeCreationExecutionSqlite.ts";
import * as Authority from "./NativeCreationAuthority.ts";
import * as Repository from "./NativeCreationRepository.ts";
import {
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  validateNativeCreationPreparation,
} from "./NativeCreationPreparation.ts";

const fail = (message: string) =>
  new Repository.NativeCreationRepositoryError({ code: "conflict", message });
const isRepositoryError = Schema.is(Repository.NativeCreationRepositoryError);
const isAuthorityError = Schema.is(Authority.NativeCreationAuthorityError);
const mapError = (cause: unknown) =>
  isRepositoryError(cause) || isAuthorityError(cause)
    ? cause
    : fail("Native creation persistence rejected the operation");
const mapRepositoryError = (cause: unknown) =>
  isRepositoryError(cause) ? cause : fail("Native creation persistence rejected the operation");
const decodeIntentJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Repository.NativeCreationStoredIntent),
);
const decodeEffectJson = Schema.decodeUnknownEffect(Schema.fromJsonString(NativeCreationEffect));
const decodeIntent = Schema.decodeUnknownEffect(Repository.NativeCreationStoredIntent);
const decodeCommand = Schema.decodeUnknownEffect(OrchestrationV2Command);
const decodeEffect = Schema.decodeUnknownEffect(NativeCreationEffect);
const decodeEnrollmentSessionId = Schema.decodeUnknownEffect(AuthSessionId);
const decodeEnrollmentRow = Schema.decodeUnknownEffect(
  Schema.Struct({
    sessionId: AuthSessionId,
    enrolledAt: Schema.DateTimeUtcFromString,
  }),
);

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const hasAutomationEnrollment: Repository.NativeCreationRepository["Service"]["hasAutomationEnrollment"] =
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
          new Repository.NativeCreationRepositoryError({
            code: "unresolved_claim",
            message: "Native automation enrollment membership is unavailable or malformed",
          }),
      ),
    );
  const readByClaim = Effect.fnUntraced(function* (claimId: string) {
    const rows = yield* sql<{
      intent_json: string;
    }>`SELECT intent_json FROM native_creation_intents WHERE claim_id = ${claimId}`;
    if (rows.length !== 1) return yield* fail("Native creation claim is missing");
    const intent = yield* decodeIntentJson(rows[0]!.intent_json);
    const normalized = yield* sql<{
      command_digest: string;
    }>`SELECT command_digest FROM native_creation_normalized_commands WHERE claim_id = ${claimId}`;
    const facts = yield* sql<{
      fact_json: string;
    }>`SELECT fact_json FROM native_creation_effect_facts WHERE claim_id = ${claimId} ORDER BY ordinal`;
    const effects = yield* Effect.forEach(facts, (row) => decodeEffectJson(row.fact_json));
    return {
      intent,
      normalizedCommandDigest: normalized[0]?.command_digest ?? null,
      effects,
    } satisfies Repository.NativeCreationHistory;
  });

  const claim: Repository.NativeCreationRepository["Service"]["claim"] = (input, authorize) =>
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
          const reservations =
            yield* sql`SELECT command_id FROM native_creation_reserved_commands WHERE command_id = ${preparation.command.commandId}`;
          if (reservations.length !== 0)
            return yield* fail("Native creation command is already reserved by another intent");
          const intent: Repository.NativeCreationStoredIntent = {
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

  const readHistory: Repository.NativeCreationRepository["Service"]["readHistory"] = (commandId) =>
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
      command_type: Repository.NativeCreationReservedCommand["commandType"];
      command_digest: string;
      canonical_command: string;
    }>`SELECT claim_id, command_id, thread_id, command_type, command_digest, canonical_command
       FROM native_creation_reserved_commands WHERE command_id = ${commandId}`;
    const row = rows[0];
    return row === undefined
      ? Option.none()
      : Option.some({
          claimId: row.claim_id,
          commandId: row.command_id,
          threadId: row.thread_id,
          commandType: row.command_type,
          commandDigest: row.command_digest,
          canonicalCommand: row.canonical_command,
        });
  });

  const reserve = Effect.fnUntraced(function* (claimId: string, input: OrchestrationV2Command) {
    const command = yield* decodeCommand(input, {
      onExcessProperty: "error",
    });
    const { intent } = yield* readByClaim(claimId);
    if (
      !("threadId" in command) ||
      command.threadId !== intent.threadId ||
      !["thread.create", "message.dispatch", "thread.delete", "prepared-run.release"].includes(
        command.type,
      )
    ) {
      return yield* fail("Creation command does not address the claimed thread");
    }
    const owners = yield* sql<{
      claim_id: string;
    }>`SELECT claim_id FROM native_creation_intents WHERE command_id = ${command.commandId}`;
    if (owners.some((row) => row.claim_id !== claimId))
      return yield* fail("Creation command is claimed by another intent");
    if (command.type === "prepared-run.release") {
      const identities =
        yield* sql`SELECT command_id FROM native_creation_reserved_command_identities WHERE command_id=${command.commandId} AND claim_id=${claimId} AND thread_id=${command.threadId}`;
      if (identities.length !== 1)
        return yield* fail("Native prepared release has no reserved execution identity");
    }
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
    yield* sql`INSERT INTO native_creation_reserved_commands (command_id, claim_id, thread_id, command_type, command_digest, canonical_command)
      VALUES (${command.commandId}, ${claimId}, ${command.threadId}, ${command.type}, ${nativeCreationSha256(canonicalCommand)}, ${canonicalCommand})`;
  });

  const reserveCommand: Repository.NativeCreationRepository["Service"]["reserveCommand"] = (
    claimId,
    command,
  ) => sql.withTransaction(reserve(claimId, command)).pipe(Effect.mapError(mapRepositoryError));
  const recordNormalizedCommand: Repository.NativeCreationRepository["Service"]["recordNormalizedCommand"] =
    (claimId, command) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const { intent } = yield* readByClaim(claimId);
            if (command.commandId !== intent.commandId || command.type !== "message.dispatch")
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
      | Parameters<Repository.NativeCreationRepository["Service"]["startEffect"]>[1]
      | Parameters<Repository.NativeCreationRepository["Service"]["completeEffect"]>[1],
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
      const reserved = yield* getReserved(fact.commandId);
      if (
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

  const startEffect: Repository.NativeCreationRepository["Service"]["startEffect"] = (
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
  const completeEffect: Repository.NativeCreationRepository["Service"]["completeEffect"] = (
    claimId,
    fact,
  ) =>
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

  const workspaceEligible = Effect.gen(function* () {
    const receiving = yield* sql<{migration_id:number}>`SELECT migration_id FROM jones_sql_migrations
      WHERE (migration_id = 100 AND name = 'NativeCreationExecution')
         OR (migration_id = 101 AND name = 'NativeWorkspacePreparation')`;
    const foreign = yield* sql`SELECT migration_id FROM jones_sql_migrations WHERE migration_id BETWEEN 7 AND 99`;
    if (receiving.length !== 2 || foreign.length !== 0) return yield* new Repository.NativeCreationRepositoryError({code:"unresolved_claim",message:"Native workspace lacks receiving migration provenance"});
  });
  const readWorkspaceAdmission = Effect.fnUntraced(function* (claimId: string) {
    yield* workspaceEligible;
    const rows = yield* sql<{basis_json:string}>`SELECT basis_json FROM jones_native_workspace_admissions WHERE claim_id = ${claimId}`;
    if (rows.length !== 1) return yield* fail("Workspace path admission is missing");
    return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Workspace.NativeWorkspaceBasis))(rows[0]!.basis_json);
  });
  const readWorkspaceVerified = Effect.fnUntraced(function* (claimId: string) {
    yield* workspaceEligible;
    const rows = yield* sql<{receipt_json:string}>`SELECT receipt_json FROM jones_native_workspace_verified WHERE claim_id = ${claimId}`;
    if (rows.length === 0) return Option.none<Workspace.NativeWorkspaceVerified>();
    if (rows.length !== 1) return yield* fail("Workspace verification receipt is ambiguous");
    return Option.some(yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Workspace.NativeWorkspaceVerified))(rows[0]!.receipt_json));
  });
  const admitWorkspace = (claimId:string, input:Workspace.NativeWorkspaceBasis) => sql.withTransaction(Effect.gen(function* () {
    yield* workspaceEligible;
    const basis = yield* Schema.decodeUnknownEffect(Workspace.NativeWorkspaceBasis)(input, {onExcessProperty:"error"});
    const {intent} = yield* readByClaim(claimId);
    if (basis.bootId !== intent.claimedBootId || basis.projectId !== intent.binding.projectId || basis.projectCwd !== intent.resources.projectCwd || basis.worktreePath !== intent.resources.worktreePath || basis.baseRef !== intent.binding.baseBranch) return yield* fail("Workspace admission differs from claimed resources");
    const rows = yield* sql<{claim_id:string;basis_json:string}>`SELECT claim_id,basis_json FROM jones_native_workspace_admissions WHERE claim_id = ${claimId} OR worktree_path = ${basis.worktreePath}`;
    const json = nativeCreationCanonicalJson(basis);
    if (rows.length > 0) {
      if (rows.length !== 1 || rows[0]!.claim_id !== claimId || rows[0]!.basis_json !== json) return yield* fail("Workspace path is permanently admitted to a different basis");
      return;
    }
    yield* sql`INSERT INTO jones_native_workspace_admissions (claim_id,worktree_path,basis_json) VALUES (${claimId},${basis.worktreePath},${json})`;
  })).pipe(Effect.mapError(mapRepositoryError));
  const recordWorkspaceVerified = (input:Workspace.NativeWorkspaceVerified) => sql.withTransaction(Effect.gen(function* () {
    yield* workspaceEligible;
    const verified = yield* Schema.decodeUnknownEffect(Workspace.NativeWorkspaceVerified)(input, {onExcessProperty:"error"});
    const history = yield* readByClaim(verified.claimId);
    const basis = yield* readWorkspaceAdmission(verified.claimId);
    if (nativeCreationCanonicalJson(basis) !== nativeCreationCanonicalJson(verified.basis) || verified.proof.worktreePath !== history.intent.resources.worktreePath || verified.proof.branch !== history.intent.resources.branch || verified.proof.baseRef !== basis.baseRef || verified.proof.physicalGitIdentity !== basis.physicalGitIdentity || verified.proof.gitCommonDirectory !== basis.gitCommonDirectory || verified.proof.configuredSubmodulesDigest !== nativeCreationSha256(basis.configuredSubmodulesDefinition) || verified.proof.baseConfigurationDigest !== nativeCreationSha256(basis.baseConfigurationDefinition)) return yield* fail("Workspace verified identity differs from admission");
    for (const kind of ["worktree", ...(history.intent.binding.startFromOrigin ? ["fetch"] : []), ...(history.intent.binding.runSetupScript ? ["setup"] : [])]) {
      const facts = history.effects.filter((fact) => fact.kind === kind);
      if (facts.length !== 2 || !facts.some((fact) => fact.phase === "started") || !facts.some((fact) => fact.phase === "completed" && "result" in fact && fact.result === "succeeded")) return yield* fail("Workspace verification requires all retained stage completions");
    }
    if (history.intent.binding.runSetupScript) {
      const setup = history.effects.find((fact) => fact.kind === "setup" && fact.phase === "completed");
      if (!setup || setup.kind !== "setup" || setup.phase !== "completed" || setup.exitCode !== 0 || !setup.terminalId || setup.terminalId !== verified.setupTerminalId) return yield* fail("Workspace verification requires exact retained successful setup terminal");
    } else if (verified.setupTerminalId !== null) return yield* fail("Unrequested setup terminal cannot be attested");
    const existing = yield* readWorkspaceVerified(verified.claimId);
    if (Option.isSome(existing)) {
      if (nativeCreationCanonicalJson(existing.value) !== nativeCreationCanonicalJson(verified)) return yield* fail("Workspace verification is immutable");
      return;
    }
    yield* sql`INSERT INTO jones_native_workspace_verified (claim_id,receipt_json) VALUES (${verified.claimId},${nativeCreationCanonicalJson(verified)})`;
  })).pipe(Effect.mapError(mapRepositoryError));
  const executionMethods = makeNativeExecutionMethods(sql, {
    // Execution references carry claim IDs; the public history reader accepts command IDs.
    readHistory: (claimId) =>
      Effect.gen(function* () {
        const rows =
          yield* sql`SELECT claim_id FROM native_creation_intents WHERE claim_id = ${claimId}`;
        if (rows.length === 0) return Option.none<Repository.NativeCreationHistory>();
        return Option.some(yield* readByClaim(claimId));
      }).pipe(Effect.mapError(mapRepositoryError)),
    getReservedCommand: (commandId) =>
      getReserved(commandId).pipe(Effect.mapError(mapRepositoryError)),
  });
  return Repository.NativeCreationRepository.of({
    ...executionMethods,
    readWorkspaceClaim: (claimId) => sql.withTransaction(Effect.gen(function* () {yield* workspaceEligible;return yield* readByClaim(claimId);})).pipe(Effect.mapError(mapRepositoryError)),
    admitWorkspace,
    readWorkspaceAdmission: (claimId) => readWorkspaceAdmission(claimId).pipe(Effect.mapError(mapRepositoryError)),
    readWorkspaceVerified: (claimId) => readWorkspaceVerified(claimId).pipe(Effect.mapError(mapRepositoryError)),
    recordWorkspaceVerified,
    hasAutomationEnrollment,
    claim,
    readHistory,
    reserveCommand,
    recordNormalizedCommand,
    getReservedCommand: (commandId) =>
      getReserved(commandId).pipe(Effect.mapError(mapRepositoryError)),
    startEffect,
    completeEffect,
  });
});

export const layer = Layer.effect(Repository.NativeCreationRepository, make);
