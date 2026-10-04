import { assert, it } from "@effect/vitest";
import {
  AuthSessionId,
  CommandId,
  NativeCommandIdentityV2,
  NativeCreationHistoricalBinding,
  OrchestrationV2Command,
  RunId,
  ThreadId,
  EventId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { AuthSessionRepository, AuthSessionRecord } from "../persistence/AuthSessions.ts";
import migration from "../persistence/Migrations/003_JonesNativeCreationIntents.ts";
import { layer as repository } from "../persistence/Layers/NativeCreationRepository.ts";
import {
  NativeCreationRepository,
  NativeCreationRepositoryError,
  type NativeCreationResolvedExecutionV2,
} from "../persistence/Services/NativeCreationRepository.ts";
import {
  NativeCreationAuthority,
  NativeCreationAuthorityLive,
  NativeCreationAuthorityUnavailable,
  NativeCreationExecutionReferenceV2,
  getNativeCreationExecutionReference,
  authorizeNativeCreationExecution,
  NativeCreationBindingResolver,
  NativeCreationGrantResolver,
  type NativeCreationGrant,
  type NativeCreationStage,
} from "./NativeCreationAuthority.ts";
import {
  NativePreparationBinding,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  nativeCreationV2CommandDigest,
  nativePreparationCommand,
  validateNativeCreationPreparation,
} from "./NativeCreationPreparation.ts";

const actorSessionId = AuthSessionId.make("fixture-session");
const decodeFixtureBinding = Schema.decodeUnknownSync(NativePreparationBinding);
const decodeFixtureHistory = Schema.decodeUnknownSync(NativeCreationHistoricalBinding);
const decodeFixtureSession = Schema.decodeUnknownSync(AuthSessionRecord);
const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const database = Layer.effectDiscard(migration).pipe(Layer.provideMerge(memory));
const repositoryLayer = repository.pipe(Layer.provideMerge(database));
const guard = {
  schema: "t3.native-creation-guard/v1" as const,
  grantId: "fixture-grant",
  grantRevision: 1,
};

it.effect("execution references carry only ledger identity and cannot carry authority", () =>
  Effect.sync(() => {
    const reference = {
      version: 2,
      claimId: "fixture-claim",
      stageCommandId: CommandId.make("fixture-stage-command"),
      effectId: "fixture-effect",
      stage: "native_command",
    };
    const decode = Schema.decodeUnknownOption(NativeCreationExecutionReferenceV2);
    assert.isTrue(Option.isSome(decode(reference)));
    for (const input of [
      { ...reference, version: 1 },
      { ...reference, claimId: "" },
      { ...reference, effectId: "" },
      { ...reference, stage: "provider_start" },
      { ...reference, actorSessionId },
      { ...reference, guard },
      {
        ...reference,
        resources: { projectCwd: "/fixture", branch: "main", worktreePath: "/worktree" },
      },
    ]) {
      assert.isTrue(Option.isNone(decode(input)));
    }
  }),
);

const fixture = Effect.gen(function* () {
  yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe("2026-10-02T12:00:00Z")));
  const binding = decodeFixtureBinding({
    backend_instance: "fixture-backend",
    environment_id: "fixture-environment",
    project_id: "fixture-project",
    project_cwd: "/fixture/project",
    account_ref: "fixture-account",
    runtime_mode: "full-access" as const,
    interaction_mode: "default" as const,
    base_branch: "main",
    start_from_origin: false,
    run_setup_script: false,
    provider_model_selection: { instanceId: "codex", model: "fixture-model" },
  });
  const command = nativePreparationCommand(
    "fixture-authority",
    binding,
    "Synthetic prompt",
    "Synthetic thread",
    "2026-10-02T12:34:56Z",
  );
  const preparation = yield* validateNativeCreationPreparation(
    new TextEncoder().encode(
      nativeCreationCanonicalJson({
        schema: "voice.t3-bootstrap-preparation/v1",
        operation_id: "fixture-authority",
        binding,
        command,
        preparation_id: command.commandId.replace("voice-command-", "voice-bootstrap-"),
        binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)),
        prompt_digest: nativeCreationSha256(command.message.text),
        command_digest: nativeCreationSha256(nativeCreationCanonicalJson(command)),
      }),
    ),
  );
  const historical = decodeFixtureHistory({
    backendInstance: binding.backend_instance,
    environmentId: binding.environment_id,
    projectId: binding.project_id,
    projectCwd: binding.project_cwd,
    accountRef: binding.account_ref,
    accountBindingId: "fixture-qualified-account",
    accountBindingRevision: 1,
    providerModelSelection: binding.provider_model_selection,
    runtimeMode: binding.runtime_mode,
    interactionMode: binding.interaction_mode,
    baseBranch: binding.base_branch,
    startFromOrigin: false,
    runSetupScript: false,
    requestedBranch: command.bootstrap.prepareWorktree.branch,
  });
  const resources = {
    projectCwd: binding.project_cwd,
    branch: historical.requestedBranch,
    worktreePath: "/fixture/worktree",
  };
  const session = decodeFixtureSession({
    sessionId: actorSessionId,
    subject: "Synthetic actor",
    scopes: ["orchestration:operate"],
    method: "bearer-access-token",
    client: {
      label: null,
      ipAddress: null,
      userAgent: null,
      deviceType: "bot",
      os: null,
      browser: null,
    },
    issuedAt: "2026-01-01T00:00:00Z",
    expiresAt: "2099-01-01T00:00:00Z",
    revokedAt: null,
    lastConnectedAt: null,
  });
  const grant: NativeCreationGrant = {
    grantId: guard.grantId,
    revision: 1,
    actorSessionId,
    issuerId: "fixture-issuer",
    expiresAt: DateTime.makeUnsafe("2099-01-01T00:00:00Z"),
    revoked: false,
    operationId: preparation.operationId,
    preparationId: preparation.preparationId,
    preparationSha256: preparation.preparationSha256,
    bindingDigest: preparation.bindingDigest,
    binding: historical,
    resources,
    allowedStages: ["claim", "normalization", "fetch", "cleanup"],
    recoveryScopes: [
      {
        scopeId: "fixture-recovery",
        resource: {
          kind: "thread",
          threadId: ThreadId.make(preparation.command.threadId),
          incarnation: { eventId: EventId.make("fixture-created-event"), sequence: 1 },
        },
      },
    ],
  };
  return { preparation, historical, resources, session, grant };
});

const sessions = (read: () => Effect.Effect<Option.Option<AuthSessionRecord>>) =>
  Layer.succeed(
    AuthSessionRepository,
    AuthSessionRepository.of({
      getById: () => read(),
      create: () => Effect.void,
      createReplacingActive: () => Effect.succeed([]),
      createIfAbsent: () => Effect.void,
      listActive: () => Effect.succeed([]),
      revoke: () => Effect.succeed(false),
      revokeAllExcept: () => Effect.succeed([]),
      setLastConnectedAt: () => Effect.void,
      setClientConnection: () => Effect.void,
    }),
  );

it.effect("production ports remain unavailable even with a current native operating session", () =>
  Effect.gen(function* () {
    const value = yield* fixture;
    const result = yield* Effect.gen(function* () {
      const authority = yield* NativeCreationAuthority;
      const sql = yield* SqlClient.SqlClient;
      assert.isFalse(yield* authority.isAutomationEnrolled(actorSessionId));
      yield* sql`INSERT INTO native_creation_automation_enrollments (session_id, enrolled_at)
        VALUES (${actorSessionId}, '2026-10-02T12:00:00Z')`;
      assert.isTrue(yield* authority.isAutomationEnrolled(actorSessionId));
      const rejection = yield* authority
        .authorize({
          actorSessionId,
          guard,
          preparation: value.preparation,
          resources: value.resources,
          stage: "claim",
        })
        .pipe(Effect.flip);
      assert.deepEqual(yield* sql`SELECT COUNT(*) AS count FROM native_creation_intents`, [
        { count: 0 },
      ]);
      assert.deepEqual(yield* sql`SELECT COUNT(*) AS count FROM native_creation_effect_facts`, [
        { count: 0 },
      ]);
      assert.isTrue(yield* authority.isAutomationEnrolled(actorSessionId));
      return rejection;
    }).pipe(
      Effect.provide(
        NativeCreationAuthorityUnavailable.pipe(
          Layer.provideMerge(repositoryLayer),
          Layer.provide(sessions(() => Effect.succeed(Option.some(value.session)))),
        ),
      ),
    );
    assert.strictEqual(result.code, "unsupported_authority");
  }),
);

it.effect(
  "rechecks the native session, issuer, scoped grant and qualified binding at every stage",
  () =>
    Effect.gen(function* () {
      const value = yield* fixture;
      let currentSession: AuthSessionRecord | null = value.session;
      let currentGrant = value.grant;
      let currentBinding = value.historical;
      let enrolledSessionId = actorSessionId;
      let issuer = "fixture-issuer";
      let reads = 0;
      const authorityLayer = NativeCreationAuthorityLive.pipe(
        Layer.provideMerge(repositoryLayer),
        Layer.provide(
          sessions(() =>
            Effect.sync(() => {
              reads++;
              return Option.fromNullishOr(currentSession);
            }),
          ),
        ),
        Layer.provide(
          Layer.succeed(NativeCreationGrantResolver, {
            resolveCurrent: () =>
              Effect.sync(() => ({
                enrolledSessionId,
                trustedIssuerId: issuer,
                grant: currentGrant,
              })),
          }),
        ),
        Layer.provide(
          Layer.succeed(NativeCreationBindingResolver, {
            resolveCurrent: () => Effect.sync(() => currentBinding),
          }),
        ),
      );
      yield* Effect.gen(function* () {
        const authority = yield* NativeCreationAuthority;
        const sql = yield* SqlClient.SqlClient;
        const authorize = (
          stage: NativeCreationStage = "claim",
          recoveryScopeId?: string,
          recoveryResource?: NativeCreationGrant["recoveryScopes"][number]["resource"],
        ) =>
          authority.authorize({
            actorSessionId,
            guard,
            preparation: value.preparation,
            resources: value.resources,
            stage,
            ...(recoveryScopeId === undefined ? {} : { recoveryScopeId }),
            ...(recoveryResource === undefined ? {} : { recoveryResource }),
          });
        assert.isFalse(yield* authority.isAutomationEnrolled(actorSessionId));
        assert.strictEqual((yield* authorize().pipe(Effect.flip)).code, "stale_grant");
        assert.strictEqual(reads, 0);
        yield* sql`INSERT INTO native_creation_automation_enrollments (session_id, enrolled_at)
          VALUES (${actorSessionId}, '2026-10-02T12:00:00Z')`;
        assert.deepEqual(yield* authorize(), value.historical);
        assert.deepEqual(yield* authorize("fetch"), value.historical);
        assert.strictEqual(reads, 2);
        for (const session of [
          null,
          { ...value.session, scopes: [] },
          { ...value.session, revokedAt: DateTime.makeUnsafe("2026-01-02T00:00:00Z") },
          { ...value.session, expiresAt: DateTime.makeUnsafe("2020-01-01T00:00:00Z") },
        ]) {
          currentSession = session;
          assert.strictEqual((yield* authorize("fetch").pipe(Effect.flip)).code, "stale_grant");
        }
        currentSession = value.session;
        for (const changed of [
          { revoked: true },
          { revision: 2 },
          { actorSessionId: AuthSessionId.make("other-session") },
          { issuerId: "untrusted" },
          { expiresAt: DateTime.makeUnsafe("2020-01-01T00:00:00Z") },
        ]) {
          currentGrant = { ...value.grant, ...changed };
          assert.strictEqual((yield* authorize().pipe(Effect.flip)).code, "stale_grant");
          assert.isTrue(yield* authority.isAutomationEnrolled(actorSessionId));
        }
        currentGrant = value.grant;
        enrolledSessionId = AuthSessionId.make("other-session");
        assert.strictEqual((yield* authorize().pipe(Effect.flip)).code, "stale_grant");
        enrolledSessionId = actorSessionId;
        issuer = "";
        assert.strictEqual((yield* authorize().pipe(Effect.flip)).code, "stale_grant");
        issuer = "fixture-issuer";
        for (const changed of [
          { preparationSha256: "0".repeat(64) },
          { operationId: "other-operation" },
          { resources: { ...value.resources, worktreePath: "/other/worktree" } },
          { allowedStages: [] },
        ]) {
          currentGrant = { ...value.grant, ...changed };
          assert.strictEqual((yield* authorize().pipe(Effect.flip)).code, "binding_mismatch");
        }
        currentGrant = value.grant;
        for (const changed of [
          { environmentId: "other-environment" },
          { projectCwd: "/other/project" },
          { accountBindingRevision: 2 },
          { accountBindingId: "unqualified-account" },
          { accountRef: "other-account" },
          {
            providerModelSelection: {
              ...value.historical.providerModelSelection,
              model: "other-model",
            },
          },
        ]) {
          currentBinding = { ...value.historical, ...changed };
          assert.strictEqual((yield* authorize().pipe(Effect.flip)).code, "binding_mismatch");
        }
        currentBinding = value.historical;
        assert.strictEqual(
          (yield* authorize("cleanup").pipe(Effect.flip)).code,
          "binding_mismatch",
        );
        assert.strictEqual(
          (yield* authorize("cleanup", "other-recovery").pipe(Effect.flip)).code,
          "binding_mismatch",
        );
        const resource = value.grant.recoveryScopes[0]!.resource;
        assert.strictEqual(
          (yield* authorize("cleanup", "fixture-recovery").pipe(Effect.flip)).code,
          "binding_mismatch",
        );
        if (resource.kind !== "thread")
          return yield* Effect.die("Fixture cleanup resource changed");
        assert.strictEqual(
          (yield* authorize("cleanup", "fixture-recovery", {
            ...resource,
            incarnation: { eventId: EventId.make("other-created-event"), sequence: 1 },
          }).pipe(Effect.flip)).code,
          "binding_mismatch",
        );
        assert.deepEqual(
          yield* authorize("cleanup", "fixture-recovery", resource),
          value.historical,
        );
        assert.isTrue(yield* authority.isAutomationEnrolled(actorSessionId));
      }).pipe(Effect.provide(authorityLayer));
    }),
);

it.effect("unknown native enrollment lookup denies through the authority port", () =>
  Effect.gen(function* () {
    const value = yield* fixture;
    yield* Effect.gen(function* () {
      const authority = yield* NativeCreationAuthority;
      assert.strictEqual(
        (yield* authority.isAutomationEnrolled(actorSessionId).pipe(Effect.flip)).code,
        "unsupported_authority",
      );
      assert.strictEqual(
        (yield* authority
          .authorize({
            actorSessionId,
            guard,
            preparation: value.preparation,
            resources: value.resources,
            stage: "claim",
          })
          .pipe(Effect.flip)).code,
        "unsupported_authority",
      );
    }).pipe(
      Effect.provide(
        NativeCreationAuthorityUnavailable.pipe(
          Layer.provide(repository.pipe(Layer.provide(memory))),
          Layer.provide(sessions(() => Effect.succeed(Option.some(value.session)))),
        ),
      ),
    );
  }),
);

it.effect("issues opaque execution only after a new start and rechecks each actual effect", () =>
  Effect.gen(function* () {
    const value = yield* fixture;
    const reference = Schema.decodeUnknownSync(NativeCreationExecutionReferenceV2)({
      version: 2,
      claimId: "fixture-execution-claim",
      stageCommandId: `${value.preparation.command.commandId}:native:v2:create`,
      effectId: "fixture-execution-effect",
      stage: "native_command",
    });
    const command = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
      type: "thread.create",
      commandId: reference.stageCommandId,
      threadId: value.preparation.command.threadId,
      ...value.preparation.command.bootstrap.createThread,
      createdBy: "user",
      creationSource: "server",
    });
    if (command.type !== "thread.create") {
      return yield* Effect.die("Synthetic create stage decoded as another command");
    }
    const commandDigest = nativeCreationV2CommandDigest(command);
    const nativeIdentity = Schema.decodeUnknownSync(NativeCommandIdentityV2)({
      kind: "native_creation_stage",
      version: 2,
      commandId: reference.stageCommandId,
      commandType: command.type,
      aggregateKind: "thread",
      aggregateId: command.threadId,
      normalizedCommandDigest: commandDigest,
      bindingDigest: value.preparation.bindingDigest,
    });
    const resolved: NativeCreationResolvedExecutionV2 = {
      reference,
      command,
      nativeIdentity,
      preparation: value.preparation,
      history: {
        intent: {
          claimId: reference.claimId,
          claimedBootId: "fixture-boot",
          claimedAt: "2026-10-02T12:00:00Z",
          actorSessionId,
          grantId: guard.grantId,
          grantRevision: guard.grantRevision,
          preparationId: value.preparation.preparationId,
          operationId: value.preparation.operationId,
          preparationSha256: value.preparation.preparationSha256,
          bindingDigest: value.preparation.bindingDigest,
          promptDigest: value.preparation.promptDigest,
          commandDigest: value.preparation.commandDigest,
          commandId: value.preparation.command.commandId,
          threadId: value.preparation.command.threadId,
          messageId: value.preparation.command.message.messageId,
          canonicalPreparation: value.preparation.canonicalText,
          binding: value.historical,
          resources: value.resources,
        },
        normalizedCommandDigest: null,
        effects: [],
        effectsV2: [],
        effectOverflow: false,
      },
    };
    const releaseCommand: Extract<OrchestrationV2Command, { type: "prepared-run.release" }> = {
      type: "prepared-run.release",
      commandId: value.preparation.command.commandId,
      threadId: value.preparation.command.threadId,
      runId: RunId.make("fixture-prepared-run"),
    };
    const releaseDigest = nativeCreationV2CommandDigest(releaseCommand);
    const releaseResolved: NativeCreationResolvedExecutionV2 = {
      ...resolved,
      command: releaseCommand,
      history: { ...resolved.history, normalizedCommandDigest: releaseDigest },
      nativeIdentity: {
        ...nativeIdentity,
        commandId: releaseCommand.commandId,
        commandType: releaseCommand.type,
        normalizedCommandDigest: releaseDigest,
      },
    };
    let currentGrant: NativeCreationGrant = {
      ...value.grant,
      allowedStages: [...value.grant.allowedStages, "native_command"],
    };
    let loseNextStart = false;
    let starts = 0;
    let resolutionUnavailable = false;
    let invalidRecordedActor = false;
    const startedIds = new Set<string>();
    const startedStageCommands = new Set<string>();
    const startReferences: Array<NativeCreationExecutionReferenceV2> = [];
    const ledger = Layer.effect(
      NativeCreationRepository,
      Effect.gen(function* () {
        const baseline = yield* NativeCreationRepository;
        return NativeCreationRepository.of({
          ...baseline,
          hasAutomationEnrollment: () => Effect.succeed(true),
          readExecutionReference: (input) =>
            resolutionUnavailable
              ? Effect.fail(new NativeCreationRepositoryError({
                  code: "unresolved_claim",
                  message: "Synthetic accepted identity is unavailable",
                }))
              : Effect.sync(() => {
                  const stage = input.stageCommandId === command.commandId
                    ? resolved
                    : releaseResolved;
                  return {
                    ...stage,
                    reference: input,
                    history: {
                      ...stage.history,
                      intent: {
                        ...stage.history.intent,
                        actorSessionId: invalidRecordedActor ? "" : actorSessionId,
                      },
                    },
                  };
                }),
          startEffectV2: (input, timestamp, authorize) =>
            Effect.gen(function* () {
              assert.deepEqual(yield* authorize, value.historical);
              startReferences.push(input);
              if (startedIds.has(input.effectId) || startedStageCommands.has(input.stageCommandId)) {
                return yield* new NativeCreationRepositoryError({
                  code: "unresolved_claim",
                  message: "Synthetic effect is already started",
                });
              }
              startedIds.add(input.effectId);
              startedStageCommands.add(input.stageCommandId);
              starts++;
              if (loseNextStart) {
                loseNextStart = false;
                return yield* new NativeCreationRepositoryError({
                  code: "unresolved_claim",
                  message: "Synthetic committed start response is lost",
                });
              }
              const stageCommand = input.stageCommandId === command.commandId
                ? command
                : releaseCommand;
              return {
                status: "started" as const,
                fact: {
                  version: 2 as const,
                  kind: "native_command" as const,
                  phase: "started" as const,
                  effectId: input.effectId,
                  ordinal: starts - 1,
                  timestamp,
                  commandId: stageCommand.commandId,
                  threadId: stageCommand.threadId,
                  commandType: stageCommand.type,
                  commandDigest: nativeCreationV2CommandDigest(stageCommand),
                },
              };
            }),
        });
      }),
    ).pipe(Layer.provide(repositoryLayer));
    const authorityLayer = NativeCreationAuthorityLive.pipe(
      Layer.provide(ledger),
      Layer.provide(sessions(() => Effect.succeed(Option.some(value.session)))),
      Layer.provide(Layer.succeed(NativeCreationGrantResolver, {
        resolveCurrent: () => Effect.succeed({
          enrolledSessionId: actorSessionId,
          trustedIssuerId: "fixture-issuer",
          grant: currentGrant,
        }),
      })),
      Layer.provide(Layer.succeed(NativeCreationBindingResolver, {
        resolveCurrent: () => Effect.succeed(value.historical),
      })),
    );
    yield* Effect.gen(function* () {
      const authority = yield* NativeCreationAuthority;
      const input = { reference, timestamp: "2026-10-02T12:00:00Z" };
      const actual = { stage: "native_command" as const, resources: value.resources };
      assert.strictEqual((yield* authority.issueExecution({
        ...input,
        reference: { ...reference, stage: "fetch" },
      }).pipe(Effect.flip)).code, "unresolved_claim");
      assert.strictEqual(starts, 0);
      resolutionUnavailable = true;
      assert.strictEqual(
        (yield* authority.issueExecution(input).pipe(Effect.flip)).code,
        "unresolved_claim",
      );
      resolutionUnavailable = false;
      assert.strictEqual(starts, 0);
      invalidRecordedActor = true;
      assert.strictEqual(
        (yield* authority.issueExecution(input).pipe(Effect.flip)).code,
        "unresolved_claim",
      );
      invalidRecordedActor = false;
      assert.strictEqual(starts, 0);
      const context = yield* authority.issueExecution(input);
      assert.strictEqual(starts, 1);
      const startedReference = getNativeCreationExecutionReference(context);
      if (startedReference === null) return yield* Effect.die("Fresh execution has no reference");
      assert.deepEqual(startedReference, reference);
      assert.notStrictEqual(startedReference, reference);
      assert.isTrue(Object.isFrozen(startedReference));
      assert.isFalse(Reflect.set(startedReference, "effectId", "changed-effect"));
      assert.strictEqual(getNativeCreationExecutionReference(context), startedReference);
      assert.isNull(getNativeCreationExecutionReference(Object.assign({}, context)));
      assert.isNull(getNativeCreationExecutionReference(Object.assign({}, context, reference)));
      assert.deepEqual(yield* authorizeNativeCreationExecution(context, actual), value.historical);
      assert.deepEqual(yield* authority.authorizeExecution(context, {
        ...actual,
        stage: "fetch",
      }), value.historical);
      assert.strictEqual((yield* authority.authorizeExecution(
        Object.assign({}, context, reference), actual,
      ).pipe(Effect.flip)).code, "unsupported_authority");
      assert.strictEqual(
        (yield* authority.issueExecution(input).pipe(Effect.flip)).code,
        "unresolved_claim",
      );
      assert.strictEqual(starts, 1);
      assert.strictEqual(
        (yield* authority.issueExecution({
          ...input,
          reference: { ...reference, effectId: "fixture-alternate-effect" },
        }).pipe(Effect.flip)).code,
        "unresolved_claim",
      );
      assert.strictEqual(starts, 1);
      assert.strictEqual((yield* authority.authorizeExecution(context, {
        ...actual,
        resources: { ...value.resources, worktreePath: "/other/worktree" },
      }).pipe(Effect.flip)).code, "binding_mismatch");
      currentGrant = { ...currentGrant, revoked: true };
      assert.deepEqual(getNativeCreationExecutionReference(context), reference);
      assert.strictEqual(
        (yield* authorizeNativeCreationExecution(context, actual).pipe(Effect.flip)).code,
        "stale_grant",
      );
      currentGrant = { ...currentGrant, revoked: false };
      loseNextStart = true;
      const lost = {
        ...input,
        reference: {
          ...reference,
          stageCommandId: releaseCommand.commandId,
          effectId: "fixture-lost-effect",
        },
      };
      assert.strictEqual(
        (yield* authority.issueExecution(lost).pipe(Effect.flip)).code,
        "unresolved_claim",
      );
      assert.strictEqual(
        (yield* authority.issueExecution(lost).pipe(Effect.flip)).code,
        "unresolved_claim",
      );
      assert.strictEqual(
        (yield* authority.issueExecution({
          ...lost,
          reference: { ...lost.reference, effectId: "fixture-lost-alternate-effect" },
        }).pipe(Effect.flip)).code,
        "unresolved_claim",
      );
      assert.strictEqual(starts, 2);
      assert.deepEqual(startReferences.map((entry) => entry.effectId), [
        reference.effectId,
        reference.effectId,
        "fixture-alternate-effect",
        "fixture-lost-effect",
        "fixture-lost-effect",
        "fixture-lost-alternate-effect",
      ]);
    }).pipe(Effect.provide(authorityLayer));
  }),
);
