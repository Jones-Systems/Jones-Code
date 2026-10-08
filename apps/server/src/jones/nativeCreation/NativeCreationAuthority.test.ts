import { assert, it } from "@effect/vitest";
import {
  AuthSessionId,
  IsoDateTime,
  OrchestrationV2Command,
  NativeCreationHistoricalBinding,
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
import * as AuthSessions from "../../persistence/AuthSessions.ts";
import migration from "../persistence/Migrations/003_JonesNativeCreationIntents.ts";
import * as Repository from "./NativeCreationRepository.ts";
import { NativeCreationExecutionReferenceV2 } from "./NativeCreationExecutionTypes.ts";
import * as RepositorySqlite from "./NativeCreationRepositorySqlite.ts";

import * as Authority from "./NativeCreationAuthority.ts";
import {
  NativePreparationBinding,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  nativePreparationCommand,
  validateNativeCreationPreparation,
} from "./NativeCreationPreparation.ts";

const actorSessionId = AuthSessionId.make("fixture-session");
const decodeFixtureBinding = Schema.decodeUnknownSync(NativePreparationBinding);
const decodeFixtureHistory = Schema.decodeUnknownSync(NativeCreationHistoricalBinding);
const decodeFixtureSession = Schema.decodeUnknownSync(AuthSessions.AuthSessionRecord);
const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const database = Layer.effectDiscard(migration).pipe(Layer.provideMerge(memory));
const repositoryLayer = RepositorySqlite.layer.pipe(Layer.provideMerge(database));
const guard = {
  schema: "t3.native-creation-guard/v1" as const,
  grantId: "fixture-grant",
  grantRevision: 1,
};
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
  const grant: Authority.NativeCreationGrant = {
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

const sessions = (read: () => Effect.Effect<Option.Option<AuthSessions.AuthSessionRecord>>) =>
  Layer.succeed(
    AuthSessions.AuthSessionRepository,
    AuthSessions.AuthSessionRepository.of({
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
      const authority = yield* Authority.NativeCreationAuthority;
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
        Authority.NativeCreationAuthorityUnavailable.pipe(
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
      let currentSession: AuthSessions.AuthSessionRecord | null = value.session;
      let currentGrant = value.grant;
      let currentBinding = value.historical;
      let enrolledSessionId = actorSessionId;
      let issuer = "fixture-issuer";
      let reads = 0;
      const authorityLayer = Authority.NativeCreationAuthorityLive.pipe(
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
          Layer.succeed(Authority.NativeCreationGrantResolver, {
            resolveCurrent: () =>
              Effect.sync(() => ({
                enrolledSessionId,
                trustedIssuerId: issuer,
                grant: currentGrant,
              })),
          }),
        ),
        Layer.provide(
          Layer.succeed(Authority.NativeCreationBindingResolver, {
            resolveCurrent: () => Effect.sync(() => currentBinding),
          }),
        ),
      );
      yield* Effect.gen(function* () {
        const authority = yield* Authority.NativeCreationAuthority;
        const sql = yield* SqlClient.SqlClient;
        const authorize = (
          stage: Authority.NativeCreationStage = "claim",
          recoveryScopeId?: string,
          recoveryResource?: Authority.NativeCreationGrant["recoveryScopes"][number]["resource"],
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
      const authority = yield* Authority.NativeCreationAuthority;
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
        Authority.NativeCreationAuthorityUnavailable.pipe(
          Layer.provide(RepositorySqlite.layer.pipe(Layer.provide(memory))),
          Layer.provide(sessions(() => Effect.succeed(Option.some(value.session)))),
        ),
      ),
    );
  }),
);

it.effect("rejects forged contexts and unavailable durable execution storage", () =>
  Effect.gen(function* () {
    const value = yield* fixture;
    const forged = Object.freeze({}) as Authority.NativeCreationExecutionContextV2;
    assert.strictEqual(Authority.getNativeCreationExecutionReference(forged), null);
    const rejection = yield* Authority.authorizeNativeCreationExecution(forged, {
      stage: "native_command",
      resources: value.resources,
    }).pipe(Effect.flip);
    assert.strictEqual(rejection.code, "unsupported_authority");
    const reference = yield* Schema.decodeUnknownEffect(NativeCreationExecutionReferenceV2)({
      version: 2,
      claimId: "fixture-claim",
      stageCommandId: "fixture-command",
      effectId: "fixture-effect",
      stage: "native_command",
    });
    const result = yield* Effect.gen(function* () {
      const authority = yield* Authority.NativeCreationAuthority;
      assert.isDefined(authority.issueExecution);
      return yield* authority.issueExecution!({
        reference,
        timestamp: "2026-10-02T12:34:56Z",
      }).pipe(Effect.flip);
    }).pipe(
      Effect.provide(
        Authority.NativeCreationAuthorityUnavailable.pipe(
          Layer.provide(repositoryLayer),
          Layer.provide(sessions(() => Effect.succeed(Option.some(value.session)))),
        ),
      ),
    );
    assert.strictEqual(result.code, "unresolved_claim");
  }),
);

it.effect("issues only after a matching durable start and rechecks revoked authority", () =>
  Effect.gen(function* () {
    const value = yield* fixture;
    let revoked = false;
    let starts = 0;
    let mismatchedStart = false;
    const reference = yield* Schema.decodeUnknownEffect(NativeCreationExecutionReferenceV2)({
      version: 2,
      claimId: "fixture-claim",
      stageCommandId: "fixture-command",
      effectId: "fixture-effect",
      stage: "native_command",
    });
    const command = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
      type: "thread.create",
      createdBy: "system",
      creationSource: "server",
      commandId: "fixture-command",
      threadId: value.preparation.command.threadId,
      projectId: value.historical.projectId,
      title: "Synthetic thread",
      modelSelection: value.historical.providerModelSelection,
      runtimeMode: value.historical.runtimeMode,
      interactionMode: value.historical.interactionMode,
      branch: value.resources.branch,
      worktreePath: value.resources.worktreePath,
    });
    const executionOwner = Layer.effect(
      Repository.NativeCreationRepository,
      Effect.gen(function* () {
        const existing = yield* Repository.NativeCreationRepository;
        return Repository.NativeCreationRepository.of({
          ...existing,
          hasAutomationEnrollment: () => Effect.succeed(true),
          readExecutionReference: () =>
            Effect.gen(function* () {
              return {
                history: {
                  intent: yield* Schema.decodeUnknownEffect(Repository.NativeCreationStoredIntent)({
                    claimId: "fixture-claim",
                    claimedBootId: "fixture-boot",
                    claimedAt: "2026-10-02T12:34:56Z",
                    actorSessionId,
                    grantId: guard.grantId,
                    grantRevision: guard.grantRevision,
                    preparationId: value.preparation.preparationId,
                    operationId: value.preparation.operationId,
                    preparationSha256: value.preparation.preparationSha256,
                    bindingDigest: value.preparation.bindingDigest,
                    promptDigest: value.preparation.promptDigest,
                    commandDigest: value.preparation.commandDigest,
                    commandId: reference.stageCommandId,
                    threadId: value.preparation.command.threadId,
                    messageId: value.preparation.command.message.messageId,
                    canonicalPreparation: value.preparation.canonicalText,
                    binding: value.historical,
                    resources: value.resources,
                  }).pipe(
                    Effect.mapError(
                      () =>
                        new Repository.NativeCreationRepositoryError({
                          code: "unresolved_claim",
                          message: "Synthetic execution claim is invalid",
                        }),
                    ),
                  ),
                  normalizedCommandDigest: "a".repeat(64),
                  effects: [],
                },
                preparation: value.preparation,
                command,
                nativeIdentity: { normalizedCommandDigest: "a".repeat(64) },
              };
            }),
          startEffectV2: (ref, timestamp, authorize) =>
            Effect.gen(function* () {
              yield* authorize;
              starts += 1;
              return {
                status: "started" as const,
                fact: {
                  version: 2 as const,
                  kind: "native_command" as const,
                  phase: "started" as const,
                  ordinal: 0,
                  timestamp: yield* Schema.decodeUnknownEffect(IsoDateTime)(timestamp).pipe(
                    Effect.mapError(
                      () =>
                        new Repository.NativeCreationRepositoryError({
                          code: "unresolved_claim",
                          message: "Synthetic start timestamp is invalid",
                        }),
                    ),
                  ),
                  effectId: mismatchedStart ? "other-effect" : ref.effectId,
                  commandId: ref.stageCommandId,
                  threadId: ThreadId.make(value.preparation.command.threadId),
                  commandType: "thread.create" as const,
                  commandDigest: "a".repeat(64),
                },
              };
            }),
        });
      }),
    ).pipe(Layer.provide(repositoryLayer));
    const context = yield* Effect.gen(function* () {
      const authority = yield* Authority.NativeCreationAuthority;
      const context = yield* authority.issueExecution!({
        reference,
        timestamp: "2026-10-02T12:34:56Z",
      });
      assert.strictEqual(starts, 1);
      assert.deepEqual(Authority.getNativeCreationExecutionReference(context), reference);
      const resourceMismatch = yield* Authority.authorizeNativeCreationExecution(context, {
        stage: "fetch",
        resources: { ...value.resources, worktreePath: "/other/worktree" },
      }).pipe(Effect.flip);
      assert.strictEqual(resourceMismatch.code, "binding_mismatch");
      mismatchedStart = true;
      const startMismatch = yield* authority.issueExecution!({
        reference,
        timestamp: "2026-10-02T12:34:56Z",
      }).pipe(Effect.flip);
      assert.strictEqual(startMismatch.code, "unresolved_claim");
      revoked = true;
      const rejection = yield* Authority.authorizeNativeCreationExecution(context, {
        stage: "fetch",
        resources: value.resources,
      }).pipe(Effect.flip);
      assert.strictEqual(rejection.code, "stale_grant");
      return context;
    }).pipe(
      Effect.provide(
        Authority.NativeCreationAuthorityLive.pipe(
          Layer.provide(executionOwner),
          Layer.provide(sessions(() => Effect.succeed(Option.some(value.session)))),
          Layer.provide(
            Layer.succeed(Authority.NativeCreationGrantResolver, {
              resolveCurrent: () =>
                Effect.succeed({
                  enrolledSessionId: actorSessionId,
                  trustedIssuerId: "fixture-issuer",
                  grant: {
                    ...value.grant,
                    revoked,
                    allowedStages: [...value.grant.allowedStages, "native_command"],
                  },
                }),
            }),
          ),
          Layer.provide(
            Layer.succeed(Authority.NativeCreationBindingResolver, {
              resolveCurrent: () => Effect.succeed(value.historical),
            }),
          ),
        ),
      ),
    );
    assert.isTrue(Object.isFrozen(context));
  }),
);
