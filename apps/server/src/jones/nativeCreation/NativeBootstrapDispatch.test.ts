import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeBuffer from "node:buffer";
import {
  AuthSessionId,
  EnvironmentAuthenticatedPrincipal,
  NativeCreationHistoricalBinding,
  NativeBootstrapSubmission,
  OrchestrationV2Command,
  ProviderDriverKind,
  ProviderInstanceId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as IdAllocator from "../../orchestration-v2/IdAllocator.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Adapters from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Repository from "./NativeCreationRepository.ts";
import * as RepositorySqlite from "./NativeCreationRepositorySqlite.ts";
import * as Authority from "./NativeCreationAuthority.ts";
import * as Workspace from "./NativeCreationWorkspacePreparation.ts";
import * as WorkspaceTypes from "./NativeCreationWorkspaceTypes.ts";
import * as Provider from "./NativeCreationProviderExecution.ts";
import * as Stage from "./NativeCreationStageDispatch.ts";
import { dispatchNativeBootstrap } from "./NativeBootstrapDispatch.ts";
import {
  NativePreparationBinding,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  nativePreparationCommand,
  validateNativeCreationPreparation,
} from "./NativeCreationPreparation.ts";
const database = SqlitePersistence.layerMemory;
const ownerLayer = RepositorySqlite.layer.pipe(Layer.provideMerge(database));
const actor = AuthSessionId.make("synthetic-bootstrap-actor");
const principal = {
  sessionId: actor,
  subject: "synthetic",
  method: "bearer-access-token" as const,
  scopes: new Set(["orchestration:operate" as const]),
};
const fixture = Effect.gen(function* () {
  const binding = yield* Schema.decodeUnknownEffect(NativePreparationBinding)({
    backend_instance: "synthetic-backend",
    environment_id: "synthetic-environment",
    project_id: "synthetic-project",
    project_cwd: "/synthetic/project",
    account_ref: "synthetic-account",
    runtime_mode: "full-access",
    interaction_mode: "default",
    base_branch: "main",
    start_from_origin: false,
    run_setup_script: false,
    provider_model_selection: { instanceId: "codex", model: "synthetic-model" },
  });
  const command = nativePreparationCommand(
    "bootstrap",
    binding,
    "Synthetic prompt",
    "Synthetic title",
    "2026-10-02T12:34:56Z",
  );
  const canonical = nativeCreationCanonicalJson({
    schema: "voice.t3-bootstrap-preparation/v1",
    operation_id: "bootstrap",
    binding,
    command,
    preparation_id: command.commandId.replace("voice-command-", "voice-bootstrap-"),
    binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)),
    prompt_digest: nativeCreationSha256(command.message.text),
    command_digest: nativeCreationSha256(nativeCreationCanonicalJson(command)),
  });
  const preparation = yield* validateNativeCreationPreparation(new TextEncoder().encode(canonical));
  const historical = yield* Schema.decodeUnknownEffect(NativeCreationHistoricalBinding)({
    backendInstance: binding.backend_instance,
    environmentId: binding.environment_id,
    projectId: binding.project_id,
    projectCwd: binding.project_cwd,
    accountRef: binding.account_ref,
    accountBindingId: "synthetic-qualified-account",
    accountBindingRevision: 1,
    providerModelSelection: binding.provider_model_selection,
    runtimeMode: binding.runtime_mode,
    interactionMode: binding.interaction_mode,
    baseBranch: binding.base_branch,
    startFromOrigin: false,
    runSetupScript: false,
    requestedBranch: command.bootstrap.prepareWorktree.branch,
  });
  const ids = yield* IdAllocator.IdAllocatorV2;
  return {
    preparation,
    historical,
    submission: {
      schema: "t3.native-bootstrap-submission/v1",
      preparationBase64: NodeBuffer.Buffer.from(canonical).toString("base64"),
      creationGuard: {
        schema: "t3.native-creation-guard/v1",
        grantId: "synthetic-grant",
        grantRevision: 1,
      },
    },
    server: {
      bootId: "synthetic-boot",
      worktreesDir: "/synthetic/worktrees",
      deriveRunId: (threadId: import("@t3tools/contracts").ThreadId) =>
        ids.derive.run({ threadId, ordinal: 1 }),
    },
  };
});
const base = Layer.mergeAll(ownerLayer, IdAllocator.layer, NodeServices.layer);
const withPrincipal = <A, E, R>(
  effect: Effect.Effect<A, E, R | EnvironmentAuthenticatedPrincipal>,
) => effect.pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal));
const unavailable = new Authority.NativeCreationAuthorityError({
  code: "unsupported_authority",
  message: "Synthetic execution is not a production provider",
});
const configure = Effect.fnUntraced(function* (
  value: Effect.Success<typeof fixture>,
  unknown = false,
  ready = true,
  query = true,
) {
  const repository = yield* Repository.NativeCreationRepository;
  const sql = yield* SqlClient.SqlClient;
  const events: string[] = [];
  const { isReservedCommandIdentity: _reservationQuery, ...withoutQuery } = repository;
  const dispatchOwner = query ? repository : withoutQuery;
  const authority = Layer.mock(Authority.NativeCreationAuthority)({
    authorize: () => Effect.succeed(value.historical),
    issueExecution: () => Effect.fail(unavailable),
  });
  const physical = Layer.succeed(Workspace.NativeWorkspacePorts, {
    ...(ready ? { assertAvailable: () => Effect.void } : {}),
    inspect: (claim) =>
      Effect.succeed({
        bootId: claim.claimedBootId,
        projectId: claim.binding.projectId,
        projectCwd: claim.resources.projectCwd,
        projectBirth: "synthetic-project-birth",
        gitCommonDirectory: "/synthetic/project/.git",
        physicalGitIdentity: "synthetic-git",
        worktreePath: claim.resources.worktreePath,
        parentBirth: "synthetic-parent",
        producerId: "synthetic-producer",
        baseRef: claim.binding.baseBranch,
        setupDefinition: null,
        configuredSubmodulesDefinition: "",
        baseConfigurationDefinition: "",
      }),
    fetch: () => Effect.die("No fetch requested in bootstrap acceptance fixture"),
    createWorktree: (basis, branch, revalidate) =>
      Effect.gen(function* () {
        yield* revalidate;
        events.push("workspace");
        if (unknown)
          return yield* new Workspace.NativeWorkspaceError({
            code: "unknown",
            message: "Synthetic lost physical workspace completion",
          });
        const proof: WorkspaceTypes.NativeWorkspaceProof = {
          worktreePath: basis.worktreePath,
          pathBirth: "synthetic-new-path",
          gitCommonDirectory: basis.gitCommonDirectory,
          physicalGitIdentity: basis.physicalGitIdentity,
          branch,
          baseRef: basis.baseRef,
          configuredSubmodulesDigest: nativeCreationSha256(basis.configuredSubmodulesDefinition),
          baseConfigurationDigest: nativeCreationSha256(basis.baseConfigurationDefinition),
        };
        return proof;
      }),
    verify: () => Effect.void,
    setup: () => Effect.die("No setup requested in bootstrap acceptance fixture"),
    cleanup: () => Effect.die("No cleanup requested in bootstrap acceptance fixture"),
  });
  const workspaceOwners = Layer.mergeAll(
    authority,
    physical,
    Layer.succeed(Repository.NativeCreationRepository, dispatchOwner),
  );
  const workspaces = Workspace.layer.pipe(Layer.provideMerge(workspaceOwners));
  const external = Layer.mergeAll(
    workspaces,
    Layer.succeed(Provider.NativeCreationProviderExecutor, {
      assertAvailable: () => Effect.void,
      executeWholeOperation: () => Effect.die("Acceptance must not execute a provider"),
    }),
  );
  const adapters = Adapters.layerFromAdapters([
    {
      instanceId: ProviderInstanceId.make("codex"),
      driver: ProviderDriverKind.make("codex"),
      getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
      openSession: () => Effect.die("No provider process in bootstrap acceptance proof"),
    },
  ]);
  const runtime = ProviderReplayHarness.layerWithRegistry({ name: "native-bootstrap" }, adapters, {
    databaseLayer: database,
    runEffectWorker: false,
  }).pipe(Layer.provideMerge(external));
  yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES('synthetic-project','Synthetic','/synthetic/project','[]','2026-10-02T12:34:56Z','2026-10-02T12:34:56Z')`;
  return { runtime, events, repository, sql };
});

it.effect("missing bootstrap capability rejects before allocating native claims", () =>
  Effect.gen(function* () {
    const value = yield* fixture;
    const error = yield* withPrincipal(
      dispatchNativeBootstrap(value.submission, value.server),
    ).pipe(Effect.flip);
    assert.strictEqual(error.code, "unsupported_authority");
    const sql = yield* SqlClient.SqlClient;
    assert.deepStrictEqual(yield* sql`SELECT * FROM native_creation_intents`, []);
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM native_creation_reserved_command_identities`,
      [],
    );
  }).pipe(Effect.provide(base)),
);

it.effect("foreign history denies bootstrap with zero claims or reservations", () =>
  Effect.gen(function* () {
    const value = yield* fixture;
    const configured = yield* configure(value);
    yield* configured.sql`INSERT INTO jones_sql_migrations(migration_id,name) VALUES(7,'ForeignHistory')`;
    yield* withPrincipal(dispatchNativeBootstrap(value.submission, value.server)).pipe(
      Effect.flip,
      Effect.provide(configured.runtime),
    );
    assert.deepStrictEqual(yield* configured.sql`SELECT * FROM native_creation_intents`, []);
    assert.deepStrictEqual(
      yield* configured.sql`SELECT * FROM native_creation_reserved_command_identities`,
      [],
    );
    assert.deepStrictEqual(configured.events, []);
  }).pipe(Effect.provide(base)),
);

it.effect(
  "workspace success orders three native acceptances without claiming provider success",
  () =>
    Effect.gen(function* () {
      const value = yield* fixture;
      const configured = yield* configure(value);
      const result = yield* withPrincipal(
        dispatchNativeBootstrap(
          yield* Schema.decodeUnknownEffect(NativeBootstrapSubmission)(value.submission),
          value.server,
        ),
      ).pipe(Effect.provide(configured.runtime));
      assert.strictEqual(result.commandAcceptance, "accepted");
      assert.deepStrictEqual(configured.events, ["workspace"]);
      const accepted = yield* configured.sql<{
        command_id: string;
      }>`SELECT command_id FROM jones_native_creation_execution_acceptances ORDER BY event_sequence`;
      assert.deepStrictEqual(
        accepted.map((row) => row.command_id),
        [
          `${result.commandId}:native:v2:create`,
          `${result.commandId}:native:v2:message`,
          result.commandId,
        ],
      );
      assert.deepStrictEqual(
        yield* configured.sql`SELECT * FROM jones_native_creation_execution_confirmations`,
        [],
      );
      const queued = yield* configured.sql<{
        payload_json: string;
      }>`SELECT payload_json FROM orchestration_v2_effect_outbox WHERE command_id=${result.commandId}`;
      assert.strictEqual(queued.length, 1);
      assert.include(queued[0]!.payload_json, '"nativeCreationExecutionReference"');
      const reservation = yield* configured.repository.getReservedCommand(
        `${result.commandId}:native:v2:create`,
      );
      assert.isTrue(Option.isSome(reservation));
      if (Option.isNone(reservation))
        return yield* Effect.die("Expected accepted create reservation");
      const create = yield* Schema.decodeUnknownEffect(
        Schema.fromJsonString(OrchestrationV2Command),
      )(reservation.value.canonicalCommand);
      assert.strictEqual(create.type, "thread.create");
      if (create.type !== "thread.create") return yield* Effect.die("Expected thread creation");
      const token = yield* Stage.issueNativeCreationStage(reservation.value.claimId, create).pipe(
        Effect.provide(configured.runtime),
      );
      yield* Stage.validateNativeCreationStage(structuredClone(token), create).pipe(
        Effect.flip,
        Effect.provide(configured.runtime),
      );
      yield* Stage.validateNativeCreationStage(token, {
        ...create,
        title: "Changed immutable body",
      }).pipe(Effect.flip, Effect.provide(configured.runtime));
      const duplicate = yield* withPrincipal(
        dispatchNativeBootstrap(value.submission, value.server),
      ).pipe(Effect.provide(configured.runtime));
      assert.deepStrictEqual(duplicate, result);
      assert.deepStrictEqual(configured.events, ["workspace"]);
    }).pipe(Effect.provide(base)),
);

it.effect("unknown workspace and duplicate incomplete claim never dispatch or resume", () =>
  Effect.gen(function* () {
    const value = yield* fixture;
    const configured = yield* configure(value, true);
    for (let attempt = 0; attempt < 2; attempt++)
      yield* withPrincipal(dispatchNativeBootstrap(value.submission, value.server)).pipe(
        Effect.flip,
        Effect.provide(configured.runtime),
      );
    assert.deepStrictEqual(configured.events, ["workspace"]);
    assert.deepStrictEqual(
      yield* configured.sql`SELECT * FROM jones_native_creation_execution_acceptances`,
      [],
    );
    assert.deepStrictEqual(yield* configured.sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
  }).pipe(Effect.provide(base)),
);

it.effect("public dispatch refuses permanent native IDs before bodies are reserved", () =>
  Effect.gen(function* () {
    const value = yield* fixture;
    const configured = yield* configure(value);
    const claimId = "synthetic-private-reservation";
    yield* configured.repository.claim(
      {
        preparation: value.preparation,
        resources: {
          projectCwd: "/synthetic/project",
          branch: value.historical.requestedBranch,
          worktreePath: "/synthetic/worktree",
        },
        actorSessionId: actor,
        claimId,
        claimedBootId: "synthetic-boot",
        claimedAt: "2026-10-02T12:34:56Z",
        grantId: "synthetic-grant",
        grantRevision: 1,
      },
      Effect.succeed(value.historical),
    );
    yield* configured.repository.reserveExecutionCommandIdentities!(claimId, [
      value.preparation.command.commandId,
    ]);
    assert.isTrue(
      Option.isNone(
        yield* configured.repository.getReservedCommand(value.preparation.command.commandId),
      ),
    );
    const command = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
      type: "thread.create",
      commandId: value.preparation.command.commandId,
      threadId: value.preparation.command.threadId,
      projectId: "synthetic-project",
      title: "Bypass",
      modelSelection: value.historical.providerModelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      yield* orchestrator.dispatch(command).pipe(Effect.flip);
    }).pipe(Effect.provide(configured.runtime));
    yield* Stage.refusePublicNativeReservation(command.commandId).pipe(Effect.flip);
    assert.deepStrictEqual(yield* configured.sql`SELECT * FROM orchestration_events`, []);
  }).pipe(Effect.provide(base)),
);

it.effect("installed ports without readiness reject before allocating a claim", () =>
  Effect.gen(function* () {
    const value = yield* fixture;
    const configured = yield* configure(value, false, false);
    const error = yield* withPrincipal(
      dispatchNativeBootstrap(value.submission, value.server),
    ).pipe(Effect.flip, Effect.provide(configured.runtime));
    assert.strictEqual(error.code, "unsupported_authority");
    assert.deepStrictEqual(yield* configured.sql`SELECT * FROM native_creation_intents`, []);
    assert.deepStrictEqual(
      yield* configured.sql`SELECT * FROM native_creation_reserved_command_identities`,
      [],
    );
    assert.deepStrictEqual(configured.events, []);
  }).pipe(Effect.provide(base)),
);

it.effect("absent receiving100 denies zero-allocation despite available synthetic ports", () =>
  Effect.gen(function* () {
    const value = yield* fixture;
    const configured = yield* configure(value);
    yield* configured.sql`DELETE FROM jones_sql_migrations WHERE migration_id=100`;
    yield* withPrincipal(dispatchNativeBootstrap(value.submission, value.server)).pipe(
      Effect.flip,
      Effect.provide(configured.runtime),
    );
    assert.deepStrictEqual(yield* configured.sql`SELECT * FROM native_creation_intents`, []);
    assert.deepStrictEqual(
      yield* configured.sql`SELECT * FROM native_creation_reserved_command_identities`,
      [],
    );
    assert.deepStrictEqual(configured.events, []);
  }).pipe(Effect.provide(base)),
);

it.effect("missing permanent reservation reader denies private bootstrap before allocation", () =>
  Effect.gen(function* () {
    const value = yield* fixture;
    const configured = yield* configure(value, false, true, false);
    const error = yield* withPrincipal(
      dispatchNativeBootstrap(value.submission, value.server),
    ).pipe(Effect.flip, Effect.provide(configured.runtime));
    assert.strictEqual(error.code, "unsupported_authority");
    assert.deepStrictEqual(yield* configured.sql`SELECT * FROM native_creation_intents`, []);
    assert.deepStrictEqual(
      yield* configured.sql`SELECT * FROM native_creation_reserved_command_identities`,
      [],
    );
    assert.deepStrictEqual(configured.events, []);
  }).pipe(Effect.provide(base)),
);

it.effect("receiving bootstrap codec rejects historical command wire before effects", () =>
  Effect.gen(function* () {
    const value = yield* fixture;
    const configured = yield* configure(value);
    yield* Schema.decodeUnknownEffect(NativeBootstrapSubmission)(value.preparation.command).pipe(
      Effect.flatMap((submission) =>
        withPrincipal(dispatchNativeBootstrap(submission, value.server)),
      ),
      Effect.provide(configured.runtime),
      Effect.flip,
    );
    assert.deepStrictEqual(configured.events, []);
    assert.deepStrictEqual(yield* configured.sql`SELECT * FROM native_creation_intents`, []);
    assert.deepStrictEqual(
      yield* configured.sql`SELECT * FROM jones_native_creation_execution_acceptances`,
      [],
    );
    assert.deepStrictEqual(yield* configured.sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
  }).pipe(Effect.provide(base)),
);
