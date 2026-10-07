import * as SetupCustody from "./NativeWorkspaceSetupCustody.ts";
import type * as WorkspaceTypes from "./NativeCreationWorkspaceTypes.ts";
import { assert, it } from "@effect/vitest";
import { NativeCreationHistoricalBinding } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Authority from "./NativeCreationAuthority.ts";
import * as Repository from "./NativeCreationRepository.ts";
import * as RepositorySqlite from "./NativeCreationRepositorySqlite.ts";
import * as Workspace from "./NativeCreationWorkspacePreparation.ts";
import {
  NativePreparationBinding,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  nativePreparationCommand,
  validateNativeCreationPreparation,
} from "./NativeCreationPreparation.ts";
import migrateClaims from "../persistence/Migrations/003_JonesNativeCreationIntents.ts";
import migrateWorkspace from "../persistence/Migrations/101_JonesNativeWorkspacePreparation.ts";
const database = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* migrateClaims;
    yield* migrateWorkspace;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE jones_sql_migrations (migration_id INTEGER PRIMARY KEY, name TEXT NOT NULL)`;
    yield* sql`INSERT INTO jones_sql_migrations VALUES (100,'NativeCreationExecution'),(101,'NativeWorkspacePreparation')`;
  }),
).pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })));
const repositoryLayer = RepositorySqlite.layer.pipe(Layer.provideMerge(database));
const timestamp = "2026-10-02T12:34:56Z";
const decodeFixtureBinding = Schema.decodeUnknownSync(NativePreparationBinding);
const decodeFixtureHistory = Schema.decodeUnknownSync(NativeCreationHistoricalBinding);
const fixture = Effect.fnUntraced(function* (
  operationId = "fixture-operation",
  text = "Synthetic prompt",
  path = "/fixture/worktree",
  setup = false,
  fetch = false,
) {
  const binding = decodeFixtureBinding({
    backend_instance: "fixture-backend",
    environment_id: "fixture-environment",
    project_id: "fixture-project",
    project_cwd: "/fixture/project",
    account_ref: "fixture-account",
    runtime_mode: "full-access" as const,
    interaction_mode: "default" as const,
    base_branch: "main",
    start_from_origin: fetch,
    run_setup_script: setup,
    provider_model_selection: { instanceId: "codex", model: "fixture-model" },
  });
  const command = nativePreparationCommand(
    operationId,
    binding,
    text,
    "Synthetic thread",
    timestamp,
  );
  const preparation = yield* validateNativeCreationPreparation(
    new TextEncoder().encode(
      nativeCreationCanonicalJson({
        schema: "voice.t3-bootstrap-preparation/v1",
        operation_id: operationId,
        binding,
        command,
        preparation_id: command.commandId.replace("voice-command-", "voice-bootstrap-"),
        binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)),
        prompt_digest: nativeCreationSha256(text),
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
    accountBindingId: "qualified-fixture-account",
    accountBindingRevision: 1,
    providerModelSelection: binding.provider_model_selection,
    runtimeMode: binding.runtime_mode,
    interactionMode: binding.interaction_mode,
    baseBranch: binding.base_branch,
    startFromOrigin: fetch,
    runSetupScript: setup,
    requestedBranch: command.bootstrap.prepareWorktree.branch,
  });
  const input: Repository.NativeCreationClaimInput = {
    preparation,
    resources: {
      projectCwd: binding.project_cwd,
      branch: historical.requestedBranch,
      worktreePath: path,
    },
    claimId: `claim-${operationId}`,
    claimedBootId: "fixture-boot",
    claimedAt: timestamp,
    actorSessionId: "fixture-session",
    grantId: "fixture-grant",
    grantRevision: 1,
  };
  return { input, historical, preparation, authorize: Effect.succeed(historical) };
});

const scenario = Effect.fnUntraced(function* (setup = false, fetch = false) {
  const repository = yield* Repository.NativeCreationRepository;
  const value = yield* fixture("workspace", "Synthetic", "/fixture/worktree", setup, fetch);
  yield* repository.claim(value.input, value.authorize);
  const input = {
    claimId: value.input.claimId,
  };
  const state = {
    mutations: [] as string[],
    revoked: false,
    changed: false,
    unknown: false,
    setupExit: 0,
    revokeAfterFetch: false,
  };
  const basis: WorkspaceTypes.NativeWorkspaceBasis = {
    bootId: "fixture-boot",
    projectId: "fixture-project",
    projectCwd: "/fixture/project",
    projectBirth: "project-birth",
    gitCommonDirectory: "/fixture/project/.git",
    physicalGitIdentity: "git-inode",
    worktreePath: "/fixture/worktree",
    parentBirth: "parent-inode",
    producerId: "fixture-producer",
    baseRef: "main",
    setupDefinition: setup ? "exact-script" : null,
    configuredSubmodulesDefinition: "submodule-definition",
    baseConfigurationDefinition: "config-definition",
  };
  const proof: WorkspaceTypes.NativeWorkspaceProof = {
    worktreePath: basis.worktreePath,
    pathBirth: "new-inode",
    gitCommonDirectory: basis.gitCommonDirectory,
    physicalGitIdentity: basis.physicalGitIdentity,
    branch: value.input.resources.branch,
    baseRef: "main",
    configuredSubmodulesDigest: nativeCreationSha256(basis.configuredSubmodulesDefinition),
    baseConfigurationDigest: nativeCreationSha256(basis.baseConfigurationDefinition),
  };
  const started = yield* Deferred.make<void>();
  const completion = yield* Deferred.make<number>();
  const mutate = (name: string, check: Effect.Effect<void, Workspace.NativeWorkspaceError>) =>
    Effect.gen(function* () {
      yield* check;
      state.mutations.push(name);
      if (state.unknown)
        return yield* new Workspace.NativeWorkspaceError({
          code: "unknown",
          message: "Synthetic lost result",
        });
    });
  const ports = Layer.succeed(Workspace.NativeWorkspacePorts, {
    inspect: () =>
      Effect.succeed(state.changed ? { ...basis, parentBirth: "replacement-or-symlink" } : basis),
    fetch: (_basis, check) =>
      Effect.gen(function* () {
        yield* mutate("fetch", check);
        if (state.revokeAfterFetch) state.revoked = true;
      }),
    createWorktree: (_basis, _branch, check) =>
      Effect.gen(function* () {
        yield* mutate("worktree", check);
        yield* check;
        state.mutations.push("submodules");
        yield* check;
        state.mutations.push("base-config");
        return proof;
      }),
    verify: (_basis, _proof) =>
      state.changed
        ? Effect.fail(
            new Workspace.NativeWorkspaceError({
              code: "conflict",
              message: "Changed lstat/Git identity",
            }),
          )
        : Effect.void,
    setup: (_basis, _proof, check, custody) =>
      Effect.gen(function* () {
        const start = yield* SetupCustody.consume(
          custody,
          (yield* repository.readWorkspaceClaim!(input.claimId).pipe(
            Effect.mapError(
              () =>
                new Workspace.NativeWorkspaceError({
                  code: "conflict",
                  message: "Synthetic workspace claim read failed",
                }),
            ),
          )).intent,
          basis,
        );
        assert.isTrue(start.terminalId?.startsWith("native-setup-") === true);
        yield* mutate("setup", check);
        yield* Deferred.succeed(started, undefined);
        return { terminalId: start.terminalId!, completion: Deferred.await(completion) };
      }),
    cleanup: (_basis, _proof, check) => mutate("cleanup", check),
  });
  const authority = Layer.succeed(Authority.NativeCreationAuthority, {
    isAutomationEnrolled: () => Effect.succeed(true),
    authorize: (current) =>
      Effect.gen(function* () {
        assert.strictEqual(current.actorSessionId, value.input.actorSessionId);
        assert.deepEqual(current.guard, {
          schema: "t3.native-creation-guard/v1",
          grantId: value.input.grantId,
          grantRevision: value.input.grantRevision,
        });
        assert.deepEqual(current.resources, value.input.resources);
        assert.strictEqual(current.preparation.canonicalText, value.preparation.canonicalText);
        if (state.revoked)
          return yield* new Authority.NativeCreationAuthorityError({
            code: "stale_grant",
            message: "Revoked synthetic grant",
          });
        return yield* value.authorize;
      }),
  });
  const service = Workspace.layer.pipe(Layer.provide(Layer.merge(ports, authority)));
  return { repository, value, input, state, basis, proof, started, completion, service, authority };
});
it.effect("absent qualified physical/terminal ports fail before every external effect", () =>
  Effect.gen(function* () {
    const s = yield* scenario();
    const result = yield* Effect.gen(function* () {
      const owner = yield* Workspace.NativeCreationWorkspacePreparation;
      return yield* owner.prepare(s.input);
    }).pipe(Effect.provide(Workspace.layer.pipe(Layer.provide(s.authority))), Effect.result);
    assert.strictEqual(result._tag, "Failure");
    assert.deepEqual(s.state.mutations, []);
    assert.deepEqual((yield* s.repository.readWorkspaceClaim!(s.input.claimId)).effects, []);
  }).pipe(Effect.scoped, Effect.provide(repositoryLayer)),
);
it.effect(
  "records verified workspace only after retained stages, retries exact claim without effects, does not attest provider",
  () =>
    Effect.gen(function* () {
      const s = yield* scenario(false, true);
      yield* Effect.gen(function* () {
        const owner = yield* Workspace.NativeCreationWorkspacePreparation;
        const first = yield* owner.prepare(s.input);
        const again = yield* owner.prepare(s.input);
        assert.deepEqual(first, again);
        assert.deepEqual(s.state.mutations, ["fetch", "worktree", "submodules", "base-config"]);
        assert.isFalse(Object.hasOwn(first, "providerSessionId"));
        s.state.changed = true;
        assert.strictEqual((yield* owner.prepare(s.input).pipe(Effect.result))._tag, "Failure");
      }).pipe(Effect.provide(s.service));
    }).pipe(Effect.scoped, Effect.provide(repositoryLayer)),
);
it.effect("setup waits for exact retained terminal completion before verified receipt", () =>
  Effect.gen(function* () {
    const s = yield* scenario(true);
    yield* Effect.gen(function* () {
      const owner = yield* Workspace.NativeCreationWorkspacePreparation;
      const fiber = yield* owner.prepare(s.input).pipe(Effect.forkChild);
      yield* Deferred.await(s.started);
      assert.isTrue(Option.isNone(yield* s.repository.readWorkspaceVerified!(s.input.claimId)));
      yield* Deferred.succeed(s.completion, 0);
      const result = yield* Fiber.join(fiber);
      assert.isTrue(result.setupTerminalId?.startsWith("native-setup-") === true);
      assert.isTrue(Option.isSome(yield* s.repository.readWorkspaceVerified!(s.input.claimId)));
    }).pipe(Effect.provide(s.service));
  }).pipe(Effect.scoped, Effect.provide(repositoryLayer)),
);
it.effect("unknown external completion retains path and started history without replay", () =>
  Effect.gen(function* () {
    const s = yield* scenario();
    s.state.unknown = true;
    yield* Effect.gen(function* () {
      const owner = yield* Workspace.NativeCreationWorkspacePreparation;
      assert.strictEqual((yield* owner.prepare(s.input).pipe(Effect.result))._tag, "Failure");
      s.state.unknown = false;
      assert.strictEqual((yield* owner.prepare(s.input).pipe(Effect.result))._tag, "Failure");
      assert.deepEqual(s.state.mutations, ["worktree"]);
      assert.isTrue(Option.isNone(yield* s.repository.readWorkspaceVerified!(s.input.claimId)));
      const facts = (yield* s.repository.readWorkspaceClaim!(s.input.claimId)).effects;
      assert.strictEqual(facts.length, 2);
      assert.strictEqual(facts[1]!.phase, "completed");
    }).pipe(Effect.provide(s.service));
  }).pipe(Effect.scoped, Effect.provide(repositoryLayer)),
);
it.effect(
  "revocation and path admission replacement cannot advance or take over a started path",
  () =>
    Effect.gen(function* () {
      const s = yield* scenario();
      s.state.revoked = true;
      yield* Effect.gen(function* () {
        const owner = yield* Workspace.NativeCreationWorkspacePreparation;
        assert.strictEqual((yield* owner.prepare(s.input).pipe(Effect.result))._tag, "Failure");
        assert.deepEqual(s.state.mutations, []);
      }).pipe(Effect.provide(s.service));
      s.state.revoked = false;
      yield* s.repository.admitWorkspace!(s.input.claimId, s.basis);
      assert.strictEqual(
        (yield* s.repository.admitWorkspace!(s.input.claimId, {
          ...s.basis,
          parentBirth: "replacement",
        }).pipe(Effect.result))._tag,
        "Failure",
      );
      const sql = yield* SqlClient.SqlClient;
      assert.strictEqual(
        (yield* sql`DELETE FROM jones_native_workspace_admissions`.pipe(Effect.result))._tag,
        "Failure",
      );
    }).pipe(Effect.scoped, Effect.provide(repositoryLayer)),
);
it.effect(
  "recovery checks issued resource and fresh grant, does not force-delete replacement or replay cleanup",
  () =>
    Effect.gen(function* () {
      const s = yield* scenario();
      yield* Effect.gen(function* () {
        const owner = yield* Workspace.NativeCreationWorkspacePreparation;
        yield* owner.prepare(s.input);
        s.state.changed = true;
        assert.strictEqual(
          (yield* owner.rollback({ ...s.input, recoveryScopeId: "recovery" }).pipe(Effect.result))
            ._tag,
          "Failure",
        );
        assert.isFalse(s.state.mutations.includes("cleanup"));
        s.state.changed = false;
        yield* owner.rollback({ ...s.input, recoveryScopeId: "recovery" });
        assert.strictEqual(
          (yield* owner.rollback({ ...s.input, recoveryScopeId: "recovery" }).pipe(Effect.result))
            ._tag,
          "Failure",
        );
        assert.strictEqual(s.state.mutations.filter((v) => v === "cleanup").length, 1);
      }).pipe(Effect.provide(s.service));
    }).pipe(Effect.scoped, Effect.provide(repositoryLayer)),
);

it.effect("grant revocation between fetch and worktree denies the next mutation", () =>
  Effect.gen(function* () {
    const s = yield* scenario(false, true);
    s.state.revokeAfterFetch = true;
    yield* Effect.gen(function* () {
      const owner = yield* Workspace.NativeCreationWorkspacePreparation;
      assert.strictEqual((yield* owner.prepare(s.input).pipe(Effect.result))._tag, "Failure");
      assert.deepEqual(s.state.mutations, ["fetch"]);
      assert.isTrue(Option.isNone(yield* s.repository.readWorkspaceVerified!(s.input.claimId)));
    }).pipe(Effect.provide(s.service));
  }).pipe(Effect.scoped, Effect.provide(repositoryLayer)),
);
it.effect("cancellation retains started setup and terminal identity and never reruns setup", () =>
  Effect.gen(function* () {
    const s = yield* scenario(true);
    yield* Effect.gen(function* () {
      const owner = yield* Workspace.NativeCreationWorkspacePreparation;
      const fiber = yield* owner.prepare(s.input).pipe(Effect.forkChild);
      yield* Deferred.await(s.started);
      yield* Fiber.interrupt(fiber);
      const facts = (yield* s.repository.readWorkspaceClaim!(s.input.claimId)).effects;
      const setup = facts.find((fact) => fact.kind === "setup" && fact.phase === "completed");
      assert.ok(setup && setup.kind === "setup" && setup.phase === "completed");
      if (setup && setup.kind === "setup" && setup.phase === "completed") {
        assert.isTrue(setup.terminalId?.startsWith("native-setup-") === true);
        assert.strictEqual(setup.result, "unknown");
      }
      assert.strictEqual((yield* owner.prepare(s.input).pipe(Effect.result))._tag, "Failure");
      assert.strictEqual(s.state.mutations.filter((v) => v === "setup").length, 1);
    }).pipe(Effect.provide(s.service));
  }).pipe(Effect.scoped, Effect.provide(repositoryLayer)),
);

it.effect("another claim authority cannot use existing permanent path admission", () =>
  Effect.gen(function* () {
    const s = yield* scenario();
    yield* Effect.gen(function* () {
      const owner = yield* Workspace.NativeCreationWorkspacePreparation;
      assert.strictEqual(
        (yield* owner.prepare({ claimId: "other-claim" }).pipe(Effect.result))._tag,
        "Failure",
      );
      assert.deepEqual(s.state.mutations, []);
    }).pipe(Effect.provide(s.service));
    const competitor = yield* fixture("competitor", "Other", "/fixture/worktree");
    assert.strictEqual(
      (yield* s.repository.claim(competitor.input, competitor.authorize).pipe(Effect.result))._tag,
      "Failure",
    );
    assert.deepEqual(s.state.mutations, []);
  }).pipe(Effect.scoped, Effect.provide(repositoryLayer)),
);

it.effect(
  "known foreign prefix with receiving 101 remains readable but cannot issue workspace effects or admissions",
  () =>
    Effect.gen(function* () {
      const s = yield* scenario();
      const sql = yield* SqlClient.SqlClient;
      yield* sql`INSERT INTO jones_sql_migrations VALUES (7,'V2NativeAcceptance')`;
      const before = yield* sql`SELECT * FROM native_creation_intents`;
      assert.strictEqual(
        (yield* s.repository.readWorkspaceClaim!(s.input.claimId).pipe(Effect.result))._tag,
        "Failure",
      );
      assert.strictEqual(
        (yield* s.repository.admitWorkspace!(s.input.claimId, s.basis).pipe(Effect.result))._tag,
        "Failure",
      );
      assert.strictEqual(
        (yield* s.repository.recordWorkspaceVerified!({
          claimId: s.input.claimId,
          basis: s.basis,
          proof: s.proof,
          setupTerminalId: null,
        }).pipe(Effect.result))._tag,
        "Failure",
      );
      yield* Effect.gen(function* () {
        const owner = yield* Workspace.NativeCreationWorkspacePreparation;
        assert.strictEqual((yield* owner.prepare(s.input).pipe(Effect.result))._tag, "Failure");
      }).pipe(Effect.provide(s.service));
      assert.deepEqual(s.state.mutations, []);
      assert.deepEqual(yield* sql`SELECT * FROM jones_native_workspace_admissions`, []);
      assert.deepEqual(yield* sql`SELECT * FROM jones_native_workspace_verified`, []);
      assert.deepEqual(yield* sql`SELECT * FROM native_creation_effect_facts`, []);
      assert.deepEqual(yield* sql`SELECT * FROM native_creation_intents`, before);
      assert.isTrue(
        Option.isSome(yield* s.repository.readHistory(s.value.input.preparation.command.commandId)),
      );
      assert.deepEqual(
        yield* sql`SELECT migration_id,name FROM jones_sql_migrations ORDER BY migration_id`,
        [
          { migration_id: 7, name: "V2NativeAcceptance" },
          { migration_id: 100, name: "NativeCreationExecution" },
          { migration_id: 101, name: "NativeWorkspacePreparation" },
        ],
      );
    }).pipe(Effect.scoped, Effect.provide(repositoryLayer)),
);
