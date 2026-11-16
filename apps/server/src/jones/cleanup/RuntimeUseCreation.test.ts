import * as PtyAdapter from "@t3tools/shared/PtyAdapter";
import * as SecureRandom from "effect/crypto/SecureRandom";
import { AgentScope } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Manager from "../../terminal/Manager.ts";
import * as Runner from "../../project/ProjectSetupScriptRunner.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as Projection from "../../orchestration-v2/ProjectionStore.ts";
import * as SetupExecutor from "../nativeCreation/NativeWorkspaceSetupExecutor.ts";
import { makeRuntimeUseStore } from "./RuntimeUseStore.ts";
import * as Path from "effect/Path";
import * as ProcessRunner from "../../processRunner.ts";
import { assert, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  DEFAULT_SERVER_SETTINGS,
  EventId,
  ProjectId,
  NativeCreationHistoricalBinding,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Ownership from "../../serverOwnership.ts";
import * as Config from "../../config.ts";
import * as Git from "../../vcs/GitVcsDriver.ts";
import * as Projects from "../../orchestration-v2/ProjectStore.ts";
import * as Settings from "../../serverSettings.ts";
import { runMigrations } from "../../persistence/Migrations.ts";
import * as Repository from "../nativeCreation/NativeCreationRepository.ts";
import * as RepositorySqlite from "../nativeCreation/NativeCreationRepositorySqlite.ts";
import * as Physical from "../nativeCreation/NativeWorkspacePhysicalPorts.ts";
import * as Authority from "../nativeCreation/NativeCreationAuthority.ts";
import * as Workspace from "../nativeCreation/NativeCreationWorkspacePreparation.ts";
import {
  NativePreparationBinding,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  nativePreparationCommand,
  validateNativeCreationPreparation,
} from "../nativeCreation/NativeCreationPreparation.ts";
import { HeldRuntimeOwner } from "./CheckedRuntimeOwner.ts";
import { readRuntimeUseCreationHistory } from "./RuntimeUseCreationStore.ts";
import { readRuntimeUseInstallation } from "./RuntimeUseInstallation.ts";
import { digest } from "./RuntimeUseProtocol.ts";
const platform = Layer.merge(Git.layer, ProcessRunner.layer).pipe(
  Layer.provideMerge(Config.layerTest(process.cwd(), { prefix: "runtime-creation-platform-" })),
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);
const timestamp = "2026-10-02T12:34:56Z";
const decodeFixtureBinding = Schema.decodeUnknownSync(NativePreparationBinding);
const decodeFixtureHistory = Schema.decodeUnknownSync(NativeCreationHistoricalBinding);
const fixture = Effect.fnUntraced(function* (
  projectCwd: string,
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
    project_cwd: projectCwd,
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

const physicalFixture = <A, E>(
  test: (input: {
    readonly prepare: Effect.Effect<
      import("../nativeCreation/NativeCreationWorkspaceTypes.ts").NativeWorkspaceVerified,
      Workspace.NativeWorkspaceError
    >;
    readonly repository: Repository.NativeCreationRepository["Service"];
    readonly claimId: string;
    readonly path: string;
    readonly prepareOldAdmission: Effect.Effect<void, Error>;
    readonly assertNoMutation: Effect.Effect<void, Error>;
  }) => Effect.Effect<A, E, SqlClient.SqlClient>,
  preexisting = false,
  replaceAfterCreation = false,
  setup = false,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const pathApi = yield* Path.Path;
      const root = yield* fs.realPath(
        yield* fs.makeTempDirectoryScoped({ prefix: "runtime-creation-" }),
      );
      const state = pathApi.join(root, "userdata"),
        project = pathApi.join(root, "project"),
        parent = pathApi.join(root, "worktrees"),
        path = pathApi.join(parent, "new");
      yield* fs.makeDirectory(state);
      yield* fs.makeDirectory(project);
      yield* fs.makeDirectory(parent);
      const git = yield* Git.GitVcsDriver;
      const command = (args: readonly string[]) =>
        git.execute({
          cwd: project,
          args,
          operation: "runtime-creation-fixture",
          env: { GIT_OPTIONAL_LOCKS: "0" },
        });
      yield* command(["init", "-b", "main"]);
      yield* fs.writeFileString(pathApi.join(project, "tracked.txt"), "synthetic creation\n");
      yield* command(["add", "tracked.txt"]);
      yield* command([
        "-c",
        "user.name=Runtime Creation Fixture",
        "-c",
        "user.email=runtime-creation@example.invalid",
        "commit",
        "-m",
        "synthetic initial",
      ]);
      const held = yield* Ownership.acquireServerOwnership(
        pathApi.join(state, "server-runtime.json"),
      );
      const persistence = Layer.effectDiscard(runMigrations()).pipe(
        Layer.provideMerge(
          NodeSqliteClient.layer({ filename: pathApi.join(state, "statev2.sqlite") }),
        ),
      );
      const stores = Layer.mergeAll(RepositorySqlite.layer, Projects.layer).pipe(
        Layer.provideMerge(persistence),
      );
      return yield* Effect.gen(function* () {
        const repository = yield* Repository.NativeCreationRepository;
        const secureRandom = yield* SecureRandom.SecureRandom;
        const sql = yield* SqlClient.SqlClient;
        const projects = yield* Projects.ProjectStoreV2;
        const value = yield* fixture(project, "real-creation", "Synthetic", path, setup);
        const setupScript = {
          id: "synthetic-setup",
          name: "Synthetic setup",
          command: "synthetic-only-no-shell-execution",
          icon: "configure" as const,
          runOnWorktreeCreate: true,
        };
        const installation = yield* readRuntimeUseInstallation();
        assert.isNotNull(installation);
        yield* projects.apply({
          sequence: 1,
          eventId: EventId.make("runtime-creation-project"),
          aggregateKind: "project",
          aggregateId: ProjectId.make("fixture-project"),
          occurredAt: timestamp,
          commandId: null,
          causationEventId: null,
          correlationId: null,
          metadata: {},
          type: "project.created",
          payload: {
            projectId: ProjectId.make("fixture-project"),
            title: "Synthetic creation",
            workspaceRoot: project,
            defaultModelSelection: null,
            scripts: setup ? [setupScript] : [],
            createdAt: timestamp,
            updatedAt: timestamp,
          },
        });
        yield* repository.claim(value.input, value.authorize);
        if (preexisting) yield* fs.makeDirectory(path);
        const observedGit = {
          ...git,
          createWorktree: (
            request: Parameters<typeof git.createWorktree>[0],
            options: Parameters<typeof git.createWorktree>[1],
          ) => {
            const journal = options?.legacyPreparation;
            assert.isDefined(journal);
            if (journal === undefined) return git.createWorktree(request, options);
            return git.createWorktree(request, {
              ...options,
              legacyPreparation: {
                ...journal,
                beforeEffect: (step) =>
                  Effect.gen(function* () {
                    yield* journal.beforeEffect(step);
                    if (
                      step.kind === "worktree.add" &&
                      !(yield* sql`SELECT * FROM jones_native_workspace_admissions WHERE claim_id=${value.input.claimId}`)
                        .length
                    )
                      assert.fail("Native original admission must precede worktree.add");
                    if (step.kind === "worktree.add" && !oldAdmission) {
                      const admitted = yield* readRuntimeUseCreationHistory(value.input.claimId);
                      assert.isNotNull(admitted);
                      assert.isNull(admitted?.material);
                      assert.strictEqual(
                        admitted?.admission.nativeIntentSha256,
                        digest((yield* repository.readWorkspaceClaim!(value.input.claimId)).intent),
                      );
                      assert.isFalse(yield* fs.exists(path));
                    }
                  }).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
                afterEffect: (step, result, material) =>
                  Effect.gen(function* () {
                    yield* journal.afterEffect(step, result, material);
                    if (step.kind === "worktree.add" && replaceAfterCreation) {
                      const pointer = yield* fs.readFileString(pathApi.join(path, ".git"));
                      yield* fs.rename(path, path + "-retained-original");
                      yield* fs.makeDirectory(path);
                      yield* fs.writeFileString(pathApi.join(path, ".git"), pointer);
                    }
                  }),
              },
            });
          },
        };
        let oldAdmission = false;
        const nativeAuthority = Layer.succeed(Authority.NativeCreationAuthority, {
          isAutomationEnrolled: () => Effect.succeed(true),
          authorize: () => value.authorize,
        });
        let dataListener: ((data: string) => void) | undefined;
        let ptySpawns = 0;
        let ptyExited = false;
        const exitListeners = new Set<(event: { exitCode: number; signal: number }) => void>();
        const terminal = yield* Manager.makeWithOptions({
          logsDir: pathApi.join(root, "terminal-logs"),
          shellResolver: () => "/bin/sh",
          env: {},
          processTable: Effect.succeed([]),
          ptyAdapter: {
            spawn: () =>
              Effect.gen(function* () {
                assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_projection_threads`, []);
                const pending = yield* (yield* makeRuntimeUseStore).pending();
                assert.strictEqual(pending.length, 1);
                assert.strictEqual(pending[0]?.actor.kind, "terminal_spawn");
                assert.strictEqual(pending[0]?.original.nativeReference, value.input.claimId);
                ptySpawns++;
                return {
                  pid: 987654321,
                  write: (data: string) => {
                    const token = /__T3_SETUP_DONE___(\w+):/.exec(data)?.[1];
                    assert.isDefined(token);
                    dataListener?.(`__T3_SETUP_DONE___${token}:0\r\nsynthetic> `);
                  },
                  resize: () => {},
                  kill: () => {
                    if (!ptyExited) {
                      ptyExited = true;
                      for (const listener of exitListeners) listener({ exitCode: 0, signal: 0 });
                    }
                  },
                  onData: (listener: (data: string) => void) => {
                    dataListener = listener;
                    return () => {
                      dataListener = undefined;
                    };
                  },
                  onExit: (listener: (event: { exitCode: number; signal: number }) => void) => {
                    if (ptyExited) listener({ exitCode: 0, signal: 0 });
                    else exitListeners.add(listener);
                    return () => {
                      exitListeners.delete(listener);
                    };
                  },
                };
              }).pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
                Effect.mapError(
                  (cause) =>
                    new PtyAdapter.PtySpawnError({ adapter: "synthetic-native-setup", cause }),
                ),
              ),
          },
        }).pipe(
          Effect.provideService(AgentScope, {
            wrap: ({ command, args }) => Effect.succeed({ command, args }),
            oomKilled: () => Effect.succeed(false),
            stop: () => Effect.void,
            kill: () => Effect.succeed(false),
            clear: () => Effect.void,
          }),
          Effect.provideService(HostProcess.Environment, { PATH: "/usr/bin:/bin", HOME: root }),
        );
        // Real runner readiness is driven by this simulated PTY's line-editor
        // output after the real Manager publishes its started event, not by
        // advancing quiet-fallback time or bypassing readiness.
        const unsubscribeReadiness = yield* terminal.subscribe((event) =>
          event.type === "started"
            ? Effect.sync(() => dataListener?.("\u001b[?2004hsynthetic> "))
            : Effect.void,
        );
        yield* Effect.addFinalizer(() => Effect.sync(unsubscribeReadiness));
        const terminalLayer = Layer.succeed(Manager.TerminalManager, terminal);
        const runner = Runner.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              terminalLayer,
              Layer.mock(ProjectService.ProjectService)({}),
              Layer.mock(Projection.ProjectionStoreV2)({
                getThreadShell: () => Effect.succeed(null),
              }),
              Layer.mock(Settings.ServerSettingsService)({
                getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS),
              }),
            ),
          ),
        );
        const setupExecutor = SetupExecutor.layer.pipe(
          Layer.provide(Layer.mergeAll(runner, terminalLayer, nativeAuthority)),
        );
        const physical = Physical.layer.pipe(
          Layer.provide(Layer.succeed(Git.GitVcsDriver, observedGit)),
          Layer.provide(
            Layer.mergeAll(
              Layer.succeed(Physical.NativeWorkspacePhysicalOwner, {
                assertAvailable: () => Effect.void,
                assertSetupAvailable: Effect.void,
                setup: (input) =>
                  SetupExecutor.NativeWorkspaceSetupExecutor.use((executor) =>
                    executor.run(input),
                  ).pipe(
                    Effect.provide(setupExecutor),
                    Effect.provideService(Repository.NativeCreationRepository, repository),
                    Effect.provideService(SecureRandom.SecureRandom, secureRandom),
                  ),
                readCurrent: () =>
                  Effect.succeed({
                    bootId: value.input.claimedBootId,
                    producerId: "synthetic-original-owner",
                  }),
              }),
              Layer.mock(Settings.ServerSettingsService)({
                getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS),
              }),
            ),
          ),
        );
        const preparation = Workspace.layer.pipe(
          Layer.provideMerge(physical),
          Layer.provide(nativeAuthority),
        );
        return yield* Effect.gen(function* () {
          const service = yield* Workspace.NativeCreationWorkspacePreparation;
          const ports = yield* Workspace.NativeWorkspacePorts;
          const claimId = value.input.claimId;
          const prepareOldAdmission = Effect.gen(function* () {
            oldAdmission = true;
            const history = yield* repository.readWorkspaceClaim!(claimId);
            const basis = yield* ports.inspect(history.intent);
            yield* repository.admitWorkspace!(claimId, basis);
          });
          const result = yield* test({
            prepare: service.prepare({ claimId }),
            repository,
            claimId,
            path,
            prepareOldAdmission,
            assertNoMutation: Effect.gen(function* () {
              assert.deepEqual(
                yield* sql`SELECT * FROM jones_workspace_runtime_creation_facts`,
                [],
              );
              assert.deepEqual(yield* sql`SELECT * FROM native_creation_effect_facts`, []);
            }),
          });
          assert.strictEqual(ptySpawns, setup ? 1 : 0);
          return result;
        }).pipe(Effect.provide(preparation));
      }).pipe(
        Effect.provide(stores),
        Effect.provideService(HeldRuntimeOwner, { token: held.heldRuntimeOwner }),
      );
    }).pipe(Effect.provide(platform)),
  );

it.effect(
  "actual fresh native Git creation records original pre-effect admission and first material exactly once",
  () =>
    physicalFixture((input) =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        assert.deepEqual(yield* sql`SELECT * FROM jones_workspace_runtime_creation_facts`, []);
        const verified = yield* input.prepare;
        const history = yield* readRuntimeUseCreationHistory(input.claimId);
        assert.isNotNull(history);
        assert.ok(history?.material);
        assert.ok(history && "verified" in history);
        assert.strictEqual(history?.admission.canonicalPath, input.path);
        assert.strictEqual(
          history?.admission.installationSha256,
          digest(yield* readRuntimeUseInstallation()),
        );
        assert.strictEqual(history?.material?.workspaceVerifiedSha256, digest(verified));
        assert.strictEqual(history?.material?.admissionSha256, digest(history?.admission));
        const before =
          yield* sql`SELECT * FROM jones_workspace_runtime_creation_facts ORDER BY phase`;
        assert.strictEqual(before.length, 2);
        assert.deepEqual(yield* input.prepare, verified);
        assert.deepEqual(
          yield* sql`SELECT * FROM jones_workspace_runtime_creation_facts ORDER BY phase`,
          before,
        );
        assert.deepEqual(yield* sql`SELECT * FROM orchestration_events`, []);
        assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
        assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
        assert.strictEqual(
          (yield* sql`UPDATE jones_workspace_runtime_creation_facts SET fact_json=fact_json`.pipe(
            Effect.result,
          ))._tag,
          "Failure",
        );
        assert.strictEqual(
          (yield* sql`DELETE FROM jones_workspace_runtime_creation_facts`.pipe(Effect.result))._tag,
          "Failure",
        );
      }),
    ),
);
it.effect("existing native admission never gains retroactive physical creation coverage", () =>
  physicalFixture((input) =>
    Effect.gen(function* () {
      yield* input.prepareOldAdmission;
      yield* input.prepare;
      const sql = yield* SqlClient.SqlClient;
      assert.deepEqual(yield* sql`SELECT * FROM jones_workspace_runtime_creation_facts`, []);
      assert.isNull(yield* readRuntimeUseCreationHistory(input.claimId));
      assert.isTrue(Option.isSome(yield* input.repository.readWorkspaceVerified!(input.claimId)));
    }),
  ),
);
it.effect("preexisting physical target refuses before native effect or companion writes", () =>
  physicalFixture(
    (input) =>
      Effect.gen(function* () {
        assert.strictEqual((yield* input.prepare.pipe(Effect.result))._tag, "Failure");
        yield* input.assertNoMutation;
      }),
    true,
  ),
);

it.effect(
  "replacement after original creation keeps unknown custody and never publishes a material witness or replays",
  () =>
    physicalFixture(
      (input) =>
        Effect.gen(function* () {
          assert.strictEqual((yield* input.prepare.pipe(Effect.result))._tag, "Failure");
          const sql = yield* SqlClient.SqlClient;
          const before = yield* sql`SELECT * FROM native_creation_effect_facts ORDER BY ordinal`;
          const creation = yield* readRuntimeUseCreationHistory(input.claimId);
          assert.isNotNull(creation);
          assert.isNull(creation?.material);
          assert.strictEqual((yield* input.prepare.pipe(Effect.result))._tag, "Failure");
          assert.deepEqual(
            yield* sql`SELECT * FROM native_creation_effect_facts ORDER BY ordinal`,
            before,
          );
          assert.deepEqual(yield* sql`SELECT * FROM jones_native_workspace_verified`, []);
          assert.strictEqual(
            (yield* sql`SELECT phase FROM jones_workspace_runtime_creation_facts`).length,
            1,
          );
        }),
      false,
      true,
    ),
);

it.effect(
  "actual native setup executor/runner/terminal journals a prepared start before PTY without a thread projection",
  () =>
    physicalFixture(
      (input) =>
        Effect.gen(function* () {
          const result = yield* input.prepare;
          assert.isTrue(result.setupTerminalId?.startsWith("native-setup-") === true);
          const sql = yield* SqlClient.SqlClient;
          assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_projection_threads`, []);
          const pending = yield* (yield* makeRuntimeUseStore).pending();
          assert.strictEqual(pending.length, 1);
          assert.strictEqual(pending[0]?.original.nativeReference, input.claimId);
          assert.strictEqual(
            pending[0]?.threadId,
            (yield* input.repository.readWorkspaceClaim!(input.claimId)).intent.threadId,
          );
          const original = yield* readRuntimeUseCreationHistory(input.claimId);
          assert.ok(original?.material);
          assert.isTrue(
            original?.facts.some(
              (f) => f.kind === "setup" && f.phase === "completed" && f.result === "succeeded",
            ) === true,
          );
          assert.deepEqual(yield* sql`SELECT * FROM orchestration_events`, []);
          assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
          // The simulated root PTY did not prove detached descendant retirement.
          assert.strictEqual(
            (yield* (yield* makeRuntimeUseStore).inventory())[0]?.state,
            "pending",
          );
        }),
      false,
      false,
      true,
    ),
);
