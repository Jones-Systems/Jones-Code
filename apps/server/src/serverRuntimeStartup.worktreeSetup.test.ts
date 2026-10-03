import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { NativeCreationHistoricalBinding, ProjectId, ThreadId, type Project } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as NativeCreationRepositoryLayer from "./persistence/Layers/NativeCreationRepository.ts";
import { NativeCreationRepository } from "./persistence/Services/NativeCreationRepository.ts";
import { SqlitePersistenceMemory } from "./persistence/Layers/Sqlite.ts";
import {
  NativePreparationBinding, nativeCreationCanonicalJson, nativeCreationSha256,
  nativePreparationCommand, validateNativeCreationPreparation,
} from "./orchestration-v2/NativeCreationPreparation.ts";
import * as ThreadManagement from "./orchestration-v2/ThreadManagementService.ts";
import * as ThreadLaunch from "./orchestration-v2/ThreadLaunchService.ts";
import * as ProjectService from "./project/ProjectService.ts";
import * as ServerConfig from "./config.ts";
import * as ServerSettings from "./serverSettings.ts";
import * as ServerActivation from "./serverActivation.ts";
import * as Startup from "./serverRuntimeStartup.ts";

const repositoryLayer = NativeCreationRepositoryLayer.layer.pipe(Layer.provideMerge(SqlitePersistenceMemory));
const testLayer = Layer.merge(repositoryLayer, NodeServices.layer);

it.layer(testLayer)("claimed creation startup", (it) => {
  it.effect("restart preserves claimed creation setup without command replay or forced readiness", () =>
    Effect.scoped(Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const repository = yield* NativeCreationRepository;
      const timestamp = "2026-10-02T12:34:56Z";
      const text = "Synthetic claimed setup";
      const binding = Schema.decodeUnknownSync(NativePreparationBinding)({
        backend_instance: "fixture-backend", environment_id: "fixture-environment",
        project_id: "fixture-project", project_cwd: "/fixture/project", account_ref: "fixture-account",
        runtime_mode: "full-access", interaction_mode: "default", base_branch: "main",
        start_from_origin: false, run_setup_script: true,
        provider_model_selection: { instanceId: "codex", model: "fixture-model" },
      });
      const command = nativePreparationCommand("fixture-startup-operation", binding, text, "Claimed thread", timestamp);
      const preparation = yield* validateNativeCreationPreparation(new TextEncoder().encode(nativeCreationCanonicalJson({
        schema: "voice.t3-bootstrap-preparation/v1", operation_id: "fixture-startup-operation", binding, command,
        preparation_id: command.commandId.replace("voice-command-", "voice-bootstrap-"),
        binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)), prompt_digest: nativeCreationSha256(text),
        command_digest: nativeCreationSha256(nativeCreationCanonicalJson(command)),
      })));
      const historical = Schema.decodeUnknownSync(NativeCreationHistoricalBinding)({
        backendInstance: binding.backend_instance, environmentId: binding.environment_id,
        projectId: binding.project_id, projectCwd: binding.project_cwd, accountRef: binding.account_ref,
        accountBindingId: "fixture-account-binding", accountBindingRevision: 1,
        providerModelSelection: binding.provider_model_selection, runtimeMode: binding.runtime_mode,
        interactionMode: binding.interaction_mode, baseBranch: binding.base_branch, startFromOrigin: false,
        runSetupScript: true, requestedBranch: command.bootstrap.prepareWorktree.branch,
      });
      const claim = yield* repository.claim({
        preparation, resources: { projectCwd: binding.project_cwd, branch: historical.requestedBranch, worktreePath: "/fixture/worktree" },
        claimId: "fixture-startup-claim", claimedBootId: "previous-boot", claimedAt: timestamp,
        actorSessionId: "fixture-session", grantId: "fixture-grant", grantRevision: 1,
      }, Effect.succeed(historical));
      assert.equal(claim.status, "claimed");
      const before = yield* sql`SELECT * FROM native_creation_intents WHERE claim_id = 'fixture-startup-claim'`;
      const project: Project = {
        id: ProjectId.make(binding.project_id), title: "Claimed project", workspaceRoot: binding.project_cwd,
        defaultModelSelection: null, scripts: [], createdAt: timestamp, updatedAt: timestamp, deletedAt: null,
      };
      const threadId = ThreadId.make(preparation.command.threadId);
      const callbacks: string[] = [];
      const activation = yield* Deferred.make<void>();
      const worker = yield* Ref.make<Fiber.Fiber<void, never> | null>(null);
      const bootstrap = Startup.resolveAutoBootstrapWelcomeTargets.pipe(
        Effect.provideService(ServerConfig.ServerConfig, { cwd: project.workspaceRoot, autoBootstrapProjectFromCwd: true } as never),
        Effect.provide(Layer.mock(ProjectService.ProjectService)({ bootstrap: () => Effect.succeed({ project, created: false }) })),
        Effect.provide(Layer.mock(ThreadManagement.ThreadManagementService)({
          getShellSnapshot: () => Effect.succeed({ threads: [{ id: threadId, projectId: project.id, lineage: { relationshipToParent: null } }] } as never),
        })),
        Effect.provide(Layer.mock(ThreadLaunch.ThreadLaunchService)({
          launch: () => Effect.sync(() => { callbacks.push("launch/setup"); }).pipe(Effect.andThen(Effect.die("claimed startup must not relaunch"))),
        })),
        Effect.provide(ServerSettings.layerTest()),
      );
      const result = yield* Startup.runOrderedV2StartupPhases({
        importLegacyShells: Effect.void,
        recover: Effect.void,
        startEffectWorker: Startup.startEffectWorkerWithRelay({
          runWorker: Effect.sync(() => { callbacks.push("provider-effect"); }).pipe(Effect.andThen(Effect.never)),
          startRelay: Effect.void,
          workerFiberRef: worker,
        }),
        autoBootstrap: bootstrap,
      }).pipe(Effect.provideService(ServerActivation.ServerActivation, Deferred.await(activation)));
      assert.deepEqual(result.bootstrap, { bootstrapProjectId: project.id, bootstrapThreadId: threadId });
      assert.deepEqual(callbacks, []);
      assert.isFalse(yield* Deferred.isDone(activation));
      assert.deepEqual(yield* sql`SELECT * FROM native_creation_intents WHERE claim_id = 'fixture-startup-claim'`, before);
      assert.deepEqual(yield* repository.readHistoryByClaim("fixture-startup-claim"), claim.history);
    })),
  );
});
