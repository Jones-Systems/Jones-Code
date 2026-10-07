import * as SetupCustody from "./NativeWorkspaceSetupCustody.ts";
import { assert, it } from "@effect/vitest";
import { NativeCreationHistoricalBinding, type NativeCreationEffect } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Runner from "../../project/ProjectSetupScriptRunner.ts";
import * as Terminal from "../../terminal/Manager.ts";
import type * as NativeSetup from "../../terminal/NativeSetupControl.ts";
import * as Authority from "./NativeCreationAuthority.ts";
import * as Repository from "./NativeCreationRepository.ts";
import * as Executor from "./NativeWorkspaceSetupExecutor.ts";
import {
  NativePreparationBinding,
  nativeCreationCanonicalJson as canonical,
  nativeCreationSha256 as digest,
  nativePreparationCommand,
  validateNativeCreationPreparation,
} from "./NativeCreationPreparation.ts";

const fixture = Effect.gen(function* () {
  const timestamp = "2026-10-07T12:00:00Z";
  const binding = yield* Schema.decodeEffect(NativePreparationBinding)({
    backend_instance: "fixture-backend",
    environment_id: "fixture-environment",
    project_id: "fixture-project",
    project_cwd: "/fixture/project",
    account_ref: "fixture-account",
    runtime_mode: "full-access",
    interaction_mode: "default",
    base_branch: "main",
    start_from_origin: false,
    run_setup_script: true,
    provider_model_selection: { instanceId: "codex", model: "fixture-model" },
  });
  const command = nativePreparationCommand(
    "fixture-setup-operation",
    binding,
    "Synthetic prompt",
    "Synthetic setup",
    timestamp,
  );
  const preparation = yield* validateNativeCreationPreparation(
    new TextEncoder().encode(
      canonical({
        schema: "voice.t3-bootstrap-preparation/v1",
        operation_id: "fixture-setup-operation",
        binding,
        command,
        preparation_id: command.commandId.replace("voice-command-", "voice-bootstrap-"),
        binding_digest: digest(canonical(binding)),
        prompt_digest: digest("Synthetic prompt"),
        command_digest: digest(canonical(command)),
      }),
    ),
  );
  const historical = yield* Schema.decodeEffect(NativeCreationHistoricalBinding)({
    backendInstance: binding.backend_instance,
    environmentId: binding.environment_id,
    projectId: binding.project_id,
    projectCwd: binding.project_cwd,
    accountRef: binding.account_ref,
    accountBindingId: "fixture-account-binding",
    accountBindingRevision: 1,
    providerModelSelection: binding.provider_model_selection,
    runtimeMode: binding.runtime_mode,
    interactionMode: binding.interaction_mode,
    baseBranch: binding.base_branch,
    startFromOrigin: false,
    runSetupScript: true,
    requestedBranch: command.bootstrap.prepareWorktree.branch,
  });
  const claim: Repository.NativeCreationStoredIntent = {
    claimId: "fixture-setup-claim",
    claimedBootId: "fixture-boot",
    claimedAt: timestamp,
    actorSessionId: "fixture-actor",
    grantId: "fixture-grant",
    grantRevision: 1,
    preparationId: preparation.preparationId,
    operationId: preparation.operationId,
    preparationSha256: preparation.preparationSha256,
    bindingDigest: preparation.bindingDigest,
    promptDigest: preparation.promptDigest,
    commandDigest: preparation.commandDigest,
    commandId: command.commandId,
    threadId: command.threadId,
    messageId: command.message.messageId,
    canonicalPreparation: preparation.canonicalText,
    binding: historical,
    resources: {
      projectCwd: binding.project_cwd,
      worktreePath: "/fixture/worktrees/setup",
      branch: historical.requestedBranch,
    },
  };
  const script = {
    id: "fixture-script",
    name: "Synthetic setup",
    command: "synthetic-setup",
    icon: "configure" as const,
    runOnWorktreeCreate: true,
  };
  const basis = {
    bootId: claim.claimedBootId,
    projectId: historical.projectId,
    projectCwd: claim.resources.projectCwd,
    projectBirth: "fixture-project-birth",
    gitCommonDirectory: "/fixture/project/.git",
    physicalGitIdentity: "fixture-git-birth",
    worktreePath: claim.resources.worktreePath,
    parentBirth: "fixture-parent-birth",
    producerId: "fixture-producer",
    baseRef: "main",
    setupDefinition: canonical(script),
    configuredSubmodulesDefinition: "fixture-submodules",
    baseConfigurationDefinition: "fixture-config",
  };
  const proof = {
    worktreePath: basis.worktreePath,
    pathBirth: "fixture-target-birth",
    gitCommonDirectory: basis.gitCommonDirectory,
    physicalGitIdentity: basis.physicalGitIdentity,
    branch: claim.resources.branch,
    baseRef: "main",
    configuredSubmodulesDigest: digest(basis.configuredSubmodulesDefinition),
    baseConfigurationDigest: digest(basis.baseConfigurationDefinition),
  };
  return { claim, basis, proof, script, revalidate: Effect.void };
});

const scenario = Effect.fnUntraced(function* (
  options: {
    missingCustody?: boolean;
    missingStart?: boolean;
    revokeAfterSpawn?: boolean;
    completed?: boolean;
    exitCode?: number | null;
  } = {},
) {
  const input = yield* fixture;
  const calls: string[] = [];
  const effectId = digest(
    canonical({ claimId: input.claim.claimId, stage: "setup", basis: input.basis }),
  );
  const started: NativeCreationEffect = {
    ordinal: 1,
    effectId,
    timestamp: "2026-10-07T12:00:00Z",
    kind: "setup",
    phase: "started",
    worktreePath: input.proof.worktreePath,
    terminalId: `native-setup-${digest(effectId)}`,
  };
  const completed: NativeCreationEffect = {
    ...started,
    phase: "completed",
    exitCode: null,
    result: "unknown",
  };
  const worktree: NativeCreationEffect = {
    ordinal: 0,
    effectId: "fixture-worktree-effect",
    timestamp: "2026-10-07T12:00:00Z",
    kind: "worktree",
    phase: "completed",
    result: "succeeded",
    ownership: "created",
    worktreePath: input.proof.worktreePath,
    projectCwd: input.basis.projectCwd,
    branch: input.proof.branch,
    baseRef: input.basis.baseRef,
  };
  const effects = options.missingStart
    ? [worktree]
    : options.completed
      ? [worktree, started, completed]
      : [worktree, started];
  let revoked = false;
  let observation: Omit<NativeSetup.NativeSetupObservation, "control"> = {
    status: "unknown",
    pid: null,
    writeEntered: false,
    exitCode: null,
  };
  const terminalLayer = Layer.mock(Terminal.TerminalManager)({
    openNativeSetup: () => Effect.die("runner fixture owns the simulated open"),
    writeNativeSetup: () => Effect.die("runner fixture owns the simulated write"),
    observeNativeSetup: (control) => Effect.succeed({ ...observation, control }),
  });
  const layer = Executor.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(Repository.NativeCreationRepository)({
          readWorkspaceClaim: () =>
            Effect.succeed({ intent: input.claim, normalizedCommandDigest: null, effects }),
          readWorkspaceAdmission: () => Effect.succeed(input.basis),
        }),
        Layer.mock(Authority.NativeCreationAuthority)({
          authorize: () =>
            revoked
              ? Effect.fail(
                  new Authority.NativeCreationAuthorityError({
                    code: "unsupported_authority",
                    message: "Synthetic revocation",
                  }),
                )
              : Effect.succeed(input.claim.binding),
        }),
        terminalLayer,
        Layer.mock(Runner.ProjectSetupScriptRunner)({
          runForThread: (request) =>
            Effect.gen(function* () {
              calls.push("runner");
              const native = request.nativePreparation!;
              const plan = {
                control: native.control,
                shell: "/bin/synthetic-shell",
                shellArgs: [],
                cwd: input.proof.worktreePath,
                script: input.script,
                commandLine: "synthetic-wrapper",
                completionToken: "synthetic-token",
              };
              yield* native.beforeSpawn(plan);
              calls.push("spawn");
              observation = { status: "running", pid: 123, writeEntered: false, exitCode: null };
              yield* native.afterSpawn({
                control: native.control,
                shell: plan.shell,
                shellArgs: plan.shellArgs,
                cwd: plan.cwd,
                pid: 123,
              });
              revoked = options.revokeAfterSpawn === true;
              yield* native.beforeWrite({ control: native.control, data: `${plan.commandLine}\r` });
              calls.push("write");
              observation = { ...observation, writeEntered: true };
              const exitCode = options.exitCode === undefined ? 0 : options.exitCode;
              const completion = Effect.gen(function* () {
                const result = { exitCode, durationMs: 1 };
                yield* native.afterCompletion(result);
                return result;
              }).pipe(
                Effect.mapError(
                  (cause) =>
                    new Runner.ProjectSetupScriptOperationError({
                      threadId: request.threadId,
                      worktreePath: request.worktreePath,
                      operation: "writeCommand",
                      cause,
                    }),
                ),
              );
              return {
                status: "started" as const,
                scriptId: input.script.id,
                scriptName: input.script.name,
                scriptCommand: input.script.command,
                terminalId: native.control.terminalId,
                cwd: request.worktreePath,
                async: true,
                completion,
              };
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new Runner.ProjectSetupScriptOperationError({
                    threadId: request.threadId,
                    worktreePath: request.worktreePath,
                    operation: "openTerminal",
                    cause,
                  }),
              ),
            ),
        }),
      ),
    ),
  );
  const custody = options.missingCustody
    ? undefined
    : SetupCustody.issue(input.claim, input.basis, started);
  const run = Effect.flatMap(Executor.NativeWorkspaceSetupExecutor, (executor) =>
    executor.run({ ...input, ...(custody === undefined ? {} : { custody }) }),
  ).pipe(Effect.provide(layer));
  return { calls, run };
});

it.effect.each([{ missingStart: true }, { completed: true }, { missingCustody: true }])(
  "native setup refuses absent or completed original durable start %#",
  (options) =>
    Effect.gen(function* () {
      const value = yield* scenario(options);
      const result = yield* value.run.pipe(Effect.result);
      assert.equal(result._tag, "Failure");
      assert.deepEqual(value.calls, []);
    }),
);
it.effect(
  "native setup revalidates authority after captured spawn and refuses the write on revocation",
  () =>
    Effect.gen(function* () {
      const value = yield* scenario({ revokeAfterSpawn: true });
      const result = yield* value.run.pipe(Effect.result);
      assert.equal(result._tag, "Failure");
      assert.deepEqual(value.calls, ["runner", "spawn"]);
    }),
);
it.effect("native setup completion needs exact retained terminal evidence", () =>
  Effect.gen(function* () {
    const value = yield* scenario();
    const result = yield* value.run;
    assert.equal(yield* result.completion, 0);
    assert.isTrue(result.terminalId.startsWith("native-setup-"));
    assert.deepEqual(value.calls, ["runner", "spawn", "write"]);
  }),
);
it.effect("native setup null completion remains unknown", () =>
  Effect.gen(function* () {
    const value = yield* scenario({ exitCode: null });
    const result = yield* value.run;
    assert.equal((yield* result.completion.pipe(Effect.result))._tag, "Failure");
    assert.deepEqual(value.calls, ["runner", "spawn", "write"]);
  }),
);

it.effect("native setup consumed fresh custody cannot replay its durable start", () =>
  Effect.gen(function* () {
    const value = yield* scenario();
    yield* value.run;
    const replay = yield* value.run.pipe(Effect.result);
    assert.equal(replay._tag, "Failure");
    assert.deepEqual(value.calls, ["runner", "spawn", "write"]);
  }),
);
