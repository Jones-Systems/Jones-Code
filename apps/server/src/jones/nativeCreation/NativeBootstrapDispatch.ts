import * as Path from "effect/Path";
import {
  CommandId,
  EnvironmentAuthenticatedPrincipal,
  MessageId,
  OrchestrationV2Command,
  ThreadId,
  type NativeBootstrapDispatchResultV2,
  type RunId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Authority from "./NativeCreationAuthority.ts";
import * as Repository from "./NativeCreationRepository.ts";
import * as Workspace from "./NativeCreationWorkspacePreparation.ts";
import * as Provider from "./NativeCreationProviderExecution.ts";
import * as Stage from "./NativeCreationStageDispatch.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import { decodeNativeBootstrapSubmission } from "./NativeCreationPreparation.ts";

export type NativeBootstrapDispatchResult = NativeBootstrapDispatchResultV2;
const denied = (code: Authority.NativeCreationAuthorityError["code"], message: string) =>
  new Authority.NativeCreationAuthorityError({ code, message });

export const dispatchNativeBootstrap = Effect.fn("dispatchNativeBootstrap")(
  function* (
    submission: unknown,
    server: {
      readonly bootId: string;
      readonly worktreesDir: string;
      readonly deriveRunId: (threadId: ThreadId) => RunId;
    },
  ) {
    const path = yield* Path.Path;
    const { preparation, guard } = yield* decodeNativeBootstrapSubmission(submission);
    const principal = yield* EnvironmentAuthenticatedPrincipal;
    const authority = yield* Effect.serviceOption(Authority.NativeCreationAuthority);
    const repository = yield* Effect.serviceOption(Repository.NativeCreationRepository);
    const workspace = yield* Effect.serviceOption(Workspace.NativeCreationWorkspacePreparation);
    const workspacePorts = yield* Effect.serviceOption(Workspace.NativeWorkspacePorts);
    const executor = yield* Effect.serviceOption(Provider.NativeCreationProviderExecutor);
    const orchestrator = yield* Effect.serviceOption(Orchestrator.OrchestratorV2);
    if (
      Option.isNone(authority) ||
      Option.isNone(repository) ||
      Option.isNone(workspace) ||
      Option.isNone(workspacePorts) ||
      Option.isNone(executor) ||
      Option.isNone(orchestrator) ||
      !orchestrator.value.dispatchNativeCreationStage ||
      !repository.value.isReservedCommandIdentity ||
      !repository.value.assertExecutionCapability ||
      !repository.value.reserveExecutionCommandIdentities ||
      !repository.value.readExecutionReference ||
      !repository.value.recordExecutionAcceptance ||
      !repository.value.readWorkspaceClaim ||
      !repository.value.readWorkspaceVerified ||
      !authority.value.issueExecution ||
      !repository.value.confirmExecution ||
      !repository.value.holdExecution ||
      !repository.value.startEffectV2 ||
      !repository.value.admitWorkspace ||
      !repository.value.recordWorkspaceVerified ||
      !workspacePorts.value.assertAvailable ||
      !executor.value.assertAvailable ||
      server.bootId.length === 0
    )
      return yield* denied(
        "unsupported_authority",
        "Native bootstrap owners and whole-operation execution are unavailable",
      );
    if (!principal.scopes.has("orchestration:operate"))
      return yield* denied("stale_grant", "Verified native principal cannot operate orchestration");
    const owner = repository.value;
    const original = preparation.command;
    const commandId = yield* Schema.decodeUnknownEffect(CommandId)(original.commandId);
    const threadId = yield* Schema.decodeUnknownEffect(ThreadId)(original.threadId);
    const messageId = yield* Schema.decodeUnknownEffect(MessageId)(original.message.messageId);
    if (
      commandId !== original.commandId ||
      threadId !== original.threadId ||
      messageId !== original.message.messageId
    )
      return yield* denied("invalid_preparation", "Native identities cannot be normalized");
    const branch = original.bootstrap.prepareWorktree.branch;
    const resources = Object.freeze({
      projectCwd: preparation.binding.project_cwd,
      branch,
      worktreePath: path.join(
        server.worktreesDir,
        path.basename(preparation.binding.project_cwd),
        branch.replaceAll("/", "-"),
      ),
    });
    const authorization = { actorSessionId: principal.sessionId, preparation, guard, resources };
    const initial = original.bootstrap.createThread;
    const decode = Schema.decodeUnknownEffect(OrchestrationV2Command);
    const create = yield* decode(
      {
        type: "thread.create",
        commandId: `${commandId}:native:v2:create`,
        threadId,
        projectId: initial.projectId,
        title: initial.title,
        modelSelection: initial.modelSelection,
        runtimeMode: initial.runtimeMode,
        interactionMode: initial.interactionMode,
        branch,
        worktreePath: resources.worktreePath,
        createdBy: "user",
        creationSource: "server",
      },
      { onExcessProperty: "error" },
    );
    const message = yield* decode(
      {
        type: "message.dispatch",
        commandId: `${commandId}:native:v2:message`,
        threadId,
        messageId,
        text: original.message.text,
        attachments: [],
        modelSelection: initial.modelSelection,
        dispatchMode: { type: "defer_start" },
        createdBy: "user",
        creationSource: "server",
      },
      { onExcessProperty: "error" },
    );
    const release = yield* decode(
      { type: "prepared-run.release", commandId, threadId, runId: server.deriveRunId(threadId) },
      { onExcessProperty: "error" },
    );
    yield* workspacePorts.value.assertAvailable!({ ...authorization, stage: "claim" }).pipe(
      Effect.mapError(() =>
        denied(
          "unsupported_authority",
          "Native workspace owner is unavailable for the exact target",
        ),
      ),
    );
    yield* executor.value.assertAvailable!({ ...authorization, stage: "claim" }).pipe(
      Effect.mapError(() =>
        denied(
          "unsupported_authority",
          "Whole-operation provider executor is unavailable for the exact target",
        ),
      ),
    );
    yield* owner.assertExecutionCapability!;
    yield* authority.value.authorize({ ...authorization, stage: "claim" });
    const claimed = yield* owner.claim(
      {
        preparation,
        resources,
        actorSessionId: principal.sessionId,
        claimId: `native:v2:${commandId}`,
        claimedBootId: server.bootId,
        claimedAt: DateTime.formatIso(yield* DateTime.now),
        grantId: guard.grantId,
        grantRevision: guard.grantRevision,
      },
      authority.value.authorize({ ...authorization, stage: "claim" }),
    );
    const claimId = claimed.history.intent.claimId;
    if (
      claimed.history.intent.grantId !== guard.grantId ||
      claimed.history.intent.grantRevision !== guard.grantRevision ||
      claimed.history.intent.actorSessionId !== principal.sessionId
    )
      return yield* denied(
        "conflict",
        "Native retry cannot replace its original principal or grant",
      );
    const result: NativeBootstrapDispatchResult = {
      version: 2,
      commandId,
      threadId,
      messageId,
      commandAcceptance: "accepted",
    };
    if (claimed.status === "duplicate") {
      // A retry only observes the original final acceptance. It never restarts uncertain preparation.
      yield* owner.readExecutionReference!({
        version: 2,
        claimId,
        stageCommandId: commandId,
        effectId: `native-stage:${commandId}`,
        stage: "native_command",
      });
      return result;
    }
    yield* authority.value.authorize({ ...authorization, stage: "normalization" });
    yield* owner.reserveExecutionCommandIdentities!(claimId, [
      create.commandId,
      message.commandId,
      commandId,
    ]);
    for (const command of [create, message, release]) yield* owner.reserveCommand(claimId, command);
    yield* owner.recordNormalizedCommand(claimId, release);
    // Workspace101 owns preparation and uncertain cleanup. No ordinary launch or deletion fallback is permitted.
    const verified = yield* workspace.value.prepare({ claimId });
    if (
      verified.claimId !== claimId ||
      verified.proof.worktreePath !== resources.worktreePath ||
      verified.proof.branch !== branch
    )
      return yield* denied(
        "binding_mismatch",
        "Workspace owner did not verify the original native target",
      );
    for (const command of [create, message, release]) {
      const lineage = yield* Stage.issueNativeCreationStage(claimId, command).pipe(
        Effect.provideService(Repository.NativeCreationRepository, owner),
        Effect.provideService(Authority.NativeCreationAuthority, authority.value),
      );
      yield* orchestrator.value.dispatchNativeCreationStage!(command, lineage);
    }
    return result;
  },
  Effect.mapError((cause) =>
    Schema.is(Authority.NativeCreationAuthorityError)(cause)
      ? cause
      : denied(
          "unresolved_claim",
          "Native bootstrap was rejected; observe its original durable claim",
        ),
  ),
);
