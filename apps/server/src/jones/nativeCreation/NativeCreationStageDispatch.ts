import {
  AuthSessionId,
  CommandId,
  NativeCreationGuard,
  type OrchestrationV2Command,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Authority from "./NativeCreationAuthority.ts";
import * as Repository from "./NativeCreationRepository.ts";
import {
  nativeCreationCanonicalJson,
  validateNativeCreationPreparation,
} from "./NativeCreationPreparation.ts";

const privateStage = Symbol("NativeCreationStageDispatch");
export interface NativeCreationStageDispatch {
  readonly [privateStage]: true;
}
const stages = new WeakMap<
  NativeCreationStageDispatch,
  {
    readonly claimId: string;
    readonly command: OrchestrationV2Command;
  }
>();
const denied = (message: string) =>
  new Authority.NativeCreationAuthorityError({
    code: "unresolved_claim",
    message,
  });

// Only an exact current-owner reservation can issue lineage; wire objects cannot supply it.
export const issueNativeCreationStage = Effect.fn("issueNativeCreationStage")(function* (
  claimId: string,
  command: OrchestrationV2Command,
) {
  const identity: NativeCreationStageDispatch = { [privateStage]: true };
  const token = Object.freeze(identity);
  stages.set(token, { claimId, command: structuredClone(command) });
  yield* validateNativeCreationStage(token, command);
  return token;
});

export const validateNativeCreationStage = Effect.fn("validateNativeCreationStage")(
  function* (token: NativeCreationStageDispatch, command: OrchestrationV2Command) {
    const lineage = stages.get(token);
    if (
      !lineage ||
      nativeCreationCanonicalJson(lineage.command) !== nativeCreationCanonicalJson(command)
    )
      return yield* denied("Native stage lineage does not match the exact command");
    const repository = yield* Repository.NativeCreationRepository;
    const authority = yield* Authority.NativeCreationAuthority;
    if (
      !repository.readWorkspaceClaim ||
      !repository.isReservedCommandIdentity ||
      !repository.readExecutionReference ||
      !repository.readWorkspaceVerified
    )
      return yield* denied("Native stage owner readers are unavailable");
    const history = yield* repository.readWorkspaceClaim(lineage.claimId);
    const intent = history.intent;
    const verified = yield* repository.readWorkspaceVerified(lineage.claimId);
    if (
      Option.isNone(verified) ||
      verified.value.claimId !== lineage.claimId ||
      verified.value.proof.worktreePath !== intent.resources.worktreePath ||
      verified.value.proof.branch !== intent.resources.branch ||
      verified.value.basis.projectCwd !== intent.resources.projectCwd
    )
      return yield* denied("Native stage lacks the original verified workspace owner");
    const original = intent.commandId;
    const ids = [`${original}:native:v2:create`, `${original}:native:v2:message`, original];
    const index = ids.indexOf(command.commandId);
    if (
      index < 0 ||
      !["thread.create", "message.dispatch", "prepared-run.release"].includes(command.type) ||
      !("threadId" in command) ||
      command.threadId !== intent.threadId ||
      !(yield* repository.isReservedCommandIdentity(command.commandId))
    )
      return yield* denied("Native stage differs from its permanent identity inventory");
    const reserved = yield* repository.getReservedCommand(command.commandId);
    if (
      Option.isNone(reserved) ||
      reserved.value.claimId !== lineage.claimId ||
      reserved.value.canonicalCommand !== nativeCreationCanonicalJson(command)
    )
      return yield* denied("Native stage differs from its immutable body reservation");
    const preparation = yield* validateNativeCreationPreparation(
      new TextEncoder().encode(intent.canonicalPreparation),
    );
    const actorSessionId = yield* Schema.decodeUnknownEffect(AuthSessionId)(intent.actorSessionId);
    const guard = yield* Schema.decodeUnknownEffect(NativeCreationGuard)({
      schema: "t3.native-creation-guard/v1",
      grantId: intent.grantId,
      grantRevision: intent.grantRevision,
    });
    yield* authority.authorize({
      actorSessionId,
      preparation,
      guard,
      resources: intent.resources,
      stage: "native_command",
    });
    // Predecessors must have native acceptance, not just ordinary command receipts.
    for (const stageCommandId of ids.slice(0, index))
      yield* repository.readExecutionReference({
        version: 2,
        claimId: lineage.claimId,
        stageCommandId: yield* Schema.decodeUnknownEffect(CommandId)(stageCommandId),
        effectId: `native-stage:${stageCommandId}`,
        stage: "native_command",
      });
    return { claimId: lineage.claimId, command };
  },
  Effect.mapError((cause) =>
    Schema.is(Authority.NativeCreationAuthorityError)(cause)
      ? cause
      : denied("Native stage authority or ordered acceptance is unavailable"),
  ),
);

export const refusePublicNativeReservation = Effect.fn("refusePublicNativeReservation")(function* (
  commandId: string,
) {
  const owner = yield* Effect.serviceOption(Repository.NativeCreationRepository);
  if (Option.isNone(owner)) return;
  if (!owner.value.isReservedCommandIdentity)
    return yield* denied("Installed native owner cannot verify public command custody");
  if (yield* owner.value.isReservedCommandIdentity(commandId))
    return yield* denied("Reserved native command requires private dispatch lineage");
});
