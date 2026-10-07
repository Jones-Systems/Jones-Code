import * as SetupCustody from "./NativeWorkspaceSetupCustody.ts";
import { AuthSessionId, NativeCreationGuard, ProjectId, ProjectScript } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Runner from "../../project/ProjectSetupScriptRunner.ts";
import * as Terminal from "../../terminal/Manager.ts";
import * as NativeSetup from "../../terminal/NativeSetupControl.ts";
import * as Authority from "./NativeCreationAuthority.ts";
import * as Repository from "./NativeCreationRepository.ts";
import * as Workspace from "./NativeCreationWorkspacePreparation.ts";
import type * as Physical from "./NativeWorkspacePhysicalPorts.ts";
import {
  nativeCreationCanonicalJson as canonical,
  nativeCreationSha256 as digest,
  validateNativeCreationPreparation,
} from "./NativeCreationPreparation.ts";

type SetupInput = Parameters<
  NonNullable<Physical.NativeWorkspacePhysicalOwner["Service"]["setup"]>
>[0];
const rejected = (code: Workspace.NativeWorkspaceError["code"], message: string) =>
  new Workspace.NativeWorkspaceError({ code, message });
export class NativeWorkspaceSetupExecutor extends Context.Service<
  NativeWorkspaceSetupExecutor,
  {
    readonly assertAvailable: Effect.Effect<void, Workspace.NativeWorkspaceError>;
    readonly run: (input: SetupInput) => Effect.Effect<
      {
        readonly terminalId: string;
        readonly completion: Effect.Effect<number, Workspace.NativeWorkspaceError>;
      },
      Workspace.NativeWorkspaceError
    >;
  }
>()("t3/jones/nativeCreation/NativeWorkspaceSetupExecutor") {}

const make = Effect.gen(function* () {
  const repository = yield* Repository.NativeCreationRepository;
  const authority = yield* Authority.NativeCreationAuthority;
  const runner = yield* Runner.ProjectSetupScriptRunner;
  const terminal = yield* Terminal.TerminalManager;
  const assertAvailable = Effect.gen(function* () {
    if (
      !repository.readWorkspaceClaim ||
      !repository.readWorkspaceAdmission ||
      !terminal.openNativeSetup ||
      !terminal.writeNativeSetup ||
      !terminal.observeNativeSetup
    )
      return yield* rejected("unavailable", "Native setup original terminal owner is unavailable");
  });
  const run = (input: SetupInput) =>
    Effect.gen(function* () {
      yield* assertAvailable;
      const ownedStart = yield* SetupCustody.consume(input.custody, input.claim, input.basis);
      const effectId = digest(
        canonical({ claimId: input.claim.claimId, stage: "setup", basis: input.basis }),
      );
      if (
        ownedStart.effectId !== effectId ||
        ownedStart.terminalId !== `native-setup-${digest(effectId)}` ||
        ownedStart.worktreePath !== input.proof.worktreePath
      )
        return yield* rejected(
          "conflict",
          "Native setup custody differs from its original reserved start",
        );
      const preparation = yield* validateNativeCreationPreparation(
        new TextEncoder().encode(input.claim.canonicalPreparation),
      );
      const actorSessionId = yield* Schema.decodeUnknownEffect(AuthSessionId)(
        input.claim.actorSessionId,
      );
      const guard = yield* Schema.decodeUnknownEffect(NativeCreationGuard)({
        schema: "t3.native-creation-guard/v1",
        grantId: input.claim.grantId,
        grantRevision: input.claim.grantRevision,
      });
      const script = yield* Schema.decodeUnknownEffect(ProjectScript)(input.script);
      if (
        !input.claim.binding.runSetupScript ||
        canonical(script) !== input.basis.setupDefinition ||
        input.basis.bootId !== input.claim.claimedBootId ||
        input.proof.worktreePath !== input.claim.resources.worktreePath ||
        input.proof.branch !== input.claim.resources.branch
      )
        return yield* rejected("conflict", "Native setup capture differs from its immutable claim");
      const control: NativeSetup.NativeSetupControl = {
        claimId: input.claim.claimId,
        effectId,
        bootId: input.basis.bootId,
        producerId: input.basis.producerId,
        threadId: input.claim.threadId,
        terminalId: `native-setup-${digest(effectId)}`,
        generation: digest(
          canonical({ claimId: input.claim.claimId, effectId, bootId: input.basis.bootId }),
        ),
        projectCwd: input.basis.projectCwd,
        worktreePath: input.proof.worktreePath,
        definitionDigest: digest(canonical(script)),
      };
      const revalidate = Effect.gen(function* () {
        const history = yield* repository.readWorkspaceClaim!(input.claim.claimId);
        const admission = yield* repository.readWorkspaceAdmission!(input.claim.claimId);
        const starts = history.effects.filter(
          (fact) => fact.kind === "setup" && fact.phase === "started",
        );
        const worktrees = history.effects.filter(
          (fact) => fact.kind === "worktree" && fact.phase === "completed",
        );
        const worktree = worktrees[0];
        if (
          worktrees.length !== 1 ||
          worktree === undefined ||
          worktree.kind !== "worktree" ||
          worktree.phase !== "completed" ||
          worktree.result !== "succeeded" ||
          worktree.ownership !== "created" ||
          worktree.worktreePath !== input.proof.worktreePath ||
          worktree.projectCwd !== input.basis.projectCwd ||
          worktree.branch !== input.proof.branch ||
          worktree.baseRef !== input.basis.baseRef
        )
          return yield* rejected(
            "conflict",
            "Native setup has no complete original worktree ownership receipt",
          );
        if (
          canonical(history.intent) !== canonical(input.claim) ||
          canonical(admission) !== canonical(input.basis) ||
          starts.length !== 1 ||
          canonical(starts[0]) !== canonical(ownedStart) ||
          worktree.ordinal >= ownedStart.ordinal ||
          starts[0]!.effectId !== effectId ||
          starts[0]!.kind !== "setup" ||
          starts[0]!.worktreePath !== input.proof.worktreePath ||
          (starts[0]!.terminalId !== null && starts[0]!.terminalId !== control.terminalId) ||
          history.effects.some(
            (fact) =>
              fact.kind === "cleanup" || (fact.kind === "setup" && fact.phase === "completed"),
          )
        )
          return yield* rejected(
            "conflict",
            "Native setup durable start is absent, changed or already completed",
          );
        yield* authority.authorize({
          actorSessionId,
          guard,
          preparation,
          resources: input.claim.resources,
          stage: "setup",
        });
        yield* input.revalidate;
      }).pipe(
        Effect.mapError(() =>
          rejected("conflict", "Native setup current authority or durable start changed"),
        ),
      );
      yield* revalidate;
      // Fresh custody is consumed once; retained observations never authorize another start.
      const prior = yield* terminal.observeNativeSetup!(control);
      if (prior.status !== "unknown" || prior.pid !== null || prior.writeEntered)
        return yield* rejected("unknown", "Native setup terminal already has an owned start");
      let captured: Parameters<Runner.NativeSetupPreparationHooks["beforeSpawn"]>[0] | undefined;
      let spawned: NativeSetup.NativeSetupSpawnProof | undefined;
      const result = yield* runner.runForThread({
        threadId: input.claim.threadId,
        projectId: input.claim.binding.projectId,
        projectCwd: input.basis.projectCwd,
        worktreePath: input.proof.worktreePath,
        preferredTerminalId: control.terminalId,
        project: {
          id: ProjectId.make(input.claim.binding.projectId),
          workspaceRoot: input.basis.projectCwd,
          scripts: [script],
        },
        observeCompletion: {},
        nativePreparation: {
          control,
          script,
          beforeSpawn: (plan) =>
            Effect.gen(function* () {
              yield* revalidate;
              if (
                captured !== undefined ||
                canonical(plan.control) !== canonical(control) ||
                canonical(plan.script) !== canonical(script) ||
                plan.cwd !== control.worktreePath ||
                !plan.completionToken ||
                !plan.commandLine
              )
                return yield* rejected(
                  "conflict",
                  "Native setup shell plan differs from its captured command",
                );
              captured = plan;
            }),
          afterSpawn: (proof) =>
            Effect.gen(function* () {
              if (
                captured === undefined ||
                spawned !== undefined ||
                canonical(proof.control) !== canonical(control) ||
                proof.shell !== captured.shell ||
                canonical(proof.shellArgs) !== canonical(captured.shellArgs) ||
                proof.cwd !== control.worktreePath ||
                !Number.isInteger(proof.pid) ||
                proof.pid <= 0
              )
                return yield* rejected(
                  "unknown",
                  "Native setup process did not prove its captured spawn",
                );
              spawned = proof;
            }),
          beforeWrite: (write) =>
            Effect.gen(function* () {
              yield* revalidate;
              if (
                captured === undefined ||
                spawned === undefined ||
                canonical(write.control) !== canonical(control) ||
                write.data !== `${captured.commandLine}\r`
              )
                return yield* rejected(
                  "conflict",
                  "Native setup write differs from its captured shell command",
                );
            }),
          afterCompletion: (completion) =>
            Effect.gen(function* () {
              const observed = yield* terminal.observeNativeSetup!(control);
              if (
                spawned === undefined ||
                observed.pid !== spawned.pid ||
                !observed.writeEntered ||
                observed.status === "unknown" ||
                completion.exitCode === null
              )
                return yield* rejected(
                  "unknown",
                  "Native setup completion has no exact retained terminal attribution",
                );
            }),
        },
      });
      if (
        result.status !== "started" ||
        result.terminalId !== control.terminalId ||
        result.cwd !== control.worktreePath ||
        result.completion === undefined ||
        spawned === undefined
      )
        return yield* rejected(
          "unknown",
          "Native setup original owner did not retain its started terminal",
        );
      return {
        terminalId: result.terminalId,
        completion: result.completion.pipe(
          Effect.flatMap((completion) =>
            completion.exitCode === null
              ? Effect.fail(rejected("unknown", "Native setup completion is unknown"))
              : Effect.succeed(completion.exitCode),
          ),
          Effect.mapError(() =>
            rejected("unknown", "Native setup completion requires original-owner observation"),
          ),
        ),
      };
    }).pipe(
      Effect.mapError((cause) =>
        Schema.is(Workspace.NativeWorkspaceError)(cause)
          ? cause
          : rejected("unknown", "Native setup start requires original-owner observation"),
      ),
    );
  return NativeWorkspaceSetupExecutor.of({ assertAvailable, run });
});
export const layer = Layer.effect(NativeWorkspaceSetupExecutor, make);
