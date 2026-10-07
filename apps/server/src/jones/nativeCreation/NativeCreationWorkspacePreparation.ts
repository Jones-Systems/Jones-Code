import type {
  NativeWorkspaceBasis,
  NativeWorkspaceProof,
  NativeWorkspaceVerified,
} from "./NativeCreationWorkspaceTypes.ts";
import { AuthSessionId, NativeCreationGuard } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Authority from "./NativeCreationAuthority.ts";
import * as Repository from "./NativeCreationRepository.ts";
import {
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  validateNativeCreationPreparation,
} from "./NativeCreationPreparation.ts";

export class NativeWorkspaceError extends Schema.TaggedError<NativeWorkspaceError>()(
  "NativeWorkspaceError",
  {
    code: Schema.Literals(["unavailable", "conflict", "failed", "unknown"]),
    message: Schema.String,
  },
) {}
export interface NativeWorkspaceInput {
  readonly claimId: string;
}
const denied = (code: NativeWorkspaceError["code"], message: string) =>
  new NativeWorkspaceError({ code, message });

// A qualified producer reads canonical paths using lstat and physical Git registration.
// Every mutation, including submodules and base configuration, invokes revalidate first.
export class NativeWorkspacePorts extends Context.Service<
  NativeWorkspacePorts,
  {
    readonly inspect: (
      claim: Repository.NativeCreationStoredIntent,
      expected?: NativeWorkspaceProof,
    ) => Effect.Effect<NativeWorkspaceBasis, NativeWorkspaceError>;
    readonly fetch: (
      basis: NativeWorkspaceBasis,
      revalidate: Effect.Effect<void, NativeWorkspaceError>,
    ) => Effect.Effect<void, NativeWorkspaceError>;
    readonly createWorktree: (
      basis: NativeWorkspaceBasis,
      branch: string,
      revalidate: Effect.Effect<void, NativeWorkspaceError>,
    ) => Effect.Effect<NativeWorkspaceProof, NativeWorkspaceError>;
    readonly verify: (
      basis: NativeWorkspaceBasis,
      proof: NativeWorkspaceProof,
    ) => Effect.Effect<void, NativeWorkspaceError>;
    readonly setup: (
      basis: NativeWorkspaceBasis,
      proof: NativeWorkspaceProof,
      revalidate: Effect.Effect<void, NativeWorkspaceError>,
    ) => Effect.Effect<
      {
        readonly terminalId: string;
        readonly completion: Effect.Effect<number, NativeWorkspaceError>;
      },
      NativeWorkspaceError
    >;
    readonly cleanup: (
      basis: NativeWorkspaceBasis,
      proof: NativeWorkspaceProof,
      revalidate: Effect.Effect<void, NativeWorkspaceError>,
    ) => Effect.Effect<void, NativeWorkspaceError>;
  }
>()("t3/jones/nativeCreation/NativeCreationWorkspacePreparation/NativeWorkspacePorts") {}

export class NativeCreationWorkspacePreparation extends Context.Service<
  NativeCreationWorkspacePreparation,
  {
    readonly prepare: (
      input: NativeWorkspaceInput,
    ) => Effect.Effect<NativeWorkspaceVerified, NativeWorkspaceError>;
    readonly rollback: (
      input: NativeWorkspaceInput & { readonly recoveryScopeId: string },
    ) => Effect.Effect<void, NativeWorkspaceError>;
  }
>()("t3/jones/nativeCreation/NativeCreationWorkspacePreparation") {}

type StageDetails<T> = T extends Repository.NativeCreationStartedFact
  ? Omit<T, "ordinal" | "effectId" | "timestamp" | "phase">
  : never;
type WorkspaceStageDetails = StageDetails<
  Extract<Repository.NativeCreationStartedFact, { kind: "fetch" | "worktree" | "setup" }>
>;

const make = Effect.gen(function* () {
  const repository = yield* Repository.NativeCreationRepository;
  const authority = yield* Authority.NativeCreationAuthority;
  const optionalPorts = yield* Effect.serviceOption(NativeWorkspacePorts);
  const load = Effect.fnUntraced(function* (input: NativeWorkspaceInput) {
    if (
      Option.isNone(optionalPorts) ||
      !repository.readWorkspaceClaim ||
      !repository.admitWorkspace ||
      !repository.readWorkspaceAdmission ||
      !repository.recordWorkspaceVerified ||
      !repository.readWorkspaceVerified
    ) {
      return yield* denied(
        "unavailable",
        "Native workspace ownership and terminal ports are not qualified",
      );
    }
    const history = yield* repository.readWorkspaceClaim(input.claimId);
    if (history.intent.claimId !== input.claimId)
      return yield* denied("conflict", "Workspace claim authority differs");
    const actorSessionId = yield* Schema.decodeUnknownEffect(AuthSessionId)(
      history.intent.actorSessionId,
    );
    const guard = yield* Schema.decodeUnknownEffect(NativeCreationGuard)({
      schema: "t3.native-creation-guard/v1",
      grantId: history.intent.grantId,
      grantRevision: history.intent.grantRevision,
    });
    const preparation = yield* validateNativeCreationPreparation(
      new TextEncoder().encode(history.intent.canonicalPreparation),
    );
    const authorize = (
      stage: Authority.NativeCreationStage,
      recoveryScopeId?: string,
      recoveryResource?: Authority.NativeCreationAuthorityInput["recoveryResource"],
    ) =>
      authority.authorize({
        actorSessionId,
        guard,
        preparation,
        resources: history.intent.resources,
        stage,
        ...(recoveryScopeId === undefined ? {} : { recoveryScopeId }),
        ...(recoveryResource === undefined ? {} : { recoveryResource }),
      });
    return { history, preparation, authorize, ports: optionalPorts.value };
  });
  const prepare = Effect.fn("NativeCreationWorkspacePreparation.prepare")(
    function* (input: NativeWorkspaceInput) {
      const { history, authorize, ports } = yield* load(input);
      const intent = history.intent;
      yield* authorize("worktree");
      const prior = yield* repository.readWorkspaceVerified!(input.claimId);
      if (Option.isSome(prior)) {
        yield* ports
          .inspect(intent, prior.value.proof)
          .pipe(
            Effect.flatMap((basis) =>
              nativeCreationCanonicalJson(basis) === nativeCreationCanonicalJson(prior.value.basis)
                ? ports.verify(basis, prior.value.proof)
                : Effect.fail(denied("conflict", "Workspace birth or Git identity changed")),
            ),
          );
        return prior.value;
      }
      const basis = yield* ports.inspect(intent);
      if (
        basis.bootId !== intent.claimedBootId ||
        basis.projectId !== intent.binding.projectId ||
        basis.projectCwd !== intent.resources.projectCwd ||
        basis.worktreePath !== intent.resources.worktreePath ||
        basis.baseRef !== intent.binding.baseBranch ||
        (intent.binding.runSetupScript && basis.setupDefinition === null)
      )
        return yield* denied("conflict", "Canonical workspace basis differs from claim");
      yield* repository.admitWorkspace!(input.claimId, basis);
      // Started or uncertain operations stay owned forever; expiry is not permission to replay them.
      if (
        history.effects.some((fact) =>
          ["fetch", "worktree", "setup", "cleanup"].includes(fact.kind),
        )
      )
        return yield* denied(
          "unknown",
          "Workspace already has external effect history requiring qualified recovery",
        );
      let proof: NativeWorkspaceProof | undefined;
      const revalidate = (stage: Authority.NativeCreationStage) =>
        Effect.gen(function* () {
          yield* authorize(stage);
          const admitted = yield* repository.readWorkspaceAdmission!(input.claimId);
          const current = yield* ports.inspect(intent, proof);
          if (
            nativeCreationCanonicalJson(admitted) !== nativeCreationCanonicalJson(basis) ||
            nativeCreationCanonicalJson(current) !== nativeCreationCanonicalJson(basis)
          )
            return yield* denied(
              "conflict",
              "Workspace path admission, birth or Git registration changed",
            );
          if (proof) yield* ports.verify(basis, proof);
        }).pipe(
          Effect.mapError(() =>
            denied("conflict", "Current workspace authority or physical identity changed"),
          ),
        );
      const effectId = (stage: string) =>
        nativeCreationSha256(nativeCreationCanonicalJson({ claimId: input.claimId, stage, basis }));
      const timestamp = DateTime.now.pipe(Effect.map(DateTime.formatIso));
      let retainedTerminalId: string | null = null;
      const run = <A>(
        details: WorkspaceStageDetails,
        action: Effect.Effect<A, NativeWorkspaceError>,
        setupCompletion?: (value: A) => { readonly terminalId: string; readonly exitCode: number },
      ) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const stage = details.kind;
            yield* revalidate(stage);
            const started = yield* repository.startEffect(
              input.claimId,
              {
                ...details,
                effectId: effectId(stage),
                timestamp: yield* timestamp,
                phase: "started",
              },
              authorize(stage),
            );
            const result = yield* Effect.exit(restore(action));
            const { ordinal: _ordinal, ...identity } = started;
            if (identity.kind === "setup") {
              const terminal =
                Exit.isSuccess(result) && setupCompletion
                  ? setupCompletion(result.value)
                  : { terminalId: retainedTerminalId, exitCode: null };
              yield* repository.completeEffect(input.claimId, {
                ...identity,
                ...terminal,
                timestamp: yield* timestamp,
                phase: "completed",
                result: Exit.isSuccess(result)
                  ? terminal.exitCode === 0
                    ? "succeeded"
                    : "failed"
                  : "unknown",
              });
              if (Exit.isSuccess(result) && terminal.exitCode !== 0)
                return yield* denied("failed", "Retained setup terminal did not succeed");
            } else if (identity.kind === "fetch" || identity.kind === "worktree") {
              yield* repository.completeEffect(input.claimId, {
                ...identity,
                timestamp: yield* timestamp,
                phase: "completed",
                result: Exit.isSuccess(result) ? "succeeded" : "unknown",
                ...(identity.kind === "worktree" && Exit.isSuccess(result)
                  ? { ownership: "created" as const }
                  : {}),
              });
            } else return yield* denied("conflict", "Unexpected workspace effect identity");
            if (Exit.isFailure(result))
              return yield* denied(
                "unknown",
                "Workspace effect completion is uncertain; retained start cannot replay",
              );
            return result.value;
          }),
        );
      if (intent.binding.startFromOrigin)
        yield* run(
          { kind: "fetch", projectCwd: basis.projectCwd, baseRef: basis.baseRef },
          ports.fetch(basis, revalidate("fetch")),
        );
      proof = yield* run(
        {
          kind: "worktree",
          projectCwd: basis.projectCwd,
          worktreePath: basis.worktreePath,
          branch: intent.resources.branch,
          baseRef: basis.baseRef,
          ownership: "claimed",
        },
        ports.createWorktree(basis, intent.resources.branch, revalidate("worktree")).pipe(
          Effect.flatMap((created) => {
            if (
              created.worktreePath !== basis.worktreePath ||
              created.branch !== intent.resources.branch ||
              created.baseRef !== basis.baseRef ||
              created.gitCommonDirectory !== basis.gitCommonDirectory ||
              created.physicalGitIdentity !== basis.physicalGitIdentity ||
              created.configuredSubmodulesDigest !==
                nativeCreationSha256(basis.configuredSubmodulesDefinition) ||
              created.baseConfigurationDigest !==
                nativeCreationSha256(basis.baseConfigurationDefinition)
            )
              return Effect.fail(
                denied("conflict", "Created workspace does not match issued resources"),
              );
            return Effect.succeed(created);
          }),
        ),
      );
      yield* revalidate("worktree_ownership");
      let setupTerminalId: string | null = null;
      if (intent.binding.runSetupScript) {
        const preparedProof = proof;
        const setup = yield* run(
          { kind: "setup", worktreePath: basis.worktreePath, terminalId: null },
          Effect.uninterruptibleMask((restoreCompletion) =>
            Effect.gen(function* () {
              // Retain the owned terminal before observing cancellation; only its completion wait is interruptible.
              const terminal = yield* ports.setup(basis, preparedProof, revalidate("setup"));
              retainedTerminalId = terminal.terminalId;
              const exitCode = yield* restoreCompletion(terminal.completion);
              return { terminalId: terminal.terminalId, exitCode };
            }),
          ),
          (terminal) => terminal,
        );
        setupTerminalId = setup.terminalId;
      }
      yield* revalidate("worktree_ownership");
      const verified = { claimId: input.claimId, basis, proof, setupTerminalId };
      yield* repository.recordWorkspaceVerified!(verified);
      return verified;
    },
    Effect.mapError((cause) =>
      Schema.is(NativeWorkspaceError)(cause)
        ? cause
        : denied("conflict", "Native workspace preparation was rejected"),
    ),
  );
  const rollback = Effect.fn("NativeCreationWorkspacePreparation.rollback")(
    function* (input: NativeWorkspaceInput & { readonly recoveryScopeId: string }) {
      const { history, authorize, ports } = yield* load(input);
      const verified = yield* repository.readWorkspaceVerified!(input.claimId);
      if (Option.isNone(verified))
        return yield* denied(
          "unknown",
          "Unverified resources require a separate qualified recovery owner",
        );
      const { basis, proof } = verified.value;
      const resource = {
        kind: "worktree" as const,
        projectCwd: basis.projectCwd,
        worktreePath: proof.worktreePath,
        branch: proof.branch,
        baseRef: proof.baseRef,
        ownership: "created" as const,
      };
      if (history.effects.some((fact) => fact.kind === "cleanup"))
        return yield* denied("unknown", "Cleanup was already started and cannot replay");
      const revalidate = Effect.gen(function* () {
        yield* authorize("cleanup", input.recoveryScopeId, resource);
        const current = yield* ports.inspect(history.intent, proof);
        if (nativeCreationCanonicalJson(current) !== nativeCreationCanonicalJson(basis))
          return yield* denied("conflict", "Cleanup birth or physical Git identity changed");
        yield* ports.verify(basis, proof);
      }).pipe(
        Effect.mapError(() => denied("conflict", "Cleanup authority or resource readback changed")),
      );
      yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          yield* revalidate;
          const effectId = nativeCreationSha256(
            nativeCreationCanonicalJson({
              claimId: input.claimId,
              recoveryScopeId: input.recoveryScopeId,
              resource,
            }),
          );
          yield* repository.startEffect(
            input.claimId,
            {
              kind: "cleanup",
              phase: "started",
              effectId,
              timestamp: DateTime.formatIso(yield* DateTime.now),
              resource,
              recoveryScopeId: input.recoveryScopeId,
            },
            authorize("cleanup", input.recoveryScopeId, resource),
          );
          const result = yield* Effect.exit(restore(ports.cleanup(basis, proof, revalidate)));
          yield* repository.completeEffect(input.claimId, {
            kind: "cleanup",
            phase: "completed",
            effectId,
            timestamp: DateTime.formatIso(yield* DateTime.now),
            resource,
            recoveryScopeId: input.recoveryScopeId,
            result: Exit.isSuccess(result) ? "succeeded" : "unknown",
          });
          if (Exit.isFailure(result))
            return yield* denied("unknown", "Cleanup completion is uncertain and cannot replay");
        }),
      );
    },
    Effect.mapError((cause) =>
      Schema.is(NativeWorkspaceError)(cause)
        ? cause
        : denied("conflict", "Workspace recovery was rejected"),
    ),
  );
  return NativeCreationWorkspacePreparation.of({ prepare, rollback });
});
export const layer = Layer.effect(NativeCreationWorkspacePreparation, make);
