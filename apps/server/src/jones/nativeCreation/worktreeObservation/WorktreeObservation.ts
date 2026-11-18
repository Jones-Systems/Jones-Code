import type * as Effect from "effect/Effect";
import type { LegacyPreparation } from "../../../orchestration-v2/RecordedTypes.ts";

export interface LegacyWorktreeBeforeObservation {
  readonly parentPath: string;
  readonly parentRealPath: string;
  readonly parentDevice: string;
  readonly parentInode: string;
  readonly commonDirectoryDevice?: string | undefined;
  readonly commonDirectoryInode?: string | undefined;
  readonly targetRefAbsent: true;
  readonly registrationAbsent: true;
}

export interface LegacyWorktreeMaterialClaim {
  readonly path: string;
  readonly realPath: string;
  readonly device: string;
  readonly inode: string;
  readonly parentRealPath: string;
  readonly gitDirectory: string;
  readonly commonDirectory: string;
  readonly registeredPath: string;
  readonly headRef: string;
  readonly headOid: string;
  readonly parentDevice?: string | undefined;
  readonly parentInode?: string | undefined;
  readonly dotGitDevice?: string | undefined;
  readonly dotGitInode?: string | undefined;
  readonly gitDirectoryDevice?: string | undefined;
  readonly gitDirectoryInode?: string | undefined;
  readonly commonDirectoryDevice?: string | undefined;
  readonly commonDirectoryInode?: string | undefined;
}

export interface LegacyWorktreePreparationStep {
  readonly kind: "worktree.add" | "worktree.submodules" | "worktree.base-config";
  readonly cwd: string;
  readonly args: ReadonlyArray<string>;
  readonly worktreePath: string;
  readonly commonDirectory: string;
  readonly baseCommitOid: string;
  readonly targetRef: string;
  readonly before?: LegacyWorktreeBeforeObservation;
}

export interface LegacyWorktreePreparationHooks {
  /** Affirmative owner refusal after exact intent readback and before invocation. */
  readonly neverInvoked?: (
    step: LegacyWorktreePreparationStep,
    reason: "input_validation_failed",
  ) => Effect.Effect<void, Error>;
  readonly beforeEffect: (step: LegacyWorktreePreparationStep) => Effect.Effect<void, Error>;
  readonly afterEffect: (
    step: LegacyWorktreePreparationStep,
    outcome: "settled_success" | "failed_or_unknown",
    claim?: LegacyWorktreeMaterialClaim,
  ) => Effect.Effect<void, Error>;
}

export type LegacyBranchRenameStep = Extract<
  LegacyPreparation["steps"][number]["effect"],
  { readonly kind: "branch.rename" }
>["input"];

export interface LegacyBranchRenameHooks {
  readonly before: LegacyWorktreeBeforeObservation;
  readonly claim: LegacyWorktreeMaterialClaim;
  readonly beforeEffect: (step: LegacyBranchRenameStep) => Effect.Effect<void, Error>;
  readonly afterEffect: (
    step: LegacyBranchRenameStep,
    outcome: "settled_success" | "failed_or_unknown",
    claim?: LegacyWorktreeMaterialClaim,
  ) => Effect.Effect<void, Error>;
}
