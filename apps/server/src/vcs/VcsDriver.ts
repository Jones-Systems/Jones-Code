import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type {
  VcsDriverCapabilities,
  VcsError,
  VcsInitInput,
  VcsListRemotesResult,
  VcsListWorkspaceFilesResult,
  ReviewDiffPreviewInput,
  ReviewDiffPreviewResult,
  VcsRepositoryIdentity,
} from "@t3tools/contracts";
import { CheckpointRef } from "@t3tools/contracts";
import * as VcsProcess from "./VcsProcess.ts";

export interface VcsCaptureCheckpointInput<E = never, R = never> {
  readonly revalidateMutation?: Effect.Effect<void, E, R>;
  readonly cwd: string;
  readonly checkpointRef: CheckpointRef;
}

export interface VcsRestoreCheckpointInput<E = never, R = never> {
  readonly revalidateMutation?: Effect.Effect<void, E, R>;
  readonly cwd: string;
  readonly checkpointRef: CheckpointRef;
  readonly fallbackToHead?: boolean;
}

export interface VcsDiffCheckpointsInput {
  readonly cwd: string;
  readonly fromCheckpointRef: CheckpointRef;
  readonly toCheckpointRef: CheckpointRef;
  readonly fallbackFromToHead?: boolean;
  readonly ignoreWhitespace: boolean;
  readonly format?: "patch" | "numstat";
}

export interface VcsDeleteCheckpointRefsInput<E = never, R = never> {
  readonly revalidateMutation?: Effect.Effect<void, E, R>;
  readonly cwd: string;
  readonly checkpointRefs: ReadonlyArray<CheckpointRef>;
}

export interface VcsCheckpointOps {
  readonly captureCheckpoint: <E = never, R = never>(
    input: VcsCaptureCheckpointInput<E, R>,
  ) => Effect.Effect<void, VcsError | E, R>;
  readonly hasCheckpointRef: (
    input: Omit<VcsRestoreCheckpointInput, "fallbackToHead">,
  ) => Effect.Effect<boolean, VcsError>;
  readonly restoreCheckpoint: <E = never, R = never>(
    input: VcsRestoreCheckpointInput<E, R>,
  ) => Effect.Effect<boolean, VcsError | E, R>;
  readonly diffCheckpoints: (input: VcsDiffCheckpointsInput) => Effect.Effect<string, VcsError>;
  readonly deleteCheckpointRefs: <E = never, R = never>(
    input: VcsDeleteCheckpointRefsInput<E, R>,
  ) => Effect.Effect<void, VcsError | E, R>;
}

export class VcsDriver extends Context.Service<
  VcsDriver,
  {
    readonly capabilities: VcsDriverCapabilities;
    readonly execute: <E = never, R = never>(
      input: Omit<VcsProcess.VcsProcessInput<E, R>, "command">,
    ) => Effect.Effect<VcsProcess.VcsProcessOutput, VcsError | E, R>;
    readonly checkpoints?: VcsCheckpointOps;
    readonly detectRepository: (
      cwd: string,
    ) => Effect.Effect<VcsRepositoryIdentity | null, VcsError>;
    readonly isInsideWorkTree: (cwd: string) => Effect.Effect<boolean, VcsError>;
    readonly listWorkspaceFiles: (
      cwd: string,
    ) => Effect.Effect<VcsListWorkspaceFilesResult, VcsError>;
    readonly listRemotes: (cwd: string) => Effect.Effect<VcsListRemotesResult, VcsError>;
    readonly filterIgnoredPaths: (
      cwd: string,
      relativePaths: ReadonlyArray<string>,
    ) => Effect.Effect<ReadonlyArray<string>, VcsError>;
    readonly initRepository: (input: VcsInitInput) => Effect.Effect<void, VcsError>;
    readonly getDiffPreview?: (
      input: ReviewDiffPreviewInput,
    ) => Effect.Effect<ReviewDiffPreviewResult, VcsError>;
  }
>()("t3/vcs/VcsDriver") {}
