import * as Context from "effect/Context";
import type { ThreadId, MessageId, RunId } from "@t3tools/contracts";

export interface OpenRequest {
  readonly requestId: string;
  readonly operationId: string;
  readonly targetThreadId: ThreadId;
  readonly messageId: MessageId;
  readonly commandId: string;
  readonly originKind: "local" | "linked";
  readonly originEnvironmentId: string;
  readonly originThreadId: ThreadId;
  readonly originLabel: string;
  readonly localDependency?: {
    readonly dependencyId: string;
    readonly waiterRunId: RunId | null;
    readonly waiterGeneration: number;
    readonly targetLabel: string;
  };
}
export const DependencyCommit = Context.Reference<OpenRequest | undefined>(
  "jones/threadResultDependencies/DependencyCommit",
  { defaultValue: () => undefined },
);
export interface DependencyResultWakeCommit {
  readonly dependencyId: string;
  readonly generation: number;
  readonly waiterThreadId: ThreadId;
  readonly messageId: MessageId;
}
export interface OriginWakeCommit {
  readonly kind: "remote_admission_origin";
  readonly dependencyId: string;
  readonly observationKey: string;
  readonly generation: number;
  readonly waiterThreadId: ThreadId;
  readonly messageId: MessageId;
  readonly linkGeneration: string;
  readonly sessionId: string;
  readonly committedAt: string;
}
export type WakeCommit = DependencyResultWakeCommit | OriginWakeCommit;
export const DependencyWakeCommit = Context.Reference<WakeCommit | undefined>(
  "jones/threadResultDependencies/DependencyWakeCommit",
  { defaultValue: () => undefined },
);
