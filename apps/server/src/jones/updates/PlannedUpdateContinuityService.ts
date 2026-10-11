import type { CommandId, ThreadId, OrchestrationV2ThreadProjection } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { WorkModeCandidate } from "../workMode/Policy.ts";
import type { NativeOperationBinding } from "./launcherOperation.ts";
import type { QualifiedRuntimeBinding } from "../cloud/qualifiedRuntime.ts";

export class PlannedContinuityError extends Schema.TaggedError<PlannedContinuityError>()(
  "PlannedContinuityError",
  { cause: Schema.Defect() },
) {}

export interface PlannedUpdateProof {
  readonly operationId: string;
  readonly outcome: "committed" | "rolled-back";
  readonly binding: NativeOperationBinding;
  readonly current: QualifiedRuntimeBinding;
}

export class PlannedUpdateContinuity extends Context.Service<
  PlannedUpdateContinuity,
  {
    readonly capture: (input: {
      readonly operationId: string;
      readonly binding: NativeOperationBinding;
      readonly continueRunningThreads: boolean;
    }) => Effect.Effect<void, PlannedContinuityError>;
    readonly activate: (proof: PlannedUpdateProof) => Effect.Effect<void, PlannedContinuityError>;
    readonly queueThreadIds: Effect.Effect<ReadonlyArray<ThreadId>>;
    readonly queueCommand: (
      projection: OrchestrationV2ThreadProjection,
    ) => Effect.Effect<CommandId | undefined, PlannedContinuityError>;
    readonly finishQueue: (threadId: ThreadId) => Effect.Effect<void, PlannedContinuityError>;
    readonly hasWork: (threadId: ThreadId) => Effect.Effect<boolean, PlannedContinuityError>;
    readonly admitWork: (
      projection: OrchestrationV2ThreadProjection,
      candidate: WorkModeCandidate,
      nowMs: number,
    ) => Effect.Effect<boolean, PlannedContinuityError>;
    readonly finishWork: (threadId: ThreadId) => Effect.Effect<void, PlannedContinuityError>;
    readonly allowWorkStart: (
      threadId: ThreadId,
      messageId: string,
    ) => Effect.Effect<boolean, PlannedContinuityError>;
  }
>()("t3/jones/updates/PlannedUpdateContinuity") {}
