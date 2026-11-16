import type { CommandId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type { RecordedLifecycleEvent } from "../../orchestration-v2/RecordedTypes.ts";
import type { TaskContinuationError } from "./sourceBinding.ts";

export interface TaskContinuationCommitContext {
  readonly accept: (envelope: {
    readonly commandId: CommandId;
    readonly threadId: ThreadId;
    readonly commandType: string;
    readonly events: ReadonlyArray<RecordedLifecycleEvent>;
  }) => Effect.Effect<void, TaskContinuationError>;
}
export const TaskContinuationCommit = Context.Reference<TaskContinuationCommitContext | undefined>(
  "jones/taskContinuation/TaskContinuationCommit",
  { defaultValue: () => undefined },
);
