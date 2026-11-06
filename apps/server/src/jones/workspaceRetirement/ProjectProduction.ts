import * as Effect from "effect/Effect";
import { completionObservation, recordCompletion } from "./CompletionObservation.ts";

export const begin = Effect.fn("ProjectProduction.begin")(
  (input: {
    threadId: string;
    worktreePath: string;
    trigger: string;
    projectId?: string;
    lineage?: Readonly<Record<string, string>>;
  }) =>
    Effect.sync(() => {
      let completed = false;
      return Effect.fn("ProjectProduction.finish")(function* (
        status: "succeeded" | "failed" | "cancelled",
      ) {
        if (completed) return;
        completed = true;
        const observation = completionObservation({
          producer: "jones-project-" + input.trigger,
          owner: input.threadId,
          worktree: input.worktreePath,
          status,
          roots: [],
          lineage: {
            ...input.lineage,
            threadId: input.threadId,
            ...(input.projectId ? { projectId: input.projectId } : {}),
          },
          omittedEnrichment: ["output_roots_not_declared"],
        });
        const result = yield* Effect.sync(() => recordCompletion(observation));
        if (result.status !== "recorded")
          yield* Effect.logDebug("Project completion metadata unavailable", {
            reason: result.reason,
            status,
            threadId: input.threadId,
          });
      });
    }),
);
