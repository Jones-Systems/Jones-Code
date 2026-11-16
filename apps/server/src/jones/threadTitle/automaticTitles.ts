import type { OrchestrationV2ThreadProjection, CommandId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { ThreadTitleAssessment, type ThreadTitleCommitContext } from "./commitContext.ts";
import { decideTitle, type TitleCommand } from "./policy.ts";
import * as TitleStore from "./store.ts";

export const make = Effect.gen(function* () {
  const store = yield* TitleStore.make;
  const plan = Effect.fn("AutomaticThreadTitles.plan")(function* (input: {
    readonly command: TitleCommand;
    readonly projection: Pick<OrchestrationV2ThreadProjection, "thread" | "messages" | "runs">;
    readonly completingRequestId?: CommandId;
  }) {
    if (!(yield* store.available)) return { commit: undefined, arm: undefined, seed: undefined };
    const assessment = yield* ThreadTitleAssessment;
    const decision = decideTitle({
      ...input,
      state: yield* store.read(input.projection.thread.id),
      ...(assessment !== undefined && assessment.requestId === input.completingRequestId
        ? { verdict: assessment.verdict }
        : {}),
    });
    const state = decision.state;
    const commit: ThreadTitleCommitContext | undefined =
      state === undefined
        ? undefined
        : {
            accept: store.write(input.projection.thread.id, state),
          };
    return { ...decision, commit };
  });
  return { plan };
});
