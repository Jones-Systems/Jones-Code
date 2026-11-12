import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Recorded from "./recordedCaller.ts";

export class RecordedCallerPreparation extends Context.Service<
  RecordedCallerPreparation,
  {
    readonly withPrepared: <A, E, R>(
      use: (callers: Recorded.RecordedCallers["Service"]) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | Recorded.RecordedCallerFence, R>;
  }
>()(
  "t3/jones/workstreams/native/runtime/recordedCallerPreparationPort/RecordedCallerPreparation",
) {}
