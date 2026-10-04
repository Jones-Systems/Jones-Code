import {
  type ProviderGoalStateObservation,
  type ProviderInstanceId,
  type ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import { unknownProviderGoal } from "../provider/providerGoal.ts";
import * as ProviderSessionGoalService from "./ProviderSessionGoalService.ts";

export const readProviderGoalState = Effect.fn("http.orchestration.readProviderGoalState")(
  function* (input: {
    readonly threadId: ThreadId;
    readonly expectedInstanceId: ProviderInstanceId;
  }) {
    const service = yield* Effect.serviceOption(
      ProviderSessionGoalService.ProviderSessionGoalService,
    );
    const result = yield* (
      Option.isSome(service)
        ? service.value.get(input)
        : Effect.succeed(unknownProviderGoal("unsupported"))
    ).pipe(Effect.catchCause(() => Effect.succeed(unknownProviderGoal("rpc_error"))));
    const now = yield* DateTime.now;
    return {
      schema: "t3.provider-goal-state/v1",
      threadId: input.threadId,
      providerInstanceId: input.expectedInstanceId,
      nativeThreadId: result.nativeThreadId,
      observedAtMs: DateTime.toEpochMillis(now),
      state: result.state,
      reasonCode: result.reasonCode,
    } satisfies ProviderGoalStateObservation;
  },
);
