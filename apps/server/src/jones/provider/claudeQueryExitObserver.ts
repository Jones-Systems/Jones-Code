import type { ProviderThreadId, ProviderTurnId, RunAttemptId, RunId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import type * as Exit from "effect/Exit";
import * as Option from "effect/Option";

/** Internal observation only; it grants no close acknowledgment or mutation authority. */
export interface QueryExitObservation {
  readonly query: object;
  readonly runtimeGeneration: string;
  readonly providerThreadId: ProviderThreadId;
  readonly providerTurnId: ProviderTurnId | null;
  readonly runId: RunId | null;
  readonly attemptId: RunAttemptId | null;
  readonly phase: "source_exited" | "after_root_settlement" | "stop_confirmation_wait";
  readonly sourceExit: Exit.Exit<unknown, unknown> | null;
}

export class ClaudeQueryExitObserver extends Context.Service<
  ClaudeQueryExitObserver,
  { readonly observe: (observation: QueryExitObservation) => Effect.Effect<void> }
>()("t3/jones/provider/claudeQueryExitObserver") {}

export const observe = (observation: QueryExitObservation) =>
  Effect.serviceOption(ClaudeQueryExitObserver).pipe(
    Effect.flatMap((observer) =>
      Option.isNone(observer) ? Effect.void : observer.value.observe(Object.freeze(observation)),
    ),
  );
