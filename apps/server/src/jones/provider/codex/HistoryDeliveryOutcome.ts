import * as CodexErrors from "effect-codex-app-server/errors";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

/** Evidence from the current call only; never evidence about a persisted pending marker. */
export class HistoryNotApplied extends Schema.TaggedError<HistoryNotApplied>()(
  "HistoryNotApplied",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "Historical context was not applied by this attempt.";
  }
}

export class PromptNotApplied extends Schema.TaggedError<PromptNotApplied>()("PromptNotApplied", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return "The provider did not consume this attempt's prompt.";
  }
}

const isCodexRequestError = Schema.is(CodexErrors.CodexAppServerRequestError);
const isHistoryNotApplied = Schema.is(HistoryNotApplied);
const isPromptNotApplied = Schema.is(PromptNotApplied);

export const historyInjectionRejection = (error: unknown) => {
  if (!isCodexRequestError(error) || error.method !== "thread/inject_items") return "ambiguous";
  if (error.operation === "encode-payload") return "not_applied";
  if (error.operation !== "receive-response") return "ambiguous";
  if (error.code === -32601) return "unsupported";
  return error.code === -32600 || error.code === -32602 ? "not_applied" : "ambiguous";
};

export const historyWasNotApplied = (error: unknown): boolean => {
  const seen = new Set<unknown>();
  let current = error;
  while (typeof current === "object" && current !== null && !seen.has(current)) {
    seen.add(current);
    if (isHistoryNotApplied(current)) return true;
    current = "cause" in current ? current.cause : undefined;
  }
  return false;
};

/** Only current-attempt proof authorizes undoing an unsent inline marker. */
export const promptWasNotApplied = (error: unknown): boolean => {
  const seen = new Set<unknown>();
  let current = error;
  while (typeof current === "object" && current !== null && !seen.has(current)) {
    seen.add(current);
    if (isPromptNotApplied(current) || Predicate.isTagged(current, "CodexStartupNotDispatched"))
      return true;
    if (
      isCodexRequestError(current) &&
      current.method === "turn/start" &&
      (current.operation === "encode-payload" ||
        (current.operation === "receive-response" &&
          (current.code === -32600 || current.code === -32601 || current.code === -32602)))
    )
      return true;
    current = "cause" in current ? current.cause : undefined;
  }
  return false;
};

/** One delivery invocation owns only the records it attempted to mark pending. */
export const makeHistoryDeliveryOutcome = <T, E>(
  restoreRecord: (record: T) => Effect.Effect<void, E>,
) => {
  const attempted = new Set<T>();
  let mayHaveApplied = false;
  let restorationStarted = false;
  const unsent = Effect.suspend(() => {
    if (attempted.size === 0 || mayHaveApplied || restorationStarted) return Effect.void;
    restorationStarted = true;
    return Effect.forEach(attempted, restoreRecord, { discard: true });
  });
  return {
    pending: <A, E2>(record: T, effect: Effect.Effect<A, E2>) =>
      Effect.sync(() => {
        attempted.add(record);
      }).pipe(Effect.andThen(effect)),
    inject: <E2>(effect: Effect.Effect<boolean, E2>) =>
      Effect.sync(() => {
        mayHaveApplied = true;
      }).pipe(
        Effect.andThen(effect),
        Effect.tap((accepted) =>
          Effect.sync(() => {
            mayHaveApplied = accepted;
          }),
        ),
        Effect.tapError((error) =>
          Effect.sync(() => {
            if (historyWasNotApplied(error)) mayHaveApplied = false;
          }),
        ),
      ),
    unsent,
    protect: <A, E2>(effect: Effect.Effect<A, E2>) =>
      effect.pipe(
        Effect.onExit((exit) =>
          Exit.isFailure(exit)
            ? unsent.pipe(
                Effect.catchCause(() =>
                  Effect.logWarning(
                    "Failed to restore unsent context handoffs; delivery remains unresolved",
                  ),
                ),
              )
            : Effect.void,
        ),
      ),
  };
};
