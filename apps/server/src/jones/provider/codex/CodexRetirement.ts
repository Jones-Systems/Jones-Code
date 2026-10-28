import type * as CodexErrors from "effect-codex-app-server/errors";
import type { ProviderRuntimeBinding } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

interface CodexRetirementReceipt {
  readonly binding: ProviderRuntimeBinding;
  readonly evidence: "thread_closed" | "producer_exited";
}

const decodeLoaded = Schema.decodeUnknownEffect(
  Schema.Struct({ data: Schema.Array(Schema.String), nextCursor: Schema.optionalKey(Schema.NullOr(Schema.String)) }),
);
const decodeThread = Schema.decodeUnknownEffect(
  Schema.Struct({ thread: Schema.Struct({ id: Schema.String, status: Schema.Struct({ type: Schema.String }) }) }),
);
const decodeGoal = Schema.decodeUnknownEffect(
  Schema.Struct({ goal: Schema.NullOr(Schema.Struct({ threadId: Schema.String, status: Schema.String })) }),
);
const decodeTerminals = Schema.decodeUnknownEffect(
  Schema.Struct({ data: Schema.Array(Schema.Unknown), nextCursor: Schema.NullOr(Schema.String) }),
);

/** Missing native ownership coverage cannot authorize terminating a shared process. */
export const codexProducerOwnershipIsExclusive = Effect.fn("codexProducerOwnershipIsExclusive")(
  function* (input: {
    readonly nativeThreadId: string;
    readonly request: (method: string, params: { readonly threadId?: string; readonly includeTurns?: boolean }) => Effect.Effect<unknown, unknown>;
  }) {
    const loaded = yield* input.request("thread/loaded/list", {}).pipe(Effect.flatMap(decodeLoaded));
    if (loaded.nextCursor != null || loaded.data.length !== 1 || loaded.data[0] !== input.nativeThreadId) return false;
    const thread = yield* input.request("thread/read", { threadId: input.nativeThreadId, includeTurns: false }).pipe(Effect.flatMap(decodeThread));
    if (thread.thread.id !== input.nativeThreadId || thread.thread.status.type !== "idle") return false;
    const goal = yield* input.request("thread/goal/get", { threadId: input.nativeThreadId }).pipe(Effect.flatMap(decodeGoal));
    if (goal.goal !== null && (goal.goal.threadId !== input.nativeThreadId || !["paused", "complete"].includes(goal.goal.status))) return false;
    const terminals = yield* input.request("thread/backgroundTerminals/list", { threadId: input.nativeThreadId }).pipe(Effect.flatMap(decodeTerminals));
    return terminals.data.length === 0 && terminals.nextCursor === null;
  },
  Effect.timeout("1 second"),
  Effect.catch(() => Effect.succeed(false)),
);

/** Scope closure and physical exit share one worker; caller deadlines never own it. */
export const makeCodexProducerRetirement = Effect.fn("makeCodexProducerRetirement")(
  function* (closeScope: Effect.Effect<void>, afterExit: Effect.Effect<void> = Effect.void) {
    const exited = yield* Deferred.make<void, CodexErrors.CodexAppServerError>();
    const closed = yield* Deferred.make<void, CodexErrors.CodexAppServerError>();
    let readProcessExit: Effect.Effect<void, CodexErrors.CodexAppServerError> | undefined;
    let observing = false;
    let closing = false;
    const observeProcessExit = Effect.uninterruptible(
      Effect.suspend(() => {
        if (observing || readProcessExit === undefined) return Effect.void;
        observing = true;
        return Deferred.complete(exited, readProcessExit).pipe(
          Effect.forkDetach({ startImmediately: true }),
          Effect.asVoid,
        );
      }),
    );
    return {
      captureProcessExit: (readback: Effect.Effect<void, CodexErrors.CodexAppServerError>) => {
        readProcessExit ??= readback;
      },
      observeProcessExit,
      get closing() {
        return closing;
      },
      get processExit() {
        return readProcessExit === undefined ? undefined : Deferred.await(exited);
      },
      close: Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          yield* observeProcessExit;
          if (!closing) {
            closing = true;
            yield* Deferred.complete(
              closed,
              closeScope.pipe(
                Effect.andThen(() =>
                  readProcessExit === undefined ? Effect.void : Deferred.await(exited).pipe(Effect.andThen(afterExit)),
                ),
              ),
            ).pipe(Effect.forkDetach({ startImmediately: true }));
          }
          return yield* restore(Deferred.await(closed));
        }),
      ),
    };
  },
);

interface ThreadRetirement {
  readonly binding: ProviderRuntimeBinding;
  readonly closed: Deferred.Deferred<void>;
  readonly wait: Effect.Effect<CodexRetirementReceipt>;
}

const sameBinding = (left: ProviderRuntimeBinding, right: ProviderRuntimeBinding) =>
  left.driver === right.driver &&
  left.providerInstanceId === right.providerInstanceId &&
  left.providerSessionId === right.providerSessionId &&
  left.providerThreadId === right.providerThreadId &&
  left.threadId === right.threadId &&
  left.runtimeGeneration === right.runtimeGeneration &&
  left.nativeThreadId === right.nativeThreadId;

export function makeCodexThreadRetirements<Producer extends object>() {
  const producers = new WeakMap<Producer, Map<string, ThreadRetirement>>();
  const find = (producer: Producer, nativeThreadId: string) =>
    producers.get(producer)?.get(nativeThreadId);
  return {
    find,
    // A committed transfer permanently fences this producer/thread incarnation.
    // Codex thread/closed carries no incarnation ID with which to distinguish a rejoin.
    owns: (producer: Producer, nativeThreadId: string) => find(producer, nativeThreadId) !== undefined,
    observeClosed: (producer: Producer, nativeThreadId: string) =>
      Effect.suspend(() => {
        const pending = find(producer, nativeThreadId);
        return pending === undefined
          ? Effect.void
          : Deferred.succeed(pending.closed, undefined).pipe(Effect.asVoid);
      }),
    begin: Effect.fn("CodexThreadRetirements.begin")(function* (input: {
      readonly producer: Producer;
      readonly binding: ProviderRuntimeBinding;
      readonly dispatch: Effect.Effect<"thread" | "producer", unknown>;
      readonly processExit: Effect.Effect<void, CodexErrors.CodexAppServerError> | undefined;
    }) {
      const existing = find(input.producer, input.binding.nativeThreadId);
      if (existing !== undefined)
        return sameBinding(existing.binding, input.binding) ? existing : undefined;
      const closed = yield* Deferred.make<void>();
      const done = yield* Deferred.make<CodexRetirementReceipt>();
      const request: ThreadRetirement = {
        binding: input.binding,
        closed,
        wait: Deferred.await(done),
      };
      const threads = producers.get(input.producer) ?? new Map<string, ThreadRetirement>();
      threads.set(input.binding.nativeThreadId, request);
      producers.set(input.producer, threads);
      yield* Effect.gen(function* () {
        // Install the record before dispatch. A closure can precede the unsubscribe ACK.
        const mode = yield* input.dispatch.pipe(Effect.catch(() => Effect.succeed("thread" as const)));
        const evidence = yield* Effect.race(
          mode === "producer" ? Effect.never : Deferred.await(closed).pipe(Effect.as("thread_closed" as const)),
          input.processExit === undefined
            ? Effect.never
            : input.processExit.pipe(
                Effect.as("producer_exited" as const),
                Effect.catch(() => Effect.never),
              ),
        );
        yield* Deferred.succeed(done, { binding: input.binding, evidence });
      }).pipe(Effect.forkDetach({ startImmediately: true }));
      return request;
    }, Effect.uninterruptible),
  };
}
