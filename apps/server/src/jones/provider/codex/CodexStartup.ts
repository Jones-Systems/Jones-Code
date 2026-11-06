import { ProviderDriverKind, type ProviderRuntimeBinding } from "@t3tools/contracts";
import {
  hasUnknownRuntimeBinding,
  ProviderAdapterProtocolError,
  ProviderRuntimeBindingError,
} from "@t3tools/provider-core/server/ProviderAdapter";
import * as CodexErrors from "effect-codex-app-server/errors";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as ControlAdmission from "../../orchestration/ControlAdmission.ts";

// This is a caller budget, not a measured Codex latency or a process-exit deadline.
export const CODEX_STARTUP_BUDGET_MS = 60_000;

type StartupOperation = "ensure" | "resume" | "start" | "fork";
interface StartupProducer {
  readonly generation: string;
  readonly scope: Scope.Closeable;
}
interface StartupAttempt {
  readonly operation: StartupOperation;
  readonly deadlineAtMillis: number;
  producer: StartupProducer | undefined;
  target: ControlAdmission.ControlAdmissionTarget | undefined;
  hold: ControlAdmission.StartupHold | undefined;
  mayHaveIssued: boolean;
  abandoned: boolean;
  complete: boolean;
}
class StartupAttemptContext extends Context.Reference<StartupAttempt | undefined>(
  "jones/CodexStartup/Attempt",
  { defaultValue: () => undefined },
) {}
class CodexStartupNotDispatched extends Schema.TaggedError<CodexStartupNotDispatched>()(
  "CodexStartupNotDispatched",
  { operation: Schema.Literals(["ensure", "resume", "start", "fork"]), budgetMs: Schema.Number },
) {
  override get message(): string {
    return "Codex startup exceeded its caller budget before an unconfirmed native request.";
  }
}

export const isNonApplyingStartupRejection = (error: unknown, method: string | undefined) =>
  method !== undefined &&
  Schema.is(CodexErrors.CodexAppServerRequestError)(error) &&
  error.operation === "receive-response" &&
  error.method === method &&
  (error.code === -32600 || error.code === -32601 || error.code === -32602);

const isPreDispatchRefusal = (error: unknown) => {
  const seen = new Set<unknown>();
  let current = error;
  while (typeof current === "object" && current !== null && !seen.has(current)) {
    seen.add(current);
    if (Schema.is(ControlAdmission.ControlAdmissionHeld)(current)) return true;
    current = "cause" in current ? current.cause : undefined;
  }
  return false;
};

const replyNativeId = (reply: unknown) => {
  if (typeof reply !== "object" || reply === null || !("thread" in reply)) return undefined;
  const thread = reply.thread;
  return typeof thread === "object" &&
    thread !== null &&
    "id" in thread &&
    typeof thread.id === "string" &&
    thread.id.trim().length > 0
    ? thread.id
    : undefined;
};

export function makeCodexStartup(
  budgetMs = CODEX_STARTUP_BUDGET_MS,
  admission?: ControlAdmission.ControlAdmission["Service"],
) {
  const uncertain = new WeakMap<StartupProducer, Set<StartupAttempt>>();
  const holdOwners = new Map<ControlAdmission.StartupHold, StartupAttempt>();
  const clearAttempt = (attempt: StartupAttempt) => {
    if (attempt.producer !== undefined) uncertain.get(attempt.producer)?.delete(attempt);
    if (attempt.hold !== undefined) holdOwners.delete(attempt.hold);
  };
  const holdAttempt = Effect.fn("CodexStartup.holdAttempt")(function* (attempt: StartupAttempt) {
    if (!attempt.mayHaveIssued || attempt.producer === undefined) return;
    const attempts = uncertain.get(attempt.producer) ?? new Set<StartupAttempt>();
    attempts.add(attempt);
    uncertain.set(attempt.producer, attempts);
    if (admission !== undefined && attempt.target !== undefined && attempt.hold === undefined) {
      attempt.hold = yield* admission.holdStartup(attempt.target);
      holdOwners.set(attempt.hold, attempt);
    }
  });
  const abandon = Effect.fn("CodexStartup.abandon")(function* (attempt: StartupAttempt) {
    if (attempt.complete) return;
    attempt.abandoned = true;
    yield* holdAttempt(attempt);
  });
  const unknown = (cause?: unknown) =>
    new ProviderRuntimeBindingError({
      driver: ProviderDriverKind.make("codex"),
      detail:
        "Codex startup has an unconfirmed native effect for this conversation; another admission is not safe.",
      ...(cause === undefined ? {} : { cause }),
    });
  const budgetFailure = (attempt: StartupAttempt) =>
    attempt.mayHaveIssued
      ? unknown()
      : new ProviderAdapterProtocolError({
          driver: ProviderDriverKind.make("codex"),
          detail: "Codex startup exceeded its caller budget before native dispatch",
          cause: new CodexStartupNotDispatched({ operation: attempt.operation, budgetMs }),
        });
  const completeWith = Effect.fn("CodexStartup.completeWith")(function* <A>(handoff: () => A) {
    const attempt = yield* StartupAttemptContext;
    return yield* Effect.suspend(() => {
      if (attempt?.abandoned) return Effect.fail(unknown());
      const value = handoff();
      if (attempt !== undefined) {
        attempt.complete = true;
        attempt.mayHaveIssued = false;
      }
      return Effect.succeed(value);
    });
  });

  return {
    isUncertain: (producer: StartupProducer) => (uncertain.get(producer)?.size ?? 0) > 0,
    run: Effect.fn("CodexStartup.run")(function* <A, E, R>(
      operation: StartupOperation,
      effect: Effect.Effect<A, E, R>,
      target?: ControlAdmission.ControlAdmissionTarget,
    ) {
      const attempt: StartupAttempt = {
        operation,
        deadlineAtMillis: (yield* Clock.currentTimeMillis) + budgetMs,
        producer: undefined,
        target,
        hold: undefined,
        mayHaveIssued: false,
        abandoned: false,
        complete: false,
      };
      const deadline = Effect.sleep(budgetMs).pipe(
        Effect.andThen(
          Effect.gen(function* () {
            if (attempt.complete) return yield* Effect.never;
            yield* abandon(attempt);
            return yield* budgetFailure(attempt);
          }),
        ),
      );
      return yield* effect.pipe(
        Effect.provideService(StartupAttemptContext, attempt),
        Effect.raceFirst(deadline),
        Effect.onExit((exit) => (Exit.isFailure(exit) ? abandon(attempt) : Effect.void)),
      );
    }),
    completeWith,
    // Fork follow-ups inherit the original issuance uncertainty. Keep their
    // raw observers without adding a binding error to the native client port.
    retainRequest: Effect.fn("CodexStartup.retainRequest")(function* <A, E, R>(
      producer: StartupProducer,
      request: Effect.Effect<A, E, R>,
    ) {
      const attempt = yield* StartupAttemptContext;
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          if (attempt?.abandoned) return yield* Effect.interrupt;
          const observation = yield* restore(request).pipe(
            Effect.forkIn(producer.scope, { startImmediately: true }),
          );
          return yield* restore(Fiber.join(observation));
        }),
      );
    }),
    // Install the hold before interruption releases the lifecycle permit.
    withinPermit: Effect.fn("CodexStartup.withinPermit")(function* <A, E, R>(
      effect: Effect.Effect<A, E, R>,
    ) {
      const attempt = yield* StartupAttemptContext;
      if (attempt === undefined) return yield* effect;
      // A simultaneous owner timeout can hand off the permit before this caller's timer runs.
      if (!attempt.complete && (yield* Clock.currentTimeMillis) >= attempt.deadlineAtMillis) {
        yield* abandon(attempt);
        return yield* budgetFailure(attempt);
      }
      return yield* effect.pipe(
        Effect.onExit((exit) => (Exit.isFailure(exit) ? abandon(attempt) : Effect.void)),
        Effect.mapError((cause) =>
          attempt.mayHaveIssued && !attempt.complete && !hasUnknownRuntimeBinding(cause)
            ? unknown(cause)
            : cause,
        ),
      );
    }),
    observe: Effect.fn("CodexStartup.observe")(function* <A, E, R>(
      producer: StartupProducer,
      request: Effect.Effect<A, E, R>,
      confirmation: "reply" | "binding" = "binding",
      method?: string,
    ) {
      const attempt = yield* StartupAttemptContext;
      if (attempt === undefined || attempt.complete) return yield* request;
      if (attempt.abandoned || (attempt.producer !== undefined && attempt.producer !== producer))
        return yield* unknown();
      if (attempt.target?.producer != null) {
        attempt.target = {
          ...attempt.target,
          producer: {
            ...attempt.target.producer,
            runtimeGeneration: producer.generation,
          },
        };
      }
      if (admission !== undefined && attempt.target !== undefined)
        yield* admission.assertOpen(attempt.target);
      else if ((uncertain.get(producer)?.size ?? 0) > 0) return yield* unknown();
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          if (attempt.abandoned) return yield* unknown();
          attempt.producer = producer;
          // A follow-up reply or rejection cannot settle an earlier fork before its handoff.
          const previouslyIssued = attempt.mayHaveIssued;
          attempt.mayHaveIssued = true;
          // Only this native observer outlives the caller. Its late result cannot run publication or a prompt.
          const observation = yield* restore(request).pipe(
            Effect.tap((reply) =>
              Effect.gen(function* () {
                // Only an active caller may use a handshake or unarchive reply to release its fence.
                if (confirmation === "reply") {
                  if (attempt.abandoned) return;
                  attempt.mayHaveIssued = previouslyIssued;
                  if (!previouslyIssued && attempt.hold !== undefined && admission !== undefined) {
                    yield* admission.resolveStartup(attempt.hold, { kind: "not_applied" });
                    clearAttempt(attempt);
                    attempt.hold = undefined;
                  }
                  return;
                }
                const nativeId = replyNativeId(reply);
                if (
                  nativeId !== undefined &&
                  attempt.target?.nativeThreadId != null &&
                  attempt.target.nativeThreadId !== nativeId
                ) {
                  attempt.target = { ...attempt.target, nativeThreadId: null };
                  if (attempt.hold !== undefined && admission !== undefined) {
                    attempt.hold = yield* admission.holdStartup(attempt.target);
                    holdOwners.set(attempt.hold, attempt);
                  }
                  return;
                }
                if (
                  nativeId !== undefined &&
                  attempt.target !== undefined &&
                  (attempt.target.nativeThreadId === null ||
                    attempt.target.nativeThreadId === nativeId)
                ) {
                  attempt.target = { ...attempt.target, nativeThreadId: nativeId };
                  if (attempt.hold !== undefined && admission !== undefined)
                    yield* admission.identifyStartup(attempt.hold, nativeId);
                }
              }),
            ),
            Effect.tapError((error) =>
              isNonApplyingStartupRejection(error, method) || isPreDispatchRefusal(error)
                ? Effect.gen(function* () {
                    if (attempt.abandoned) return;
                    attempt.mayHaveIssued = previouslyIssued;
                    if (previouslyIssued) return;
                    if (attempt.hold !== undefined && admission !== undefined) {
                      yield* admission.resolveStartup(attempt.hold, { kind: "not_applied" });
                    }
                    clearAttempt(attempt);
                    attempt.hold = undefined;
                  })
                : Effect.void,
            ),
            Effect.forkIn(producer.scope, { startImmediately: true }),
          );
          return yield* restore(Fiber.join(observation)).pipe(
            Effect.mapError((cause) =>
              attempt.mayHaveIssued && !hasUnknownRuntimeBinding(cause) ? unknown(cause) : cause,
            ),
          );
        }),
      );
    }),
    retainValidated: Effect.fn("CodexStartup.retainValidated")(function* (
      target: ControlAdmission.ControlAdmissionTarget,
    ) {
      const attempt = yield* StartupAttemptContext;
      if (attempt?.abandoned) return yield* unknown();
      if (admission === undefined) return undefined;
      if (attempt !== undefined) {
        attempt.target = target;
        attempt.hold ??= yield* admission.holdStartup(target);
        holdOwners.set(attempt.hold, attempt);
        return attempt.hold;
      }
      return yield* admission.holdStartup(target);
    }),
    published: Effect.fn("CodexStartup.published")(function* (
      hold: ControlAdmission.StartupHold | undefined,
      binding: ProviderRuntimeBinding,
    ) {
      if (hold !== undefined && admission !== undefined) {
        yield* admission.resolveStartup(hold, { kind: "bound", binding });
        const owner = holdOwners.get(hold);
        if (owner !== undefined) {
          owner.mayHaveIssued = false;
          clearAttempt(owner);
          owner.hold = undefined;
        }
        holdOwners.delete(hold);
      }
      const attempt = yield* StartupAttemptContext;
      if (attempt !== undefined) {
        attempt.mayHaveIssued = false;
        clearAttempt(attempt);
        attempt.hold = undefined;
      }
    }),
    // A bound thread's turn/start has its own acknowledgement deadline.
    // Finish the startup budget before entering that existing turn lifecycle.
    complete: completeWith(() => undefined),
  };
}
