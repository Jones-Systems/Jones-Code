import { ProviderDriverKind } from "@t3tools/contracts";
import {
  ProviderAdapterProtocolError,
  ProviderRuntimeBindingError,
} from "@t3tools/provider-core/server/ProviderAdapter";
import * as CodexErrors from "effect-codex-app-server/errors";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

// This is a caller budget, not a measured Codex latency or a process-exit deadline.
export const CODEX_STARTUP_BUDGET_MS = 60_000;

type StartupOperation = "ensure" | "resume" | "start";

interface StartupProducer {
  readonly generation: string;
  readonly scope: Scope.Closeable;
}

interface StartupAttempt {
  producer?: StartupProducer;
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
  { operation: Schema.Literals(["ensure", "resume", "start"]), budgetMs: Schema.Number },
) {
  override get message(): string {
    return "Codex startup exceeded its caller budget before an unconfirmed native request.";
  }
}

export function makeCodexStartup(budgetMs = CODEX_STARTUP_BUDGET_MS) {
  const uncertain = new WeakSet<StartupProducer>();
  const abandon = (attempt: StartupAttempt) =>
    Effect.sync(() => {
      if (attempt.complete) return;
      attempt.abandoned = true;
      if (attempt.mayHaveIssued && attempt.producer !== undefined)
        uncertain.add(attempt.producer);
    });
  const unknown = (cause?: unknown) =>
    new ProviderRuntimeBindingError({
      driver: ProviderDriverKind.make("codex"),
      detail: "Codex startup has an unconfirmed native effect; another admission is not safe.",
      ...(cause === undefined ? {} : { cause }),
    });

  return {
    isUncertain: (producer: StartupProducer) => uncertain.has(producer),
    run: Effect.fn("CodexStartup.run")(function* <A, E, R>(
      operation: StartupOperation,
      effect: Effect.Effect<A, E, R>,
    ) {
      const attempt: StartupAttempt = {
        mayHaveIssued: false,
        abandoned: false,
        complete: false,
      };
      const deadline = Effect.sleep(budgetMs).pipe(
        Effect.andThen(
          Effect.suspend(() =>
            attempt.complete
              ? Effect.never
              : Effect.fail(
                  attempt.mayHaveIssued
                    ? unknown()
                    : new ProviderAdapterProtocolError({
                        driver: ProviderDriverKind.make("codex"),
                        detail: "Codex startup exceeded its caller budget before native dispatch",
                        cause: new CodexStartupNotDispatched({ operation, budgetMs }),
                      }),
                ),
          ),
        ),
      );
      return yield* effect.pipe(
        Effect.provideService(StartupAttemptContext, attempt),
        Effect.raceFirst(deadline),
        Effect.onExit((exit) => (Exit.isFailure(exit) ? abandon(attempt) : Effect.void)),
      );
    }),
    // Install this inside the lifecycle permit. The fence must exist before
    // interruption releases that permit and wakes the next queued admission.
    withinPermit: Effect.fn("CodexStartup.withinPermit")(function* <A, E, R>(
      effect: Effect.Effect<A, E, R>,
    ) {
      const attempt = yield* StartupAttemptContext;
      if (attempt === undefined) return yield* effect;
      return yield* effect.pipe(
        Effect.onExit((exit) => (Exit.isFailure(exit) ? abandon(attempt) : Effect.void)),
        Effect.mapError((cause) =>
          attempt.mayHaveIssued && !attempt.complete ? unknown(cause) : cause,
        ),
      );
    }),
    observe: Effect.fn("CodexStartup.observe")(function* <A, E, R>(
      producer: StartupProducer,
      request: Effect.Effect<A, E, R>,
      confirmation: "reply" | "binding" = "binding",
    ) {
      const attempt = yield* StartupAttemptContext;
      if (uncertain.has(producer)) return yield* unknown();
      if (attempt === undefined || attempt.complete) return yield* request;
      if (
        attempt.abandoned ||
        (attempt.producer !== undefined && attempt.producer !== producer)
      )
        return yield* unknown();
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          attempt.producer = producer;
          attempt.mayHaveIssued = true;
          // Retain only the raw RPC observer in this exact producer's scope.
          // Late replies never run binding code or clear an abandoned generation's fence.
          const observation = yield* restore(request).pipe(
            // Handshake/unarchive replies confirm their whole effect. Opening a
            // thread still needs the caller's binding publication after its reply.
            Effect.tap(() =>
              confirmation === "reply"
                ? Effect.sync(() => {
                    attempt.mayHaveIssued = false;
                  })
                : Effect.void,
            ),
            Effect.tapError((error) =>
              Schema.is(CodexErrors.CodexAppServerRequestError)(error)
                ? Effect.sync(() => {
                    attempt.mayHaveIssued = false;
                  })
                : Effect.void,
            ),
            Effect.forkIn(producer.scope, { startImmediately: true }),
          );
          return yield* restore(Fiber.join(observation));
        }),
      );
    }),
    // A bound thread's turn/start has its own acknowledgement deadline.
    // Finish the startup budget before entering that existing turn lifecycle.
    complete: Effect.gen(function* () {
      const attempt = yield* StartupAttemptContext;
      if (attempt !== undefined) {
        attempt.complete = true;
        attempt.mayHaveIssued = false;
      }
    }),
  };
}
