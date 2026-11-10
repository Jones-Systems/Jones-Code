// @effect-diagnostics globalConsole:off -- Standalone build diagnostics expose durable duties independently of optional capture.
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import {
  beginProduction,
  finishProduction,
} from "../../../apps/server/src/jones/workspaceRetirement/ProducerFinalizer.ts";
import { completeProducer } from "./completion.ts";

class FiniteProductionAdmissionError extends Schema.TaggedError<FiniteProductionAdmissionError>()(
  "FiniteProductionAdmissionError",
  { producer: Schema.String, worktree: Schema.String, root: Schema.String, cause: Schema.Defect() },
) {
  override get message(): string {
    return "Durable production admission failed; allocation did not start. Persistence may be uncertain; do not retry blindly.";
  }
}

export const withFiniteProduction = Effect.fn("DirectProducer.finiteProduction")(function* <A, E, R>(
  input: {
    producer: "jones-server-build" | "jones-server-build-exe";
    worktree: string;
    root: string;
  },
  use: (evidence: {
    commandStarted: () => void;
    leaderExited: () => void;
  }) => Effect.Effect<A, E, R>,
  dependencies: {
    begin?: typeof beginProduction;
    finish?: typeof finishProduction;
    capture?: typeof completeProducer;
    report?: (message: string) => void;
  } = {},
) {
  let commandStarted = false;
  let leaderExited = false;
  const report = (message: string) => {
    try {
      (dependencies.report ?? console.error)(message);
    } catch {
      // Diagnostics cannot replace the native outcome or replay production.
    }
  };

  // acquireUseRelease admits uninterruptibly and installs release before use.
  // The inner scope releases the existing child handles before our finalizer.
  return yield* Effect.acquireUseRelease(
    Effect.tryPromise({
      try: () => (dependencies.begin ?? beginProduction)(input.worktree, input.producer, [input.root]),
      catch: (cause) => new FiniteProductionAdmissionError({ ...input, cause }),
    }),
    () =>
      Effect.scoped(
        Effect.suspend(() =>
          use({
            commandStarted: () => {
              commandStarted = true;
            },
            leaderExited: () => {
              leaderExited = true;
            },
          }),
        ),
      ),
    (session, exit) =>
      Effect.promise(async () => {
        const reference = `id=${session.record.id}; root=${input.root}; path=${session.path}`;
        const status = Exit.isSuccess(exit)
          ? "succeeded"
          : !Cause.hasInterrupts(exit.cause) && (!commandStarted || leaderExited)
            ? "failed"
            : undefined;
        if (status === undefined) {
          // Interruption, spawn/error and scope release do not prove a native
          // terminal or drained cancellation. Keep the already persisted active duty.
          report(`[${input.producer}] native_terminal_not_observed; active duty ${reference}`);
          return;
        }
        try {
          // These entrypoints still use Effect's leader-only receipt. Even an
          // absent root cannot be called disposed while a writer may outlive it.
          await (dependencies.finish ?? finishProduction)(session, status, [
            "writer_completion_not_observed",
          ]);
          report(`[${input.producer}] writer_completion_not_observed; unresolved duty ${reference}`);
        } catch {
          // finishProduction may mutate memory or publish before rejecting.
          // Retain the same exact durable duty; do not retry or claim its stored status.
          report(`[${input.producer}] finalization_not_confirmed; durable duty ${reference}`);
        }
        try {
          (dependencies.capture ?? completeProducer)({
            producer: input.producer,
            worktree: input.worktree,
            status,
            roots: [input.root],
          });
        } catch {
          report(
            `[${input.producer}] optional_capture_unavailable; native outcome=${status}; ${reference}`,
          );
        }
      }),
  );
});
