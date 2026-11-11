// @effect-diagnostics nodeBuiltinImport:off globalConsole:off -- Standalone build diagnostics expose durable duties independently of optional capture.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import {
  beginProduction,
  finishProduction,
  productionSpawnIntent,
  productionSpawnResult,
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

export interface FiniteProductionEvidence {
  leaderExited: () => void;
  spawn: <A extends { readonly pid: number }, E, R>(
    start: () => Effect.Effect<A, E, R>,
    detached: boolean,
  ) => Effect.Effect<A, E | FiniteProductionAdmissionError, R>;
}

export const withFiniteProduction = Effect.fn("DirectProducer.finiteProduction")(function* <
  A,
  E,
  R,
>(
  input: {
    producer: "jones-server-build" | "jones-server-build-exe";
    worktree: string;
    root: string;
  },
  use: (evidence: FiniteProductionEvidence) => Effect.Effect<A, E, R>,
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
      try: async () => {
        // Only lstat ENOENT at the requested repository's .git permits an
        // unowned distribution build. Present/broken/unreadable Git blocks.
        try { await NodeFSP.lstat(NodePath.join(input.worktree, ".git")); }
        catch (cause) {
          if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
          report(`[${input.producer}] git_absent; unowned build root=${input.root}`);
          return null;
        }
        return (dependencies.begin ?? beginProduction)(input.worktree, input.producer, [input.root]);
      },
      catch: (cause) => new FiniteProductionAdmissionError({ ...input, cause }),
    }),
    (session) =>
      Effect.scoped(
        Effect.suspend(() =>
          use({
            leaderExited: () => {
              leaderExited = true;
            },
            spawn: (start, detached) => Effect.uninterruptibleMask((restore) =>
              Effect.gen(function* () {
                const intent = session === null ? null : yield* Effect.tryPromise({
                  try: () => productionSpawnIntent(session),
                  catch: (cause) => new FiniteProductionAdmissionError({ ...input, cause }),
                });
                commandStarted = true;
                const child = yield* restore(Effect.suspend(start));
                if (session !== null) yield* Effect.promise(async () => {
                  try { await productionSpawnResult(session, intent, { kind: "session", pid: child.pid, detached }); }
                  catch { report(`[${input.producer}] spawn_binding_uncertain; id=${session.record.id}; path=${session.path}`); }
                });
                return child;
              }),
            ),
          }),
        ),
      ),
    (session, exit) =>
      Effect.promise(async () => {
        if (session === null) return;
        const reference = `id=${session.record.id}; root=${input.root}; path=${session.path}`;
        const status = Exit.isSuccess(exit)
          ? "succeeded"
          : !Cause.hasInterrupts(exit.cause) && (!commandStarted || leaderExited)
            ? "failed"
            : undefined;
        try {
          const record = await (dependencies.finish ?? finishProduction)(session, status,
            [],
            { inProcessSettled: Exit.isSuccess(exit) });
          report(`[${input.producer}] native=${record.status}; allocation=${record.outcome}; reasons=${record.reasons.join(",") || "none"}; ${reference}`);
        } catch {
          report(`[${input.producer}] finalization_not_confirmed; durable duty ${reference}`);
        }
        if (status === undefined) {
          report(`[${input.producer}] native_terminal_not_observed; durable duty ${reference}`);
          return;
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
