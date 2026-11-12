import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import type {
  ServiceLauncherClient,
  ServiceLauncherClientError,
} from "../../cloud/serviceLauncherClient.ts";
import { OrchestratorV2 } from "../../orchestration-v2/Orchestrator.ts";
import { TerminalManager } from "../../terminal/Manager.ts";
import { readHostWork } from "./hostWork.ts";
import { workBlockers } from "./followerPolicy.ts";
import { UpdateAdmissionFence } from "./UpdateAdmissionFence.ts";

/** A lost answer keeps admission closed; it never becomes permission to retry a possibly accepted exit. */
export const launcherHandoffAttempt = Effect.fn("LauncherHandoff.attempt")(function* (input: {
  readonly fence: UpdateAdmissionFence["Service"];
  readonly quiet: Effect.Effect<boolean>;
  readonly request: Effect.Effect<
    "accepted" | "deferred" | "unavailable",
    ServiceLauncherClientError
  >;
  readonly reopened: Effect.Effect<void>;
}) {
  if (!(yield* input.fence.attached)) return true;
  const ticket = yield* input.fence.raise("automatic").pipe(Effect.orElseSucceed(() => undefined));
  if (ticket === undefined) return true;
  let mayHaveAccepted = false;
  return yield* Effect.gen(function* () {
    if (!(yield* input.quiet) || !(yield* ticket.handOff)) return true;
    // From the send until a definitive refusal, cancellation and timeout leave the fence closed.
    mayHaveAccepted = true;
    const outcome = yield* input.request;
    if (outcome === "accepted") return false;
    mayHaveAccepted = false;
    return outcome === "deferred";
  }).pipe(
    Effect.catchCause(() =>
      Effect.logWarning(
        "Launcher handoff outcome is unproven; work admission remains closed until process exit.",
      ).pipe(Effect.as(false)),
    ),
    Effect.ensuring(
      Effect.suspend(() =>
        mayHaveAccepted
          ? ticket.accept
          : ticket.release.pipe(
              Effect.flatMap((refused) => (refused ? input.reopened : Effect.void)),
            ),
      ),
    ),
  );
});

/** Uses the same process-work fence and durable quiet oracle as automatic application updates. */
export const startLauncherHandoff = Effect.fn("LauncherHandoff.start")(function* (
  launcher: ServiceLauncherClient["Service"],
) {
  if (
    launcher.requestLauncherHandoff === undefined ||
    launcher.currentVersion === launcher.launcherVersion
  )
    return;
  const fence = yield* Effect.serviceOption(UpdateAdmissionFence);
  const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
  const terminals = yield* Effect.serviceOption(TerminalManager);
  const orchestrator = yield* Effect.serviceOption(OrchestratorV2);
  if (
    Option.isNone(fence) ||
    Option.isNone(sql) ||
    Option.isNone(terminals) ||
    Option.isNone(orchestrator)
  )
    return;
  const running = new Map<string, boolean>();
  let sawTerminalSnapshot = false;
  const key = (threadId: string, terminalId: string) => JSON.stringify([threadId, terminalId]);
  const unsubscribe = yield* terminals.value.subscribeMetadata((event) =>
    Effect.sync(() => {
      if (event.type === "snapshot") {
        running.clear();
        for (const terminal of event.terminals)
          running.set(key(terminal.threadId, terminal.terminalId), terminal.hasRunningSubprocess);
        sawTerminalSnapshot = true;
      } else if (event.type === "upsert") {
        running.set(
          key(event.terminal.threadId, event.terminal.terminalId),
          event.terminal.hasRunningSubprocess,
        );
      } else {
        running.delete(key(event.threadId, event.terminalId));
      }
    }),
  );
  yield* Effect.addFinalizer(() => Effect.sync(unsubscribe));
  const quiet = readHostWork.pipe(
    Effect.provideService(SqlClient.SqlClient, sql.value),
    Effect.map(
      (work) =>
        sawTerminalSnapshot &&
        workBlockers(work, [...running.values()].filter(Boolean).length).length === 0,
    ),
    Effect.orElseSucceed(() => false),
  );
  const attempt = launcherHandoffAttempt({
    fence: fence.value,
    quiet,
    request: launcher.requestLauncherHandoff,
    reopened: orchestrator.value.resumeQueuedRuns.pipe(
      Effect.catchCause(() =>
        Effect.logWarning("Could not resume work after a deferred launcher handoff."),
      ),
      Effect.asVoid,
    ),
  });
  yield* Effect.gen(function* () {
    for (;;) {
      if (!(yield* attempt)) return;
      yield* Effect.sleep("30 seconds");
    }
  }).pipe(Effect.forkScoped);
});
