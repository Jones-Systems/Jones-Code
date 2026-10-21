import type { BackgroundPolicySnapshot } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import { BackgroundPolicy } from "../../background/BackgroundPolicy.ts";
import { TerminalManager } from "../../terminal/Manager.ts";
import type { HostWorkSnapshot } from "../updates/followerPolicy.ts";
import { readHostWork } from "../updates/hostWork.ts";

/** What an automatic activation waits on. Each read is current when it is made. */
export interface FleetQuietSignals {
  /** Epoch milliseconds of the latest input from any client, never before this server started. */
  readonly lastInputAt: Effect.Effect<number>;
  readonly runningTerminals: Effect.Effect<number>;
  readonly work: Effect.Effect<HostWorkSnapshot | "unavailable">;
}

/**
 * Input comes from client activity leases: a client reports `recentlyInteracted` while its user
 * is using it, and an unexpired lease that says so counts as input now. History does not survive
 * a restart, so the idle window restarts with the server.
 */
export const makeFleetQuietSignals = Effect.gen(function* () {
  const background = yield* BackgroundPolicy;
  const terminals = yield* TerminalManager;
  const sql = yield* SqlClient.SqlClient;
  let lastInput = yield* Clock.currentTimeMillis;
  const note = (snapshot: BackgroundPolicySnapshot) => {
    for (const lease of snapshot.leases)
      if (lease.recentlyInteracted)
        lastInput = Math.max(lastInput, DateTime.toEpochMillis(lease.updatedAt));
  };
  const { latest, changes } = yield* background.subscribe;
  note(latest);
  yield* changes.pipe(
    Stream.runForEach((snapshot) => Effect.sync(() => note(snapshot))),
    Effect.forkScoped,
  );
  const running = new Map<string, boolean>();
  const key = (threadId: string, terminalId: string) => JSON.stringify([threadId, terminalId]);
  const unsubscribe = yield* terminals.subscribeMetadata((event) =>
    Effect.sync(() => {
      if (event.type === "snapshot") {
        running.clear();
        for (const terminal of event.terminals)
          running.set(key(terminal.threadId, terminal.terminalId), terminal.hasRunningSubprocess);
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
  return {
    lastInputAt: Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const snapshot = yield* background.snapshot;
      note(snapshot);
      const interacting = snapshot.leases.some(
        (lease) => lease.recentlyInteracted && DateTime.toEpochMillis(lease.expiresAt) > now,
      );
      return interacting ? now : lastInput;
    }),
    runningTerminals: Effect.sync(() => [...running.values()].filter(Boolean).length),
    work: readHostWork.pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
      Effect.catch(() => Effect.succeed("unavailable" as const)),
    ),
  } satisfies FleetQuietSignals;
});
