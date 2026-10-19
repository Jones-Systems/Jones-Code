import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";

import type { EnvironmentShellState } from "../../state/shell.ts";
import {
  SEQUENCE_RESET_MS,
  type EnvironmentSettlementBaseline,
  type SettlementArrival,
} from "./model.ts";

export type ObserveResult =
  | { kind: "skip"; staleSince?: number }
  | { kind: "baseline"; baseline: EnvironmentSettlementBaseline }
  | {
      kind: "compare";
      baseline: EnvironmentSettlementBaseline;
      arrivals: SettlementArrival[];
      threadTitles: ReadonlyMap<ThreadId, string>;
      goneThreadIds: ReadonlySet<ThreadId>;
    };

export function observeEnvironment(
  prev: EnvironmentSettlementBaseline | null,
  input: {
    environmentId: EnvironmentId;
    state: EnvironmentShellState;
    nowMs: number;
    staleSince: number | null;
  },
): ObserveResult {
  if (input.state.status !== "live" || Option.isNone(input.state.snapshot)) return { kind: "skip" };
  const snapshot = input.state.snapshot.value;
  const lowerSequence = prev !== null && snapshot.snapshotSequence < prev.snapshotSequence;
  if (lowerSequence) {
    const staleSince = input.staleSince ?? input.nowMs;
    if (input.nowMs - staleSince < SEQUENCE_RESET_MS) return { kind: "skip", staleSince };
  }

  const threads: Record<string, number | null> = {};
  const threadTitles = new Map<ThreadId, string>();
  const goneThreadIds = new Set<ThreadId>();
  const arrivals: SettlementArrival[] = [];
  let watermarkMs: number | null = null;
  for (const thread of [...snapshot.threads, ...snapshot.archivedThreads]) {
    const updatedAtMs = DateTime.toEpochMillis(thread.updatedAt);
    watermarkMs = watermarkMs === null ? updatedAtMs : Math.max(watermarkMs, updatedAtMs);
    const settledAtMs =
      thread.settledOverride === "settled" && thread.settledAt !== null
        ? DateTime.toEpochMillis(thread.settledAt)
        : null;
    Object.defineProperty(threads, thread.id, {
      value: settledAtMs,
      enumerable: true,
      configurable: true,
    });
    if (thread.deletedAt !== null) {
      goneThreadIds.add(thread.id);
      continue;
    }
    threadTitles.set(thread.id, thread.title);
    if (prev === null || lowerSequence || settledAtMs === null) continue;
    const known = Object.hasOwn(prev.threads, thread.id);
    if (
      known
        ? prev.threads[thread.id] !== settledAtMs
        : prev.watermarkMs === null || DateTime.toEpochMillis(thread.createdAt) >= prev.watermarkMs
    ) {
      arrivals.push({
        environmentId: input.environmentId,
        threadId: thread.id,
        title: thread.title,
        settledAtMs,
      });
    }
  }
  if (prev !== null) {
    for (const id of Object.keys(prev.threads)) {
      if (!Object.hasOwn(threads, id)) goneThreadIds.add(id as ThreadId);
    }
  }
  const baseline: EnvironmentSettlementBaseline = {
    version: 1,
    environmentId: input.environmentId,
    snapshotSequence: snapshot.snapshotSequence,
    watermarkMs,
    threads,
    confirmedAtMs: input.nowMs,
  };
  return prev === null || lowerSequence
    ? { kind: "baseline", baseline }
    : { kind: "compare", baseline, arrivals, threadTitles, goneThreadIds };
}
