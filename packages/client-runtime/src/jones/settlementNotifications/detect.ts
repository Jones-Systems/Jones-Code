import type { EnvironmentId, OrchestrationV2ThreadShell, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";

import type { EnvironmentShellState } from "../../state/shell.ts";
import { settlementThreadEvidence, type SettlementThreadFamilyEvidence } from "./threadEvidence.ts";
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

export function isSettlementChild(
  thread: Pick<OrchestrationV2ThreadShell, "lineage" | "delegatedFrom">,
): boolean {
  return thread.lineage.relationshipToParent === "subagent" || thread.delegatedFrom !== undefined;
}

export function observeEnvironment(
  prev: EnvironmentSettlementBaseline | null,
  input: {
    environmentId: EnvironmentId;
    state: EnvironmentShellState;
    nowMs: number;
    staleSince: number | null;
    families?: ReadonlyArray<SettlementThreadFamilyEvidence>;
  },
): ObserveResult {
  if (input.state.status !== "live" || Option.isNone(input.state.snapshot)) return { kind: "skip" };
  const snapshot = input.state.snapshot.value;
  const lowerSequence = prev !== null && snapshot.snapshotSequence < prev.snapshotSequence;
  if (lowerSequence) {
    const staleSince = input.staleSince ?? input.nowMs;
    if (input.nowMs - staleSince < SEQUENCE_RESET_MS) return { kind: "skip", staleSince };
  }

  // Omitted shell rows retain their prior epoch until a removal or family read proves absence.
  const threads: Record<string, number | null> = lowerSequence ? {} : { ...prev?.threads };
  const threadTitles = new Map<ThreadId, string>();
  const evidence = settlementThreadEvidence(input.state, input.families);
  const goneThreadIds = evidence.gone;
  const arrivals: SettlementArrival[] = [];
  let watermarkMs: number | null = lowerSequence ? null : (prev?.watermarkMs ?? null);
  for (const thread of evidence.threads) {
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
    if (prev === null || lowerSequence || settledAtMs === null || isSettlementChild(thread))
      continue;
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
  for (const id of goneThreadIds) delete threads[id];
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
