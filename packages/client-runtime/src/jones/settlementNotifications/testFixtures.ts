import { EnvironmentId, ThreadId, type OrchestrationV2ThreadShell } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";

import { v2ThreadShell } from "../../state/orchestrationV2TestFixtures.ts";
import type { EnvironmentShellState } from "../../state/shell.ts";
import type { EnvironmentSettlementBaseline, SettlementEntry } from "./model.ts";
import { settlementEntryId } from "./model.ts";

export const env = EnvironmentId.make("settlement-test-environment");
export const otherEnv = EnvironmentId.make("settlement-test-other-environment");

export function thread(
  id = "thread-a",
  settledAtMs: number | null = null,
  overrides: Partial<OrchestrationV2ThreadShell> = {},
): OrchestrationV2ThreadShell {
  return {
    ...v2ThreadShell,
    id: ThreadId.make(id),
    title: id,
    createdAt: DateTime.makeUnsafe(100),
    updatedAt: DateTime.makeUnsafe(settledAtMs ?? 100),
    settledOverride: settledAtMs === null ? null : "settled",
    settledAt: settledAtMs === null ? null : DateTime.makeUnsafe(settledAtMs),
    ...overrides,
  };
}

export function shell(
  sequence: number,
  threads: readonly OrchestrationV2ThreadShell[],
  archivedThreads: readonly OrchestrationV2ThreadShell[] = [],
  status: EnvironmentShellState["status"] = "live",
): EnvironmentShellState {
  return {
    status,
    error: Option.none(),
    snapshot: Option.some({
      schemaVersion: 1,
      snapshotSequence: sequence,
      projects: [],
      threads,
      archivedThreads,
    }),
  };
}

export function baseline(
  overrides: Partial<EnvironmentSettlementBaseline> = {},
): EnvironmentSettlementBaseline {
  return {
    version: 1,
    environmentId: env,
    snapshotSequence: 1,
    watermarkMs: 100,
    threads: { "thread-a": null },
    confirmedAtMs: 100,
    ...overrides,
  };
}

export function entry(id = "thread-a", settledAtMs = 200, environmentId = env): SettlementEntry {
  const threadId = ThreadId.make(id);
  return {
    id: settlementEntryId(environmentId, threadId, settledAtMs),
    environmentId,
    threadId,
    title: id,
    settledAtMs,
    detectedAtMs: settledAtMs,
    threadGone: false,
  };
}
