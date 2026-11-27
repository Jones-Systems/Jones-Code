import {
  EnvironmentId,
  RunId,
  ThreadId,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import { presentThreadShell } from "@t3tools/client-runtime/state/shell";
import { DateTime } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { makeRawThreadShell } from "../../test-fixtures";
import { orchestratorOptionsKey, orchestratorRequest } from "./draft";
import {
  adjacentOrchestratorThread,
  FINISHED_WINDOW_MS,
  orchestratorQueues,
  orchestratorThreadRef,
} from "./queues";

const now = Date.parse("2026-11-27T19:00:00.000Z");
const at = (age: number) => DateTime.makeUnsafe(now - age);

function thread(
  id: string,
  age: number | null,
  changes: Partial<OrchestrationV2ThreadShell> = {},
  environment = "local",
) {
  return presentThreadShell(
    EnvironmentId.make(environment),
    makeRawThreadShell({
      id: ThreadId.make(id),
      updatedAt: at(0),
      latestRunId: RunId.make(`run-${id}`),
      ...(age === null ? {} : { latestRunCompletedAt: at(age) }),
      ...changes,
    }),
  );
}

describe("mobile Orchestrator queues", () => {
  it("orders by recorded completion and keeps a visited thread eligible until the hour expires", () => {
    const earlier = thread("earlier", 1_000, { lastVisitedAt: at(0) });
    const latest = thread("latest", 500);
    const legacy = thread("legacy-unknown-end", null);
    const expired = thread("expired", FINISHED_WINDOW_MS);
    const queued = thread("queued", 100);
    const ts = [earlier, latest, legacy, expired, queued];
    const queue = (time: number) => orchestratorQueues(ts, time, (row) => row === queued).done;
    expect(queue(now)).toEqual([latest, earlier]);
    expect(queue(now + FINISHED_WINDOW_MS - 1_000)).toEqual([latest]);
  });

  it("puts approvals, questions, and rolled-up child requests in Input first", () => {
    const pending = thread("pending", 200);
    const approval = { ...pending, hasPendingApprovals: true };
    const question = { ...thread("question", 300), hasPendingUserInput: true };
    const descendant = { ...thread("parent", 400), pendingSubagentRequest: "approval" as const };
    const working = thread("working", 100, { status: "running", activeRunId: RunId.make("next") });
    const settled = thread("settled", 100, { settledOverride: "settled", settledAt: at(0) });
    const archived = thread("archived", 100, { archivedAt: at(0) });
    const result = orchestratorQueues(
      [working, descendant, archived, question, approval, settled],
      now,
      () => false,
    );
    expect(result.input).toEqual([descendant, approval, question]);
    expect(result.done).toEqual([]);
  });

  it("skips removed rows on swipe and keeps identical thread ids on different environments distinct", () => {
    const a = orchestratorThreadRef(thread("same", 0));
    const b = orchestratorThreadRef(thread("same", 1, {}, "remote"));
    const c = orchestratorThreadRef(thread("third", 2));
    const d = orchestratorThreadRef(thread("fourth", 3));
    const entry = [a, b, c, d];
    expect(adjacentOrchestratorThread(b, entry, entry, 1)).toEqual(c);
    expect(adjacentOrchestratorThread(b, [a, d], entry, 1)).toEqual(d);
    expect(adjacentOrchestratorThread(b, [a, d], entry, -1)).toEqual(a);
    expect(adjacentOrchestratorThread(d, [a, d], entry, 1)).toBeUndefined();
    expect(orchestratorOptionsKey(a)).not.toBe(orchestratorOptionsKey(b));
  });

  it("includes routing instructions and preserves dictated punctuation and line breaks", () => {
    const text = "Check the open request.\nThen let me know where it went.";
    expect(orchestratorRequest(text)).toContain("Workstream");
    expect(orchestratorRequest(text).endsWith(`\n\n${text}`)).toBe(true);
  });
});
