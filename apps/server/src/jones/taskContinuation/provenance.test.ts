import { describe, expect, it } from "vite-plus/test";
import { ProviderDriverKind, ProviderThreadId, RunAttemptId, RunId } from "@t3tools/contracts";
import { nativeSessionProvenance } from "./provenance.ts";
import { fixture } from "./testkit.ts";

describe("captured delegated Claude session provenance", () => {
  it("reports the original capture after a later wake replaces the row's conversation", () => {
    const f = fixture();
    const wake = { ...f.run, id: RunId.make("later-wake"), ordinal: 2 };
    const child = {
      ...f.child,
      runs: [f.run, wake],
      attempts: [
        ...f.child.attempts,
        {
          ...f.attempt,
          id: RunAttemptId.make("wake-attempt"),
          runId: wake.id,
          nativeThreadId: "replacement-native",
        },
      ],
      providerThreads: [
        {
          ...f.provider,
          nativeThreadRef: {
            ...f.provider.nativeThreadRef!,
            nativeId: "replacement-native",
          },
        },
      ],
    };
    expect(nativeSessionProvenance(f.task, child)).toEqual({
      nativeSessionId: "original-native",
      sourceRunId: f.run.id,
      sourceAttemptId: f.attempt.id,
      evidence: "attempt",
    });
  });
  it("leaves unsupported, incomplete, unlinked or contradictory source evidence unavailable", () => {
    const f = fixture();
    for (const task of [
      { ...f.task, driver: ProviderDriverKind.make("codex") },
      { ...f.task, status: "running" as const, result: null },
      { ...f.task, childThreadId: null },
    ])
      expect(nativeSessionProvenance(task, f.child)).toBeUndefined();
    const { nativeThreadId: _capture, ...uncaptured } = f.attempt;
    for (const child of [
      { ...f.child, attempts: [uncaptured] },
      { ...f.child, contextTransfers: [] },
      {
        ...f.child,
        providerTurns: [
          {
            ...f.child.providerTurns[0]!,
            providerThreadId: ProviderThreadId.make("conflicting-row"),
          },
        ],
      },
    ])
      expect(nativeSessionProvenance(f.task, child)).toBeUndefined();
  });
});
