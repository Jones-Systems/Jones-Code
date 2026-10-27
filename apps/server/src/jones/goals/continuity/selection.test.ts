import { describe, expect, it } from "vite-plus/test";
import type { ContinuityOwner, ContinuitySelectionRequest, ContinuitySnapshot } from "@t3tools/contracts";
import { canonicalJson, continuityDigest, CONTINUITY_LIMITS, utf8Bytes } from "./model.ts";
import { continuityFieldValue, selectContinuity } from "./selection.ts";

function snapshot(): ContinuitySnapshot {
  return {
    schemaVersion: 1, owner: { environmentId: "env", threadId: "thread", providerInstanceId: "codex", providerThreadId: "pt", driver: "codex", nativeThreadId: "native" } as ContinuityOwner,
    attachment: { state: "attached" }, generation: "9007199254740993", status: "active", focus: "Exact work",
    intent: { goalId: "goal", revision: "1", northStar: "Outcome", preservedQualities: [], finishItems: [{ id: "finish", text: "Finish" }],
      source: { type: "owner_request", reference: "ref", identity: "owner" }, derivation: null, initializationMode: "direct", predecessor: null,
      startEvidence: [{ id: "start", reference: "ref", assertion: "Requested" }] },
    boundaries: [], blockers: [], decisions: Array.from({ length: 9 }, (_, i) => ({ id: `d${i}`, text: `decision ${i}` })), nextActions: [],
    evidence: [{ id: "start", reference: "ref", assertion: "Requested" }], unresolvedEffect: null, recentDispositions: [], completionEvidenceIds: [],
  };
}
function request(state: ContinuitySnapshot, fields: ContinuitySelectionRequest["fields"]): ContinuitySelectionRequest {
  return { goalId: state.intent.goalId, revision: state.intent.revision, generation: state.generation, fields };
}

describe("all-or-refused selective continuity reload", () => {
  it("paginates four items bound to generation and full-field digest without losing exact counters", () => {
    const state = snapshot();
    const before = canonicalJson(state);
    const first = selectContinuity(state, request(state, [{ field: "decisions" }]));
    expect(first.outcome).toBe("selected");
    if (first.outcome !== "selected") return;
    expect(first.snapshotDigest).toBe(continuityDigest(state));
    expect(first.generation).toBe("9007199254740993");
    expect(first.fields[0]!.value).toEqual(state.decisions.slice(0, 4));
    expect(first.fields[0]!.fieldDigest).toBe(continuityDigest(state.decisions));
    const cursor = first.fields[0]!.nextCursor;
    if (cursor === null) throw new Error("Expected next page");
    const second = selectContinuity(state, request(state, [{ field: "decisions", cursor }]));
    expect(second.outcome).toBe("selected");
    if (second.outcome !== "selected") return;
    expect(second.fields[0]!.value).toEqual(state.decisions.slice(4, 8));
    const finalCursor = second.fields[0]!.nextCursor;
    if (finalCursor === null) throw new Error("Expected final page");
    const last = selectContinuity(state, request(state, [{ field: "decisions", cursor: finalCursor }]));
    expect(last.outcome).toBe("selected");
    if (last.outcome === "selected") {
      expect(last.fields[0]!.value).toEqual(state.decisions.slice(8));
      expect(last.fields[0]!.nextCursor).toBeNull();
    }
    expect(canonicalJson(state)).toBe(before);
  });

  it("refuses stale, foreign and digest-mismatched cursors with no partial data", () => {
    const state = snapshot();
    const cursor = { generation: state.generation, fieldDigest: continuityDigest(state.decisions), offset: 4 };
    for (const [change, reason] of [
      [{ generation: "1" }, "stale"], [{ fieldDigest: "a".repeat(64) }, "digest_mismatch"],
      [{ offset: 3 }, "selection_bounds"], [{ offset: 12 }, "selection_bounds"],
    ] as const) expect(selectContinuity(state, request(state, [{ field: "focus" }, { field: "decisions", cursor: { ...cursor, ...change } }]))).toEqual({ outcome: "refused", reason });
    expect(selectContinuity(state, { ...request(state, [{ field: "focus" }]), goalId: "other" })).toEqual({ outcome: "refused", reason: "stale" });
  });

  it("enforces eight distinct fields and four collection items across the complete page", () => {
    const state = snapshot();
    expect(selectContinuity(state, request(state, [{ field: "focus" }, { field: "focus" }]))).toEqual({ outcome: "refused", reason: "selection_bounds" });
    expect(selectContinuity(state, request(state, ["intent", "focus", "status", "attachment", "blockers", "nextActions", "unresolvedEffect", "finishItems", "preservedQualities"].map((field) => ({ field })) as ContinuitySelectionRequest["fields"]))).toEqual({ outcome: "refused", reason: "selection_bounds" });
    expect(selectContinuity(state, request(state, [{ field: "decisions" }, { field: "evidence" }]))).toEqual({ outcome: "refused", reason: "selection_bounds" });
    const header = continuityFieldValue(state, "intent");
    expect(header).not.toHaveProperty("finishItems");
    expect(header).not.toHaveProperty("startEvidence");
    expect(selectContinuity(state, request(state, [{ field: "intent" }, { field: "finishItems" }])).outcome).toBe("selected");
  });

  it("counts complete UTF-8 response bytes and refuses oversized scalar data atomically", () => {
    const state = snapshot();
    const result = selectContinuity(state, request(state, [{ field: "intent" }, { field: "focus" }]));
    expect(result.outcome).toBe("selected");
    expect(utf8Bytes(canonicalJson(result))).toBeLessThanOrEqual(CONTINUITY_LIMITS.selectionBytes);
    const oversized = { ...state, focus: "界".repeat(6000) };
    expect(selectContinuity(oversized, request(oversized, [{ field: "status" }, { field: "focus" }]))).toEqual({ outcome: "refused", reason: "selection_bounds" });
    expect(selectContinuity({ ...state, schemaVersion: 2 } as unknown as ContinuitySnapshot, request(state, [{ field: "status" }]))).toEqual({ outcome: "refused", reason: "corrupt_snapshot" });
  });
});
