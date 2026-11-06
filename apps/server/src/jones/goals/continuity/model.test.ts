import { describe, expect, it } from "vite-plus/test";
import type {
  ContinuityIntent,
  ContinuityOwner,
  ContinuityPatch,
  ContinuitySnapshot,
  ContinuityTarget,
  ContinuityWitness,
} from "@t3tools/contracts";
import {
  abandonContinuity,
  canonicalJson,
  checkpointContinuity,
  closeContinuity,
  continuityDigest,
  CONTINUITY_LIMITS,
  detachContinuity,
  handoffContinuity,
  incrementCounter,
  initializeContinuity,
  isContinuityCounter,
  sameContinuityOwner,
  sameContinuityWitness,
  snapshotRef,
  validateSnapshot,
} from "./model.ts";

const owner = {
  environmentId: "environment",
  threadId: "thread",
  providerInstanceId: "codex",
  providerThreadId: "provider-thread",
  driver: "codex",
  nativeThreadId: "native-thread",
} as ContinuityOwner;
function intent(patch: Partial<ContinuityIntent> = {}): ContinuityIntent {
  return {
    goalId: "goal",
    revision: "1",
    northStar: "Ship the accepted outcome",
    preservedQualities: [{ id: "quality", text: "Preserve native Stop" }],
    finishItems: [{ id: "finish", text: "Provide proof" }],
    source: { type: "owner_request", reference: "request:1", identity: "owner:1" },
    derivation: null,
    initializationMode: "direct",
    startEvidence: [{ id: "start", reference: "request:1", assertion: "Explicit owner request" }],
    predecessor: null,
    ...patch,
  };
}
function initial(): ContinuitySnapshot {
  const result = initializeContinuity({ intent: intent(), owner, current: null });
  if (result.outcome !== "applied") throw new Error(result.reason);
  return result.snapshot;
}
function target(snapshot: ContinuitySnapshot): ContinuityTarget {
  return {
    goalId: snapshot.intent.goalId,
    revision: snapshot.intent.revision,
    generation: snapshot.generation,
    owner: snapshot.owner,
  };
}
function checkpoint(snapshot: ContinuitySnapshot, patch: ContinuityPatch): ContinuitySnapshot {
  const result = checkpointContinuity(snapshot, target(snapshot), patch);
  if (result.outcome !== "applied") throw new Error(result.reason);
  return result.snapshot;
}

describe("goal continuity semantic transitions", () => {
  it("preserves long server-owned conversation identities separately from checkpoint keys", () => {
    const nestedOwner = {
      ...owner,
      threadId: `thread:delegated-task:${"nested-task:".repeat(32)}`,
      providerThreadId: `provider-thread:${"native-binding:".repeat(24)}`,
    } as ContinuityOwner;
    const result = initializeContinuity({ intent: intent(), owner: nestedOwner, current: null });
    expect(result.outcome).toBe("applied");
    if (result.outcome !== "applied") throw new Error(result.reason);
    expect(result.snapshot.owner).toEqual(nestedOwner);
    expect(validateSnapshot(result.snapshot)).toBeNull();
    expect(
      initializeContinuity({
        intent: intent({ goalId: "g".repeat(257) }),
        owner: nestedOwner,
        current: null,
      }),
    ).toEqual({ outcome: "refused", reason: "invalid_request" });
  });

  it("preserves omitted progress and immutable intent, owning its returned values", () => {
    const original = initial();
    const actions = [{ id: "next", text: "Exercise the boundary" }];
    const first = checkpoint(original, {
      focus: "Contracts",
      nextActions: actions,
      blockers: { add: [{ id: "gate", text: "Await proof" }], remove: [] },
    });
    const second = checkpoint(first, {
      decisions: { add: [{ id: "decision", text: "Use native ownership" }], remove: [] },
    });
    actions[0]!.text = "changed outside";
    expect(second.intent).toEqual(original.intent);
    expect(second.nextActions[0]!.text).toBe("Exercise the boundary");
    expect(second.focus).toBe("Contracts");
    expect(second.blockers).toEqual(first.blockers);
    expect(second.generation).toBe("2");
    expect(original.generation).toBe("0");
    expect(Object.isFrozen(second.intent.finishItems[0])).toBe(true);
  });

  it("rejects duplicate, contradictory, unknown, stale and no-change patches", () => {
    const snapshot = checkpoint(initial(), {
      decisions: { add: [{ id: "decision", text: "native" }], remove: [] },
    });
    for (const [patch, reason] of [
      [{}, "no_change"],
      [{ focus: null }, "no_change"],
      [{ decisions: { add: [], remove: ["missing"] } }, "unknown_removal"],
      [{ decisions: { add: [{ id: "decision", text: "replace" }], remove: [] } }, "duplicate"],
      [
        { decisions: { add: [{ id: "decision", text: "replace" }], remove: ["decision"] } },
        "contradictory_patch",
      ],
      [
        {
          nextActions: [
            { id: "a", text: "a" },
            { id: "a", text: "b" },
          ],
        },
        "corrupt_snapshot",
      ],
      [{ status: "complete" }, "invalid_request"],
    ] as const) {
      expect(checkpointContinuity(snapshot, target(snapshot), patch as ContinuityPatch)).toEqual({
        outcome: "refused",
        reason,
      });
    }
    expect(
      checkpointContinuity(snapshot, { ...target(snapshot), generation: "0" }, { focus: "new" }),
    ).toEqual({ outcome: "refused", reason: "stale" });
    expect(
      checkpointContinuity(
        snapshot,
        { ...target(snapshot), owner: { ...owner, nativeThreadId: "fork" } },
        { focus: "new" },
      ),
    ).toEqual({ outcome: "refused", reason: "binding_mismatch" });
    expect(
      checkpointContinuity(snapshot, target(snapshot), {
        intent: intent(),
      } as unknown as ContinuityPatch),
    ).toEqual({ outcome: "refused", reason: "invalid_request" });
  });

  it("requires structured finish coverage and evidence while every refused close preserves bytes and generation", () => {
    const initialSnapshot = initial();
    const withBlocker = checkpoint(initialSnapshot, {
      status: "blocked",
      blockers: { add: [{ id: "gate", text: "Review" }], remove: [] },
    });
    const withBoundary = checkpoint(initialSnapshot, {
      boundaries: {
        append: [{ id: "boundary", text: "done", finishIds: ["finish"], evidenceIds: ["start"] }],
      },
    });
    const unresolved = checkpoint(withBoundary, {
      unresolvedEffect: {
        operation: "set",
        effect: { id: "effect", description: "Lost dispatch response" },
      },
    });
    for (const [snapshot, evidence, reason] of [
      [initialSnapshot, ["start"], "uncovered_finish"],
      [withBlocker, ["start"], "blockers"],
      [withBoundary, [], "missing_evidence"],
      [withBoundary, ["missing"], "missing_evidence"],
      [unresolved, ["start"], "unresolved_effect"],
    ] as const) {
      const bytes = canonicalJson(snapshot);
      const generation = snapshot.generation;
      expect(closeContinuity(snapshot, target(snapshot), evidence)).toEqual({
        outcome: "refused",
        reason,
      });
      expect(canonicalJson(snapshot)).toBe(bytes);
      expect(snapshot.generation).toBe(generation);
    }
    expect(
      checkpointContinuity(initialSnapshot, target(initialSnapshot), {
        boundaries: {
          append: [{ id: "wrong", text: "not coverage", finishIds: ["foreign"], evidenceIds: [] }],
        },
      }),
    ).toEqual({ outcome: "refused", reason: "foreign_finish" });
    const beforeRemoval = canonicalJson(withBoundary);
    expect(
      checkpointContinuity(withBoundary, target(withBoundary), {
        boundaries: { append: [], remove: ["boundary"] },
      } as unknown as ContinuityPatch),
    ).toEqual({ outcome: "refused", reason: "invalid_request" });
    expect(canonicalJson(withBoundary)).toBe(beforeRemoval);
    const closed = closeContinuity(withBoundary, target(withBoundary), ["start"]);
    expect(closed.outcome).toBe("applied");
    if (closed.outcome === "applied") {
      expect(closed.snapshot.status).toBe("complete");
      expect(
        checkpointContinuity(closed.snapshot, target(closed.snapshot), { status: "active" }),
      ).toEqual({ outcome: "refused", reason: "terminal" });
      expect(closeContinuity(closed.snapshot, target(closed.snapshot), ["start"])).toEqual({
        outcome: "refused",
        reason: "terminal",
      });
    }
  });

  it("preserves completed boundary payloads through close, detach and abandonment", () => {
    const snapshot = checkpoint(initial(), {
      boundaries: {
        append: [
          {
            id: "immutable",
            text: "Completed exactly once",
            finishIds: ["finish"],
            evidenceIds: ["start"],
          },
        ],
      },
    });
    const bytes = canonicalJson(snapshot.boundaries);
    for (const transition of [
      closeContinuity(snapshot, target(snapshot), ["start"]),
      detachContinuity(snapshot, target(snapshot), "Owner detached"),
      abandonContinuity(snapshot, target(snapshot), "Owner abandoned", ["start"]),
    ]) {
      expect(transition.outcome).toBe("applied");
      if (transition.outcome === "applied")
        expect(canonicalJson(transition.snapshot.boundaries)).toBe(bytes);
    }
    for (const forbidden of [
      { replace: [] },
      { update: [{ id: "immutable", text: "rewritten" }] },
    ]) {
      expect(
        checkpointContinuity(snapshot, target(snapshot), {
          boundaries: { append: [], ...forbidden },
        } as unknown as ContinuityPatch),
      ).toEqual({ outcome: "refused", reason: "invalid_request" });
    }
    expect(canonicalJson(snapshot.boundaries)).toBe(bytes);
  });

  it("resolves uncertainty only explicitly against evidence retained in the resulting snapshot", () => {
    const snapshot = checkpoint(initial(), {
      status: "blocked",
      unresolvedEffect: {
        operation: "set",
        effect: { id: "dispatch", description: "May have committed" },
      },
    });
    expect(
      checkpointContinuity(snapshot, target(snapshot), {
        unresolvedEffect: {
          operation: "resolve",
          effectId: "other",
          resolution: "known_no_effect",
          evidenceIds: ["start"],
        },
      }),
    ).toEqual({ outcome: "refused", reason: "unresolved_effect" });
    expect(
      checkpointContinuity(snapshot, target(snapshot), {
        unresolvedEffect: {
          operation: "resolve",
          effectId: "dispatch",
          resolution: "known_no_effect",
          evidenceIds: ["missing"],
        },
      }),
    ).toEqual({ outcome: "refused", reason: "missing_evidence" });
    const resolved = checkpoint(snapshot, {
      status: "active",
      evidence: {
        add: [{ id: "readback", reference: "receipt:1", assertion: "Commit observed" }],
        remove: [],
      },
      unresolvedEffect: {
        operation: "resolve",
        effectId: "dispatch",
        resolution: "effect_confirmed",
        evidenceIds: ["readback"],
      },
    });
    expect(resolved.unresolvedEffect).toBeNull();
    expect(resolved.status).toBe("active");
    expect(resolved.recentDispositions).toEqual([
      {
        effectId: "dispatch",
        resolution: "effect_confirmed",
        evidenceIds: ["readback"],
        generation: "2",
      },
    ]);
    expect(
      checkpointContinuity(resolved, target(resolved), {
        evidence: { add: [], remove: ["readback"] },
      }),
    ).toEqual({ outcome: "refused", reason: "missing_evidence" });
  });

  it("does not clear uncertainty on detach/abandon and requires explicit, evidenced handoff", () => {
    const snapshot = checkpoint(initial(), {
      unresolvedEffect: { operation: "set", effect: { id: "effect", description: "Unknown" } },
    });
    const detached = detachContinuity(snapshot, target(snapshot), "Owner detached");
    expect(detached.outcome).toBe("applied");
    if (detached.outcome !== "applied") return;
    expect(detached.snapshot.unresolvedEffect).toEqual(snapshot.unresolvedEffect);
    expect(
      checkpointContinuity(detached.snapshot, target(detached.snapshot), { focus: "change" }),
    ).toEqual({ outcome: "refused", reason: "detached" });
    const destination = { ...owner, nativeThreadId: "explicit-successor-conversation" };
    const handoff = handoffContinuity(detached.snapshot, target(detached.snapshot), destination, [
      "start",
    ]);
    expect(handoff.outcome).toBe("applied");
    if (handoff.outcome === "applied") {
      expect(handoff.snapshot.owner).toEqual(destination);
      expect(handoff.snapshot.unresolvedEffect).toEqual(snapshot.unresolvedEffect);
      expect(handoff.snapshot.intent).toEqual(snapshot.intent);
    }
    const abandoned = abandonContinuity(
      detached.snapshot,
      target(detached.snapshot),
      "Owner stopped",
      ["start"],
    );
    expect(abandoned.outcome).toBe("applied");
    if (abandoned.outcome === "applied")
      expect(abandoned.snapshot.unresolvedEffect).toEqual(snapshot.unresolvedEffect);
  });

  it("requires an exact predecessor for terminal successors and rejects replacement of active work", () => {
    const snapshot = initial();
    expect(initializeContinuity({ owner, current: snapshot, intent: intent() })).toEqual({
      outcome: "refused",
      reason: "successor_required",
    });
    const abandoned = abandonContinuity(snapshot, target(snapshot), "Superseded", ["start"]);
    if (abandoned.outcome !== "applied") throw new Error("Expected abandonment");
    expect(
      initializeContinuity({
        owner,
        current: abandoned.snapshot,
        intent: intent({ revision: "2" }),
      }),
    ).toEqual({ outcome: "refused", reason: "successor_required" });
    const successor = initializeContinuity({
      owner,
      current: abandoned.snapshot,
      intent: intent({ revision: "2", predecessor: snapshotRef(abandoned.snapshot) }),
    });
    expect(successor.outcome).toBe("applied");
    if (successor.outcome === "applied")
      expect(successor.snapshot.intent.predecessor).toEqual(snapshotRef(abandoned.snapshot));
  });

  it("carries unresolved effects into explicit successors of abandoned work", () => {
    const snapshot = checkpoint(initial(), {
      unresolvedEffect: {
        operation: "set",
        effect: { id: "unknown", description: "Lost response" },
      },
    });
    const abandoned = abandonContinuity(snapshot, target(snapshot), "Renewed scope", ["start"]);
    if (abandoned.outcome !== "applied") throw new Error("Expected abandonment");
    const successor = initializeContinuity({
      owner,
      current: abandoned.snapshot,
      intent: intent({ revision: "2", predecessor: snapshotRef(abandoned.snapshot) }),
    });
    expect(successor.outcome).toBe("applied");
    if (successor.outcome === "applied")
      expect(successor.snapshot.unresolvedEffect).toEqual(snapshot.unresolvedEffect);
  });

  it("binds confirm-first activation to the exact stored proposal and owner response", () => {
    const proposed = intent({ initializationMode: "confirm_first" });
    expect(initializeContinuity({ owner, current: null, intent: proposed })).toEqual({
      outcome: "refused",
      reason: "confirmation_required",
    });
    const proposal = {
      proposalId: "proposal",
      intent: proposed,
      owner,
      intentDigest: continuityDigest(proposed),
    };
    const confirmation = {
      confirmationId: "confirmation",
      proposalId: "proposal",
      intentDigest: proposal.intentDigest,
      owner,
      ownerResponseId: "response",
      authenticatedOwnerId: "owner",
    };
    expect(
      initializeContinuity({
        owner,
        current: null,
        intent: proposed,
        confirmation: { proposal, confirmation },
      }).outcome,
    ).toBe("applied");
    expect(
      initializeContinuity({
        owner,
        current: null,
        intent: { ...proposed, northStar: "Different work" },
        confirmation: { proposal, confirmation },
      }),
    ).toEqual({ outcome: "refused", reason: "confirmation_mismatch" });
  });

  it("handles counters beyond safe-number range, exhaustion and corrupt/oversized snapshots", () => {
    expect(incrementCounter("9007199254740993")).toBe("9007199254740994");
    for (const bad of ["00", "01", "-1", "1.0", "1e2", "9223372036854775808", " 1", "1\n"])
      expect(isContinuityCounter(bad)).toBe(false);
    expect(incrementCounter("9223372036854775807")).toEqual({
      outcome: "refused",
      reason: "counter_exhausted",
    });
    const snapshot = { ...initial(), generation: "9007199254740993" };
    expect(checkpoint(snapshot, { focus: "next" }).generation).toBe("9007199254740994");
    expect(
      validateSnapshot({ ...snapshot, schemaVersion: 2 } as unknown as ContinuitySnapshot),
    ).toEqual({ outcome: "refused", reason: "corrupt_snapshot" });
    expect(
      validateSnapshot({ ...snapshot, focus: "界".repeat(CONTINUITY_LIMITS.snapshotBytes / 3) }),
    ).toEqual({ outcome: "refused", reason: "snapshot_too_large" });
    expect(continuityDigest({ b: "2", a: "1" })).toBe(continuityDigest({ a: "1", b: "2" }));
    expect(() => canonicalJson({ dropped: undefined })).toThrow();
  });

  it("separates durable conversation ownership from a current execution witness", () => {
    const witness = {
      ...owner,
      providerSessionId: "session",
      runtimeGeneration: "generation",
      runId: "run",
      attemptId: "attempt",
    } as ContinuityWitness;
    const restart = {
      ...witness,
      providerSessionId: "new-session",
      runtimeGeneration: "new-generation",
    } as ContinuityWitness;
    expect(sameContinuityOwner(witness, restart)).toBe(true);
    expect(sameContinuityWitness(witness, restart)).toBe(false);
    expect(
      sameContinuityOwner(owner, { ...owner, providerInstanceId: "other" } as ContinuityOwner),
    ).toBe(false);
  });
});
