import { createHash } from "node:crypto";
import type {
  ContinuityCounter,
  ContinuityEvidence,
  ContinuityGoalRef,
  ContinuityIntent,
  ContinuityItem,
  ContinuityOwner,
  ContinuityOwnerConfirmation,
  ContinuityPatch,
  ContinuityProposal,
  ContinuityRefusal,
  ContinuityRefusalReason,
  ContinuitySnapshot,
  ContinuityTarget,
  ContinuityWitness,
} from "@t3tools/contracts";

// Runtime bounds live here so these pure modules can run without loading Effect.
// The contract's counter schema independently enforces the same signed-64 domain.
export const CONTINUITY_LIMITS = Object.freeze({
  snapshotBytes: 1024 * 1024,
  capsuleBytes: 4096,
  selectionBytes: 16 * 1024,
  selectionFields: 8,
  pageItems: 4,
  recentDispositions: 16,
  keyCharacters: 256,
  counterMax: (1n << 63n) - 1n,
});

export type ContinuityTransition =
  | { readonly outcome: "applied"; readonly snapshot: ContinuitySnapshot }
  | ContinuityRefusal;

export function refused(reason: ContinuityRefusalReason): ContinuityRefusal {
  return { outcome: "refused", reason };
}

export function utf8Bytes(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

/** Canonical JSON is also the receipt fingerprint input. Reject non-JSON values
 * instead of silently dropping undefined, rounding non-finite numbers or invoking toJSON.
 */
export function canonicalJson(value: unknown): string {
  const ancestors = new Set<object>();
  function encode(item: unknown, depth: number): string {
    if (depth > 64) throw new TypeError("Continuity JSON depth exceeded.");
    if (item === null) return "null";
    if (typeof item === "string" || typeof item === "boolean") return JSON.stringify(item);
    if (typeof item === "number" && Number.isSafeInteger(item)) return JSON.stringify(item);
    if (typeof item !== "object" || ancestors.has(item))
      throw new TypeError("Invalid continuity JSON.");
    const prototype = Object.getPrototypeOf(item);
    if (!Array.isArray(item) && prototype !== Object.prototype && prototype !== null) {
      throw new TypeError("Continuity JSON must contain plain objects.");
    }
    ancestors.add(item);
    let encoded: string;
    if (Array.isArray(item)) {
      encoded = `[${Array.from(item, (entry) => encode(entry, depth + 1)).join(",")}]`;
    } else {
      const record = item as Record<string, unknown>;
      encoded = `{${Object.keys(record)
        .sort()
        .map((name) => {
          const descriptor = Object.getOwnPropertyDescriptor(record, name);
          if (!descriptor || !("value" in descriptor))
            throw new TypeError("Continuity JSON accessor refused.");
          return `${JSON.stringify(name)}:${encode(descriptor.value, depth + 1)}`;
        })
        .join(",")}}`;
    }
    ancestors.delete(item);
    return encoded;
  }
  return encode(value, 0);
}

export function continuityDigest(value: unknown): string {
  return createHash("sha256").update(canonicalJson(value), "utf8").digest("hex");
}

export function isContinuityCounter(value: unknown): value is ContinuityCounter {
  return (
    typeof value === "string" &&
    /^(0|[1-9][0-9]{0,18})(?![\s\S])/.test(value) &&
    BigInt(value) <= CONTINUITY_LIMITS.counterMax
  );
}

export function incrementCounter(value: ContinuityCounter): ContinuityCounter | ContinuityRefusal {
  if (!isContinuityCounter(value)) return refused("invalid_counter");
  const parsed = BigInt(value);
  return parsed === CONTINUITY_LIMITS.counterMax
    ? refused("counter_exhausted")
    : String(parsed + 1n);
}

function validText(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}
function validKey(value: unknown): value is string {
  return (
    validText(value) && value.length <= CONTINUITY_LIMITS.keyCharacters && value.trim() === value
  );
}
function validDigest(value: unknown): value is string {
  return typeof value === "string" && /^[a-f0-9]{64}(?![\s\S])/.test(value);
}
function unique(values: readonly string[]): boolean {
  return values.length === new Set(values).size;
}
function keys(values: readonly string[]): boolean {
  return values.every(validKey) && unique(values);
}
function items(values: readonly ContinuityItem[]): boolean {
  return keys(values.map((item) => item.id)) && values.every((item) => validText(item.text));
}
function evidenceValid(values: readonly ContinuityEvidence[]): boolean {
  return (
    keys(values.map((item) => item.id)) &&
    values.every((item) => validText(item.reference) && validText(item.assertion))
  );
}

export function isStrongContinuityOwner(owner: ContinuityOwner): boolean {
  // Server-owned entity IDs follow their existing contract, independently of
  // the shorter keys authored inside a checkpoint. Nested task IDs may grow.
  return (
    [
      owner.environmentId,
      owner.threadId,
      owner.providerInstanceId,
      owner.providerThreadId,
      owner.driver,
    ].every((value) => validText(value) && value.trim() === value) &&
    validText(owner.nativeThreadId)
  );
}
export function sameContinuityOwner(left: ContinuityOwner, right: ContinuityOwner): boolean {
  return (
    left.environmentId === right.environmentId &&
    left.threadId === right.threadId &&
    left.providerInstanceId === right.providerInstanceId &&
    left.providerThreadId === right.providerThreadId &&
    left.driver === right.driver &&
    left.nativeThreadId === right.nativeThreadId
  );
}
export function sameContinuityWitness(left: ContinuityWitness, right: ContinuityWitness): boolean {
  return (
    sameContinuityOwner(left, right) &&
    left.providerSessionId === right.providerSessionId &&
    left.runtimeGeneration === right.runtimeGeneration &&
    left.runId === right.runId &&
    left.attemptId === right.attemptId
  );
}

export function snapshotRef(snapshot: ContinuitySnapshot): ContinuityGoalRef {
  return {
    goalId: snapshot.intent.goalId,
    revision: snapshot.intent.revision,
    generation: snapshot.generation,
    snapshotDigest: continuityDigest(snapshot),
  };
}

function validIntent(intent: ContinuityIntent): boolean {
  return (
    validKey(intent.goalId) &&
    isContinuityCounter(intent.revision) &&
    intent.revision !== "0" &&
    validText(intent.northStar) &&
    items(intent.preservedQualities) &&
    intent.finishItems.length > 0 &&
    items(intent.finishItems) &&
    ["owner_request", "document_reference", "explicit_derivation"].includes(intent.source.type) &&
    validText(intent.source.reference) &&
    validText(intent.source.identity) &&
    (intent.derivation === null || validText(intent.derivation)) &&
    ["direct", "confirm_first"].includes(intent.initializationMode) &&
    intent.startEvidence.length > 0 &&
    evidenceValid(intent.startEvidence) &&
    (intent.predecessor === null ||
      (validKey(intent.predecessor.goalId) &&
        isContinuityCounter(intent.predecessor.revision) &&
        isContinuityCounter(intent.predecessor.generation) &&
        validDigest(intent.predecessor.snapshotDigest)))
  );
}

/** Structural decoding belongs to the contract schema. This also checks semantic
 * integrity on persisted reads; malformed typed inputs return a refusal, never partial data.
 */
export function validateSnapshot(snapshot: ContinuitySnapshot): ContinuityRefusal | null {
  try {
    const encoded = canonicalJson(snapshot);
    if (utf8Bytes(encoded) > CONTINUITY_LIMITS.snapshotBytes) return refused("snapshot_too_large");
    if (
      snapshot.schemaVersion !== 1 ||
      !validIntent(snapshot.intent) ||
      !isStrongContinuityOwner(snapshot.owner) ||
      !isContinuityCounter(snapshot.generation) ||
      !["active", "blocked", "complete", "abandoned"].includes(snapshot.status) ||
      (snapshot.attachment.state !== "attached" &&
        (snapshot.attachment.state !== "detached" || !validText(snapshot.attachment.reason))) ||
      (snapshot.focus !== null && !validText(snapshot.focus)) ||
      !items(snapshot.decisions) ||
      !items(snapshot.blockers) ||
      !items(snapshot.nextActions) ||
      !evidenceValid(snapshot.evidence) ||
      !keys(snapshot.boundaries.map((boundary) => boundary.id)) ||
      !keys(snapshot.completionEvidenceIds) ||
      snapshot.recentDispositions.length > CONTINUITY_LIMITS.recentDispositions
    )
      return refused("corrupt_snapshot");
    const finishIds = new Set(snapshot.intent.finishItems.map((item) => item.id));
    const evidenceIds = new Set(snapshot.evidence.map((item) => item.id));
    for (const boundary of snapshot.boundaries) {
      if (!validText(boundary.text) || !keys(boundary.finishIds) || !keys(boundary.evidenceIds))
        return refused("corrupt_snapshot");
      if (boundary.finishIds.some((id) => !finishIds.has(id))) return refused("foreign_finish");
      if (boundary.evidenceIds.some((id) => !evidenceIds.has(id)))
        return refused("missing_evidence");
    }
    if (
      snapshot.unresolvedEffect !== null &&
      (!validKey(snapshot.unresolvedEffect.id) || !validText(snapshot.unresolvedEffect.description))
    )
      return refused("corrupt_snapshot");
    for (const disposition of snapshot.recentDispositions) {
      if (
        !validKey(disposition.effectId) ||
        !isContinuityCounter(disposition.generation) ||
        BigInt(disposition.generation) > BigInt(snapshot.generation) ||
        !["known_no_effect", "effect_confirmed"].includes(disposition.resolution) ||
        disposition.evidenceIds.length === 0 ||
        !keys(disposition.evidenceIds)
      )
        return refused("corrupt_snapshot");
      if (disposition.evidenceIds.some((id) => !evidenceIds.has(id)))
        return refused("missing_evidence");
    }
    if (snapshot.completionEvidenceIds.some((id) => !evidenceIds.has(id)))
      return refused("missing_evidence");
    if (snapshot.status === "complete") {
      const coverage = new Set(snapshot.boundaries.flatMap((boundary) => boundary.finishIds));
      if (
        snapshot.blockers.length > 0 ||
        snapshot.unresolvedEffect !== null ||
        snapshot.completionEvidenceIds.length === 0 ||
        [...finishIds].some((id) => !coverage.has(id))
      )
        return refused("corrupt_snapshot");
    } else if (snapshot.completionEvidenceIds.length > 0) return refused("corrupt_snapshot");
    return null;
  } catch {
    return refused("corrupt_snapshot");
  }
}

function expectedIssue(
  snapshot: ContinuitySnapshot,
  expected: ContinuityTarget,
  requireAttached = true,
): ContinuityRefusal | null {
  const issue = validateSnapshot(snapshot);
  if (issue) return issue;
  if (!isContinuityCounter(expected.revision) || !isContinuityCounter(expected.generation))
    return refused("invalid_counter");
  if (!sameContinuityOwner(snapshot.owner, expected.owner)) return refused("binding_mismatch");
  if (
    snapshot.intent.goalId !== expected.goalId ||
    snapshot.intent.revision !== expected.revision ||
    snapshot.generation !== expected.generation
  )
    return refused("stale");
  if (snapshot.status === "complete" || snapshot.status === "abandoned") return refused("terminal");
  if (requireAttached && snapshot.attachment.state !== "attached") return refused("detached");
  return null;
}

function applied(snapshot: ContinuitySnapshot): ContinuityTransition {
  const issue = validateSnapshot(snapshot);
  if (issue) return issue;
  // Own the returned value: neither request mutation nor consumer mutation may
  // change immutable intent or a previously returned canonical snapshot.
  const copy: ContinuitySnapshot = JSON.parse(canonicalJson(snapshot));
  function freeze(value: unknown): void {
    if (value !== null && typeof value === "object") {
      Object.values(value).forEach(freeze);
      Object.freeze(value);
    }
  }
  freeze(copy);
  return { outcome: "applied", snapshot: copy };
}

export function initializeContinuity(input: {
  readonly intent: ContinuityIntent;
  readonly owner: ContinuityOwner;
  readonly current: ContinuitySnapshot | null;
  readonly confirmation?: {
    readonly proposal: ContinuityProposal;
    readonly confirmation: ContinuityOwnerConfirmation;
  };
}): ContinuityTransition {
  try {
    const { intent, owner, current } = input;
    if (!validIntent(intent) || !isStrongContinuityOwner(owner)) return refused("invalid_request");
    if (intent.initializationMode === "confirm_first") {
      const proof = input.confirmation;
      if (!proof) return refused("confirmation_required");
      if (
        !validKey(proof.confirmation.confirmationId) ||
        !validKey(proof.confirmation.ownerResponseId) ||
        !validKey(proof.confirmation.authenticatedOwnerId) ||
        proof.proposal.proposalId !== proof.confirmation.proposalId ||
        proof.proposal.intentDigest !== continuityDigest(intent) ||
        proof.confirmation.intentDigest !== proof.proposal.intentDigest ||
        canonicalJson(proof.proposal.intent) !== canonicalJson(intent) ||
        !sameContinuityOwner(owner, proof.proposal.owner) ||
        !sameContinuityOwner(owner, proof.confirmation.owner)
      ) {
        return refused("confirmation_mismatch");
      }
    } else if (input.confirmation !== undefined) return refused("invalid_request");
    if (current === null) {
      if (intent.predecessor !== null || intent.revision !== "1")
        return refused("successor_required");
    } else {
      const issue = validateSnapshot(current);
      if (issue) return issue;
      if (current.status !== "complete" && current.status !== "abandoned")
        return refused("successor_required");
      if (!sameContinuityOwner(current.owner, owner)) return refused("binding_mismatch");
      if (
        intent.predecessor === null ||
        canonicalJson(intent.predecessor) !== canonicalJson(snapshotRef(current))
      )
        return refused("successor_required");
      const revision =
        intent.goalId === current.intent.goalId ? incrementCounter(current.intent.revision) : "1";
      if (typeof revision !== "string") return revision;
      if (intent.revision !== revision) return refused("successor_required");
    }
    return applied({
      schemaVersion: 1,
      intent,
      owner,
      attachment: { state: "attached" },
      generation: "0",
      status: "active",
      focus: null,
      boundaries: [],
      decisions: [],
      blockers: [],
      nextActions: [],
      evidence: [...intent.startEvidence],
      // A successor cannot launder uncertainty left on its abandoned predecessor.
      unresolvedEffect: current?.unresolvedEffect ?? null,
      recentDispositions: [],
      completionEvidenceIds: [],
    });
  } catch {
    return refused("invalid_request");
  }
}

function applyCollection<T extends { readonly id: string }>(
  current: readonly T[],
  operation: { readonly add: readonly T[]; readonly remove: readonly string[] },
): T[] | ContinuityRefusal {
  const additions = operation.add.map((item) => item.id);
  if (!keys(additions) || !keys(operation.remove)) return refused("duplicate");
  const removing = new Set(operation.remove);
  if (additions.some((id) => removing.has(id))) return refused("contradictory_patch");
  const existing = new Set(current.map((item) => item.id));
  if (operation.remove.some((id) => !existing.has(id))) return refused("unknown_removal");
  if (additions.some((id) => existing.has(id))) return refused("duplicate");
  return [...current.filter((item) => !removing.has(item.id)), ...operation.add];
}

export function checkpointContinuity(
  snapshot: ContinuitySnapshot,
  expected: ContinuityTarget,
  patch: ContinuityPatch,
): ContinuityTransition {
  const issue = expectedIssue(snapshot, expected);
  if (issue) return issue;
  try {
    const allowed = new Set([
      "focus",
      "status",
      "boundaries",
      "decisions",
      "blockers",
      "evidence",
      "nextActions",
      "unresolvedEffect",
    ]);
    if (
      Object.keys(patch).some((name) => !allowed.has(name)) ||
      (patch.status !== undefined && patch.status !== "active" && patch.status !== "blocked")
    )
      return refused("invalid_request");
    let next: ContinuitySnapshot = { ...snapshot };
    if ("focus" in patch) {
      if (patch.focus === undefined) return refused("invalid_request");
      next = { ...next, focus: patch.focus };
    }
    if (patch.status !== undefined) next = { ...next, status: patch.status };
    if (patch.nextActions !== undefined) next = { ...next, nextActions: patch.nextActions };
    if (patch.boundaries !== undefined) {
      if (Object.keys(patch.boundaries).some((name) => name !== "append"))
        return refused("invalid_request");
      const value = applyCollection(next.boundaries, { add: patch.boundaries.append, remove: [] });
      if (!Array.isArray(value)) return value;
      next = { ...next, boundaries: value };
    }
    for (const field of ["decisions", "blockers", "evidence"] as const) {
      const operation = patch[field];
      if (operation === undefined) continue;
      // Each branch retains its collection's element type without widening the DTO.
      if (field === "evidence") {
        const value = applyCollection(next.evidence, patch.evidence!);
        if (!Array.isArray(value)) return value;
        next = { ...next, evidence: value };
      } else {
        const value = applyCollection(
          next[field],
          operation as { add: readonly ContinuityItem[]; remove: readonly string[] },
        );
        if (!Array.isArray(value)) return value;
        next = { ...next, [field]: value };
      }
    }
    const generation = incrementCounter(snapshot.generation);
    if (typeof generation !== "string") return generation;
    if (patch.unresolvedEffect !== undefined) {
      const operation = patch.unresolvedEffect;
      if (operation.operation === "set") {
        if (next.unresolvedEffect !== null) return refused("unresolved_effect");
        next = { ...next, unresolvedEffect: operation.effect };
      } else if (operation.operation === "resolve") {
        if (next.unresolvedEffect?.id !== operation.effectId) return refused("unresolved_effect");
        if (
          !keys(operation.evidenceIds) ||
          operation.evidenceIds.length === 0 ||
          operation.evidenceIds.some((id) => !next.evidence.some((entry) => entry.id === id))
        )
          return refused("missing_evidence");
        next = {
          ...next,
          unresolvedEffect: null,
          recentDispositions: [
            ...next.recentDispositions,
            {
              effectId: operation.effectId,
              resolution: operation.resolution,
              evidenceIds: operation.evidenceIds,
              generation,
            },
          ].slice(-CONTINUITY_LIMITS.recentDispositions),
        };
      } else return refused("invalid_request");
    }
    if (canonicalJson(next) === canonicalJson(snapshot)) return refused("no_change");
    return applied({ ...next, generation });
  } catch {
    return refused("invalid_request");
  }
}

export function closeContinuity(
  snapshot: ContinuitySnapshot,
  expected: ContinuityTarget,
  completionEvidenceIds: readonly string[],
): ContinuityTransition {
  const issue = expectedIssue(snapshot, expected);
  if (issue) return issue;
  if (snapshot.unresolvedEffect !== null) return refused("unresolved_effect");
  if (snapshot.blockers.length > 0) return refused("blockers");
  const coverage = new Set(snapshot.boundaries.flatMap((boundary) => boundary.finishIds));
  if (snapshot.intent.finishItems.some((item) => !coverage.has(item.id)))
    return refused("uncovered_finish");
  if (
    !keys(completionEvidenceIds) ||
    completionEvidenceIds.length === 0 ||
    completionEvidenceIds.some((id) => !snapshot.evidence.some((entry) => entry.id === id))
  )
    return refused("missing_evidence");
  const generation = incrementCounter(snapshot.generation);
  if (typeof generation !== "string") return generation;
  return applied({ ...snapshot, status: "complete", generation, completionEvidenceIds });
}

export function abandonContinuity(
  snapshot: ContinuitySnapshot,
  expected: ContinuityTarget,
  reason: string,
  evidenceIds: readonly string[],
): ContinuityTransition {
  const issue = expectedIssue(snapshot, expected, false);
  if (issue) return issue;
  if (
    !validText(reason) ||
    !keys(evidenceIds) ||
    evidenceIds.length === 0 ||
    evidenceIds.some((id) => !snapshot.evidence.some((entry) => entry.id === id))
  )
    return refused("missing_evidence");
  const generation = incrementCounter(snapshot.generation);
  if (typeof generation !== "string") return generation;
  // The reason/evidence remain in the durable operation receipt; progress and
  // unresolved effects survive abandonment unchanged.
  return applied({ ...snapshot, status: "abandoned", generation });
}

export function detachContinuity(
  snapshot: ContinuitySnapshot,
  expected: ContinuityTarget,
  reason: string,
): ContinuityTransition {
  const issue = expectedIssue(snapshot, expected);
  if (issue) return issue;
  if (!validText(reason)) return refused("invalid_request");
  const generation = incrementCounter(snapshot.generation);
  if (typeof generation !== "string") return generation;
  return applied({ ...snapshot, generation, attachment: { state: "detached", reason } });
}

/** Authorization of both owners is a service responsibility. Explicit handoff
 * preserves intent/progress and may reattach a detached nonterminal record.
 */
export function handoffContinuity(
  snapshot: ContinuitySnapshot,
  expected: ContinuityTarget,
  destination: ContinuityOwner,
  evidenceIds: readonly string[],
): ContinuityTransition {
  const issue = expectedIssue(snapshot, expected, false);
  if (issue) return issue;
  if (!isStrongContinuityOwner(destination)) return refused("binding_mismatch");
  if (
    !keys(evidenceIds) ||
    evidenceIds.length === 0 ||
    evidenceIds.some((id) => !snapshot.evidence.some((entry) => entry.id === id))
  )
    return refused("missing_evidence");
  if (sameContinuityOwner(snapshot.owner, destination) && snapshot.attachment.state === "attached")
    return refused("no_change");
  const generation = incrementCounter(snapshot.generation);
  if (typeof generation !== "string") return generation;
  return applied({
    ...snapshot,
    owner: destination,
    generation,
    attachment: { state: "attached" },
  });
}
