import type {
  ContinuitySelectedValue,
  ContinuitySelectionField,
  ContinuitySelectionRequest,
  ContinuitySelectionResult,
  ContinuitySnapshot,
} from "@t3tools/contracts";
import {
  canonicalJson,
  continuityDigest,
  CONTINUITY_LIMITS,
  isContinuityCounter,
  refused,
  utf8Bytes,
  validateSnapshot,
} from "./model.ts";

export function continuityFieldValue(
  snapshot: ContinuitySnapshot,
  field: ContinuitySelectionField,
): ContinuitySelectedValue {
  switch (field) {
    case "intent":
      return {
        goalId: snapshot.intent.goalId,
        revision: snapshot.intent.revision,
        northStar: snapshot.intent.northStar,
        source: snapshot.intent.source,
        derivation: snapshot.intent.derivation,
        initializationMode: snapshot.intent.initializationMode,
        predecessor: snapshot.intent.predecessor,
      };
    case "finishItems":
      return snapshot.intent.finishItems;
    case "preservedQualities":
      return snapshot.intent.preservedQualities;
    case "startEvidence":
      return snapshot.intent.startEvidence;
    case "focus":
      return snapshot.focus;
    case "status":
      return snapshot.status;
    case "attachment":
      return snapshot.attachment;
    case "boundaries":
      return snapshot.boundaries;
    case "decisions":
      return snapshot.decisions;
    case "blockers":
      return snapshot.blockers;
    case "nextActions":
      return snapshot.nextActions;
    case "evidence":
      return snapshot.evidence;
    case "unresolvedEffect":
      return snapshot.unresolvedEffect;
    case "recentDispositions":
      return snapshot.recentDispositions;
    case "completionEvidenceIds":
      return snapshot.completionEvidenceIds;
  }
}

/** One immutable snapshot in, one complete bounded response out. No partial
 * result escapes on a stale cursor, oversized field, or corrupt snapshot.
 */
export function selectContinuity(
  snapshot: ContinuitySnapshot,
  request: ContinuitySelectionRequest,
): ContinuitySelectionResult {
  const issue = validateSnapshot(snapshot);
  if (issue) return issue;
  try {
    if (!isContinuityCounter(request.revision) || !isContinuityCounter(request.generation))
      return refused("invalid_counter");
    if (
      request.goalId !== snapshot.intent.goalId ||
      request.revision !== snapshot.intent.revision ||
      request.generation !== snapshot.generation
    )
      return refused("stale");
    if (
      request.fields.length < 1 ||
      request.fields.length > CONTINUITY_LIMITS.selectionFields ||
      new Set(request.fields.map((entry) => entry.field)).size !== request.fields.length
    )
      return refused("selection_bounds");
    const fields: Extract<ContinuitySelectionResult, { outcome: "selected" }>["fields"][number][] =
      [];
    let collectionItems = 0;
    for (const entry of request.fields) {
      const value = continuityFieldValue(snapshot, entry.field);
      if (value === undefined) return refused("invalid_request");
      const fieldDigest = continuityDigest(value);
      const totalItems = Array.isArray(value) ? value.length : 1;
      const offset = entry.cursor?.offset ?? 0;
      if (entry.cursor !== undefined) {
        if (entry.cursor.generation !== snapshot.generation) return refused("stale");
        if (entry.cursor.fieldDigest !== fieldDigest) return refused("digest_mismatch");
        if (
          !Array.isArray(value) ||
          !Number.isSafeInteger(offset) ||
          offset < 0 ||
          offset >= totalItems ||
          offset % CONTINUITY_LIMITS.pageItems !== 0
        )
          return refused("selection_bounds");
      }
      const page: ContinuitySelectedValue = Array.isArray(value)
        ? value.slice(offset, offset + CONTINUITY_LIMITS.pageItems)
        : value;
      if (Array.isArray(page)) collectionItems += page.length;
      if (collectionItems > CONTINUITY_LIMITS.pageItems) return refused("selection_bounds");
      const nextOffset = offset + CONTINUITY_LIMITS.pageItems;
      fields.push({
        field: entry.field,
        fieldDigest,
        totalItems,
        value: page,
        nextCursor:
          Array.isArray(value) && nextOffset < totalItems
            ? { generation: snapshot.generation, fieldDigest, offset: nextOffset }
            : null,
      });
    }
    const result: ContinuitySelectionResult = {
      outcome: "selected",
      goalId: snapshot.intent.goalId,
      revision: snapshot.intent.revision,
      generation: snapshot.generation,
      snapshotDigest: continuityDigest(snapshot),
      fields,
    };
    const encoded = canonicalJson(result);
    if (utf8Bytes(encoded) > CONTINUITY_LIMITS.selectionBytes) return refused("selection_bounds");
    return JSON.parse(encoded) as ContinuitySelectionResult;
  } catch {
    return refused("corrupt_snapshot");
  }
}
