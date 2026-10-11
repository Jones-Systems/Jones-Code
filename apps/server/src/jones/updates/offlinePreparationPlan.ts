// @effect-diagnostics-next-line nodeBuiltinImport:off -- Hash the exact validated plan bytes without opening application state.
import * as NodeCrypto from "node:crypto";
import * as Schema from "effect/Schema";

export const offlinePreparationPlanLimits = Object.freeze({
  bytes: 8 * 1024 * 1024,
  selections: 4096,
  preservedRows: 16384,
  idBytes: 256,
  payloadBytes: 256 * 1024,
  detailBytes: 64 * 1024,
});
const source = "bd20d2d2fc0ba89797698192b877a7f1312f8fe3";
const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/;
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const boundedText = (bytes: number) =>
  Schema.String.check(
    Schema.makeFilter(
      (value) => value.length <= bytes && encoder.encode(value).byteLength <= bytes,
    ),
  );
const identifier = boundedText(offlinePreparationPlanLimits.idBytes).check(
  Schema.makeFilter(
    (value) =>
      value.length > 0 &&
      value.trim() === value &&
      !Array.from(value).some(
        (character) => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127,
      ),
  ),
);
const timestamp = Schema.String.check(
  Schema.makeFilter((value) => {
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value)) return false;
    const millis = Date.parse(value);
    return Number.isFinite(millis) && new Date(millis).toISOString() === value;
  }),
);
const hash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const rowSchema = Schema.Struct({
  effect_id: identifier,
  command_id: identifier,
  thread_id: identifier,
  effect_type: identifier,
  payload_json: boundedText(offlinePreparationPlanLimits.payloadBytes),
  status: Schema.Literals(["pending", "running", "succeeded", "failed", "cancelled"]),
  attempt_count: Schema.Int.check(
    Schema.isGreaterThanOrEqualTo(0),
    Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
  ),
  available_at: timestamp,
  created_at: timestamp,
  updated_at: timestamp,
  lease_owner: Schema.NullOr(identifier),
  lease_expires_at: Schema.NullOr(timestamp),
  completed_at: Schema.NullOr(timestamp),
  last_error: Schema.NullOr(boundedText(offlinePreparationPlanLimits.detailBytes)),
});
// Preserve the contract brands without the live decoder's whitespace normalization.
const threadId = identifier.pipe(Schema.brand("ThreadId"));
const runId = identifier.pipe(Schema.brand("RunId"));
const selectionSchema = Schema.Struct({
  threadId,
  runId,
  existingId: Schema.NullOr(identifier),
  projectionSha256: hash,
});
const planSchema = Schema.Struct({
  id: Schema.String.check(Schema.isPattern(uuid)),
  timestamp,
  source: Schema.Literal(source),
  snapshotSha256: hash,
  settingsSha256: hash,
  selections: Schema.Array(selectionSchema).check(
    Schema.isMaxLength(offlinePreparationPlanLimits.selections),
  ),
  expectedRows: Schema.Array(rowSchema).check(
    Schema.isMaxLength(offlinePreparationPlanLimits.selections),
  ),
  preservedRows: Schema.Array(rowSchema).check(
    Schema.isMaxLength(offlinePreparationPlanLimits.preservedRows),
  ),
});
export type OfflinePreparationPlan = typeof planSchema.Type;
type Row = typeof rowSchema.Type;
const decodePlan = Schema.decodeUnknownSync(planSchema, { onExcessProperty: "error" });
const expectedRequestSchema = Schema.Struct({
  type: Schema.Literal("provider-runtime.continue"),
  sourceRunId: runId,
  preparedForRestart: Schema.Literal(true),
  continueWithoutPreference: Schema.Literal(false),
});
const decodeExpectedRequest = Schema.decodeUnknownSync(
  Schema.fromJsonString(expectedRequestSchema),
  { onExcessProperty: "error" },
);

export class OfflinePreparationPlanError extends Schema.TaggedError<OfflinePreparationPlanError>()(
  "OfflinePreparationPlanError",
  { reason: Schema.Literals(["size", "encoding", "shape", "relationships", "noncanonical"]) },
) {
  override get message() {
    return "Invalid offline preparation plan: " + this.reason;
  }
}

function requireRelationship(valid: boolean): asserts valid {
  if (!valid) throw new OfflinePreparationPlanError({ reason: "relationships" });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function preflight(value: unknown): void {
  if (!isRecord(value)) throw new OfflinePreparationPlanError({ reason: "shape" });
  let remaining = offlinePreparationPlanLimits.bytes;
  const charge = (entry: unknown) => {
    if (typeof entry !== "string") return;
    if (entry.length > remaining) throw new OfflinePreparationPlanError({ reason: "size" });
    remaining -= encoder.encode(entry).byteLength;
    if (remaining < 0) throw new OfflinePreparationPlanError({ reason: "size" });
  };
  let fields = 0;
  for (const key in value) {
    if (!Object.hasOwn(value, key)) continue;
    if (++fields > 8) throw new OfflinePreparationPlanError({ reason: "shape" });
    const entry = value[key];
    if (key === "selections" || key === "expectedRows" || key === "preservedRows") {
      if (!Array.isArray(entry)) throw new OfflinePreparationPlanError({ reason: "shape" });
      const limit =
        key === "preservedRows"
          ? offlinePreparationPlanLimits.preservedRows
          : offlinePreparationPlanLimits.selections;
      if (entry.length > limit) throw new OfflinePreparationPlanError({ reason: "size" });
      const entries: ReadonlyArray<unknown> = entry;
      for (const row of entries) {
        if (!isRecord(row)) throw new OfflinePreparationPlanError({ reason: "shape" });
        let rowFields = 0;
        for (const name in row) {
          if (!Object.hasOwn(row, name)) continue;
          if (++rowFields > (key === "selections" ? 4 : 14))
            throw new OfflinePreparationPlanError({ reason: "shape" });
          charge(row[name]);
        }
      }
    } else charge(entry);
  }
}

// SQLite's legacy JSON query and JSON.parse disagree on duplicate keys. Reject
// that ambiguity for referenced markers without rewriting historical payloads.
function uniqueMarkerKeys(raw: string): boolean {
  const keys = new Set<string>();
  let depth = 0;
  for (const token of raw.matchAll(/"(?:[^"\\]|\\.)*"|[{}]|\[|\]/g)) {
    const text = token[0];
    if (text === "{" || text === "[") depth++;
    else if (text === "}" || text === "]") depth--;
    else if (depth === 1) {
      let next = token.index + text.length;
      while (next < raw.length && /\s/.test(raw[next]!)) next++;
      if (raw[next] !== ":") continue;
      const key = decodeJson(text);
      if (typeof key !== "string" || keys.has(key)) return false;
      keys.add(key);
    }
  }
  return true;
}

function markerRunId(row: Row): string | null {
  let payload: unknown;
  try {
    payload = decodeJson(row.payload_json);
  } catch {
    throw new OfflinePreparationPlanError({ reason: "relationships" });
  }
  if (!isRecord(payload)) return null;
  requireRelationship(uniqueMarkerKeys(row.payload_json));
  return typeof payload.sourceRunId === "string" &&
    (payload.preparedForRestart === true || payload.preparedForRestart === 1)
    ? payload.sourceRunId
    : null;
}

function validate(value: unknown): OfflinePreparationPlan {
  preflight(value);
  let plan: OfflinePreparationPlan;
  try {
    plan = decodePlan(value);
  } catch {
    throw new OfflinePreparationPlanError({ reason: "shape" });
  }
  const rows = new Map<string, Row>();
  for (const row of [...plan.preservedRows, ...plan.expectedRows]) {
    requireRelationship(!rows.has(row.effect_id));
    rows.set(row.effect_id, row);
  }
  const preserved = new Map(plan.preservedRows.map((row) => [row.effect_id, row]));
  const preservedCommands = new Set(plan.preservedRows.map((row) => row.command_id));
  const pendingByThread = new Map<string, Row[]>();
  for (const row of plan.preservedRows) {
    if (
      row.status !== "pending" ||
      row.effect_type !== "provider-runtime.continue" ||
      !row.command_id.startsWith("command:server-update-prepare:")
    )
      continue;
    const pending = pendingByThread.get(row.thread_id);
    if (pending === undefined) pendingByThread.set(row.thread_id, [row]);
    else pending.push(row);
  }
  const expectedByThread = new Map<string, Row>();
  const commands = new Set<string>();
  for (const row of plan.expectedRows) {
    requireRelationship(!expectedByThread.has(row.thread_id) && !commands.has(row.command_id));
    expectedByThread.set(row.thread_id, row);
    commands.add(row.command_id);
    const prefix = "effect:server-update-continuation:";
    const suffix = row.effect_id.slice(prefix.length);
    requireRelationship(row.effect_id.startsWith(prefix) && uuid.test(suffix));
    requireRelationship(row.command_id === "command:server-update-prepare:" + suffix);
    requireRelationship(
      row.effect_type === "provider-runtime.continue" &&
        row.status === "pending" &&
        row.attempt_count === 0,
    );
    requireRelationship(
      row.available_at === plan.timestamp &&
        row.created_at === plan.timestamp &&
        row.updated_at === plan.timestamp,
    );
    requireRelationship(
      row.lease_owner === null &&
        row.lease_expires_at === null &&
        row.completed_at === null &&
        row.last_error === null,
    );
    requireRelationship(!preservedCommands.has(row.command_id));
  }
  const selectedThreads = new Set<string>();
  const selectedRuns = new Set<string>();
  const existingIds = new Set<string>();
  for (const selection of plan.selections) {
    requireRelationship(
      !selectedThreads.has(selection.threadId) && !selectedRuns.has(selection.runId),
    );
    selectedThreads.add(selection.threadId);
    selectedRuns.add(selection.runId);
    if (selection.existingId === null) {
      requireRelationship(
        !(pendingByThread.get(selection.threadId) ?? []).some(
          (row) => markerRunId(row) === selection.runId,
        ),
      );
      const row = expectedByThread.get(selection.threadId);
      requireRelationship(row !== undefined);
      let request: typeof expectedRequestSchema.Type;
      try {
        request = decodeExpectedRequest(row.payload_json);
      } catch {
        throw new OfflinePreparationPlanError({ reason: "relationships" });
      }
      requireRelationship(request.sourceRunId === selection.runId);
      requireRelationship(
        row.payload_json ===
          encodeJson({
            type: request.type,
            sourceRunId: request.sourceRunId,
            preparedForRestart: true,
            continueWithoutPreference: false,
          }),
      );
      expectedByThread.delete(selection.threadId);
    } else {
      requireRelationship(
        !existingIds.has(selection.existingId) && !expectedByThread.has(selection.threadId),
      );
      existingIds.add(selection.existingId);
      const row = preserved.get(selection.existingId);
      requireRelationship(row !== undefined);
      requireRelationship(
        row.thread_id === selection.threadId &&
          row.status === "pending" &&
          row.effect_type === "provider-runtime.continue" &&
          row.command_id.startsWith("command:server-update-prepare:"),
      );
      // Match the legacy query, not a newer payload decoder: unrelated payloads
      // remain opaque and an existing marker may predate optional request fields.
      requireRelationship(markerRunId(row) === selection.runId);
    }
  }
  requireRelationship(expectedByThread.size === 0);
  return plan;
}

// Fix only the plan envelope's key order. Persisted payload strings are evidence
// bytes and must never be reformatted while encoding the surrounding document.
function rowValue(row: Row): Row {
  return {
    effect_id: row.effect_id,
    command_id: row.command_id,
    thread_id: row.thread_id,
    effect_type: row.effect_type,
    payload_json: row.payload_json,
    status: row.status,
    attempt_count: row.attempt_count,
    available_at: row.available_at,
    created_at: row.created_at,
    updated_at: row.updated_at,
    lease_owner: row.lease_owner,
    lease_expires_at: row.lease_expires_at,
    completed_at: row.completed_at,
    last_error: row.last_error,
  };
}
function canonical(plan: OfflinePreparationPlan): Uint8Array {
  const bytes = encoder.encode(
    encodeJson({
      id: plan.id,
      timestamp: plan.timestamp,
      source: plan.source,
      snapshotSha256: plan.snapshotSha256,
      settingsSha256: plan.settingsSha256,
      selections: plan.selections.map((selection) => ({
        threadId: selection.threadId,
        runId: selection.runId,
        existingId: selection.existingId,
        projectionSha256: selection.projectionSha256,
      })),
      expectedRows: plan.expectedRows.map(rowValue),
      preservedRows: plan.preservedRows.map(rowValue),
    }),
  );
  if (bytes.byteLength > offlinePreparationPlanLimits.bytes)
    throw new OfflinePreparationPlanError({ reason: "size" });
  return bytes;
}

/** Validate data and emit canonical bytes; this supplies no owner or state admission. */
export function encodeOfflinePreparationPlan(value: unknown): Uint8Array {
  return canonical(validate(value));
}

/** Reload an immutable value and hash its exact bytes, without accepting a caller's hash. */
export function decodeOfflinePreparationPlan(bytes: Uint8Array): {
  readonly plan: OfflinePreparationPlan;
  readonly sha256: string;
} {
  if (bytes.byteLength === 0 || bytes.byteLength > offlinePreparationPlanLimits.bytes)
    throw new OfflinePreparationPlanError({ reason: "size" });
  const captured = Uint8Array.from(bytes);
  let raw: string;
  try {
    raw = decoder.decode(captured);
  } catch {
    throw new OfflinePreparationPlanError({ reason: "encoding" });
  }
  let value: unknown;
  try {
    value = decodeJson(raw);
  } catch {
    throw new OfflinePreparationPlanError({ reason: "shape" });
  }
  const plan = validate(value);
  const expected = canonical(plan);
  if (
    expected.byteLength !== captured.byteLength ||
    expected.some((byte, index) => byte !== captured[index])
  )
    throw new OfflinePreparationPlanError({ reason: "noncanonical" });
  for (const row of [...plan.selections, ...plan.expectedRows, ...plan.preservedRows])
    Object.freeze(row);
  Object.freeze(plan.selections);
  Object.freeze(plan.expectedRows);
  Object.freeze(plan.preservedRows);
  Object.freeze(plan);
  return Object.freeze({
    plan,
    sha256: NodeCrypto.createHash("sha256").update(captured).digest("hex"),
  });
}
