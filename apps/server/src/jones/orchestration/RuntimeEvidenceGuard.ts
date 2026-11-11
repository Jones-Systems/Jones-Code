import {
  type OrchestrationV2ProviderThread,
  OrchestrationV2ProviderThreadJson,
  type ProviderRuntimeEvidenceCapture,
  type RuntimeIdentityAttestation,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import type { RecordedLifecycleEvent } from "../../orchestration-v2/RecordedTypes.ts";

/** Why a captured producer may not write to the provider thread it names. */
type RuntimeEvidenceMismatch =
  | "provider_thread_missing"
  | "app_thread_mismatch"
  | "provider_thread_mismatch"
  | "provider_session_mismatch"
  | "provider_instance_mismatch"
  | "driver_mismatch"
  | "native_thread_mismatch"
  | "identity_absent"
  | "generation_absent"
  | "generation_replaced"
  | "evidence_revision_absent"
  | "evidence_revision_behind"
  | "evidence_revision_mismatch";

/** Later guard checks reject for these reasons after the capture itself matched. */
type RuntimeEvidenceRejection =
  | RuntimeEvidenceMismatch
  | "requested_identity_changed"
  | "observation_stale";

export function runtimeEvidenceMismatch(
  current: OrchestrationV2ProviderThread | null,
  capture: ProviderRuntimeEvidenceCapture,
): RuntimeEvidenceMismatch | undefined {
  if (current === null) return "provider_thread_missing";
  if (current.appThreadId !== capture.threadId) return "app_thread_mismatch";
  if (current.id !== capture.providerThreadId) return "provider_thread_mismatch";
  if (current.providerSessionId !== capture.providerSessionId) return "provider_session_mismatch";
  if (current.providerInstanceId !== capture.providerInstanceId)
    return "provider_instance_mismatch";
  const native = current.nativeThreadRef;
  if (current.driver !== capture.driver || native?.driver !== capture.driver)
    return "driver_mismatch";
  if (native.nativeId !== capture.nativeThreadId) return "native_thread_mismatch";
  const identity = current.runtimeIdentity;
  if (identity === undefined) return "identity_absent";
  if (
    capture.runtimeGeneration !== undefined &&
    identity.runtimeGeneration !== capture.runtimeGeneration
  )
    return identity.runtimeGeneration === undefined ? "generation_absent" : "generation_replaced";
  if (capture.evidenceRevision !== undefined) {
    const revision = identity.evidenceRevision;
    if (revision === undefined) return "evidence_revision_absent";
    if (capture.runtimeGeneration === undefined) {
      if (revision !== capture.evidenceRevision) return "evidence_revision_mismatch";
    } else if (revision < capture.evidenceRevision) return "evidence_revision_behind";
  }
  return undefined;
}

/**
 * The stored row names no producer at all, so the capture lost its evidence rather than its
 * ownership. A replaced generation or a different binding names another owner and never counts.
 */
const isLostRuntimeEvidence = (reason: RuntimeEvidenceRejection): boolean =>
  reason === "identity_absent" || reason === "generation_absent";

/**
 * Only a boundary write may change the stored generation. An update without producer evidence
 * that names the same binding but predates the stored identity keeps the stored identity; a
 * stale snapshot would otherwise strip the generation or regress the revision, and every later
 * capture from that producer would fail. Returns undefined when the update applies as written.
 */
function retainedRuntimeIdentity(
  stored: OrchestrationV2ProviderThread,
  incoming: OrchestrationV2ProviderThread,
): RuntimeIdentityAttestation | undefined {
  const current = stored.runtimeIdentity;
  if (current?.runtimeGeneration === undefined) return undefined;
  if (
    stored.id !== incoming.id ||
    stored.appThreadId !== incoming.appThreadId ||
    stored.driver !== incoming.driver ||
    stored.providerInstanceId !== incoming.providerInstanceId ||
    stored.providerSessionId !== incoming.providerSessionId ||
    stored.nativeThreadRef?.nativeId !== incoming.nativeThreadRef?.nativeId
  )
    return undefined;
  const next = incoming.runtimeIdentity;
  if (next?.runtimeGeneration !== undefined && next.runtimeGeneration !== current.runtimeGeneration)
    return undefined;
  const ahead = (next?.evidenceRevision ?? 0) - (current.evidenceRevision ?? 0);
  // Rollback resets the identity without evidence by advancing the revision.
  if (ahead > 0) return undefined;
  // A snapshot of the stored generation and revision is current, so its other fields apply.
  if (next?.runtimeGeneration !== undefined && ahead === 0) return undefined;
  return current;
}

const decodeStoredThread = Schema.decodeUnknownOption(
  Schema.fromJsonString(OrchestrationV2ProviderThreadJson),
);
const REPORT_EVERY = 100;
const MAX_TRACKED_REJECTIONS = 512;

export const makeRuntimeEvidenceGuard = (sql: SqlClient.SqlClient) => {
  const rejections = new Map<string, number>();

  /** A stuck producer repeats the same rejection for every event, so repeats are sampled. */
  const reportRejection = (input: {
    readonly capture: ProviderRuntimeEvidenceCapture;
    readonly reason: RuntimeEvidenceRejection;
    readonly events: ReadonlyArray<RecordedLifecycleEvent>;
    readonly settled?: boolean;
  }) =>
    Effect.suspend(() => {
      const key = `${input.capture.providerThreadId}\0${input.reason}`;
      const count = (rejections.get(key) ?? 0) + 1;
      if (count === 1 && rejections.size >= MAX_TRACKED_REJECTIONS) rejections.clear();
      rejections.set(key, count);
      const annotate = Effect.annotateCurrentSpan({
        "orchestration_v2.runtime_evidence_rejection": input.reason,
      });
      if (input.settled !== true && count !== 1 && count % REPORT_EVERY !== 0) return annotate;
      const message =
        input.settled === true
          ? "orchestration-v2.runtime-evidence-lost-run-settled"
          : "orchestration-v2.runtime-evidence-rejected";
      return annotate.pipe(
        Effect.andThen(
          Effect.logWarning(message, {
            threadId: input.capture.threadId,
            providerThreadId: input.capture.providerThreadId,
            reason: input.reason,
            eventTypes: [...new Set(input.events.map((event) => event.type))],
            rejections: count,
          }),
        ),
      );
    });

  return {
    reportRejection,
    /** One primary-key read per provider thread update; batches without one read nothing. */
    retainStoredIdentity: Effect.fnUntraced(function* (
      events: ReadonlyArray<RecordedLifecycleEvent>,
    ) {
      if (!events.some((event) => event.type === "provider-thread.updated")) return events;
      const known = new Map<string, OrchestrationV2ProviderThread | null>();
      const retained: Array<RecordedLifecycleEvent> = [];
      for (const event of events) {
        if (event.type !== "provider-thread.updated") {
          retained.push(event);
          continue;
        }
        let stored = known.get(event.payload.id);
        if (stored === undefined) {
          const rows = yield* sql<{ readonly payload_json: string }>`
            SELECT payload_json
            FROM orchestration_v2_projection_provider_threads
            WHERE provider_thread_id = ${event.payload.id}`;
          const row = rows[0];
          stored =
            row === undefined ? null : Option.getOrNull(decodeStoredThread(row.payload_json));
        }
        const identity =
          stored === null ? undefined : retainedRuntimeIdentity(stored, event.payload);
        const next =
          identity === undefined
            ? event
            : { ...event, payload: { ...event.payload, runtimeIdentity: identity } };
        known.set(next.payload.id, next.payload);
        retained.push(next);
      }
      return retained;
    }),
    /**
     * The guard rejected this capture. Returns null to keep the rejection, or the events to
     * commit when a current-run settlement lost only its producer evidence.
     */
    rejected: Effect.fnUntraced(function* (input: {
      readonly capture: ProviderRuntimeEvidenceCapture;
      readonly current: OrchestrationV2ProviderThread | null;
      readonly reason: RuntimeEvidenceRejection;
      readonly events: ReadonlyArray<RecordedLifecycleEvent>;
      readonly settleRunOnLostEvidence: boolean;
    }) {
      const current = input.current;
      const report = {
        capture: input.capture,
        reason: input.reason,
        events: input.events,
      };
      if (
        !input.settleRunOnLostEvidence ||
        current === null ||
        !isLostRuntimeEvidence(input.reason)
      ) {
        yield* reportRejection(report);
        return null;
      }
      yield* reportRejection({ ...report, settled: true });
      return input.events.map((event): RecordedLifecycleEvent =>
        event.type === "provider-thread.updated" && event.payload.id === current.id
          ? { ...event, payload: { ...event.payload, runtimeIdentity: current.runtimeIdentity } }
          : event,
      );
    }),
  };
};
