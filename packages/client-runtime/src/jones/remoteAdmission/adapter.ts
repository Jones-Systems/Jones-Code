import type * as Wire from "@t3tools/contracts/jones/remoteAdmissionReview";
import type { Access, Item, ReviewClient } from "./controller.ts";

export interface ReviewTransport {
  capabilities(): Promise<Wire.RemoteAdmissionReviewCapabilities>;
  review(query: Wire.RemoteAdmissionReviewQuery): Promise<Wire.RemoteAdmissionReviewPage>;
  item(query: Wire.RemoteAdmissionReviewItemQuery): Promise<Wire.RemoteAdmissionReviewItem>;
  grants(query: Wire.RemoteAdmissionGrantQuery): Promise<Wire.RemoteAdmissionGrantsPage>;
  decision(
    input: Wire.RemoteAdmissionReviewDecision,
  ): Promise<Wire.RemoteAdmissionReviewDecisionResult>;
  updateGrant(input: Wire.RemoteAdmissionGrantUpdate): Promise<Wire.RemoteAdmissionGrant>;
  revokeGrant(input: Wire.RemoteAdmissionGrantRevoke): Promise<Wire.RemoteAdmissionGrant>;
  restoreGrant(
    input: Wire.RemoteAdmissionGrantRestore,
  ): Promise<Wire.RemoteAdmissionGrantRestoreResult>;
  retry(input: Wire.RemoteAdmissionRetryInput): Promise<Wire.RemoteAdmissionRetryAttempt>;
  recheck(input: Wire.RemoteAdmissionRecheckInput): Promise<Wire.RemoteAdmissionRecheckResult>;
  changes(
    query: Wire.RemoteAdmissionChangesQuery,
    signal?: AbortSignal,
  ): Promise<Wire.RemoteAdmissionChanges>;
}
function reviewItem(item: Wire.RemoteAdmissionReviewItem): Item {
  const x = item.extension;
  const result = x.recoveredResult?.result ?? x.result;
  return {
    record: item.record,
    launchResult: result,
    originalLaunchResult: x.result,
    attempt: x.recoveredResult
      ? `recovered:${x.recoveredResult.revision}`
      : (x.attemptId ?? "original"),
    blocked: x.blocked !== null,
    accessId: x.managedAccessGrantId ?? x.grant?.grantId ?? null,
    grantEligible: x.grantable,
    outcome: result
      ? result.outcome === "refused"
        ? "failed"
        : result.outcome
      : ["approved", "passed", "dispatching"].includes(item.record.status)
        ? "awaiting"
        : null,
    availability: result?.stages.availability ?? "unknown",
    threadId: result?.threadId ?? item.record.envelope.target.threadId ?? null,
    messageStage: result ? `Initial message ${result.stages.initialMessage}` : null,
    reason: result?.reasonCode ?? x.blocked?.reason ?? x.grantUnavailableReason,
    canRetry: x.retryEligible,
    canRecheck: x.recheckEligible,
  };
}
function reviewAccess(grant: Wire.RemoteAdmissionGrant): Access {
  return {
    id: grant.grantId,
    generation: grant.generation,
    state: grant.state,
    title: grant.binding.destinationThreadId ? "Destination thread" : "Pending destination",
    origin: grant.binding.originEnvironmentId,
    destinationThreadId: grant.binding.destinationThreadId,
    expiresAt: grant.expiresAt,
  };
}
export function createCanonicalReviewClient(transport: ReviewTransport): ReviewClient {
  return {
    async capabilities() {
      const c = await transport.capabilities();
      return {
        canDecide: c.principal.canDecide,
        features: new Set(
          (Object.keys(c.features) as (keyof typeof c.features)[]).filter((key) => c.features[key]),
        ),
      };
    },
    async page(view, cursor) {
      const page = await transport.review({ view, ...(cursor ? { cursor } : {}) });
      return {
        revision: page.snapshotRevision,
        policyRevision: page.policy.revision,
        counts: page.counts,
        records: page.items.map(reviewItem),
        access: page.grants.map(reviewAccess),
        nextCursor: page.nextCursor,
        recordCount: page.retainedRecordCount,
        diagnostics: [
          ...page.blockedEvents.map((event) => ({
            id: event.eventId,
            reason: event.reason,
            repeats: event.repeatCount,
            requestId: event.reviewLink?.kind === "request" ? event.reviewLink.admissionId : null,
          })),
          ...page.blockedSummaries.map((summary) => ({
            id: JSON.stringify([summary.managedScopeId, summary.reason]),
            reason: `${summary.reason} · ${summary.logicalEvents} folded logical events`,
            repeats: summary.repeatArrivals,
            requestId: null,
          })),
        ],
        diagnosticCounts: page.blockedDiagnosticCounts,
      };
    },
    async lookup(id, digest) {
      const item = await transport.item({ id });
      if (digest !== undefined && item.record.bindingDigest !== digest)
        throw new Error("Request binding changed");
      return reviewItem(item);
    },
    async lookupAccess(id) {
      const page = await transport.grants({ grantId: id });
      const grant = page.grants.find((entry) => entry.grantId === id);
      if (!grant) throw new Error("Exact access record unavailable");
      return reviewAccess(grant);
    },
    async lookupRestore(predecessorId, originalCommandId) {
      const page = await transport.grants({
        grantId: predecessorId,
        restoreCommandKey: originalCommandId,
      });
      const successor = page.grants.find(
        (entry) => entry.predecessorGrantId === predecessorId && entry.grantId !== predecessorId,
      );
      if (!successor || page.grants.length !== 1)
        throw new Error("Original Restore successor unavailable");
      return reviewAccess(successor);
    },
    async mutate(input) {
      try {
        if (input.action === "decide") {
          await transport.decision(
            input.input.decision === "deny"
              ? { id: input.input.id, bindingDigest: input.input.bindingDigest, decision: "deny" }
              : { ...input.input, decision: "approve" },
          );
          return { item: reviewItem(await transport.item({ id: input.input.id })) };
        }
        if (input.action === "recheck")
          return {
            item: reviewItem(
              (await transport.recheck({ id: input.id, bindingDigest: input.bindingDigest })).item,
            ),
          };
        if (input.action === "retry") {
          await transport.retry({
            id: input.id,
            bindingDigest: input.bindingDigest,
            commandKey: input.commandId,
            ...(input.durationSeconds ? { grant: { durationSeconds: input.durationSeconds } } : {}),
          });
          return {};
        }
        if (!("expectedGeneration" in input)) throw new Error("Access generation is required");
        const base = { grantId: input.id, expectedGeneration: input.expectedGeneration };
        if (input.action === "revoke")
          return { access: reviewAccess(await transport.revokeGrant(base)) };
        if (!input.durationSeconds) throw new Error("Choose Duration");
        if (input.action === "restore")
          return {
            access: reviewAccess(
              (
                await transport.restoreGrant({
                  ...base,
                  durationSeconds: input.durationSeconds,
                  commandKey: input.commandId,
                })
              ).successor,
            ),
          };
        return {
          access: reviewAccess(
            await transport.updateGrant({ ...base, durationSeconds: input.durationSeconds }),
          ),
        };
      } catch (error) {
        if (
          typeof error === "object" &&
          error !== null &&
          "code" in error &&
          error.code === "conflict" &&
          "current" in error
        ) {
          const conflict = error as Wire.RemoteAdmissionReviewConflict;
          throw {
            code: "conflict",
            current: {
              grant: conflict.current.grant ? reviewAccess(conflict.current.grant) : null,
              chainHead: conflict.current.chainHead
                ? reviewAccess(conflict.current.chainHead)
                : null,
            },
          };
        }
        throw error;
      }
    },
    async head() {
      return (await transport.changes({ timeoutMs: 0 })).after;
    },
    async changes(after, signal) {
      const changes = await transport.changes({ after, timeoutMs: 50000 }, signal);
      const ids = new Set(
        changes.changes.flatMap((change) =>
          change.link.kind === "request" ? [change.link.admissionId] : [],
        ),
      );
      const results: Item[] = [];
      for (const id of ids) results.push(reviewItem(await transport.item({ id })));
      return { cursor: changes.after, revision: changes.snapshotRevision, results };
    },
  };
}
