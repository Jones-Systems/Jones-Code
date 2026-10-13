import type {
  VoiceReviewQueueDelivery,
  VoiceReviewRecentEntry,
  WorkQueueMetadata,
} from "@t3tools/contracts";
import type { ConfirmedSentPrompt } from "./SentPrompts";

export function classifySentPrompts(
  snapshot: WorkQueueMetadata | null,
  voice: readonly VoiceReviewRecentEntry[],
  nativeEnvironmentId: string | null,
) {
  const sent: ConfirmedSentPrompt[] = [];
  const confirmedRequests = new Set<string>();
  const matchedDeliveries = new Set<string>();
  const remainingVoice: VoiceReviewRecentEntry[] = [];
  const sourceBound =
    snapshot !== null &&
    nativeEnvironmentId !== null &&
    snapshot.source.environment_ref === nativeEnvironmentId;
  const linksByRequest = new Map<
    string,
    { entry: VoiceReviewRecentEntry; delivery: VoiceReviewQueueDelivery }[]
  >();
  if (sourceBound) {
    for (const entry of voice) {
      if (entry.queue_delivery_links?.status !== "ready") continue;
      for (const delivery of entry.queue_delivery_links.deliveries) {
        const links = linksByRequest.get(delivery.queue_request_id) ?? [];
        links.push({ entry, delivery });
        linksByRequest.set(delivery.queue_request_id, links);
      }
    }
  }
  if (
    snapshot?.schema === "codex.t3-work-queue-metadata/v2" ||
    snapshot?.schema === "codex.t3-work-queue-metadata/v3"
  ) {
    for (const item of snapshot.items) {
      const proof = item.native_handoff;
      if (
        !proof ||
        !item.target ||
        !item.payload_sha256 ||
        !/^[a-f0-9]{64}$/.test(item.payload_sha256) ||
        proof.command_status !== "accepted" ||
        (proof.correlation_status === "exact"
          ? !proof.turn_id
          : snapshot.schema !== "codex.t3-work-queue-metadata/v3" ||
            proof.correlation_status !== "pending" ||
            proof.turn_id !== null ||
            proof.turn_state !== "pending") ||
        !proof.command_id ||
        !proof.message_id ||
        !Number.isSafeInteger(proof.receipt_sequence) ||
        proof.receipt_sequence < 0 ||
        !Number.isSafeInteger(proof.observed_at_ms) ||
        proof.observed_at_ms < 0 ||
        !Number.isFinite(new Date(proof.observed_at_ms).getTime()) ||
        proof.observed_at_ms > snapshot.observed_at_ms
      )
        continue;
      confirmedRequests.add(item.request_id);
      const candidates =
        sourceBound &&
        item.target.environment_ref === nativeEnvironmentId &&
        item.target.host_id === snapshot.source.host_id
          ? (linksByRequest.get(item.request_id) ?? []).filter(
              ({ delivery }) =>
                delivery.payload_sha256 === item.payload_sha256 &&
                delivery.destination.thread_id === item.target?.thread_id,
            )
          : [];
      const match = candidates.length === 1 ? candidates[0] : undefined;
      if (match) matchedDeliveries.add(`${match.entry.draft.id}\0${match.delivery.delivery_id}`);
      sent.push({
        id: item.request_id,
        text: match?.entry.text_state === "available" ? match.entry.text : null,
        target: { threadId: item.target.thread_id, label: null },
        sentAt: proof.observed_at_ms,
        confirmation: {
          status: "confirmed",
          kind: "handoff-receipt",
          receiptId: proof.command_id,
          source: `${snapshot.source.queue_id} / ${snapshot.source.host_id} / ${snapshot.source.environment_ref}`,
        },
        sourceLabel: match
          ? `Voice · ${match.delivery.destination.backend} · ${match.delivery.destination.instance}`
          : null,
        workstreams: [item.workstream_id],
        providerState: proof.turn_state,
      });
    }
  }
  for (const entry of voice) {
    const links = entry.queue_delivery_links;
    if (links?.status !== "ready" || links.deliveries.length === 0) {
      remainingVoice.push(entry);
      continue;
    }
    const deliveries = links.deliveries.filter(
      (delivery) => !matchedDeliveries.has(`${entry.draft.id}\0${delivery.delivery_id}`),
    );
    // Older servers cannot prove the linked deliveries cover every sibling.
    if (deliveries.length > 0 || links.unlinked_delivery_count !== 0)
      remainingVoice.push({
        ...entry,
        queue_delivery_links: { ...links, deliveries },
      });
  }
  return { sent, confirmedRequests, remainingVoice, sourceBound };
}
