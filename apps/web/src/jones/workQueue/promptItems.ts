import type {
  VoiceReviewDraft,
  VoiceReviewQueueDelivery,
  VoiceReviewRecentEntry,
  WorkQueueMetadata,
} from "@t3tools/contracts";
import { classifySentPrompts } from "./classifySentPrompts";
import type { ConfirmedSentPrompt } from "./SentPrompts";

/**
 * The Prompts item model. One item is one prompt in one stage: a voice draft, the queue
 * requests it produced and the handoff evidence for each, or a queue request with no voice
 * link. Stage placement reuses `classifySentPrompts`; nothing reaches Sent on weaker evidence.
 */

export type PromptStage = "pending" | "queued" | "sent";
export type WorkQueueItem = WorkQueueMetadata["items"][number];

export type PromptTextState =
  | { readonly kind: "available"; readonly text: string }
  | { readonly kind: "deleted" | "expired" | "unavailable" | "not_carried" };

export interface PromptDestination {
  readonly key: string;
  readonly deliveryId: string | null;
  readonly queueRequestId: string | null;
  readonly threadId: string | null;
  /** The environment the queue says owns the thread; null when only the voice source named it. */
  readonly targetEnvironmentRef: string | null;
  readonly targetHostId: string | null;
  readonly backend: string | null;
  readonly instance: string | null;
  /** The queue request this delivery is proved to be, or the request itself for queue items. */
  readonly queueItem: WorkQueueItem | null;
  readonly handoff: ConfirmedSentPrompt | null;
}

export interface PromptItem {
  readonly key: string;
  readonly stage: PromptStage;
  readonly origin: "voice" | "queue";
  readonly draft: VoiceReviewDraft | null;
  readonly entry: VoiceReviewRecentEntry | null;
  readonly text: PromptTextState;
  readonly destinations: readonly PromptDestination[];
  /** Deliveries the voice source reported without a queue link; null when it cannot say. */
  readonly unlinkedDeliveries: number | null;
  /** Milliseconds for ordering and display: capture, submission or handoff time. */
  readonly time: number | null;
  readonly timeKind: "captured" | "submitted" | "handoff" | null;
}

export interface PromptItemsInput {
  readonly drafts: readonly VoiceReviewDraft[];
  readonly recent: readonly VoiceReviewRecentEntry[];
  readonly snapshot: WorkQueueMetadata | null;
  readonly nativeEnvironmentId: string | null;
}

export interface PromptItemsResult {
  readonly pending: readonly PromptItem[];
  readonly queued: readonly PromptItem[];
  readonly sent: readonly PromptItem[];
  /** Some confirmed entries were dropped because their receipt fields are incomplete. */
  readonly sentOmitted: boolean;
  readonly sourceBound: boolean;
  readonly confirmedRequests: ReadonlySet<string>;
}

const terminalDraftStates: ReadonlySet<string> = new Set(["released", "deleted", "expired"]);

export function voiceItemKey(draftId: string) {
  return `voice:${draftId}`;
}
export function queueItemKey(requestId: string) {
  return `queue:${requestId}`;
}

export function hasConfirmedHandoff(entry: ConfirmedSentPrompt) {
  return (
    entry.confirmation?.status === "confirmed" &&
    entry.confirmation.kind === "handoff-receipt" &&
    typeof entry.confirmation.receiptId === "string" &&
    entry.confirmation.receiptId.trim().length > 0 &&
    typeof entry.confirmation.source === "string" &&
    entry.confirmation.source.trim().length > 0 &&
    Number.isFinite(new Date(entry.sentAt).getTime())
  );
}

function parseTime(iso: string | null | undefined) {
  if (!iso) return null;
  const value = Date.parse(iso);
  return Number.isFinite(value) ? value : null;
}

function draftText(draft: VoiceReviewDraft): PromptTextState {
  return draft.text === null ? { kind: "unavailable" } : { kind: "available", text: draft.text };
}

function entryText(entry: VoiceReviewRecentEntry): PromptTextState {
  if (entry.text_state === "available")
    return entry.text === null ? { kind: "unavailable" } : { kind: "available", text: entry.text };
  return { kind: entry.text_state };
}

function payloadDigest(item: WorkQueueItem) {
  return "payload_sha256" in item ? item.payload_sha256 : null;
}

function deliveryDestination(
  delivery: VoiceReviewQueueDelivery,
  queueItem: WorkQueueItem | null,
  handoff: ConfirmedSentPrompt | null,
): PromptDestination {
  return {
    key: delivery.delivery_id,
    deliveryId: delivery.delivery_id,
    queueRequestId: delivery.queue_request_id,
    threadId: delivery.destination.thread_id,
    targetEnvironmentRef: queueItem?.target?.environment_ref ?? null,
    targetHostId: queueItem?.target?.host_id ?? null,
    backend: delivery.destination.backend,
    instance: delivery.destination.instance,
    queueItem,
    handoff,
  };
}

function requestDestination(
  item: WorkQueueItem,
  handoff: ConfirmedSentPrompt | null,
): PromptDestination {
  return {
    key: item.request_id,
    deliveryId: null,
    queueRequestId: item.request_id,
    threadId: item.target?.thread_id ?? handoff?.target.threadId ?? null,
    targetEnvironmentRef: item.target?.environment_ref ?? null,
    targetHostId: item.target?.host_id ?? null,
    backend: null,
    instance: null,
    queueItem: item,
    handoff,
  };
}

function unlinkedCount(entry: VoiceReviewRecentEntry) {
  const links = entry.queue_delivery_links;
  return links?.status === "ready" ? (links.unlinked_delivery_count ?? null) : null;
}

/**
 * Pairs each unconfirmed queue request with at most one voice delivery, under the same rule
 * `classifySentPrompts` applies before attaching voice text to a Sent row: the queue source is
 * bound to this native environment, the request targets that environment on the source host,
 * and exactly one delivery shares the request ID, payload digest and destination thread.
 */
function linkQueuedDeliveries(
  snapshot: WorkQueueMetadata | null,
  entries: readonly VoiceReviewRecentEntry[],
  nativeEnvironmentId: string | null,
  sourceBound: boolean,
  confirmed: ReadonlySet<string>,
) {
  const linked = new Map<string, WorkQueueItem>();
  if (!snapshot || !sourceBound || nativeEnvironmentId === null) return linked;
  const linksByRequest = new Map<
    string,
    { entry: VoiceReviewRecentEntry; delivery: VoiceReviewQueueDelivery }[]
  >();
  for (const entry of entries) {
    if (entry.queue_delivery_links?.status !== "ready") continue;
    for (const delivery of entry.queue_delivery_links.deliveries) {
      const links = linksByRequest.get(delivery.queue_request_id) ?? [];
      links.push({ entry, delivery });
      linksByRequest.set(delivery.queue_request_id, links);
    }
  }
  for (const item of snapshot.items) {
    if (confirmed.has(item.request_id)) continue;
    const digest = payloadDigest(item);
    if (
      !item.target ||
      !digest ||
      item.target.environment_ref !== nativeEnvironmentId ||
      item.target.host_id !== snapshot.source.host_id
    )
      continue;
    const candidates = (linksByRequest.get(item.request_id) ?? []).filter(
      ({ delivery }) =>
        delivery.payload_sha256 === digest &&
        delivery.destination.thread_id === item.target?.thread_id,
    );
    const match = candidates.length === 1 ? candidates[0] : undefined;
    if (match) linked.set(`${match.entry.draft.id}\0${match.delivery.delivery_id}`, item);
  }
  return linked;
}

export function buildPromptItems(input: PromptItemsInput): PromptItemsResult {
  const { drafts, recent, snapshot, nativeEnvironmentId } = input;
  const pending: PromptItem[] = drafts
    .filter((draft) => !terminalDraftStates.has(draft.state))
    .map((draft) => ({
      key: voiceItemKey(draft.id),
      stage: "pending",
      origin: "voice",
      draft,
      entry: null,
      text: draftText(draft),
      destinations: [],
      unlinkedDeliveries: null,
      time: parseTime(draft.created_at),
      timeKind: "captured",
    }));

  const classified = classifySentPrompts(snapshot, recent, nativeEnvironmentId);
  const requests = new Map((snapshot?.items ?? []).map((item) => [item.request_id, item]));

  // classifySentPrompts removes each delivery it proved from the remaining voice entries, so
  // the difference names the voice prompt behind each confirmed request without re-deriving it.
  const remainingById = new Map(classified.remainingVoice.map((entry) => [entry.draft.id, entry]));
  const duplicateIds = new Set<string>();
  const seenIds = new Set<string>();
  for (const entry of recent) {
    if (seenIds.has(entry.draft.id)) duplicateIds.add(entry.draft.id);
    seenIds.add(entry.draft.id);
  }
  const voiceForRequest = new Map<
    string,
    { entry: VoiceReviewRecentEntry; delivery: VoiceReviewQueueDelivery }
  >();
  for (const entry of recent) {
    const links = entry.queue_delivery_links;
    if (links?.status !== "ready" || duplicateIds.has(entry.draft.id)) continue;
    const remaining = remainingById.get(entry.draft.id);
    const kept = new Set(
      remaining?.queue_delivery_links?.status === "ready"
        ? remaining.queue_delivery_links.deliveries.map((delivery) => delivery.delivery_id)
        : [],
    );
    for (const delivery of links.deliveries) {
      if (!kept.has(delivery.delivery_id))
        voiceForRequest.set(delivery.queue_request_id, { entry, delivery });
    }
  }

  const confirmedEntries = classified.sent.filter(hasConfirmedHandoff);
  const sentOmitted = confirmedEntries.length !== classified.sent.length;
  const sentByVoice = new Map<
    string,
    { entry: VoiceReviewRecentEntry; destinations: PromptDestination[] }
  >();
  const sent: PromptItem[] = [];
  for (const handoff of confirmedEntries) {
    const queueItem = requests.get(handoff.id) ?? null;
    const voice = voiceForRequest.get(handoff.id);
    if (voice) {
      const destination = deliveryDestination(voice.delivery, queueItem, handoff);
      const group = sentByVoice.get(voice.entry.draft.id);
      if (group) group.destinations.push(destination);
      else
        sentByVoice.set(voice.entry.draft.id, { entry: voice.entry, destinations: [destination] });
      continue;
    }
    sent.push({
      key: queueItemKey(handoff.id),
      stage: "sent",
      origin: "queue",
      draft: null,
      entry: null,
      text: { kind: "not_carried" },
      destinations: queueItem
        ? [requestDestination(queueItem, handoff)]
        : [
            {
              key: handoff.id,
              deliveryId: null,
              queueRequestId: handoff.id,
              threadId: handoff.target.threadId,
              targetEnvironmentRef: null,
              targetHostId: null,
              backend: null,
              instance: null,
              queueItem: null,
              handoff,
            },
          ],
      unlinkedDeliveries: null,
      time: handoff.sentAt,
      timeKind: "handoff",
    });
  }
  for (const { entry, destinations } of sentByVoice.values()) {
    sent.push({
      key: voiceItemKey(entry.draft.id),
      stage: "sent",
      origin: "voice",
      draft: entry.draft,
      entry,
      text: entryText(entry),
      destinations,
      unlinkedDeliveries: unlinkedCount(entry),
      time: Math.max(...destinations.map((destination) => destination.handoff?.sentAt ?? 0)),
      timeKind: "handoff",
    });
  }

  const linked = linkQueuedDeliveries(
    snapshot,
    classified.remainingVoice,
    nativeEnvironmentId,
    classified.sourceBound,
    classified.confirmedRequests,
  );
  const absorbed = new Set<string>();
  const queued: PromptItem[] = [];
  for (const entry of classified.remainingVoice) {
    if (entry.draft.state !== "released" && entry.command_id === null) continue;
    const links = entry.queue_delivery_links;
    const deliveries = links?.status === "ready" ? links.deliveries : [];
    const destinations = deliveries.map((delivery) => {
      const queueItem = linked.get(`${entry.draft.id}\0${delivery.delivery_id}`) ?? null;
      if (queueItem) absorbed.add(queueItem.request_id);
      return deliveryDestination(delivery, queueItem, null);
    });
    queued.push({
      key: voiceItemKey(entry.draft.id),
      stage: "queued",
      origin: "voice",
      draft: entry.draft,
      entry,
      text: entryText(entry),
      destinations,
      unlinkedDeliveries: unlinkedCount(entry),
      time: parseTime(entry.draft.created_at),
      timeKind: "captured",
    });
  }
  for (const item of snapshot?.items ?? []) {
    if (classified.confirmedRequests.has(item.request_id) || absorbed.has(item.request_id))
      continue;
    queued.push({
      key: queueItemKey(item.request_id),
      stage: "queued",
      origin: "queue",
      draft: null,
      entry: null,
      text: { kind: "not_carried" },
      destinations: [requestDestination(item, null)],
      unlinkedDeliveries: null,
      time: item.submitted_at_ms,
      timeKind: item.submitted_at_ms === null ? null : "submitted",
    });
  }

  return {
    pending,
    queued,
    sent,
    sentOmitted,
    sourceBound: classified.sourceBound,
    confirmedRequests: classified.confirmedRequests,
  };
}

/** Search across every stage for an item key, used by deep links. */
export function findPromptItem(result: PromptItemsResult, key: string) {
  for (const stage of ["pending", "queued", "sent"] as const) {
    const item = result[stage].find((candidate) => candidate.key === key);
    if (item) return item;
  }
  return null;
}
