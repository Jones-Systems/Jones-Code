import type { EnvironmentId } from "@t3tools/contracts";
import {
  createReviewController,
  type ReviewClient,
} from "@t3tools/client-runtime/jones/remoteAdmission/controller";
import { useSyncExternalStore } from "react";
import { receiverAttention } from "./attentionPersistence";

const controllers = new Map<EnvironmentId, ReturnType<typeof createReviewController>>();
const unsupportedReceivers = new Map<EnvironmentId, "held" | "legacy">();
const listeners = new Set<() => void>();
let revision = 0;
const subscribe = (listener: () => void) => {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
};
const getRevision = () => revision;
const publish = () => {
  ++revision;
  for (const listener of listeners) listener();
};
/** Installation is observable: legacy consumers switch only after capability discovery. */
export function installReviewClient(receiver: EnvironmentId, client: ReviewClient) {
  if (controllers.has(receiver))
    throw new Error("Review client already registered for this receiver");
  const controller = createReviewController(client);
  controller.setAttentionPersistence(receiverAttention(receiver));
  controllers.set(receiver, controller);
  publish();
}
export const retainedReviewControllerFor = (receiver: EnvironmentId) =>
  controllers.get(receiver) ?? null;
export const reviewControllerFor = (receiver: EnvironmentId) =>
  unsupportedReceivers.get(receiver) === "legacy" ? null : retainedReviewControllerFor(receiver);
export const reviewSupportKnownMissing = (receiver: EnvironmentId) =>
  unsupportedReceivers.has(receiver);
export const useReviewSupportKnownMissing = (receiver: EnvironmentId) =>
  useSyncExternalStore(
    subscribe,
    () => reviewSupportKnownMissing(receiver),
    () => false,
  );

export function restoreReviewSupport(receiver: EnvironmentId) {
  if (unsupportedReceivers.delete(receiver)) publish();
}

export function fallBackToLegacyReview(receiver: EnvironmentId): boolean {
  const controller = reviewControllerFor(receiver);
  if (!controller) return false;
  const { pending, uncertain, policyBarrier } = controller.getSnapshot();
  // Retain the protocol and its readback controls while an effect is unsettled.
  if (pending.size || uncertain.size || policyBarrier) {
    if (unsupportedReceivers.get(receiver) !== "held") {
      unsupportedReceivers.set(receiver, "held");
      publish();
    }
    return false;
  }
  controller.invalidateConnection();
  unsupportedReceivers.set(receiver, "legacy");
  publish();
  return true;
}
export const useReviewRegistry = () => useSyncExternalStore(subscribe, getRevision, getRevision);
export const useReviewController = (receiver: EnvironmentId) =>
  useSyncExternalStore(
    subscribe,
    () => reviewControllerFor(receiver),
    () => null,
  );
/** The revision is an explicit dependency for compiler-memoized collection readers. */
export const reviewControllersFor = (receivers: readonly EnvironmentId[], _revision: number) =>
  receivers.map(reviewControllerFor);
