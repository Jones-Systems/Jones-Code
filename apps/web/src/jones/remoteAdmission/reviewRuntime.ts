import type { EnvironmentId } from "@t3tools/contracts";
import { createReviewController, type ReviewClient } from "@t3tools/client-runtime/jones/remoteAdmission/controller";
import { useSyncExternalStore } from "react";
import { receiverAttention } from "./attentionPersistence";

const controllers = new Map<EnvironmentId, ReturnType<typeof createReviewController>>();
const listeners = new Set<() => void>();
let revision = 0;
const subscribe = (listener: () => void) => { listeners.add(listener); return () => { listeners.delete(listener); }; };
const getRevision = () => revision;
/** Installation is observable: legacy consumers switch only after capability discovery. */
export function installReviewClient(receiver: EnvironmentId, client: ReviewClient) {
  if (controllers.has(receiver)) throw new Error("Review client already registered for this receiver");
  const controller = createReviewController(client);
  controller.setAttentionPersistence(receiverAttention(receiver));
  controllers.set(receiver, controller);
  ++revision;
  for (const listener of listeners) listener();
}
export const reviewControllerFor = (receiver: EnvironmentId) => controllers.get(receiver) ?? null;
export const useReviewRegistry = () => useSyncExternalStore(subscribe, getRevision, getRevision);
export const useReviewController = (receiver: EnvironmentId) => useSyncExternalStore(
  subscribe, () => reviewControllerFor(receiver), () => null,
);
/** The revision is an explicit dependency for compiler-memoized collection readers. */
export const reviewControllersFor = (receivers: readonly EnvironmentId[], _revision: number) => receivers.map(reviewControllerFor);
