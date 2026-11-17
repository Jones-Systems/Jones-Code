import { useEffect, useState, useSyncExternalStore, type FocusEvent } from "react";
import type { EnvironmentId } from "@t3tools/contracts";
import type { ReviewState } from "@t3tools/client-runtime/jones/remoteAdmission/controller";
import { reviewControllerFor, useReviewController } from "./reviewRuntime";

const noSubscription = () => () => {};
const noSnapshot = () => null;
export function useOptionalReviewState(receiver: EnvironmentId) {
  const controller = useReviewController(receiver);
  const state = useSyncExternalStore<ReviewState | null>(
    controller?.subscribe ?? noSubscription,
    controller?.getSnapshot ?? noSnapshot,
  );
  return { controller, state };
}

export function useReview(receiver: EnvironmentId) {
  const controller = reviewControllerFor(receiver);
  if (!controller) throw new Error("Review client is not installed");
  const state = useSyncExternalStore(controller.subscribe, controller.getSnapshot);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Mount the selected owner controller read alongside its useSyncExternalStore subscription; all rendered snapshots remain controller-owned.
  useEffect(() => {
    void controller.load();
  }, [controller]);
  return { ...state, controller };
}

export function useFrozenRows<T>(rows: readonly T[], key: (item: T) => string) {
  const [pointer, setPointer] = useState(false);
  const [focus, setFocus] = useState(false);
  const [pinned, setPinned] = useState<readonly T[]>([]);
  const frozen = pointer || focus;
  const shown = frozen ? pinned : rows;
  return {
    rows: shown,
    frozen,
    isGone: (row: T) => !rows.some((current) => key(current) === key(row)),
    handlers: {
      onPointerEnter: () => {
        if (!frozen) setPinned(rows);
        setPointer(true);
      },
      onPointerLeave: () => setPointer(false),
      onFocusCapture: () => {
        if (!frozen) setPinned(rows);
        setFocus(true);
      },
      onBlurCapture: (event: FocusEvent<HTMLElement>) => {
        if (!event.currentTarget.contains(event.relatedTarget)) setFocus(false);
      },
    },
  };
}
