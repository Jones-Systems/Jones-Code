import { planPinnedReorder } from "@t3tools/client-runtime/state/thread-sort";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import type { NativeThreadExternalDrop } from "../../components/workstreams/WorkstreamNativeSidebar";

export function isWorkstreamDropLane(
  list: HTMLElement,
  point: { x: number; y: number },
  draggedThreadKey?: string,
) {
  const { ownerDocument } = list;
  // The sortable source card can cover the lane beneath the pointer.
  const hit =
    draggedThreadKey === undefined
      ? ownerDocument.elementFromPoint(point.x, point.y)
      : ownerDocument
          .elementsFromPoint(point.x, point.y)
          .find(
            (element) =>
              element.closest<HTMLElement>("[data-thread-item]")?.dataset.threadItem !==
              draggedThreadKey,
          );
  const lane = hit?.closest("[data-native-workstream]");
  return lane != null && list.contains(lane);
}

export function planWritableSidebarReorder(
  order: readonly string[],
  keysById: ReadonlyMap<string, string | null | undefined>,
  movedId: string,
  writable?: ReadonlySet<string>,
) {
  if (!writable) return planPinnedReorder({ orderedIds: order, keysById, movedId });
  if (!writable.has(movedId)) return null;
  const assignments = planPinnedReorder({
    orderedIds: order.filter((key) => writable.has(key) || keysById.get(key) != null),
    keysById,
    movedId,
  });
  return assignments.every(({ id }) => writable.has(id)) ? assignments : null;
}

export function dropPinnedThreadOnWorkstream(input: {
  readonly bridge: NativeThreadExternalDrop | null;
  readonly point: { x: number; y: number } | null;
  readonly thread: EnvironmentThreadShell;
  readonly unpin: () => Promise<void>;
  readonly onError: (cause: unknown) => void;
}): boolean {
  if (!input.point || input.thread.pinnedAt == null) return false;
  const placement = input.bridge?.dropExternal(input.point, input.thread, {
    unpin: async () => {
      try {
        await input.unpin();
        return true;
      } catch (cause) {
        input.onError(cause);
        return false;
      }
    },
  });
  if (!placement) return false;
  void placement.catch(input.onError);
  return true;
}
