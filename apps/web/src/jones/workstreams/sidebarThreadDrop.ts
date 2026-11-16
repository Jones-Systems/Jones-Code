import { planPinnedReorder } from "@t3tools/client-runtime/state/thread-sort";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import type { NativeThreadExternalDrop } from "../../components/workstreams/WorkstreamNativeSidebar";

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
  const assignment = input.bridge?.dropExternal(input.point, input.thread);
  if (!assignment) return false;
  void assignment
    .then(async (committed) => {
      if (committed) await input.unpin();
    })
    .catch(input.onError);
  return true;
}
