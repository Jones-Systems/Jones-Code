// @effect-diagnostics globalDate:off - Atom subscriptions own view lifetime outside an Effect runtime.
import type { ScopedThreadRef } from "@t3tools/contracts";
import { createThreadReadDwell } from "@t3tools/client-runtime/jones/thread-read-dwell";
import { parseThreadKey, threadKey } from "@t3tools/client-runtime/state/entities";
import { runAtomCommand } from "@t3tools/client-runtime/state/runtime";
import { claimThreadOpen } from "@t3tools/client-runtime/state/thread-search";
import type { ThreadVisitTrackerOptions } from "@t3tools/client-runtime/state/thread-visits";
import { Atom } from "effect/reactivity";

const idle = Atom.make<void>(undefined);

export function createMobileThreadVisitTracker(options: ThreadVisitTrackerOptions) {
  const family = Atom.family((key: string) => {
    const ref = parseThreadKey(key);
    return Atom.make((get) => {
      const dwell = createThreadReadDwell();
      let opened = claimThreadOpen(ref.environmentId, ref.threadId, Date.now());
      get.addFinalizer(() => dwell.dispose());
      get.subscribe(
        options.threadShellAtom(ref),
        (shell) => {
          if (shell === null) {
            dwell.update(null);
            return;
          }
          const wokeLater =
            shell.wokeAt != null && Date.parse(shell.wokeAt) > Date.parse(shell.updatedAt);
          const visitedAt = wokeLater ? shell.wokeAt! : shell.updatedAt;
          dwell.update({
            threadKey: `${ref.environmentId}:${ref.threadId}`,
            updatedAt: visitedAt,
            lastVisitedAt: opened ? null : shell.lastVisitedAt,
            completedAt: wokeLater ? shell.wokeAt : shell.latestRun?.completedAt,
            visible: true,
            onVisit: (watermark) => {
              if (shell.lastVisitedAt === undefined) {
                options.markLocalVisit?.(ref, watermark);
                opened = false;
                return;
              }
              const recordOpen = opened;
              opened = false;
              void runAtomCommand(
                get.registry,
                options.visit,
                {
                  environmentId: ref.environmentId,
                  input: {
                    threadId: ref.threadId,
                    visitedAt: watermark,
                    ...(recordOpen ? { opened: true } : {}),
                  },
                },
                { reportFailure: false },
              );
            },
          });
        },
        { immediate: true },
      );
    });
  });
  return (ref: ScopedThreadRef | null) => (ref === null ? idle : family(threadKey(ref)));
}
