// @effect-diagnostics globalDate:off - Atom subscriptions own view lifetime outside an Effect runtime.
import type { ScopedThreadRef } from "@t3tools/contracts";
import {
  createThreadReadDwell,
  type ThreadVisitOutcome,
} from "@t3tools/client-runtime/jones/thread-read-dwell";
import { parseThreadKey, threadKey } from "@t3tools/client-runtime/state/entities";
import { runAtomCommand } from "@t3tools/client-runtime/state/runtime";
import { claimThreadOpen } from "@t3tools/client-runtime/state/thread-search";
import type { ThreadVisitTrackerOptions } from "@t3tools/client-runtime/state/thread-visits";
import { AsyncResult, Atom } from "effect/reactivity";

const idle = Atom.make<void>(undefined);

export function createMobileThreadVisitTracker(options: ThreadVisitTrackerOptions) {
  const family = Atom.family((key: string) => {
    const ref = parseThreadKey(key);
    return Atom.make((get) => {
      const dwell = createThreadReadDwell();
      const shellAtom = options.threadShellAtom(ref);
      const sessionGenerationAtom = options.sessionGenerationAtom(ref.environmentId);
      let opened = claimThreadOpen(ref.environmentId, ref.threadId, Date.now());
      get.addFinalizer(() => dwell.dispose());
      const sync = (shell: Atom.Type<typeof shellAtom>) => {
        // Read before any early return: a bare subscription never computes a derived atom.
        const sessionGeneration = get.registry.get(sessionGenerationAtom);
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
          sessionGeneration,
          onVisit: (watermark) => {
            if (shell.lastVisitedAt === undefined) {
              options.markLocalVisit?.(ref, watermark);
              opened = false;
              return;
            }
            const recordOpen = opened;
            opened = false;
            return runAtomCommand(
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
            ).then((result): ThreadVisitOutcome => {
              if (!AsyncResult.isFailure(result)) return { accepted: true };
              if (!recordOpen) return { accepted: false };
              // The server never recorded this open, so it rides on the retry.
              return {
                accepted: false,
                restore: () => {
                  opened = true;
                },
              };
            });
          },
        });
      };
      get.subscribe(shellAtom, sync, { immediate: true });
      // A new session makes a rejected visit due again; the dwell still decides when.
      get.subscribe(sessionGenerationAtom, () => sync(get.registry.get(shellAtom)));
    });
  });
  return (ref: ScopedThreadRef | null) => (ref === null ? idle : family(threadKey(ref)));
}
