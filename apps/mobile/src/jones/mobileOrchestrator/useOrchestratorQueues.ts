import { useAtomValue } from "@effect/atom-react";
import { Atom } from "effect/reactivity";
import { useMemo, useState, useSyncExternalStore } from "react";

import { subscribeAppActive } from "../../lib/appState";
import { useNowAfter, useNowMinuteMs } from "../../lib/clock";
import { scopedThreadKey } from "../../lib/scopedEntities";
import { steadyNavigationThreadShellsAtom } from "../../state/threads";
import { useThreadOutboxMessages } from "../../state/use-thread-outbox";
import { nextFinishedExpiry, orchestratorQueues, type OrchestratorQueue } from "./queues";

export const orchestratorViewAtom = Atom.make<OrchestratorQueue | "compose">("compose").pipe(
  Atom.keepAlive,
);

const foregroundClock = (() => {
  let lastForegroundAt = 0;
  return {
    subscribe: (notify: () => void) =>
      subscribeAppActive(() => {
        lastForegroundAt = Date.now();
        notify();
      }),
    getSnapshot: () => lastForegroundAt,
  };
})();

export function useOrchestratorQueues() {
  const threads = useAtomValue(steadyNavigationThreadShellsAtom);
  const outbox = useThreadOutboxMessages();
  const minute = useNowMinuteMs();
  const foregroundAt = useSyncExternalStore(foregroundClock.subscribe, foregroundClock.getSnapshot);
  const [latest, setLatest] = useState(Date.now);
  const [observedShells, setObservedShells] = useState(() => ({ threads, at: Date.now() }));
  if (threads !== observedShells.threads) {
    setObservedShells(() => ({ threads, at: Date.now() }));
  }
  const observed = Math.max(minute, foregroundAt, observedShells.at);
  const afterExpiry = useNowAfter(nextFinishedExpiry(threads, Math.max(latest, observed)));
  const now = Math.max(latest, observed, afterExpiry);
  if (now !== latest) setLatest(now);
  const queues = useMemo(
    () =>
      orchestratorQueues(
        threads,
        now,
        (thread) => (outbox[scopedThreadKey(thread.environmentId, thread.id)]?.length ?? 0) > 0,
      ),
    [now, outbox, threads],
  );
  return { threads, queues };
}
