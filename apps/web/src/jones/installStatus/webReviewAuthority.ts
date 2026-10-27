import { useLayoutEffect, useMemo } from "react";
import type { EnvironmentId } from "@t3tools/contracts";

export function createWebReviewAuthority(environmentId: EnvironmentId) {
  let active = false;
  const cancellations = new Set<() => void>();
  return {
    environmentId,
    isCurrent: () => active,
    activate() {
      active = true;
    },
    revoke() {
      active = false;
      for (const cancel of cancellations) cancel();
      cancellations.clear();
    },
    wait<A>(work: () => Promise<A>, cancelled: A): Promise<A> {
      if (!active) return Promise.resolve(cancelled);
      return new Promise((resolve, reject) => {
        const cancel = () => {
          cancellations.delete(cancel);
          resolve(cancelled);
        };
        cancellations.add(cancel);
        let pending: Promise<A>;
        try {
          pending = work();
        } catch (cause) {
          cancellations.delete(cancel);
          reject(cause);
          return;
        }
        void pending.then(
          (value) => {
            cancellations.delete(cancel);
            resolve(active ? value : cancelled);
          },
          (cause: unknown) => {
            cancellations.delete(cancel);
            if (active) reject(cause);
            else resolve(cancelled);
          },
        );
      });
    },
  };
}
export type WebReviewAuthority = ReturnType<typeof createWebReviewAuthority>;

export function useWebReviewAuthority(
  environmentId: EnvironmentId,
  allowed: boolean,
  selection: string = environmentId,
) {
  const authority = useMemo(
    () => createWebReviewAuthority(environmentId),
    [environmentId, allowed, selection],
  );
  // oxlint-disable-next-line t3code/react-effect-budget -- Commit/unmount revokes only this control's asynchronous review authority, before any continuation can dispatch.
  useLayoutEffect(() => {
    if (allowed) authority.activate();
    return () => authority.revoke();
  }, [authority, allowed]);
  return authority;
}
