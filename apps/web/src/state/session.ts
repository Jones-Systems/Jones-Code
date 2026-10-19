import { useAtomValue } from "@effect/atom-react";
import { createEnvironmentSessionAtoms } from "@t3tools/client-runtime/state/session";
import {
  type AuthEnvironmentScope,
  type AuthSessionState,
  type EnvironmentId,
  sessionGrantsScope,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import { AsyncResult, Atom } from "effect/reactivity";

import { connectionAtomRuntime } from "../connection/runtime";
import { appAtomRegistry } from "../rpc/atomRegistry";
import { environmentAtomList, environmentIdsKey } from "./environmentAtomList";

export const environmentSession = createEnvironmentSessionAtoms(connectionAtomRuntime);
export const environmentSessionStatesAtom = environmentAtomList(
  "web-environment-session-states",
  environmentSession.sessionStateAtom,
);

const EMPTY_SESSION_STATE_ATOM = Atom.make(AsyncResult.initial<AuthSessionState>());

export function useEnvironmentScope(
  environmentId: EnvironmentId | null,
  scope: AuthEnvironmentScope,
): boolean {
  const result = useAtomValue(
    environmentId === null
      ? EMPTY_SESSION_STATE_ATOM
      : environmentSession.sessionStateAtom(environmentId),
  );
  return sessionHasScope(result, scope);
}

function sessionHasScope(
  result: AsyncResult.AsyncResult<AuthSessionState, unknown>,
  scope: AuthEnvironmentScope,
): boolean {
  return environmentScopeAccess(result, scope).status === "granted";
}

export type EnvironmentScopeAccess = {
  readonly status: "granted" | "checking" | "check_failed" | "signed_out" | "scope_missing";
};

function environmentScopeAccess(
  result: AsyncResult.AsyncResult<AuthSessionState, unknown>,
  scope: AuthEnvironmentScope,
): EnvironmentScopeAccess {
  if (result._tag === "Failure") return { status: "check_failed" };
  if (result._tag === "Initial" || result.waiting) return { status: "checking" };
  if (!result.value.authenticated) return { status: "signed_out" };
  return { status: sessionGrantsScope(result.value, scope) ? "granted" : "scope_missing" };
}

export function readEnvironmentScopeAccess(
  environmentId: EnvironmentId,
  scope: AuthEnvironmentScope,
): EnvironmentScopeAccess {
  return environmentScopeAccess(
    appAtomRegistry.get(environmentSession.sessionStateAtom(environmentId)),
    scope,
  );
}

const pendingSessionChecks = new Map<EnvironmentId, Promise<void>>();

function awaitEnvironmentSession(environmentId: EnvironmentId, refresh: boolean): Promise<void> {
  const pending = pendingSessionChecks.get(environmentId);
  if (pending) return pending;
  const check = Promise.resolve()
    .then(
      () =>
        new Promise<void>((resolve) => {
          const atom = environmentSession.sessionStateAtom(environmentId);
          const unmount = appAtomRegistry.mount(atom);
          if (refresh) appAtomRegistry.refresh(atom);
          let unsubscribe = () => {};
          const finish = () => {
            clearTimeout(timeout);
            unsubscribe();
            unmount();
            resolve();
          };
          const timeout = setTimeout(finish, 3_000);
          unsubscribe = appAtomRegistry.subscribe(atom, (result) => {
            if (result._tag !== "Initial" && !result.waiting) finish();
          });
          const current = appAtomRegistry.get(atom);
          if (current._tag !== "Initial" && !current.waiting) finish();
        }),
    )
    .finally(() => {
      if (pendingSessionChecks.get(environmentId) === check)
        pendingSessionChecks.delete(environmentId);
    });
  pendingSessionChecks.set(environmentId, check);
  return check;
}

export async function ensureEnvironmentScope(
  environmentId: EnvironmentId,
  scope: AuthEnvironmentScope,
): Promise<EnvironmentScopeAccess> {
  const result = appAtomRegistry.get(environmentSession.sessionStateAtom(environmentId));
  if (
    pendingSessionChecks.has(environmentId) ||
    result._tag === "Initial" ||
    result._tag === "Failure" ||
    result.waiting
  ) {
    await awaitEnvironmentSession(environmentId, result._tag === "Failure");
  }
  return readEnvironmentScopeAccess(environmentId, scope);
}

export function refreshEnvironmentSession(environmentId: EnvironmentId): Promise<void> {
  return awaitEnvironmentSession(environmentId, true);
}

// Keyed by scope and ids so callers get one Set per session change, not one per render.
const environmentsWithScopeAtom = Atom.family((key: string) => {
  const [scope, idsKey = ""] = key.split(/\n(.*)/s) as [AuthEnvironmentScope, string?];
  const ids = idsKey === "" ? [] : (idsKey.split("\n") as EnvironmentId[]);
  const statesAtom = environmentSessionStatesAtom(idsKey);
  return Atom.make((get) => {
    const states = get(statesAtom);
    return new Set(ids.filter((_, index) => sessionHasScope(states[index]!, scope)));
  }).pipe(Atom.withLabel(`web-environments-with-scope:${key}`));
});

export function useEnvironmentsWithScope(
  environments: ReadonlyArray<{ readonly environmentId: EnvironmentId }>,
  scope: AuthEnvironmentScope,
): ReadonlySet<EnvironmentId> {
  return useAtomValue(
    environmentsWithScopeAtom(
      `${scope}\n${environmentIdsKey(environments.map(({ environmentId }) => environmentId))}`,
    ),
  );
}

export function readEnvironmentScope(
  environmentId: EnvironmentId,
  scope: AuthEnvironmentScope,
): boolean {
  return sessionHasScope(
    appAtomRegistry.get(environmentSession.sessionStateAtom(environmentId)),
    scope,
  );
}

const EMPTY_PREPARED_CONNECTION_ATOM = Atom.make(Option.none()).pipe(
  Atom.withLabel("web-prepared-connection:empty"),
);

export function usePreparedConnection(environmentId: EnvironmentId | null) {
  return useAtomValue(
    environmentId === null
      ? EMPTY_PREPARED_CONNECTION_ATOM
      : environmentSession.preparedConnectionValueAtom(environmentId),
  );
}

export function readPreparedConnection(environmentId: EnvironmentId) {
  return Option.getOrNull(
    appAtomRegistry.get(environmentSession.preparedConnectionValueAtom(environmentId)),
  );
}

/**
 * `data` may be stale during revalidation; permission checks use the strict access helpers.
 * `isPending` includes initial reads and bounded recovery attempts.
 */
export function useEnvironmentSessionState(environmentId: EnvironmentId) {
  const result = useAtomValue(environmentSession.sessionStateAtom(environmentId));
  return {
    data: Option.getOrNull(AsyncResult.value(result)),
    hasError: result._tag === "Failure",
    isPending: result.waiting,
  };
}
