import {
  WS_METHODS,
  type ExecutionEnvironmentDescriptor,
  type ScopedProjectRef,
} from "@t3tools/contracts";
import {
  RepoFreshnessUnavailableError,
  type RepoFreshnessRefreshResult,
} from "@t3tools/contracts/jones/repoFreshness";
import * as Effect from "effect/Effect";
import { Atom } from "effect/reactivity";

import type { EnvironmentRegistry } from "../connection/registry.ts";
import { request, requestGuarded } from "../rpc/client.ts";
import type { RpcSession } from "../rpc/session.ts";
import {
  createEnvironmentRpcCommand,
  createEnvironmentRpcQueryAtomFamily,
} from "../state/runtime.ts";

export const supportsRepoFreshness = (
  environment: Pick<ExecutionEnvironmentDescriptor, "capabilities"> | null | undefined,
): boolean => environment?.capabilities?.repoFreshness === true;

export const repoFreshnessTarget = ({ environmentId, projectId }: ScopedProjectRef) => ({
  environmentId,
  input: { projectId },
});

const requestKey = ({
  environmentId,
  input: { projectId },
}: ReturnType<typeof repoFreshnessTarget>) => JSON.stringify([environmentId, projectId]);

const unavailable = () =>
  new RepoFreshnessUnavailableError({
    message: "This connection has no repository freshness capability.",
  });

const validateSession = (session: RpcSession) =>
  session.initialConfig.pipe(
    Effect.mapError(unavailable),
    Effect.flatMap(({ environment }) =>
      supportsRepoFreshness(environment) ? Effect.void : Effect.fail(unavailable()),
    ),
  );

export function createRepoFreshnessAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
) {
  const revision = Atom.family((_key: string) => Atom.make(0));
  return {
    status: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "jones:repo-freshness:status",
      tag: WS_METHODS.vcsGetRepoFreshness,
      execute: (input) => request(WS_METHODS.vcsGetRepoFreshness, input, { validateSession }),
      idleTtlMs: 0,
      refreshTrigger: (target) => revision(requestKey(target)),
    }),
    refresh: createEnvironmentRpcCommand(runtime, {
      label: "jones:repo-freshness:refresh",
      tag: WS_METHODS.vcsRefreshRepoFreshness,
      execute: (input) =>
        requestGuarded(WS_METHODS.vcsRefreshRepoFreshness, input, { validateSession }),
      concurrency: { mode: "singleFlight", key: requestKey },
      onSuccess: (target, registry) =>
        Effect.sync(() => registry.update(revision(requestKey(target)), (value) => value + 1)),
    }),
  };
}

/** A failed attempt can carry a prior verified SHA. Only the returned attempt is this fetch. */
export function manualRepoFreshnessVerification(result: RepoFreshnessRefreshResult) {
  const { target, latestAttempt } = result.status;
  return target.status === "resolved" && latestAttempt?.status === "succeeded"
    ? latestAttempt.verification
    : null;
}
