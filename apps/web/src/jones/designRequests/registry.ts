import type { T3WorkstreamListResult } from "@t3tools/contracts";
import type { DesignRequestRegistryStatus } from "@t3tools/client-runtime/jones/design-requests";
import {
  loadLiveT3Placements,
  loadWorkstreamAssignmentIndex,
  orderWorkstreamMetadata,
  type LiveT3Placements,
} from "@t3tools/client-runtime/state/workstreams";
import * as Effect from "effect/Effect";

import { PrimaryEnvironmentHttpClient } from "../../environments/primary/httpClient";
import { runPrimaryHttp } from "../../lib/runtime";
import { mergeLivePlacements, nativePlacementBatches } from "../../state/workstreams";
import { classifyWorkstreamReadFailure } from "../workstreams/workstreamReadErrors";

export interface DesignRequestRegistrySnapshot {
  readonly status: DesignRequestRegistryStatus;
  readonly data: T3WorkstreamListResult | null;
  readonly placements: LiveT3Placements | null;
}

/**
 * A server without the workstreams API answers 404, and one without control-plane activation
 * answers a not-configured conflict; authentication failures hold as not-authenticated.
 */
export const registryStatusFromError = (cause: unknown): DesignRequestRegistryStatus =>
  classifyWorkstreamReadFailure(cause);

const PAGE_LIMIT = 100;

const primary = <A, E>(
  run: (client: Effect.Success<typeof PrimaryEnvironmentHttpClient>) => Effect.Effect<A, E>,
  signal?: AbortSignal,
) => runPrimaryHttp(PrimaryEnvironmentHttpClient.pipe(Effect.flatMap(run)), { signal });

/**
 * Fresh, complete registry read through the existing workstreams client: every list page and
 * stored assignment at one registry version, then current proof for the local threads whose
 * open primary is in the bound Workstream. Routing is a change, so it still requires proof;
 * any incomplete coverage or failed proof batch withholds placements instead of guessing.
 */
export async function loadDesignRequestRegistry(
  threads: readonly { readonly environmentId: string; readonly id: string }[],
  signal?: AbortSignal,
  workstreamId?: string | null,
): Promise<DesignRequestRegistrySnapshot> {
  let loaded: Awaited<ReturnType<typeof loadWorkstreamAssignmentIndex>>;
  try {
    loaded = await loadWorkstreamAssignmentIndex(
      {
        listWorkstreams: ({ cursor, signal: pageSignal }) =>
          primary(
            (client) =>
              client.workstreams.list({
                headers: {},
                payload: { limit: 50, ...(cursor === undefined ? {} : { cursor }) },
              }),
            pageSignal,
          ),
        references: ({ cursor, signal: pageSignal }) =>
          primary(
            (client) =>
              client.workstreams.references({
                headers: {},
                payload: { limit: PAGE_LIMIT, ...(cursor === undefined ? {} : { cursor }) },
              }),
            pageSignal,
          ),
        memberships: ({ workstreamId: id, cursor, signal: pageSignal }) =>
          primary(
            (client) =>
              client.workstreams.memberships({
                headers: {},
                params: { workstreamId: id },
                payload: { limit: PAGE_LIMIT, ...(cursor === undefined ? {} : { cursor }) },
              }),
            pageSignal,
          ),
      },
      signal === undefined ? {} : { signal },
    );
  } catch (cause) {
    signal?.throwIfAborted();
    return { status: registryStatusFromError(cause), data: null, placements: null };
  }
  const data = { ...loaded.list, items: [...orderWorkstreamMetadata(loaded.list.items)] };
  const { index } = loaded;
  if (workstreamId === undefined || workstreamId === null || !index.complete)
    return { status: "ready", data, placements: null };
  const batches = nativePlacementBatches(threads, {
    include: (key) =>
      index.entries.get(key)?.primaries.some((item) => item.workstreamId === workstreamId) === true,
  });
  const proven: LiveT3Placements[] = [];
  try {
    // An empty request still proves that the bound Workstream has no current primary.
    for (const identities of batches.length === 0 ? [[]] : batches)
      proven.push(
        await loadLiveT3Placements(data, identities, () =>
          primary(
            (client) =>
              client.workstreams.threadPlacements({ headers: {}, payload: { identities } }),
            signal,
          ),
        ),
      );
  } catch (cause) {
    signal?.throwIfAborted();
    const status = registryStatusFromError(cause);
    return { status: status === "error" ? "ready" : status, data, placements: null };
  }
  return { status: "ready", data, placements: mergeLivePlacements(proven) };
}
