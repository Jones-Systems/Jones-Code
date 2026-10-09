import type { T3PlacementIdentity, T3WorkstreamListResult } from "@t3tools/contracts";
import type { DesignRequestRegistryStatus } from "@t3tools/client-runtime/jones/design-requests";
import {
  loadLiveT3Placements,
  orderWorkstreamMetadata,
  type LiveT3Placements,
} from "@t3tools/client-runtime/state/workstreams";
import * as Effect from "effect/Effect";

import { PrimaryEnvironmentHttpClient } from "../../environments/primary/httpClient";
import { runPrimaryHttp } from "../../lib/runtime";
import { loadCompleteWorkstreamList, nativePlacementInventory } from "../../state/workstreams";

export interface DesignRequestRegistrySnapshot {
  readonly status: DesignRequestRegistryStatus;
  readonly data: T3WorkstreamListResult | null;
  readonly placements: LiveT3Placements | null;
}

const UNAUTHENTICATED = new Set([
  "EnvironmentAuthInvalidError",
  "EnvironmentAuthorizationError",
  "EnvironmentHttpUnauthorizedError",
  "EnvironmentHttpForbiddenError",
  "EnvironmentScopeRequiredError",
]);

/** A server without the workstreams API answers 404; authentication failures hold as not-authenticated. */
export function registryStatusFromError(cause: unknown): DesignRequestRegistryStatus {
  const value = cause && typeof cause === "object" ? (cause as Record<string, unknown>) : {};
  const tag = typeof value._tag === "string" ? value._tag : "";
  if (UNAUTHENTICATED.has(tag)) return "unauthenticated";
  const response = value.response as { status?: unknown } | undefined;
  const status = typeof value.status === "number" ? value.status : response?.status;
  if (status === 401 || status === 403) return "unauthenticated";
  if (status === 404 || tag === "EnvironmentResourceNotFoundError") return "unsupported";
  return "error";
}

/**
 * Fresh, complete registry read through the existing workstreams client: every list page, then
 * the attested placements for the given local thread identities.
 */
export async function loadDesignRequestRegistry(
  threads: readonly { readonly environmentId: string; readonly id: string }[],
  signal?: AbortSignal,
): Promise<DesignRequestRegistrySnapshot> {
  let data: T3WorkstreamListResult;
  try {
    const list = await loadCompleteWorkstreamList(
      (cursor) =>
        runPrimaryHttp(
          PrimaryEnvironmentHttpClient.pipe(
            Effect.flatMap((client) =>
              client.workstreams.list({
                headers: {},
                payload: { limit: 50, ...(cursor === undefined ? {} : { cursor }) },
              }),
            ),
          ),
          { signal },
        ),
      signal === undefined ? {} : { signal },
    );
    data = { ...list, items: [...orderWorkstreamMetadata(list.items)] };
  } catch (cause) {
    signal?.throwIfAborted();
    return { status: registryStatusFromError(cause), data: null, placements: null };
  }
  const identities = JSON.parse(nativePlacementInventory(threads).json) as T3PlacementIdentity[];
  try {
    const placements = await loadLiveT3Placements(data, identities, () =>
      runPrimaryHttp(
        PrimaryEnvironmentHttpClient.pipe(
          Effect.flatMap((client) =>
            client.workstreams.threadPlacements({ headers: {}, payload: { identities } }),
          ),
        ),
        { signal },
      ),
    );
    return { status: "ready", data, placements };
  } catch (cause) {
    signal?.throwIfAborted();
    const status = registryStatusFromError(cause);
    return { status: status === "error" ? "ready" : status, data, placements: null };
  }
}
