import type { EnvironmentId } from "@t3tools/contracts";
import { EnvironmentRegistry, type ConnectionCatalogEntry } from "@t3tools/client-runtime/connection";
import { reviewHttp } from "@t3tools/client-runtime/jones/remoteAdmission/reviewHttp";
import type { ReviewTransport } from "@t3tools/client-runtime/jones/remoteAdmission/adapter";
import { createRuntimeCommand, runAtomCommand } from "@t3tools/client-runtime/state/runtime";
import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { connectionAtomRuntime } from "../../connection/runtime";
import { appAtomRegistry } from "../../rpc/atomRegistry";

type ReviewServices = Effect.Services<ReturnType<typeof reviewHttp.capabilities>>;
function makeRequest<I, A, E>(label: string, execute: (entry: ConnectionCatalogEntry, input: I) => Effect.Effect<A, E, ReviewServices>) {
  const command = createRuntimeCommand(connectionAtomRuntime, {
    label: `jones:admission-review:${label}`,
    // Conflicting writes are serialized by the controller's exact binding lock.
    // A long poll or launch must never delay another request's denial.
    concurrency: { mode: "parallel" },
    execute: Effect.fn(function* (input: { receiver: EnvironmentId; value: I }) {
      const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
      const entry = (yield* SubscriptionRef.get(registry.entries)).get(input.receiver);
      if (!entry) return yield* new EnvironmentRegistry.EnvironmentNotRegisteredError({ environmentId: input.receiver });
      return yield* execute(entry, input.value);
    }),
  });
  return async (receiver: EnvironmentId, value: I, signal?: AbortSignal): Promise<A> => {
    const result = await runAtomCommand(appAtomRegistry, command, { receiver, value }, {
      reportFailure: false, reportDefect: false, ...(signal ? { signal } : {}),
    });
    if (result._tag !== "Success") throw Cause.squash(result.cause);
    return result.value;
  };
}
const capabilities = makeRequest("capabilities", (entry: ConnectionCatalogEntry, _input: void) => reviewHttp.capabilities(entry));
const review = makeRequest("review", reviewHttp.review);
const item = makeRequest("item", reviewHttp.item);
const grants = makeRequest("grants", reviewHttp.grants);
const decision = makeRequest("decision", reviewHttp.decision);
const updateGrant = makeRequest("update", reviewHttp.updateGrant);
const revokeGrant = makeRequest("revoke", reviewHttp.revokeGrant);
const restoreGrant = makeRequest("restore", reviewHttp.restoreGrant);
const retry = makeRequest("retry", reviewHttp.retry);
const recheck = makeRequest("recheck", reviewHttp.recheck);
const changes = makeRequest("changes", (entry: ConnectionCatalogEntry, input: { query: Parameters<typeof reviewHttp.changes>[1]; signal?: AbortSignal }) => reviewHttp.changes(entry, input.query, input.signal));

export function createReviewTransport(receiver: EnvironmentId): ReviewTransport {
  return {
    capabilities: () => capabilities(receiver, undefined),
    review: (input) => review(receiver, input),
    item: (input) => item(receiver, input),
    grants: (input) => grants(receiver, input),
    decision: (input) => decision(receiver, input),
    updateGrant: (input) => updateGrant(receiver, input),
    revokeGrant: (input) => revokeGrant(receiver, input),
    restoreGrant: (input) => restoreGrant(receiver, input),
    retry: (input) => retry(receiver, input),
    recheck: (input) => recheck(receiver, input),
    changes: (query, signal) => changes(receiver, { query, ...(signal ? { signal } : {}) }, signal),
  };
}
