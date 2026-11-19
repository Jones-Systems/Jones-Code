import type { EnvironmentId } from "@t3tools/contracts";
import { reviewHttp } from "@t3tools/client-runtime/jones/remoteAdmission/reviewHttp";
import type { ReviewTransport } from "@t3tools/client-runtime/jones/remoteAdmission/adapter";
import { createEnvironmentCommand, runAtomCommand } from "@t3tools/client-runtime/state/runtime";
import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import { connectionAtomRuntime } from "../../connection/runtime";
import { appAtomRegistry } from "../../rpc/atomRegistry";

type ReviewServices = Effect.Services<ReturnType<typeof reviewHttp.capabilities>>;
function makeRequest<I, A, E>(
  label: string,
  execute: (input: I) => Effect.Effect<A, E, ReviewServices>,
) {
  const command = createEnvironmentCommand(connectionAtomRuntime, {
    label: `jones:admission-review:${label}`,
    // Conflicting writes are serialized by the controller's exact binding lock.
    // A long poll or launch must never delay another request's denial.
    concurrency: { mode: "parallel" },
    execute,
  });
  return async (receiver: EnvironmentId, value: I, signal?: AbortSignal): Promise<A> => {
    const result = await runAtomCommand(
      appAtomRegistry,
      command,
      { environmentId: receiver, input: value },
      {
        reportFailure: false,
        reportDefect: false,
        ...(signal ? { signal } : {}),
      },
    );
    if (result._tag !== "Success") throw Cause.squash(result.cause);
    return result.value;
  };
}
const capabilities = makeRequest("capabilities", reviewHttp.capabilities);
const review = makeRequest("review", reviewHttp.review);
const item = makeRequest("item", reviewHttp.item);
const grants = makeRequest("grants", reviewHttp.grants);
const decision = makeRequest("decision", reviewHttp.decision);
const updateGrant = makeRequest("update", reviewHttp.updateGrant);
const revokeGrant = makeRequest("revoke", reviewHttp.revokeGrant);
const restoreGrant = makeRequest("restore", reviewHttp.restoreGrant);
const retry = makeRequest("retry", reviewHttp.retry);
const recheck = makeRequest("recheck", reviewHttp.recheck);
const changes = makeRequest(
  "changes",
  (input: { query: Parameters<typeof reviewHttp.changes>[0]; signal?: AbortSignal }) =>
    reviewHttp.changes(input.query, input.signal),
);

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
