import type { EnvironmentId } from "@t3tools/contracts";
import { EnvironmentRegistry } from "@t3tools/client-runtime/connection";
import {
  requestRemoteAdmission,
  type RemoteAdmissionRequest,
} from "@t3tools/client-runtime/jones/remote-admission";
import { createRuntimeCommand, runAtomCommand } from "@t3tools/client-runtime/state/runtime";
import * as Effect from "effect/Effect";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { connectionAtomRuntime } from "../../connection/runtime";
import { appAtomRegistry } from "../../rpc/atomRegistry";

interface Input {
  readonly environmentId: EnvironmentId;
  readonly request: RemoteAdmissionRequest;
}
const command = createRuntimeCommand(connectionAtomRuntime, {
  label: "jones:remote-admission",
  concurrency: { mode: "serial", key: (input: Input) => input.environmentId },
  execute: Effect.fn(function* (input: Input) {
    const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
    const entry = (yield* SubscriptionRef.get(registry.entries)).get(input.environmentId);
    if (!entry)
      return yield* new EnvironmentRegistry.EnvironmentNotRegisteredError({
        environmentId: input.environmentId,
      });
    return yield* requestRemoteAdmission(entry, input.request);
  }),
});
export async function remoteAdmissionRequest(
  environmentId: EnvironmentId,
  request: RemoteAdmissionRequest,
) {
  const result = await runAtomCommand(
    appAtomRegistry,
    command,
    { environmentId, request },
    { reportFailure: false, reportDefect: false },
  );
  if (result._tag !== "Success")
    throw new Error("Remote request review is unavailable for this session.");
  return result.value;
}
