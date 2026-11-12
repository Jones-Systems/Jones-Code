import { EnvironmentRegistry, EnvironmentSupervisor } from "@t3tools/client-runtime/connection";
import { fetchEnvironmentHostStatus } from "@t3tools/client-runtime/jones/host-status/http";
import { executeAtomQuery, squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import type { EnvironmentId, HostStatusSnapshot } from "@t3tools/contracts";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { connectionAtomRuntime } from "../../connection/runtime";
import { appAtomRegistry } from "../../rpc/atomRegistry";

class HostStatusEnvironmentUnavailable extends Data.TaggedError(
  "HostStatusEnvironmentUnavailable",
) {}

const request = Effect.gen(function* () {
  const supervisor = yield* EnvironmentSupervisor.EnvironmentSupervisor;
  const prepared = yield* SubscriptionRef.get(supervisor.prepared);
  if (Option.isNone(prepared)) return yield* new HostStatusEnvironmentUnavailable();
  return yield* fetchEnvironmentHostStatus(prepared.value);
});

export async function fetchHostStatus(
  environmentId: EnvironmentId,
  signal: AbortSignal,
): Promise<HostStatusSnapshot> {
  const atom = connectionAtomRuntime.atom(
    Effect.gen(function* () {
      const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
      return yield* registry.run(environmentId, request);
    }),
  );
  const result = await executeAtomQuery(appAtomRegistry, atom, {
    signal,
    reportFailure: false,
    reportDefect: false,
  });
  if (result._tag === "Failure") throw squashAtomCommandFailure(result);
  return result.value;
}
