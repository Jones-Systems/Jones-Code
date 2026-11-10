import type { WorkstreamSetupStatus } from "@t3tools/contracts/jones/workstreamOperations";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as DurableEntries from "../native/operations/durableEntries.ts";
import type * as RecordedCallers from "../native/runtime/recordedCaller.ts";
import type * as HomeExecutor from "../native/operations/homeExecutor.ts";
import type { WorkstreamGateway } from "../../../workstreams/WorkstreamGateway.ts";
import type { WorkstreamsRegistrationContext } from "../registrationContext/service.ts";

/**
 * Startup metadata only; the operations service never receives activation secrets.
 * registryId is the validated signed binding's registry_instance_id UUID, not
 * the legacy gateway binding's registryId (which is an HTTPS origin).
 */
export class Activation extends Context.Service<
  Activation,
  { readonly status: WorkstreamSetupStatus["activation"]; readonly registryId: string | null }
>()("t3/jones/workstreams/operations/ports/Activation") {}

export type NativeExecutionContext = {
  readonly mode: "native";
  readonly callers?: RecordedCallers.RecordedCallers["Service"];
  readonly entries: DurableEntries.DurableEntries["Service"];
  readonly executor: HomeExecutor.HomeExecutor["Service"];
};

export const ExecutionContext = Context.Reference<
  NativeExecutionContext | { readonly mode: "legacy" }
>("t3/jones/workstreams/operations/ExecutionContext", {
  defaultValue: () => ({ mode: "legacy" }),
});

export class Ports extends Context.Service<
  Ports,
  {
    readonly native?: NativeExecutionContext;
    readonly environmentId: string;
    /** Public operation identity, also returned by setupStatus.registry.registry_id. */
    readonly registryId: string | null;
    readonly activation: WorkstreamSetupStatus["activation"];
    readonly gateway: Pick<
      WorkstreamGateway["Service"],
      | "readSession"
      | "readReceipt"
      | "resubmit"
      | "readReference"
      | "readDetail"
      | "readMetadata"
      | "readMemberships"
      | "readThreadPlacements"
    >;
    readonly registration: WorkstreamsRegistrationContext["Service"];
    readonly threadExists: (id: string) => Effect.Effect<boolean, "unavailable">;
    readonly now: Effect.Effect<number>;
    readonly sleep: (milliseconds: number) => Effect.Effect<void>;
    readonly jitter: Effect.Effect<number>;
  }
>()("t3/jones/workstreams/operations/ports") {}
