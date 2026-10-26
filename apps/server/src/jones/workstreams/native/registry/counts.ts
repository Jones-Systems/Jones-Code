import type { WorkstreamRegistryCounts } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { lifecycleCounts } from "../domain/collections.ts";
import type { AdmittedCaller } from "../mode/authorityMode.ts";
import type { RegistryError } from "./session.ts";
import * as Registry from "./service.ts";
export class Counts extends Context.Service<
  Counts,
  {
    readonly read: (
      caller: AdmittedCaller,
    ) => Effect.Effect<WorkstreamRegistryCounts, RegistryError>;
  }
>()("t3/jones/workstreams/native/registry/counts") {}
export const layer = Layer.effect(
  Counts,
  Effect.gen(function* () {
    const registry = yield* Registry.Registry;
    return Counts.of({
      read: Effect.fn("NativeCounts.read")(function* (caller) {
        const s = yield* registry.snapshot(caller);
        return {
          context: {
            owner_id: s.context.owner_id,
            server_generation: s.context.server_generation,
            registry_version: s.context.registry_version,
          },
          principal_id: caller.principal_id,
          observed_at: DateTime.formatIso(yield* DateTime.now),
          authority_effect: "none",
          ...lifecycleCounts(s.state),
        };
      }),
    });
  }),
);
