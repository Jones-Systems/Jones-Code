import * as Schema from "effect/Schema";
import {
  T3PlacementContext,
  T3PlacementPage,
  T3ThreadPlacement,
  TrustedT3PlacementEnvironment,
} from "../workstreamPlacements.ts";
import {
  closedWorkstreamsNative as closed,
  WorkstreamsNativeUuid,
  WorkstreamsNativeId as Id,
  WorkstreamsNativeVersion,
  WorkstreamsNativeTimestamp,
} from "./workstreamsNative.ts";

export const WorkstreamsNativePlacementProvenance = closed(
  Schema.Struct({
    authority: Schema.Literal("native-home"),
    registry_instance_id: WorkstreamsNativeUuid,
    home_environment_id: Id,
    home_generation: WorkstreamsNativeVersion,
    served_by_environment_id: Id,
    observed_at: WorkstreamsNativeTimestamp,
  }),
);
const timestampOrderKey = (value: string) =>
  value.slice(0, 19) + value.slice(19, -1).replace(/^\./, "").padEnd(6, "0");
export const WorkstreamsNativeThreadPlacement = closed(
  Schema.Struct({
    ...T3ThreadPlacement.fields,
    attestation_source: Schema.Literal("native"),
    evidence_contract: Schema.Literal("workstreams-t3-provider/2.0.0"),
  }),
).check(
  Schema.makeFilter(
    (item) => timestampOrderKey(item.attested_at) < timestampOrderKey(item.expires_at),
  ),
);
export type WorkstreamsNativeThreadPlacement = typeof WorkstreamsNativeThreadPlacement.Type;
const context = closed(T3PlacementContext);
const fields = {
  inventory_sha256: T3PlacementPage.fields.inventory_sha256,
  context,
  provenance: WorkstreamsNativePlacementProvenance,
};
const coherent = (value: {
  context: T3PlacementContext;
  provenance: typeof WorkstreamsNativePlacementProvenance.Type;
}) => value.context.server_generation === value.provenance.home_generation;
export const WorkstreamsNativePlacementPage = closed(
  Schema.Struct({
    ...fields,
    items: Schema.Array(WorkstreamsNativeThreadPlacement).check(Schema.isMaxLength(100)),
    next_cursor: T3PlacementPage.fields.next_cursor,
  }),
).check(Schema.makeFilter(coherent));
export type WorkstreamsNativePlacementPage = typeof WorkstreamsNativePlacementPage.Type;
export const WorkstreamsNativePlacementResult = closed(
  Schema.Struct({
    page: closed(
      Schema.Struct({
        ...fields,
        items: Schema.Array(WorkstreamsNativeThreadPlacement).check(Schema.isMaxLength(1000)),
        next_cursor: Schema.Null,
      }),
    ).check(Schema.makeFilter(coherent)),
    trustedEnvironments: Schema.Array(closed(TrustedT3PlacementEnvironment)).check(
      Schema.isMaxLength(100),
    ),
    readiness: Schema.Literals(["ready", "trust-provider-required"]),
  }),
);
export type WorkstreamsNativePlacementResult = typeof WorkstreamsNativePlacementResult.Type;
