import { T3PlacementRequest } from "../workstreamPlacements.ts";
import * as Schema from "effect/Schema";
import {
  closedWorkstreamsNative as closed,
  WorkstreamsNativeId as Id,
  WorkstreamsNativeUuid,
  WorkstreamsNativeOrigin,
  WorkstreamsNativeVersion,
  WorkstreamsNativeTimestamp as Timestamp,
  WorkstreamsNativeDigest,
  WorkstreamsNativeSequence,
  WORKSTREAMS_NATIVE_READ_CONTRACT,
} from "./workstreamsNative.ts";

export const WorkstreamsNativeReadSource = closed(
  Schema.Struct({
    source_instance_id: Id,
    mapping: Schema.Literals(["mapped", "unmapped"]),
    attestation: Schema.Literals(["current", "expired", "unavailable", "unknown"]),
    observed_at: Schema.NullOr(Timestamp),
  }),
).check(
  Schema.makeFilter(
    (source) =>
      (source.attestation !== "current" ||
        (source.mapping === "mapped" && source.observed_at !== null)) &&
      (source.mapping !== "unmapped" || source.attestation !== "current"),
  ),
);
export const WorkstreamsNativeReadProvenance = closed(
  Schema.Struct({
    authority: Schema.Literals(["native-home", "legacy-archive"]),
    home_environment_id: Id,
    served_by_environment_id: Id,
    source: Schema.Literals(["live", "cache"]),
    observed_at: Timestamp,
    stale: Schema.Boolean,
  }),
);
export const WorkstreamsNativeReadCoverage = closed(
  Schema.Struct({
    registry: Schema.Literals(["complete", "stale", "unavailable"]),
    sources: Schema.Array(WorkstreamsNativeReadSource).check(
      Schema.isMaxLength(1000),
      Schema.makeFilter((xs) => new Set(xs.map((x) => x.source_instance_id)).size === xs.length),
    ),
  }),
);
export const WorkstreamsNativeReadEnvelope = closed(
  Schema.Struct({
    contract: Schema.Literal(WORKSTREAMS_NATIVE_READ_CONTRACT),
    registry: closed(
      Schema.Struct({
        registry_instance_id: WorkstreamsNativeUuid,
        owner_id: Id,
        canonical_origin: WorkstreamsNativeOrigin,
        server_generation: WorkstreamsNativeVersion,
        registry_version: WorkstreamsNativeSequence,
      }),
    ),
    provenance: WorkstreamsNativeReadProvenance,
    coverage: WorkstreamsNativeReadCoverage,
  }),
).check(
  Schema.makeFilter(
    (read) =>
      (read.provenance.source !== "cache" ||
        (read.provenance.stale && read.coverage.registry !== "complete")) &&
      (read.coverage.registry !== "complete" ||
        (read.provenance.source === "live" &&
          !read.provenance.stale &&
          read.provenance.authority === "native-home")) &&
      (read.coverage.registry !== "stale" || read.provenance.stale),
  ),
);
export type WorkstreamsNativeReadEnvelope = typeof WorkstreamsNativeReadEnvelope.Type;
const pageParams = {
  limit: Schema.optionalKey(
    WorkstreamsNativeSequence.check(Schema.isBetween({ minimum: 1, maximum: 100 })),
  ),
  cursor: Schema.optionalKey(Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_-]{1,2048}$/))),
};
export const WorkstreamsNativeHomeRead = Schema.Union([
  closed(
    Schema.Struct({
      route: Schema.Literals(["workstreams", "references", "read-projection"]),
      params: closed(Schema.Struct(pageParams)),
    }),
  ),
  closed(
    Schema.Struct({
      route: Schema.Literal("workstream"),
      params: closed(Schema.Struct({ workstream_id: Id })),
    }),
  ),
  closed(
    Schema.Struct({
      route: Schema.Literal("reference"),
      params: closed(Schema.Struct({ native_reference_id: Id })),
    }),
  ),
  closed(
    Schema.Struct({
      route: Schema.Literals(["memberships", "declarations", "lifecycle-declarations", "edges"]),
      params: closed(Schema.Struct({ ...pageParams, workstream_id: Id })),
    }),
  ),
  closed(
    Schema.Struct({
      route: Schema.Literal("history"),
      params: closed(Schema.Struct({ ...pageParams, workstream_id: Schema.optionalKey(Id) })),
    }),
  ),
  closed(Schema.Struct({ route: Schema.Literal("counts"), params: closed(Schema.Struct({})) })),
  closed(
    Schema.Struct({
      route: Schema.Literal("appearance"),
      params: closed(
        Schema.Struct({
          workstream_ids: Schema.Array(Id).check(
            Schema.isMaxLength(100),
            Schema.makeFilter((xs) => new Set(xs).size === xs.length),
          ),
        }),
      ),
    }),
  ),
  closed(
    Schema.Struct({
      route: Schema.Literal("placements"),
      params: closed(
        Schema.Struct({
          ...pageParams,
          identities: T3PlacementRequest.fields.identities,
        }),
      ),
    }),
  ),
  closed(
    Schema.Struct({
      route: Schema.Literal("rollup-summary"),
      params: closed(Schema.Struct({ workstream_id: Id })),
    }),
  ),
]);
export type WorkstreamsNativeHomeRead = typeof WorkstreamsNativeHomeRead.Type;
export const WorkstreamsNativeReadExportTrailer = closed(
  Schema.Struct({
    complete: Schema.Boolean,
    registry_version: WorkstreamsNativeSequence,
    sha256: WorkstreamsNativeDigest,
  }),
);
