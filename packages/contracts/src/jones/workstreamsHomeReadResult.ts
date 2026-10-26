import * as Schema from "effect/Schema";
import {
  Workstream,
  WorkstreamReadContext,
  WorkstreamLifecycleDeclaration,
  WorkstreamRegistryCounts,
  WorkstreamPage,
  WorkstreamReferencePage,
  WorkstreamDetail,
  WorkstreamReferenceDetail,
  WorkstreamMembershipPage,
  WorkstreamDeclarationPage,
  WorkstreamEdgePage,
  WorkstreamHistoryPage,
} from "../workstreams.ts";
import { T3PlacementPage, T3PlacementContext, T3ThreadPlacement } from "../workstreamPlacements.ts";
import { WorkstreamAppearance, WorkstreamAppearanceResult } from "./workstreamAppearance.ts";
import {
  closedWorkstreamsNative as closed,
  WorkstreamsNativeSequence,
  WorkstreamsNativeVersion,
} from "./workstreamsNative.ts";
import {
  WorkstreamsNativeHomeRead,
  WorkstreamsNativeReadEnvelope,
} from "./workstreamsNativeRead.ts";
import { WorkstreamsNativePlacementPage } from "./workstreamsNativePlacement.ts";
import { WorkstreamsRollupSummary } from "./workstreamsRollups.ts";

export const WORKSTREAMS_HOME_READ_RESULT_CONTRACT = "workstreams-native-home-read-result/1.0.0";
export const WorkstreamsHomeReadResultRequest = closed(
  Schema.Struct({
    contract: Schema.Literal(WORKSTREAMS_HOME_READ_RESULT_CONTRACT),
    request: WorkstreamsNativeHomeRead,
    placement_contract: Schema.optionalKey(
      Schema.Literals(["workstreams-t3-placement/1.0.0", "workstreams-t3-placement/2.0.0"]),
    ),
  }),
).check(
  Schema.makeFilter((r) => r.placement_contract === undefined || r.request.route === "placements"),
);
export type WorkstreamsHomeReadResultRequest = typeof WorkstreamsHomeReadResultRequest.Type;
export const WorkstreamsProjectionValue = closed(
  Schema.Struct({
    items: Schema.Array(
      closed(
        Schema.Struct({
          workstream: Workstream,
          active_memberships: closed(
            Schema.Struct({
              primary: WorkstreamsNativeSequence,
              secondary: WorkstreamsNativeSequence,
            }),
          ),
          appearance: WorkstreamAppearance,
          lifecycle: Schema.NullOr(WorkstreamLifecycleDeclaration),
        }),
      ),
    ).check(Schema.isMaxLength(100)),
    next_cursor: Schema.NullOr(Schema.String),
  }),
);
export const WorkstreamsLifecycleValue = closed(
  Schema.Struct({
    context: WorkstreamReadContext,
    items: Schema.Array(WorkstreamLifecycleDeclaration).check(Schema.isMaxLength(100)),
    next_cursor: Schema.NullOr(Schema.String),
  }),
);
const fields = {
  contract: Schema.Literal(WORKSTREAMS_HOME_READ_RESULT_CONTRACT),
  envelope: WorkstreamsNativeReadEnvelope,
  authorization_revision: WorkstreamsNativeVersion,
};
const result = <R extends string, V extends Schema.Top>(route: R, value: V) =>
  closed(Schema.Struct({ ...fields, route: Schema.Literal(route), value }));
// A native proof cannot be decoded as v1 by silently discarding provenance.
const frozenPlacementPage = closed(
  Schema.Struct({
    ...T3PlacementPage.fields,
    context: closed(T3PlacementContext),
    items: Schema.Array(closed(T3ThreadPlacement)).check(Schema.isMaxLength(100)),
  }),
);
export const WorkstreamsHomeReadResult = Schema.Union([
  result("counts", WorkstreamRegistryCounts),
  result("workstreams", WorkstreamPage),
  result("references", WorkstreamReferencePage),
  result("workstream", WorkstreamDetail),
  result("reference", WorkstreamReferenceDetail),
  result("memberships", WorkstreamMembershipPage),
  result("declarations", WorkstreamDeclarationPage),
  result("edges", WorkstreamEdgePage),
  result("history", WorkstreamHistoryPage),
  result("appearance", WorkstreamAppearanceResult),
  result("lifecycle-declarations", WorkstreamsLifecycleValue),
  result("read-projection", WorkstreamsProjectionValue),
  result("rollup-summary", WorkstreamsRollupSummary),
  closed(
    Schema.Struct({
      ...fields,
      route: Schema.Literal("placements"),
      placement_contract: Schema.Literal("workstreams-t3-placement/1.0.0"),
      value: frozenPlacementPage,
    }),
  ),
  closed(
    Schema.Struct({
      ...fields,
      route: Schema.Literal("placements"),
      placement_contract: Schema.Literal("workstreams-t3-placement/2.0.0"),
      value: WorkstreamsNativePlacementPage,
    }),
  ),
]);
export type WorkstreamsHomeReadResult = typeof WorkstreamsHomeReadResult.Type;
