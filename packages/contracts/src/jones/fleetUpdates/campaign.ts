import * as Schema from "effect/Schema";
import { TrimmedNonEmptyString } from "../../baseSchemas.ts";
import { FleetEnrollment, FleetOperationId, FleetSourceSha } from "./host.ts";

export const FleetMemberPhase = Schema.Literals([
  "waiting", "offline", "bootstrap-required", "staging", "stage-blocked", "staged",
  "dispatching", "install-blocked", "reconciling", "retiring", "pending", "current", "committed", "rolled-back", "blocked", "superseded",
]);
export type FleetMemberPhase = typeof FleetMemberPhase.Type;
export const FleetCampaignMember = Schema.Struct({
  enrollment: FleetEnrollment,
  operationId: FleetOperationId,
  phase: FleetMemberPhase,
  expectedInstalledSource: Schema.optionalKey(FleetSourceSha),
  reason: Schema.optionalKey(Schema.String),
});
export type FleetCampaignMember = typeof FleetCampaignMember.Type;
export const FleetDesktopCampaign = Schema.Struct({
  campaignId: FleetOperationId,
  targetSource: FleetSourceSha,
  desktopStagedHandle: TrimmedNonEmptyString,
  phase: Schema.Literals(["prepared", "installing", "committed", "rolled-back", "blocked"]),
  installation: Schema.optionalKey(Schema.Struct({
    transactionId: TrimmedNonEmptyString,
    fromGeneration: TrimmedNonEmptyString,
  })),
  committedGeneration: Schema.optionalKey(TrimmedNonEmptyString),
  members: Schema.Array(FleetCampaignMember),
});
export type FleetDesktopCampaign = typeof FleetDesktopCampaign.Type;
export const FleetDesktopState = Schema.Struct({
  schema: Schema.Literal(1),
  enrollments: Schema.Array(FleetEnrollment),
  campaigns: Schema.Array(FleetDesktopCampaign),
});
export type FleetDesktopState = typeof FleetDesktopState.Type;
export const FleetPrepareCampaignInput = Schema.Struct({
  campaignId: FleetOperationId,
  targetSource: FleetSourceSha,
  desktopStagedHandle: TrimmedNonEmptyString,
});
export type FleetPrepareCampaignInput = typeof FleetPrepareCampaignInput.Type;
export const FleetUpdateMemberInput = Schema.Struct({
  campaignId: FleetOperationId,
  operationId: FleetOperationId,
  expectedPhase: FleetMemberPhase,
  phase: FleetMemberPhase,
  expectedInstalledSource: Schema.optionalKey(FleetSourceSha),
  reason: Schema.optionalKey(Schema.String),
});
export type FleetUpdateMemberInput = typeof FleetUpdateMemberInput.Type;

/** Read/write IPC never accepts native commit proof from the renderer. */
export type FleetDesktopRequest =
  | { readonly action: "read" }
  | { readonly action: "enroll"; readonly enrollment: FleetEnrollment }
  | { readonly action: "prepare"; readonly input: FleetPrepareCampaignInput }
  | { readonly action: "updateMember"; readonly input: FleetUpdateMemberInput };
export const FleetDesktopRequest = Schema.Union([
  Schema.Struct({ action: Schema.Literal("read") }),
  Schema.Struct({ action: Schema.Literal("enroll"), enrollment: FleetEnrollment }),
  Schema.Struct({ action: Schema.Literal("prepare"), input: FleetPrepareCampaignInput }),
  Schema.Struct({ action: Schema.Literal("updateMember"), input: FleetUpdateMemberInput }),
]);
