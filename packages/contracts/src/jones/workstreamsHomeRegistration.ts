import * as Schema from "effect/Schema";
import {
  closedWorkstreamsNative as closed,
  WorkstreamsNativeHomeTarget,
  WorkstreamsNativeId,
} from "./workstreamsNative.ts";
import { WorkstreamsHomeAccessResult } from "./workstreamsHomeAccess.ts";

export const WORKSTREAMS_HOME_REGISTRATION_CONTRACT = "workstreams-native-home-registration/1.0.0";
export const WorkstreamsHomeRegistrationRequest = closed(
  Schema.Struct({
    contract: Schema.Literal(WORKSTREAMS_HOME_REGISTRATION_CONTRACT),
    expected_home: WorkstreamsNativeHomeTarget,
  }),
);
export type WorkstreamsHomeRegistrationRequest = typeof WorkstreamsHomeRegistrationRequest.Type;
// Current request observation only. The receiving home owns the grant; a member never stores it as authority.
export const WorkstreamsHomeRegistrationResult = closed(
  Schema.Struct({
    contract: Schema.Literal(WORKSTREAMS_HOME_REGISTRATION_CONTRACT),
    access: WorkstreamsHomeAccessResult,
    grant_id: WorkstreamsNativeId,
  }),
);
export type WorkstreamsHomeRegistrationResult = typeof WorkstreamsHomeRegistrationResult.Type;
