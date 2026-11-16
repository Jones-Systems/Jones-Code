import * as Schema from "effect/Schema";
import * as Rpc from "effect/rpc/Rpc";
import { EnvironmentAuthorizationError } from "../auth.ts";
import {
  AssignPersonalWorkstreamInput,
  AssignPersonalWorkstreamResult,
  PersonalWorkstreamsSnapshot,
  PutPersonalWorkstreamInput,
  PutPersonalWorkstreamResult,
  WorkstreamsPersonalError,
} from "./workstreamsPersonal.ts";

export {
  AssignPersonalWorkstreamInput,
  AssignPersonalWorkstreamResult,
  PutPersonalWorkstreamInput,
  PutPersonalWorkstreamResult,
  WorkstreamsPersonalError,
} from "./workstreamsPersonal.ts";

const Failure = Schema.Union([WorkstreamsPersonalError, EnvironmentAuthorizationError]);

export const SubscribePersonalWorkstreamsRpc = Rpc.make("workstreamsPersonal.subscribe", {
  payload: Schema.Struct({}),
  success: PersonalWorkstreamsSnapshot,
  error: Failure,
  stream: true,
});

export const PutPersonalWorkstreamRpc = Rpc.make("workstreamsPersonal.put", {
  payload: PutPersonalWorkstreamInput,
  success: PutPersonalWorkstreamResult,
  error: Failure,
});

export const AssignPersonalWorkstreamRpc = Rpc.make("workstreamsPersonal.assign", {
  payload: AssignPersonalWorkstreamInput,
  success: AssignPersonalWorkstreamResult,
  error: Failure,
});
