import * as Schema from "effect/Schema";
import { EnvironmentAuthorizationError } from "../auth.ts";
import * as Rpc from "effect/rpc/Rpc";
import {
  LinkedThreadsListInput,
  LinkedThreadsListResult,
  LinkThreadsInput,
  UnlinkThreadsInput,
  AcceptThreadHandoffInput,
  LinkedThreadMutationResult,
  LinkedThreadsError,
} from "./linkedThreads.ts";

export const ListLinkedThreadsRpc = Rpc.make("orchestration.listLinkedThreads", {
  payload: LinkedThreadsListInput,
  success: LinkedThreadsListResult,
  error: Schema.Union([LinkedThreadsError, EnvironmentAuthorizationError]),
});
export const LinkThreadsRpc = Rpc.make("orchestration.linkThreads", {
  payload: LinkThreadsInput,
  success: LinkedThreadMutationResult,
  error: Schema.Union([LinkedThreadsError, EnvironmentAuthorizationError]),
});
export const UnlinkThreadsRpc = Rpc.make("orchestration.unlinkThreads", {
  payload: UnlinkThreadsInput,
  success: LinkedThreadMutationResult,
  error: Schema.Union([LinkedThreadsError, EnvironmentAuthorizationError]),
});
export const AcceptThreadHandoffRpc = Rpc.make("orchestration.acceptThreadHandoff", {
  payload: AcceptThreadHandoffInput,
  success: LinkedThreadMutationResult,
  error: Schema.Union([LinkedThreadsError, EnvironmentAuthorizationError]),
});
