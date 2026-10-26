import * as Schema from "effect/Schema";
import { NodeId, ProviderThreadId, RunId, TrimmedNonEmptyString } from "../baseSchemas.ts";
import { ModelSelection } from "../modelSelection.ts";
import { ProviderInstanceId } from "../providerInstance.ts";
import { OrchestratorMcpDelegateTaskResult } from "../orchestratorMcp.ts";

export const TaskContinueInput = Schema.Struct({
  taskId: NodeId,
  message: TrimmedNonEmptyString.check(Schema.isMaxLength(120_000)),
  clientRequestId: TrimmedNonEmptyString.check(Schema.isMaxLength(256)),
  expectedNativeSessionId: Schema.optional(TrimmedNonEmptyString.check(Schema.isMaxLength(256))),
});
export type TaskContinueInput = typeof TaskContinueInput.Type;

export const TaskContinuationEvidence = Schema.Struct({
  sourceTaskId: NodeId,
  sourceRunId: RunId,
  sourceEvidence: Schema.Literal("attempt"),
  providerInstanceId: ProviderInstanceId,
  providerThreadId: ProviderThreadId,
  // The source destination is expected until a guarded attempt confirms its init.
  nativeSessionId: Schema.String,
  modelSelection: ModelSelection,
  verification: Schema.Literals(["pending", "verified", "unverified"]),
  verifiedRunId: Schema.NullOr(RunId),
  account: Schema.Literal("unavailable"),
});
export type TaskContinuationEvidence = typeof TaskContinuationEvidence.Type;

export const TaskContinueResult = Schema.Struct({
  ...OrchestratorMcpDelegateTaskResult.fields,
  replayed: Schema.Boolean,
  continuation: TaskContinuationEvidence,
});
export type TaskContinueResult = typeof TaskContinueResult.Type;
