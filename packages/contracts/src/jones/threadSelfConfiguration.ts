import * as Schema from "effect/Schema";
import { ProviderOptionSelection } from "../model.ts";
import { ProviderInstanceId } from "../providerInstance.ts";
import { ModelSelection } from "../modelSelection.ts";
import {
  NonNegativeInt,
  RunId,
  RunAttemptId,
  ProviderThreadId,
  ProviderSessionId,
  ThreadId,
  TrimmedNonEmptyString,
} from "../baseSchemas.ts";
import { RuntimeIdentityAttestation } from "./providerRuntimeIdentity.ts";

export const ThreadExecutionPrecondition = Schema.Struct({
  runId: RunId,
  attemptId: RunAttemptId,
  providerThreadId: ProviderThreadId,
  providerSessionId: ProviderSessionId,
  runtimeGeneration: TrimmedNonEmptyString,
});
export type ThreadExecutionPrecondition = typeof ThreadExecutionPrecondition.Type;

export const ThreadConfigurationEvidence = Schema.Struct({
  configurationRevision: NonNegativeInt,
  savedSelection: ModelSelection,
  executionPrecondition: Schema.NullOr(ThreadExecutionPrecondition),
  currentExecution: Schema.NullOr(
    Schema.Struct({
      runId: RunId,
      attemptId: Schema.NullOr(RunAttemptId),
      requestedSelection: ModelSelection,
      providerObservation: Schema.NullOr(RuntimeIdentityAttestation),
      reasoningEffortObservation: Schema.Literal("unavailable"),
    }),
  ),
});
export type ThreadConfigurationEvidence = typeof ThreadConfigurationEvidence.Type;

export const ThreadSelfConfigurationInput = Schema.Struct({
  clientRequestId: TrimmedNonEmptyString.check(Schema.isMaxLength(200)),
  modelSelection: Schema.Struct({
    instanceId: ProviderInstanceId,
    model: TrimmedNonEmptyString,
    options: Schema.optionalKey(Schema.Array(ProviderOptionSelection)),
  }),
  expectedConfigurationRevision: NonNegativeInt,
  executionPrecondition: ThreadExecutionPrecondition,
});
export type ThreadSelfConfigurationInput = typeof ThreadSelfConfigurationInput.Type;
export const THREAD_SELF_CONFIGURATION_APPLICATION =
  "Applies to subsequently admitted runs that inherit this selection; this operation does not alter the current run or already captured runs.";
export const ThreadSelfConfigurationResult = Schema.Struct({
  acceptance: Schema.Struct({
    threadId: ThreadId,
    sequence: NonNegativeInt,
    committedSelection: ModelSelection,
    protectedExecution: ThreadExecutionPrecondition,
    application: Schema.Literal("subsequently_admitted_inheriting_runs"),
    applicationMessage: Schema.Literal(THREAD_SELF_CONFIGURATION_APPLICATION),
  }),
  freshObservation: ThreadConfigurationEvidence,
});
export type ThreadSelfConfigurationResult = typeof ThreadSelfConfigurationResult.Type;
