import { TextGenerationError, type ModelSelection } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Schema from "effect/Schema";

// Leave time for durable admission state and the linked sender's 60-second deadline.
export const REMOTE_ADMISSION_EVALUATION_TIMEOUT_MS = 45_000;

export type EvaluationFailureKind =
  | "timeout"
  | "auth_access"
  | "provider_limit"
  | "unsupported_selection"
  | "invalid_assessment"
  | "oversized_request"
  | "provider_failure"
  | "internal_error"
  | "interrupted";

const templates: Record<EvaluationFailureKind, string> = {
  timeout:
    "Evaluation timed out. Check provider availability before retrying or review this request.",
  auth_access:
    "Evaluator authentication or access is unavailable. Check the selected provider account and its access.",
  provider_limit:
    "The evaluator reached a provider usage or rate limit. Check its allowance before retrying.",
  unsupported_selection:
    "The evaluator provider, model, or options are unavailable or unsupported. Choose a supported selection.",
  invalid_assessment:
    "The evaluator returned an invalid assessment or output. Review this request or choose another evaluator.",
  oversized_request:
    "The evaluation request exceeds the supported input limits. Shorten the request before retrying.",
  provider_failure:
    "The evaluator provider failed. Check provider availability or review this request.",
  internal_error:
    "Evaluation encountered an internal error. Review this request and check the evaluator setup.",
  interrupted: "Evaluation was interrupted.",
};

const knownDetails = new Map<string, EvaluationFailureKind>([
  ["Choose an evaluator model.", "unsupported_selection"],
  [
    "The selected provider instance does not support tool-free remote request evaluation.",
    "unsupported_selection",
  ],
  [
    "The selected evaluator model is unavailable on this provider instance.",
    "unsupported_selection",
  ],
  [
    "The selected evaluator options are unsupported by this exact model and instance.",
    "unsupported_selection",
  ],
  ["Managed Codex evaluation does not support the selected options.", "unsupported_selection"],
  [
    "Claude remote request evaluation cannot use malformed launch arguments.",
    "unsupported_selection",
  ],
  [
    "Claude remote request evaluation does not support positional or malformed launch arguments.",
    "unsupported_selection",
  ],
  ["The evaluator account does not match the selected provider instance.", "auth_access"],
  ["The selected managed Codex account is unavailable.", "auth_access"],
  ["Claude CLI request timed out.", "timeout"],
  ["Codex CLI request timed out.", "timeout"],
  ["The managed Codex evaluator timed out.", "timeout"],
  ["Claude CLI returned unexpected output format.", "invalid_assessment"],
  ["Claude returned invalid structured output.", "invalid_assessment"],
  ["ClaudeTextGeneration returned invalid structured output.", "invalid_assessment"],
  ["The managed Codex evaluator did not return an event stream.", "invalid_assessment"],
  ["The managed Codex evaluator did not return one assessment.", "invalid_assessment"],
  ["The managed Codex evaluator returned an invalid assessment.", "invalid_assessment"],
  [
    "The managed Codex evaluator returned an incomplete, refused, or unexpected response.",
    "invalid_assessment",
  ],
  [
    "Remote request evaluation requires a nonblank message of at most 24000 characters and context of at most 8000 characters.",
    "oversized_request",
  ],
]);
const isTextGenerationError = Schema.is(TextGenerationError);

export const classifyEvaluationFailure = (cause: Cause.Cause<unknown>): EvaluationFailureKind => {
  if (Cause.hasInterrupts(cause)) return "interrupted";
  if (Cause.hasDies(cause)) return "internal_error";
  for (const reason of cause.reasons) {
    if (!Cause.isFailReason(reason)) continue;
    const error = reason.error;
    if (Cause.isTimeoutError(error)) return "timeout";
    if (Schema.isSchemaError(error)) return "invalid_assessment";
    if (!isTextGenerationError(error) || error.operation !== "evaluateRemoteRequest") continue;
    if (error.detail.length > 2048) return "provider_failure";
    const known = knownDetails.get(error.detail);
    if (known) return known;
    const httpFailure =
      /^The managed Codex evaluator did not return a successful response \(HTTP (401|403|429)\)\.$/.exec(
        error.detail,
      );
    if (httpFailure && httpFailure[0] === error.detail)
      return httpFailure[1] === "429" ? "provider_limit" : "auth_access";
    const launchFailure =
      /^Claude remote request evaluation does not support the configured --[a-zA-Z][a-zA-Z0-9-]{0,63} launch flag\.$/.exec(
        error.detail,
      );
    if (launchFailure && launchFailure[0] === error.detail) return "unsupported_selection";

    // CLI failures may contain arbitrary stderr. Recognize only failure classes;
    // neither their text nor their nested cause is ever rendered or logged here.
    if (/^(Claude|Codex) CLI command failed: /.test(error.detail)) {
      const detail = error.detail.slice(0, 2048).toLowerCase();
      if (
        /rate.?limit|usage.?limit|quota|exceeded.*limit|hit.*limit|credit balance|insufficient credits/.test(
          detail,
        )
      )
        return "provider_limit";
      if (
        /not logged in|authentication|unauthori[sz]ed|forbidden|access denied|permission denied|invalid api key|oauth.*expired/.test(
          detail,
        )
      )
        return "auth_access";
    }
    return "provider_failure";
  }
  return "internal_error";
};

// Configuration identifiers are metadata, but a custom model is free text.
// Omit unsafe identifiers in full instead of attempting to redact arbitrary text.
const safeIdentifier = (value: string | undefined): string => {
  if (value === undefined) return "unselected";
  if (
    value.length > 128 ||
    !/^[a-zA-Z0-9][a-zA-Z0-9._/\[\]-]*$/.test(value) ||
    /^(sk-|gh[pousr]_|github_pat_|eyJ)/i.test(value)
  )
    return "[redacted]";
  return value;
};

export const evaluationDiagnostic = (
  failureKind: EvaluationFailureKind,
  selection: ModelSelection | null,
) => {
  const instanceId = safeIdentifier(selection?.instanceId);
  const model = safeIdentifier(selection?.model);
  return {
    failureKind,
    instanceId,
    model,
    reason:
      `Evaluator instance=${instanceId}, model=${model} (${failureKind}): ${templates[failureKind]} Owner review is required.`.slice(
        0,
        600,
      ),
  };
};
