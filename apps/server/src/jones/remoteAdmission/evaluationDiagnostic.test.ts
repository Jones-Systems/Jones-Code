import { expect, it } from "@effect/vitest";
import { ProviderInstanceId, TextGenerationError } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import {
  classifyEvaluationFailure,
  evaluationDiagnostic,
  type EvaluationFailureKind,
} from "./evaluationDiagnostic.ts";

const selection = {
  instanceId: ProviderInstanceId.make("claude-malcolm"),
  model: "claude-opus-5-5",
};
const sensitive = "SYNTHETIC_PAYLOAD sk-SYNTHETIC_KEY https://example.test/path?signature=PRIVATE";
const failure = (detail: string) =>
  Cause.fail(
    new TextGenerationError({
      operation: "evaluateRemoteRequest",
      detail,
      cause: new Error(sensitive),
    }),
  );

const cases: ReadonlyArray<readonly [EvaluationFailureKind, Cause.Cause<unknown>]> = [
  ["timeout", Cause.fail(new Cause.TimeoutError(sensitive))],
  ["timeout", failure("Claude CLI request timed out.")],
  ["timeout", failure("The managed Codex evaluator timed out.")],
  ["auth_access", failure(`Claude CLI command failed: Not logged in. ${sensitive}`)],
  ["auth_access", failure("The selected managed Codex account is unavailable.")],
  [
    "auth_access",
    failure("The managed Codex evaluator did not return a successful response (HTTP 401)."),
  ],
  [
    "auth_access",
    failure("The managed Codex evaluator did not return a successful response (HTTP 403)."),
  ],
  [
    "provider_limit",
    failure(`Claude CLI command failed: You've hit your usage limit. ${sensitive}`),
  ],
  ["provider_limit", failure(`Codex CLI command failed: rate limit exceeded. ${sensitive}`)],
  ["provider_limit", failure(`Claude CLI command failed: Credit balance is too low. ${sensitive}`)],
  [
    "provider_limit",
    failure("The managed Codex evaluator did not return a successful response (HTTP 429)."),
  ],
  ["unsupported_selection", failure("Choose an evaluator model.")],
  [
    "unsupported_selection",
    failure("The selected provider instance does not support tool-free remote request evaluation."),
  ],
  [
    "unsupported_selection",
    failure("The selected evaluator model is unavailable on this provider instance."),
  ],
  [
    "unsupported_selection",
    failure("The selected evaluator options are unsupported by this exact model and instance."),
  ],
  [
    "unsupported_selection",
    failure("Claude remote request evaluation cannot use malformed launch arguments."),
  ],
  [
    "unsupported_selection",
    failure(
      "Claude remote request evaluation does not support positional or malformed launch arguments.",
    ),
  ],
  [
    "unsupported_selection",
    failure(
      "Claude remote request evaluation does not support the configured --mcp-config launch flag.",
    ),
  ],
  ["invalid_assessment", failure("Claude returned invalid structured output.")],
  ["invalid_assessment", failure("ClaudeTextGeneration returned invalid structured output.")],
  ["invalid_assessment", failure("The managed Codex evaluator returned an invalid assessment.")],
  [
    "oversized_request",
    failure(
      "Remote request evaluation requires a nonblank message of at most 24000 characters and context of at most 8000 characters.",
    ),
  ],
  ["provider_failure", failure(sensitive)],
  ["provider_failure", failure(`Claude CLI command failed: ${sensitive}`)],
  ["provider_failure", failure(`unknown authentication usage limit ${sensitive}`)],
  [
    "provider_failure",
    failure("The managed Codex evaluator did not return a successful response (HTTP 500)."),
  ],
  [
    "provider_failure",
    failure(
      `The managed Codex evaluator did not return a successful response (HTTP 401). ${sensitive}`,
    ),
  ],
  [
    "provider_failure",
    failure(
      `Claude remote request evaluation does not support the configured --${sensitive} launch flag.`,
    ),
  ],
  ["provider_failure", failure(sensitive.repeat(200))],
  ["internal_error", Cause.die(new Error(sensitive))],
  ["internal_error", Cause.fail(new Error(sensitive))],
  ["interrupted", Cause.interrupt(42)],
];

it.each(cases)("classifies %s into fixed safe owner text", (expected, cause) => {
  const kind = classifyEvaluationFailure(cause);
  expect(kind).toBe(expected);
  const diagnostic = evaluationDiagnostic(kind, selection);
  expect(diagnostic).toMatchObject({ instanceId: selection.instanceId, model: selection.model });
  expect(diagnostic.reason).toContain(`(${expected})`);
  expect(diagnostic.reason).toContain(`instance=${selection.instanceId}`);
  expect(diagnostic.reason).toContain(`model=${selection.model}`);
  expect(diagnostic.reason.length).toBeLessThanOrEqual(600);
  for (const privateText of [
    "SYNTHETIC_PAYLOAD",
    "sk-SYNTHETIC_KEY",
    "https://",
    "signature=",
    "PRIVATE",
  ])
    expect(JSON.stringify(diagnostic)).not.toContain(privateText);
});

it("accepts only the bounded managed HTTP status and launch flag formats", () => {
  for (const detail of [
    "The managed Codex evaluator did not return a successful response (HTTP 0401).",
    "The managed Codex evaluator did not return a successful response (HTTP 4010).",
    "The managed Codex evaluator did not return a successful response (HTTP 401).\n",
    `Claude remote request evaluation does not support the configured --${"a".repeat(65)} launch flag.`,
    "Claude remote request evaluation does not support the configured --mcp-config launch flag. trailing",
    "Claude remote request evaluation does not support the configured --mcp-config launch flag.\n",
  ])
    expect(classifyEvaluationFailure(failure(detail))).toBe("provider_failure");
  expect(
    classifyEvaluationFailure(
      failure(
        `Claude remote request evaluation does not support the configured --${"a".repeat(64)} launch flag.`,
      ),
    ),
  ).toBe("unsupported_selection");
});

it("bounds owner reasons and omits unsafe model identifiers instead of redacting error text", () => {
  for (const [kind] of cases) {
    expect(
      evaluationDiagnostic(kind, {
        instanceId: ProviderInstanceId.make("c".repeat(64)),
        model: "m".repeat(128),
      }).reason.length,
    ).toBeLessThanOrEqual(600);
  }
  for (const model of [
    sensitive,
    "sk-SYNTHETIC_KEY",
    "model\nSYNTHETIC_PAYLOAD",
    "m".repeat(10_000),
  ]) {
    const diagnostic = evaluationDiagnostic("provider_failure", { ...selection, model });
    expect(diagnostic.model).toBe("[redacted]");
    expect(diagnostic.reason.length).toBeLessThanOrEqual(600);
    expect(diagnostic.reason).not.toContain(model);
  }
  expect(evaluationDiagnostic("unsupported_selection", null)).toMatchObject({
    instanceId: "unselected",
    model: "unselected",
  });
});
