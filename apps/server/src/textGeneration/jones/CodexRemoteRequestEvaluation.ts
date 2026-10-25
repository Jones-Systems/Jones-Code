import { TextGenerationError, type ProviderInstanceId } from "@t3tools/contracts";
import {
  RemoteRequestEvaluationResult,
  type ProviderTextGeneration,
} from "@t3tools/provider-core/server/textGeneration";
import { buildRemoteRequestEvaluationPrompt } from "@t3tools/provider-core/server/textGenerationOperations";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { HttpClient, HttpClientRequest } from "effect/http";

const ResponseEnvelope = Schema.Struct({
  status: Schema.Literal("completed"),
  output: Schema.Array(
    Schema.Union([
      Schema.Struct({ type: Schema.Literal("reasoning") }),
      Schema.Struct({
        type: Schema.Literal("message"),
        role: Schema.Literal("assistant"),
        status: Schema.Literal("completed"),
        content: Schema.Array(
          Schema.Struct({ type: Schema.Literal("output_text"), text: Schema.String }),
        ),
      }),
    ]),
  ),
});
const decodeResponseEnvelope = Schema.decodeUnknownEffect(ResponseEnvelope);
const decodeAssessment = Schema.decodeUnknownEffect(
  Schema.fromJsonString(RemoteRequestEvaluationResult),
);

// Responses strict schemas require an object root. The domain decoder below
// additionally enforces the pass/none and hold/risk category pairing.
const outputSchema = {
  type: "object",
  additionalProperties: false,
  properties: {
    verdict: { type: "string", enum: ["pass", "hold"] },
    category: {
      type: "string",
      enum: ["none", "malicious", "ridiculous", "risky", "out_of_scope"],
    },
    reason: { type: "string", maxLength: 600 },
  },
  required: ["verdict", "category", "reason"],
};

export const makeCodexRemoteRequestEvaluation = Effect.fn("makeCodexRemoteRequestEvaluation")(
  function* <E>(options: {
    readonly instanceId: ProviderInstanceId;
    readonly access: Effect.Effect<{ readonly accessToken: string }, E>;
  }) {
    const http = yield* HttpClient.HttpClient;
    const evaluate: ProviderTextGeneration["evaluateRemoteRequest"] = Effect.fn(
      "CodexRemoteRequestEvaluation.evaluate",
    )(
      function* (input) {
        if (input.modelSelection.instanceId !== options.instanceId)
          return yield* new TextGenerationError({
            operation: "evaluateRemoteRequest",
            detail: "The evaluator account does not match the selected provider instance.",
          });
        const seen = new Set<string>();
        let effort: string | undefined;
        for (const option of input.modelSelection.options ?? []) {
          if (
            seen.has(option.id) ||
            option.id !== "reasoningEffort" ||
            typeof option.value !== "string" ||
            !["none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"].includes(
              option.value,
            )
          )
            return yield* new TextGenerationError({
              operation: "evaluateRemoteRequest",
              detail: "Managed Codex evaluation does not support the selected options.",
            });
          seen.add(option.id);
          effort = option.value;
        }
        const { prompt } = yield* buildRemoteRequestEvaluationPrompt(input);
        const credentials = yield* options.access.pipe(
          Effect.mapError(
            (cause) =>
              new TextGenerationError({
                operation: "evaluateRemoteRequest",
                detail: "The selected managed Codex account is unavailable.",
                cause,
              }),
          ),
        );
        // No agent/CLI, cwd, configuration discovery, hooks, MCP or tool executor
        // participates. Only the instance's existing token-sharing access is used.
        const response = yield* http
          .execute(
            HttpClientRequest.post("https://api.openai.com/v1/responses").pipe(
              HttpClientRequest.bearerToken(credentials.accessToken),
              HttpClientRequest.bodyJsonUnsafe({
                model: input.modelSelection.model,
                input: [{ role: "user", content: [{ type: "input_text", text: prompt }] }],
                tools: [],
                tool_choice: "none",
                parallel_tool_calls: false,
                store: false,
                stream: false,
                max_output_tokens: 4096,
                ...(effort ? { reasoning: { effort } } : {}),
                text: {
                  format: {
                    type: "json_schema",
                    name: "remote_request_evaluation",
                    strict: true,
                    schema: outputSchema,
                  },
                },
              }),
            ),
          )
          .pipe(
            Effect.mapError(
              (cause) =>
                new TextGenerationError({
                  operation: "evaluateRemoteRequest",
                  detail: "The managed Codex evaluator request failed.",
                  cause,
                }),
            ),
          );
        if (response.status !== 200)
          return yield* new TextGenerationError({
            operation: "evaluateRemoteRequest",
            detail: "The managed Codex evaluator did not return a successful response.",
          });
        const envelope = yield* response.json.pipe(
          Effect.flatMap(decodeResponseEnvelope),
          Effect.mapError(
            (cause) =>
              new TextGenerationError({
                operation: "evaluateRemoteRequest",
                detail:
                  "The managed Codex evaluator returned an incomplete, refused, or unexpected response.",
                cause,
              }),
          ),
        );
        const messages = envelope.output.filter((item) => item.type === "message");
        if (messages.length !== 1 || messages[0]!.content.length !== 1)
          return yield* new TextGenerationError({
            operation: "evaluateRemoteRequest",
            detail: "The managed Codex evaluator did not return one assessment.",
          });
        return yield* decodeAssessment(messages[0]!.content[0]!.text).pipe(
          Effect.mapError(
            (cause) =>
              new TextGenerationError({
                operation: "evaluateRemoteRequest",
                detail: "The managed Codex evaluator returned an invalid assessment.",
                cause,
              }),
          ),
        );
      },
      Effect.timeout("25 seconds"),
      Effect.catchTags({
        TimeoutError: (cause) =>
          Effect.fail(
            new TextGenerationError({
              operation: "evaluateRemoteRequest",
              detail: "The managed Codex evaluator timed out.",
              cause,
            }),
          ),
      }),
    );
    return evaluate;
  },
);
