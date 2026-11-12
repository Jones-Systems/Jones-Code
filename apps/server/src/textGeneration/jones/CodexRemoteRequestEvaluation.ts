import { TextGenerationError, type ProviderInstanceId } from "@t3tools/contracts";
import {
  RemoteRequestEvaluationResult,
  type ProviderTextGeneration,
} from "@t3tools/provider-core/server/textGeneration";
import { buildRemoteRequestEvaluationPrompt } from "@t3tools/provider-core/server/textGenerationOperations";
import { toJsonSchemaObject } from "@t3tools/provider-core/server/textGenerationUtils";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Sse from "effect/encoding/Sse";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";

const MAX_STREAM_BYTES = 1024 * 1024;
const OutputText = Schema.Struct({ type: Schema.Literal("output_text"), text: Schema.String });
const StreamingOutputItem = Schema.Union([
  Schema.Struct({ type: Schema.Literal("reasoning") }),
  Schema.Struct({
    type: Schema.Literal("message"),
    role: Schema.Literal("assistant"),
    content: Schema.Array(OutputText),
  }),
]);
const ResponseEnvelope = Schema.Struct({
  status: Schema.Literal("completed"),
  error: Schema.optional(Schema.Null),
  incomplete_details: Schema.optional(Schema.Null),
  output: Schema.Array(
    Schema.Union([
      Schema.Struct({ type: Schema.Literal("reasoning") }),
      Schema.Struct({
        type: Schema.Literal("message"),
        role: Schema.Literal("assistant"),
        status: Schema.Literal("completed"),
        content: Schema.Array(OutputText),
      }),
    ]),
  ),
});
const decodeStreamEvent = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Union([
      Schema.Struct({ type: Schema.Literal("response.completed"), response: ResponseEnvelope }),
      Schema.Struct({
        type: Schema.Literals(["response.created", "response.in_progress"]),
        response: Schema.Struct({
          status: Schema.Literals(["queued", "in_progress"]),
          output: Schema.Array(StreamingOutputItem),
        }),
      }),
      Schema.Struct({
        type: Schema.Literals(["response.output_item.added", "response.output_item.done"]),
        item: StreamingOutputItem,
      }),
      Schema.Struct({
        type: Schema.Literals(["response.content_part.added", "response.content_part.done"]),
        part: OutputText,
      }),
      Schema.Struct({
        type: Schema.Literals([
          "response.output_text.delta",
          "response.reasoning_summary_text.delta",
        ]),
        delta: Schema.String,
      }),
      Schema.Struct({
        type: Schema.Literals([
          "response.output_text.done",
          "response.reasoning_summary_text.done",
        ]),
        text: Schema.String,
      }),
      Schema.Struct({
        type: Schema.Literals([
          "response.reasoning_summary_part.added",
          "response.reasoning_summary_part.done",
        ]),
        part: Schema.Struct({ type: Schema.Literal("summary_text"), text: Schema.String }),
      }),
    ]),
  ),
);
const decodeAssessment = Schema.decodeUnknownEffect(
  Schema.fromJsonString(RemoteRequestEvaluationResult),
);

const readStream = Effect.fn("CodexRemoteRequestEvaluation.readStream")(function* (
  response: HttpClientResponse.HttpClientResponse,
) {
  const invalidStream = {
    operation: "evaluateRemoteRequest",
    detail: "The managed Codex evaluator returned an incomplete, refused, or unexpected response.",
  } as const;
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let bytes = 0;
  let text = "";
  // Retain only a bounded stream and read through EOF: text/done events are not
  // success, and token-sharing failures may arrive after output has begun.
  yield* response.stream.pipe(
    Stream.mapError(
      (cause) =>
        new TextGenerationError({
          operation: "evaluateRemoteRequest",
          detail: "The managed Codex evaluator stream failed.",
          cause,
        }),
    ),
    Stream.runForEach((chunk) => {
      bytes += chunk.byteLength;
      if (bytes > MAX_STREAM_BYTES) return Effect.fail(new TextGenerationError(invalidStream));
      return Effect.try({
        try: () => {
          text += decoder.decode(chunk, { stream: true });
        },
        catch: (cause) => new TextGenerationError({ ...invalidStream, cause }),
      });
    }),
  );
  const framed = yield* Effect.try({
    try: () => (text + decoder.decode()).replace(/\r\n?/g, "\n"),
    catch: (cause) => new TextGenerationError({ ...invalidStream, cause }),
  });
  // SSE discards unterminated frames at EOF. Fail closed instead of losing a
  // trailing failure behind an otherwise valid completion.
  if (!framed.endsWith("\n\n")) return yield* new TextGenerationError(invalidStream);
  const frames: Array<Sse.AnyEvent> = [];
  const parser = Sse.makeParser((frame) => frames.push(frame), {
    maxEventSize: MAX_STREAM_BYTES,
    dispatchEmptyNamedEvents: true,
  });
  const parseError = parser.feed(framed);
  if (parseError) return yield* new TextGenerationError({ ...invalidStream, cause: parseError });
  let completed: typeof ResponseEnvelope.Type | undefined;
  let done = false;
  for (const frame of frames) {
    if (frame._tag !== "Event" || done) return yield* new TextGenerationError(invalidStream);
    if (frame.data === "[DONE]") {
      if (!completed || frame.event !== "message")
        return yield* new TextGenerationError(invalidStream);
      done = true;
      continue;
    }
    if (completed) return yield* new TextGenerationError(invalidStream);
    const event = yield* decodeStreamEvent(frame.data).pipe(
      Effect.mapError((cause) => new TextGenerationError({ ...invalidStream, cause })),
    );
    if (frame.event !== "message" && frame.event !== event.type)
      return yield* new TextGenerationError(invalidStream);
    if (event.type === "response.completed") completed = event.response;
  }
  if (!completed) return yield* new TextGenerationError(invalidStream);
  return completed;
});

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
        const { prompt, outputSchema } = yield* buildRemoteRequestEvaluationPrompt(input);
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
        const response = yield* HttpClient.withScope(http)
          .execute(
            HttpClientRequest.post("https://api.openai.com/v1/responses").pipe(
              HttpClientRequest.bearerToken(credentials.accessToken),
              HttpClientRequest.setHeader("accept", "text/event-stream"),
              HttpClientRequest.bodyJsonUnsafe({
                model: input.modelSelection.model,
                input: [{ role: "user", content: [{ type: "input_text", text: prompt }] }],
                tools: [],
                tool_choice: "none",
                parallel_tool_calls: false,
                store: false,
                stream: true,
                ...(effort ? { reasoning: { effort } } : {}),
                text: {
                  format: {
                    type: "json_schema",
                    name: "remote_request_evaluation",
                    strict: true,
                    schema: toJsonSchemaObject(outputSchema),
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
            detail: `The managed Codex evaluator did not return a successful response (HTTP ${response.status}).`,
          });
        if (
          response.headers["content-type"]?.split(";")[0]?.trim().toLowerCase() !==
          "text/event-stream"
        )
          return yield* new TextGenerationError({
            operation: "evaluateRemoteRequest",
            detail: "The managed Codex evaluator did not return an event stream.",
          });
        const envelope = yield* readStream(response);
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
      Effect.scoped,
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
