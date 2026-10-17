import { APP_IDENTITY } from "../../jones/runtimeIdentity/AppIdentity.ts";
import type { ProviderDriverKind, ProviderReplayTranscript } from "@t3tools/contracts";

import { buildRuntimeInstructions } from "@t3tools/provider-core/server/runtimeInstructions";

export function materializeReplayTranscriptRuntimeInstructions(
  transcript: ProviderReplayTranscript,
  runtime: { readonly driver: ProviderDriverKind; readonly model: string },
): ProviderReplayTranscript {
  if (runtime.driver === "muse") {
    return materializeMuseRuntimeInstructions(transcript, runtime.model);
  }
  const harness =
    runtime.driver === "cursor"
      ? "Cursor"
      : runtime.driver === "grok"
        ? "Grok"
        : runtime.driver === "acpRegistry"
          ? "acpRegistry"
          : undefined;
  if (harness === undefined) return transcript;
  const instructions = buildRuntimeInstructions({
    appIdentity: APP_IDENTITY,
    harness,
    model: runtime.model,
  });

  return {
    ...transcript,
    entries: transcript.entries.map((entry) => {
      if (entry.type !== "expect_outbound") return entry;
      const frame = entry.frame;
      if (typeof frame !== "object" || frame === null) return entry;
      if (
        runtime.driver === "cursor" &&
        "type" in frame &&
        frame.type === "run.start" &&
        "message" in frame &&
        typeof frame.message === "string"
      ) {
        return frame.message.endsWith(instructions)
          ? entry
          : { ...entry, frame: { ...frame, message: `${frame.message}\n\n${instructions}` } };
      }
      if (
        "method" in frame &&
        frame.method === "session/prompt" &&
        "params" in frame &&
        typeof frame.params === "object" &&
        frame.params !== null &&
        "prompt" in frame.params &&
        Array.isArray(frame.params.prompt)
      ) {
        const prompt: ReadonlyArray<unknown> = frame.params.prompt;
        const lastPart = prompt.at(-1);
        if (
          typeof lastPart === "object" &&
          lastPart !== null &&
          "type" in lastPart &&
          lastPart.type === "text" &&
          "text" in lastPart &&
          lastPart.text === instructions
        ) {
          return entry;
        }
        return {
          ...entry,
          frame: {
            ...frame,
            params: { ...frame.params, prompt: [...prompt, { type: "text", text: instructions }] },
          },
        };
      }
      return entry;
    }),
  };
}

/** Muse records its runtime block as a turn/start input part; swap in this build's text. */
function materializeMuseRuntimeInstructions(
  transcript: ProviderReplayTranscript,
  model: string,
): ProviderReplayTranscript {
  return {
    ...transcript,
    entries: transcript.entries.map((entry) => {
      if (entry.type !== "expect_outbound") return entry;
      const frame = entry.frame;
      if (
        typeof frame !== "object" ||
        frame === null ||
        !("method" in frame) ||
        frame.method !== "turn/start" ||
        !("params" in frame) ||
        typeof frame.params !== "object" ||
        frame.params === null ||
        !("input" in frame.params) ||
        !Array.isArray(frame.params.input)
      ) {
        return entry;
      }
      const params = frame.params;
      const input: ReadonlyArray<unknown> = frame.params.input;
      const reasoningEffort =
        "reasoningEffort" in params && typeof params.reasoningEffort === "string"
          ? params.reasoningEffort
          : undefined;
      const instructions = buildRuntimeInstructions({
        appIdentity: APP_IDENTITY,
        harness: "Muse Code",
        model,
        ...(reasoningEffort === undefined ? {} : { reasoningEffort }),
      });
      return {
        ...entry,
        frame: {
          ...frame,
          params: {
            ...params,
            input: input.map((part) =>
              typeof part === "object" &&
              part !== null &&
              "type" in part &&
              part.type === "text" &&
              "text" in part &&
              typeof part.text === "string" &&
              part.text.startsWith("<runtime_info>")
                ? { ...part, text: instructions }
                : part,
            ),
          },
        },
      };
    }),
  };
}
