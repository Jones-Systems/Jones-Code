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
        : runtime.driver === "kiro"
          ? "Kiro"
          : runtime.driver === "acpRegistry"
            ? "acpRegistry"
            : runtime.driver === "fx"
              ? "fx"
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
          typeof lastPart.text === "string" &&
          lastPart.text === instructions
        ) {
          return entry;
        }
        // A recording that switched models mid-thread already carries the instructions for
        // the model each prompt ran on, in the recording app's wording. Swap in this build's
        // text for that same harness, model and effort.
        if (
          typeof lastPart === "object" &&
          lastPart !== null &&
          "type" in lastPart &&
          lastPart.type === "text" &&
          "text" in lastPart &&
          typeof lastPart.text === "string" &&
          lastPart.text.startsWith("<runtime_info>")
        ) {
          const recorded = recordedRuntimeInstructions(lastPart.text);
          if (recorded === undefined) return entry;
          return {
            ...entry,
            frame: {
              ...frame,
              params: {
                ...frame.params,
                prompt: [
                  ...prompt.slice(0, -1),
                  {
                    ...lastPart,
                    text: buildRuntimeInstructions({ appIdentity: APP_IDENTITY, ...recorded }),
                  },
                ],
              },
            },
          };
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

const RECORDED_RUNTIME_INFO =
  /^<runtime_info>In case you're asked: you are running (?:in .+? )?through the (.+?) harness(?:, as (.+?))?(?: with (\S+) reasoning effort)?\. /;

function recordedRuntimeInstructions(text: string):
  | {
      readonly harness: string;
      readonly model?: string;
      readonly modelName?: string;
      readonly reasoningEffort?: string;
    }
  | undefined {
  const match = RECORDED_RUNTIME_INFO.exec(text);
  if (match === null) return undefined;
  const [, harness, label, reasoningEffort] = match;
  const named = label === undefined ? null : /^(.*) \(model slug: (.*)\)$/.exec(label);
  return {
    harness: harness!,
    ...(named === null
      ? label === undefined
        ? {}
        : { model: label }
      : { modelName: named[1]!, model: named[2]! }),
    ...(reasoningEffort === undefined ? {} : { reasoningEffort }),
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
