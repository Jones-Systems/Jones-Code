import type { ProviderAdapterV2TurnInput } from "../../orchestration-v2/ProviderAdapter.ts";

/** Native resume may omit input only when the original thread needs no new context. */
export function continuationPrompt(
  input: ProviderAdapterV2TurnInput,
  options: {
    readonly sameNativeThread: boolean;
    readonly noteContinuation: boolean;
    readonly context: string;
    readonly userText: string;
  },
): ProviderAdapterV2TurnInput {
  const { restartContinuationOfRunId: _resumedRunId, ...promptedInput } = input;
  const needsPrompt =
    !options.sameNativeThread || options.noteContinuation || options.context !== "";
  return {
    ...(needsPrompt ? promptedInput : input),
    message: {
      ...input.message,
      text:
        options.context === ""
          ? options.userText
          : `${options.context}\n\nUser message:\n${options.userText}`,
    },
  };
}
