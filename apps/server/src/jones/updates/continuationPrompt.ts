import type { ProviderAdapterV2TurnInput } from "@t3tools/provider-core/server/ProviderAdapter";

/** Codex requires input even after resuming the original native conversation. */
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
    input.providerThread.driver === "codex" ||
    !options.sameNativeThread ||
    options.noteContinuation ||
    options.context !== "";
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
