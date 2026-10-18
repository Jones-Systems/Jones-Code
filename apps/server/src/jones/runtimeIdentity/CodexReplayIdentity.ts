import type { ProviderReplayTranscript } from "@t3tools/contracts";
import { buildAppIdentityInstructions } from "./AppIdentity.ts";

/** Materializes current app identity in legacy fixtures; replay still compares exact frames. */
export function materializeCodexReplayAppIdentity(
  transcript: ProviderReplayTranscript,
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
        frame.params === null
      )
        return entry;
      const params = frame.params;
      const context = "additionalContext" in params ? params.additionalContext : undefined;
      if (context !== undefined) return entry;
      return {
        ...entry,
        frame: {
          ...frame,
          params: {
            ...params,
            additionalContext: {
              jones_code_identity: { kind: "application", value: buildAppIdentityInstructions() },
            },
          },
        },
      };
    }),
  };
}
