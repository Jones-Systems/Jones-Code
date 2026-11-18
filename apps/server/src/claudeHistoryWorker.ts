import {
  forkSession,
  getSessionMessages,
  type ForkSessionOptions,
} from "@anthropic-ai/claude-agent-sdk";
import * as Schema from "effect/Schema";

// Bundled import.meta.main is always true; imports must have no side effects.
const decodeHistoryOptions = Schema.decodeSync(
  Schema.fromJsonString(
    Schema.Struct({
      dir: Schema.optionalKey(Schema.String),
      includeSystemMessages: Schema.optionalKey(Schema.Boolean),
      upToMessageId: Schema.optionalKey(Schema.String),
    }),
  ),
);

/** The SDK reports this exact lookup failure before creating the fork transcript. */
export async function forkClaudeHistorySession(sessionId: string, options: ForkSessionOptions) {
  try {
    return await forkSession(sessionId, options);
  } catch (cause) {
    const missing = options.dir
      ? `Session ${sessionId} not found in project directory for ${options.dir}`
      : `Session ${sessionId} not found`;
    if (cause instanceof Error && cause.message === missing) {
      return { unappliedSourceNativeId: sessionId } as const;
    }
    throw cause;
  }
}

export async function runClaudeHistoryWorker(
  method: string | undefined,
  sessionId: string | undefined,
  rawOptions: string | undefined,
): Promise<void> {
  const options = decodeHistoryOptions(rawOptions ?? "{}");
  if (!sessionId) throw new Error("Claude history session id is required.");
  const result =
    method === "getSessionMessages"
      ? await getSessionMessages(sessionId, options)
      : method === "forkSession"
        ? await forkClaudeHistorySession(sessionId, options).then(async (forked) => {
            if ("unappliedSourceNativeId" in forked) return forked;
            const headMessageId = await getSessionMessages(forked.sessionId, options)
              .then((messages) => messages.at(-1)?.uuid)
              .catch(() => undefined);
            return headMessageId === undefined ? forked : { ...forked, headMessageId };
          })
        : (() => {
            throw new Error("Unknown Claude history operation.");
          })();
  process.stdout.write(JSON.stringify(result));
}
