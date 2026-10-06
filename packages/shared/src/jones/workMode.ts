export const WORK_MODE_SENTINEL = "@@@@@";
export const WORK_MODE_INTERVAL_MS = 55 * 60 * 1000;
export const WORK_MODE_INSTRUCTIONS =
  "When the user input is exactly @@@@@, respond with only @@@@@. Do not use tools, take any other action, or continue further work for that input.";

export function isWorkModeSentinelMessage(message: {
  readonly role: string;
  readonly text: string;
  readonly attachments?: ReadonlyArray<unknown> | undefined;
  readonly context?: { readonly records: ReadonlyArray<unknown> } | undefined;
  readonly content?: ReadonlyArray<unknown> | undefined;
  readonly status?: string | undefined;
}): boolean {
  return (
    (message.role === "user" || message.role === "assistant") &&
    message.text === WORK_MODE_SENTINEL &&
    (message.attachments?.length ?? 0) === 0 &&
    (message.context?.records.length ?? 0) === 0 &&
    (message.content?.length ?? 0) === 0 &&
    message.status !== "failed" &&
    message.status !== "cancelled" &&
    message.status !== "interrupted"
  );
}
