import { WORK_MODE_INSTRUCTIONS, WORK_MODE_SENTINEL } from "@t3tools/shared/jones/workMode";

// Keep durable history and ordinary prompts unchanged; only the reserved input
// needs an explicit instruction when an existing provider session is resumed.
export function workModeProviderPrompt(text: string): string {
  return text === WORK_MODE_SENTINEL
    ? `<jones_work_mode>\n${WORK_MODE_INSTRUCTIONS}\nThis is a keep-warm request. The original user input is provided below; it has no work to perform.\n</jones_work_mode>\n\n<user_request>\n${text}\n</user_request>`
    : text;
}

export function isWorkModeKeepWarm(message: {
  readonly id: string;
  readonly createdBy?: string;
  readonly creationSource?: string;
  readonly text: string;
  readonly attachments: ReadonlyArray<unknown>;
  readonly context?: { readonly records: ReadonlyArray<unknown> } | null | undefined;
}): boolean {
  return (
    message.createdBy === "system" &&
    message.creationSource === "server" &&
    message.id.startsWith("work-mode:") &&
    message.text === WORK_MODE_SENTINEL &&
    message.attachments.length === 0 &&
    (message.context?.records.length ?? 0) === 0
  );
}
