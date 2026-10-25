export const goalResumeNeedsNativeThread = (
  driver: string,
  message: { readonly text: string; readonly attachments: ReadonlyArray<unknown> },
): boolean => {
  if (driver !== "codex" || message.attachments.length !== 0) return false;
  const match = /^\/goal(?:\s+([\s\S]*))?$/u.exec(message.text.trim());
  return match?.[1]?.trim().toLowerCase() === "resume";
};
