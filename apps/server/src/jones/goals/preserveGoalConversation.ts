export const nativeConversationMustBePreserved = (
  providerThread: {
    readonly driver: string;
    readonly nativeThreadRef: { readonly nativeId: string | null } | null;
    readonly goal?: unknown;
  },
  message: { readonly text: string; readonly attachments: ReadonlyArray<unknown> },
): boolean => {
  if (providerThread.driver !== "codex") return false;
  // A missing projected goal is not proof that an established native conversation
  // has no goal. Summary fallback cannot preserve provider-owned state.
  if (providerThread.nativeThreadRef !== null || providerThread.goal != null) return true;
  if (message.attachments.length !== 0) return false;
  if (message.text.trim().toLowerCase() === "/compact") return true;
  const match = /^\/goal(?:\s+([\s\S]*))?$/u.exec(message.text.trim());
  return match?.[1]?.trim().toLowerCase() === "resume";
};
