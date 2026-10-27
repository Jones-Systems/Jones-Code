import { useSyncExternalStore } from "react";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type { ChatMessage } from "../../types";
import { pendingSends } from "./store";

export function usePendingSends() {
  return useSyncExternalStore(pendingSends.subscribe, pendingSends.getSnapshot);
}
export function usePendingMessages(environmentId: EnvironmentId, threadId: ThreadId) {
  const entries = usePendingSends();
  const messages = entries
    .filter(
      (entry) =>
        entry.environmentId === environmentId && entry.threadId === threadId && !entry.visible,
    )
    .map((entry) => entry.message);
  const setMessages = (value: ChatMessage[] | ((previous: ChatMessage[]) => ChatMessage[])) => {
    const current = pendingSends
      .getSnapshot()
      .filter(
        (entry) =>
          entry.environmentId === environmentId && entry.threadId === threadId && !entry.visible,
      )
      .map((entry) => entry.message);
    for (const message of typeof value === "function" ? value(current) : value)
      pendingSends.admit(environmentId, threadId, message);
  };
  return [messages, setMessages] as const;
}
