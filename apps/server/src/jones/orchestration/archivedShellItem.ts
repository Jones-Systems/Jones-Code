import type {
  OrchestrationV2ShellStreamItem,
  OrchestrationV2StoredEvent,
  OrchestrationV2ThreadShell,
} from "@t3tools/contracts";

/** Archive materialization retains its committed sequence in the unified shell wire shape. */
export function archivedShellStreamItemFromThreadShell(input: {
  readonly stored: OrchestrationV2StoredEvent;
  readonly shell: OrchestrationV2ThreadShell | null;
}): Extract<
  OrchestrationV2ShellStreamItem,
  { readonly kind: "thread.updated" | "thread.removed" }
> | null {
  if (input.shell !== null && input.shell.archivedAt !== null) {
    return {
      kind: "thread.updated",
      location: "archive",
      sequence: input.stored.sequence,
      thread: input.shell,
    };
  }
  if (
    input.stored.event.type === "thread.unarchived" ||
    (input.stored.event.type === "thread.deleted" && input.stored.event.payload.archivedAt !== null)
  ) {
    return {
      kind: "thread.removed",
      location: "archive",
      sequence: input.stored.sequence,
      threadId: input.stored.event.threadId,
    };
  }
  return null;
}
