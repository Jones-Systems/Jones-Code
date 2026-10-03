import { CommandId, type ContextMenuItem, type ScopedThreadRef, type ThreadId, type OrchestrationV2ThreadRuntimeAttachmentResult, type OrchestrationV2CurrentThreadRuntimeTarget, type OrchestrationV2StopCurrentThreadRuntimeInput, type OrchestrationV2StopCurrentThreadRuntimeResult } from "@t3tools/contracts";
import type { SnoozePreset } from "@t3tools/client-runtime/state/thread-settled";
import { scopedThreadKey } from "@t3tools/client-runtime/environment";
import { captureCurrentThreadRuntimeStopTarget, resolveCurrentThreadRuntimeStop } from "@t3tools/client-runtime/state/thread-continuation";

export function currentRuntimeStopMenuTarget(result: OrchestrationV2ThreadRuntimeAttachmentResult | null, threadId: ThreadId): OrchestrationV2CurrentThreadRuntimeTarget | null {
  return result?.stopCapability?.version === 2 ? captureCurrentThreadRuntimeStopTarget(result, threadId) : null;
}

export function createCurrentRuntimeStopController(options: {
  readonly read: (threadRef: ScopedThreadRef) => OrchestrationV2StopCurrentThreadRuntimeInput | null;
  readonly reserve: (threadRef: ScopedThreadRef, input: OrchestrationV2StopCurrentThreadRuntimeInput) => void;
  readonly clear: (threadRef: ScopedThreadRef, input: OrchestrationV2StopCurrentThreadRuntimeInput) => void;
  readonly stop: (threadRef: ScopedThreadRef, input: OrchestrationV2StopCurrentThreadRuntimeInput) => Promise<OrchestrationV2StopCurrentThreadRuntimeResult>;
  readonly observe: (threadRef: ScopedThreadRef, input: Pick<OrchestrationV2StopCurrentThreadRuntimeInput, "threadId" | "commandId">) => Promise<OrchestrationV2StopCurrentThreadRuntimeResult>;
}) {
  const inFlight = new Map<string, Promise<ReturnType<typeof resolveCurrentThreadRuntimeStop>>>();
  const unsent = new Map<string, OrchestrationV2StopCurrentThreadRuntimeInput>();
  return (threadRef: ScopedThreadRef, target: OrchestrationV2CurrentThreadRuntimeTarget) => {
    const key = scopedThreadKey(threadRef);
    const pending = inFlight.get(key);
    if (pending) return pending;
    const request = Promise.resolve().then(async () => {
      const saved = options.read(threadRef);
      const retrySave = unsent.get(key);
      const knownUnsent = retrySave !== undefined && (saved === null || saved.commandId === retrySave.commandId);
      const input = knownUnsent ? retrySave : saved ?? { commandId: CommandId.make(crypto.randomUUID()), threadId: threadRef.threadId, target };
      if (input.threadId !== threadRef.threadId || input.target.binding.threadId !== threadRef.threadId) {
        return { status: "unknown" as const, commandAccepted: false, queueFenceInstalled: false, reason: "The saved stop belongs to another thread." };
      }
      if (saved === null || knownUnsent) {
        try {
          options.reserve(threadRef, input);
        } catch (error) {
          // Only this live pre-RPC failure permits an explicit save retry of the same ID.
          unsent.set(key, input);
          throw error;
        }
      }
      unsent.delete(key);
      let result;
      try {
        result = saved === null || knownUnsent
          ? await options.stop(threadRef, input)
          : await options.observe(threadRef, { threadId: input.threadId, commandId: input.commandId });
      } catch {
        return { status: "unknown" as const, commandAccepted: false, queueFenceInstalled: false, reason: "The stop response is unavailable. Check the same operation's status." };
      }
      const outcome = resolveCurrentThreadRuntimeStop(result, input);
      if (outcome.status === "stopped" || outcome.status === "rejected") {
        try { options.clear(threadRef, input); }
        catch { return { ...outcome, reason: "The stop result is confirmed, but its saved correlation could not be cleared." }; }
      }
      return outcome;
    }).finally(() => { inFlight.delete(key); });
    inFlight.set(key, request);
    return request;
  };
}

/**
 * Ids for the per-thread action menu. Snooze presets are dispatched as
 * `snooze:<presetId>` so the union stays closed while the preset list
 * remains data-driven.
 */
export type ThreadActionMenuId =
  | "new-thread-on-branch"
  | "filter-by-project"
  | "project-settings"
  | "pin"
  | "unpin"
  | "settle"
  | "unsettle"
  | "kill-thread"
  | "auto-settle"
  | "auto-settle:enabled"
  | "auto-settle:disabled"
  | "snooze"
  | `snooze:${string}`
  | "unsnooze"
  | "rename"
  | "regenerate-title"
  | "mark-unread"
  | "copy"
  | "copy-path"
  | "copy-branch"
  | "copy-thread-id"
  | "archive"
  | "delete";

export interface ThreadActionMenuState {
  readonly branch: string | null;
  /**
   * Project scoping for the thread list. Null on surfaces with no scoped
   * list behind the menu (the chat header), where the item must not show.
   */
  readonly projectFilter: {
    readonly label: string;
    /** True when the list is already scoped to this thread's project. */
    readonly isActive: boolean;
  } | null;
  readonly isPinned: boolean;
  readonly isSettled: boolean;
  /** False while the user has turned automatic settlement off for this thread. */
  readonly autoSettleEnabled: boolean;
  readonly isSnoozed: boolean;
  readonly canSnoozeNow: boolean;
  readonly isRegeneratingTitle: boolean;
  /** Archive rejects a thread with an attached provider, so disable it here rather than let the action fail. */
  readonly isRunning: boolean;
  readonly canStopSession: boolean;
  readonly supports: {
    readonly settlement: boolean;
    /** Server understands thread.auto-settle.set. */
    readonly autoSettleOptOut: boolean;
    readonly snooze: boolean;
    readonly pinning: boolean;
    readonly titleRegeneration: boolean;
  };
  readonly snoozePresets: ReadonlyArray<SnoozePreset>;
}

/**
 * Single source for the per-thread action menu: the sidebar row's right-click
 * menu and the chat header menu share labels, ordering, and capability gating.
 * Each surface supplies state for the actions it supports.
 */
export function buildThreadActionMenuItems(
  state: ThreadActionMenuState,
): ReadonlyArray<ContextMenuItem<ThreadActionMenuId>> {
  return [
    ...(state.branch
      ? [
          {
            id: "new-thread-on-branch" as const,
            label: `New thread on ${state.branch}`,
            icon: "message-square-plus",
          },
        ]
      : []),
    ...(state.supports.pinning
      ? [
          state.isPinned
            ? { id: "unpin" as const, label: "Unpin thread", icon: "pin-off" }
            : { id: "pin" as const, label: "Pin thread", icon: "pin" },
        ]
      : []),
    // Both lifecycle actions stay available on pinned threads: settling
    // clears the pin ("done" beats "keep on top"), and snoozing hides the
    // card until wake with the pin intact.
    ...(state.supports.settlement
      ? [
          state.isSettled
            ? { id: "unsettle" as const, label: "Un-settle thread", icon: "circle-check" }
            : { id: "settle" as const, label: "Settle thread", icon: "circle-check" },
        ]
      : []),
    {
      id: "kill-thread",
      label: "Kill Thread",
      icon: "square",
      disabled: !state.canStopSession,
    },
    ...(state.supports.snooze
      ? [
          state.isSnoozed
            ? { id: "unsnooze" as const, label: "Wake thread", icon: "clock" }
            : {
                id: "snooze" as const,
                label: "Snooze",
                icon: "clock",
                disabled: !state.canSnoozeNow,
                children: [
                  ...state.snoozePresets.map((preset) => ({
                    id: `snooze:${preset.id}` as const,
                    label: `${preset.label} (${preset.whenLabel})`,
                  })),
                  { id: "snooze:custom" as const, label: "Custom…", separatorBefore: true },
                ],
              },
        ]
      : []),
    { id: "rename", label: "Rename thread", icon: "pencil", separatorBefore: true },
    ...(state.supports.titleRegeneration
      ? [
          {
            id: "regenerate-title" as const,
            label: state.isRegeneratingTitle ? "Regenerating…" : "Regenerate title",
            icon: "refresh-cw",
            disabled: state.isRegeneratingTitle,
          },
        ]
      : []),
    { id: "mark-unread", label: "Mark unread", icon: "mail-open" },
    ...(state.projectFilter
      ? [
          {
            id: "filter-by-project" as const,
            label: state.projectFilter.isActive
              ? "Show all projects"
              : `Filter by ${state.projectFilter.label}`,
            icon: "folder-tree",
          },
        ]
      : []),
    // A submenu with the current option checked, not a one-shot action:
    // this is a setting, and it sits with the other per-thread settings
    // rather than the lifecycle verbs above. Disabled keeps long-running
    // threads out of the settled shelf no matter how quiet they get.
    ...(state.supports.autoSettleOptOut
      ? [
          {
            id: "auto-settle" as const,
            label: "Auto-settle behavior",
            icon: "timer",
            children: [
              {
                id: "auto-settle:enabled" as const,
                label: "Enabled",
                checked: state.autoSettleEnabled,
              },
              {
                id: "auto-settle:disabled" as const,
                label: "Disabled",
                checked: !state.autoSettleEnabled,
              },
            ],
          },
        ]
      : []),
    {
      id: "copy",
      label: "Copy",
      icon: "copy",
      separatorBefore: true,
      children: [
        { id: "copy-path", label: "Path", icon: "folder" },
        ...(state.branch
          ? [{ id: "copy-branch" as const, label: "Branch", icon: "git-branch" }]
          : []),
        { id: "copy-thread-id", label: "Thread ID", icon: "hash" },
      ],
    },
    { id: "project-settings", label: "Project settings", icon: "settings" },
    // Archive removes the thread from the sidebar while keeping its
    // conversation under Settings > Archived threads — distinct from Settle
    // (stays visible in the Settled shelf) and Delete (clears history for
    // good), so it sits beside Delete without borrowing its destructive
    // styling.
    {
      id: "archive",
      label: "Archive thread",
      icon: "archive",
      disabled: state.isRunning,
      separatorBefore: true,
    },
    {
      id: "delete",
      label: "Delete",
      destructive: true,
      icon: "trash",
    },
  ];
}
