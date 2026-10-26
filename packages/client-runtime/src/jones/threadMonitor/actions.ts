import type {
  ThreadMonitorAction,
  ThreadMonitorDraft,
  ThreadMonitorLifetime,
} from "@t3tools/contracts/jones/desktopThreadMonitor";
import { THREAD_MONITOR_LIMITS } from "@t3tools/contracts/jones/desktopThreadMonitor";

export interface MonitorActionContext {
  readonly acceptsLifetime: (ref: ThreadMonitorLifetime) => boolean;
  readonly threadExists: boolean;
  readonly transportConnected: boolean;
  readonly snapshotLive: boolean;
  readonly canOperate: boolean;
  readonly canQueue: boolean;
  readonly workOrRequestActive: boolean;
}
export function monitorActionUnavailableReason(
  action: ThreadMonitorAction,
  context: MonitorActionContext,
): string | null {
  if (!context.acceptsLifetime(action)) return "This environment lifetime has ended";
  if (!context.threadExists) return "Thread is no longer available";
  if (action.type === "open") return null;
  if (!context.transportConnected || !context.snapshotLive)
    return "Waiting for current environment data";
  if (!context.canOperate) return "Operation permission required";
  if (action.type === "send") {
    if (action.text.trim().length === 0) return "Write a reply first";
    if (action.text.length > THREAD_MONITOR_LIMITS.draftCodeUnits)
      return "Reply exceeds the text limit";
    if (context.workOrRequestActive && !context.canQueue)
      return "Server cannot safely queue this reply";
  }
  return null;
}
export function monitorReplyDispatchMode(_workOrRequestActive: boolean): "queue" | "auto" {
  // Queue also starts normally when idle and never negotiates steering or restart.
  return "queue";
}
export function clearAcceptedMonitorDraft(
  current: ThreadMonitorDraft,
  submittedRevision: number,
): ThreadMonitorDraft {
  return current.revision === submittedRevision
    ? { ...current, revision: current.revision + 1, text: "", answers: {} }
    : current;
}
export type MonitorActionPhase = "pending" | "accepted" | "rejected" | "unknown";
export class MonitorActionLedger {
  private readonly actions = new Map<
    string,
    { readonly commandId: string; readonly lifetimeKey: string; readonly phase: MonitorActionPhase }
  >();
  admit(action: ThreadMonitorAction): "new" | "duplicate" | "conflict" | "full" {
    const key = JSON.stringify([
      action.publisherGeneration,
      action.environmentId,
      action.environmentEpoch,
    ]);
    const prior = this.actions.get(action.actionId);
    if (prior !== undefined)
      return prior.commandId === action.commandId && prior.lifetimeKey === key
        ? "duplicate"
        : "conflict";
    // Never evict unknown identities and accidentally permit a new mutation.
    if (this.actions.size >= THREAD_MONITOR_LIMITS.retiredIds) return "full";
    this.actions.set(action.actionId, {
      commandId: action.commandId,
      lifetimeKey: key,
      phase: "pending",
    });
    return "new";
  }
  transition(actionId: string, phase: MonitorActionPhase): boolean {
    const prior = this.actions.get(actionId);
    if (prior === undefined || prior.phase === "accepted" || prior.phase === "rejected")
      return false;
    this.actions.set(actionId, { ...prior, phase });
    return true;
  }
  phase(actionId: string): MonitorActionPhase | undefined {
    return this.actions.get(actionId)?.phase;
  }
}
