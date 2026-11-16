import { resolveThreadAttentionPill } from "@t3tools/shared/jones/threadAttention";
import { hasUnseenRunFailure } from "@t3tools/client-runtime/state/thread-settled";
import type {
  resolveThreadStatusPill as upstreamStatusPill,
  ThreadStatusPill,
} from "../../components/Sidebar.logic";

const THREAD_PILL_STYLES = {
  Failed: {
    colorClass: "text-red-600 dark:text-red-300/90",
    dotClass: "bg-red-500 dark:bg-red-300/90",
    pulse: false,
  },
  "Pending Approval": {
    colorClass: "text-amber-600 dark:text-amber-300/90",
    dotClass: "bg-amber-500 dark:bg-amber-300/90",
    pulse: false,
  },
  "Awaiting Input": {
    colorClass: "text-indigo-600 dark:text-indigo-300/90",
    dotClass: "bg-indigo-500 dark:bg-indigo-300/90",
    pulse: false,
  },
  Working: {
    colorClass: "text-sky-600 dark:text-sky-300/80",
    dotClass: "bg-sky-500 dark:bg-sky-300/80",
    pulse: true,
  },
  Connecting: {
    colorClass: "text-sky-600 dark:text-sky-300/80",
    dotClass: "bg-sky-500 dark:bg-sky-300/80",
    pulse: true,
  },
  Waiting: {
    colorClass: "text-sidebar-muted-foreground",
    dotClass: "bg-sidebar-muted-foreground",
    pulse: false,
  },
  "Plan Ready": {
    colorClass: "text-violet-600 dark:text-violet-300/90",
    dotClass: "bg-violet-500 dark:bg-violet-300/90",
    pulse: false,
  },
  Completed: {
    colorClass: "text-emerald-600 dark:text-emerald-300/90",
    dotClass: "bg-emerald-500 dark:bg-emerald-300/90",
    pulse: false,
  },
} as const;

export function resolveNavigationAttentionPill(
  input: Parameters<typeof upstreamStatusPill>[0],
): ThreadStatusPill | null {
  const { thread } = input;
  // Guest requests are an upstream extension to the Jones attention input.
  if (
    (thread.pendingGuestMessageCount ?? 0) > 0 &&
    !thread.hasPendingApprovals &&
    !thread.hasPendingUserInput &&
    thread.pendingSubagentRequest == null
  ) return {
    label: "Guest Request",
    colorClass: "text-guest-foreground",
    dotClass: "bg-guest",
    pulse: false,
  };
  const attention = resolveThreadAttentionPill(input);
  const label =
    attention === "Working" &&
    thread.runtime?.status === "running" &&
    thread.blockedOnThreadWait === true
      ? "Waiting"
      : (attention === null || attention === "Completed") && hasUnseenRunFailure(thread)
        ? "Failed"
        : attention;
  return label === null ? null : { label, ...THREAD_PILL_STYLES[label] };
}
