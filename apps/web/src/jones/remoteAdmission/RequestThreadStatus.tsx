import { useEnvironments } from "../../state/environments";
import { ThreadId, type EnvironmentId } from "@t3tools/contracts";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { formatWorkingDurationLabel } from "@t3tools/client-runtime/state/models";
import { useThreadShell } from "../../state/entities";
import { resolveThreadStatusPill, resolveWorkingStartedAt } from "../../components/Sidebar.logic";
import { ThreadStatusLabel } from "../../components/ThreadStatusIndicators";
import { useNowSecond } from "../../hooks/useNowSecond";

export function RequestThreadStatus({ environmentId, threadId }: { environmentId: EnvironmentId; threadId: string | null }) {
  const shell = useThreadShell(threadId ? scopeThreadRef(environmentId, ThreadId.make(threadId)) : null);
  const now = useNowSecond();
  if (!shell) return null;
  const status = resolveThreadStatusPill({ thread: shell });
  const started = resolveWorkingStartedAt(shell);
  return <span className="ml-auto inline-flex items-center justify-end gap-1 text-right text-xs">{status ? <ThreadStatusLabel status={status} /> : null}{status?.label === "Working" && started && Number.isFinite(Date.parse(started)) ? <span className="tabular-nums">{formatWorkingDurationLabel(now - Date.parse(started))}</span> : null}</span>;
}

export function RequestThreadTitle({ environmentId, threadId, fallback }: { environmentId: EnvironmentId; threadId: string | null; fallback: string }) {
  const shell = useThreadShell(threadId ? scopeThreadRef(environmentId, ThreadId.make(threadId)) : null);
  return <>{shell?.title || fallback}</>;
}
export function RequestEnvironmentLabel({ environmentId }: { environmentId: string }) {
  const { environments } = useEnvironments();
  return <>{environments.find((entry) => entry.environmentId === environmentId)?.displayLabel ?? "Linked environment"}</>;
}
export function AccessExpiry({ expiresAt }: { expiresAt: string }) {
  const timestamp = Date.parse(expiresAt);
  return <span>{Number.isFinite(timestamp) ? `Access until ${new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short" }).format(timestamp)}` : "Access expiry unavailable"}</span>;
}
