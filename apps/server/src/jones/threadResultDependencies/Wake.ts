import type { OrchestrationV2Notification } from "@t3tools/contracts";
import type { Dependency } from "./DependencyStore.ts";

export function notification(dependency: Dependency): OrchestrationV2Notification {
  const outcome = dependency.outcome === "fulfilled" ? "completed"
    : dependency.outcome === "declined" || dependency.outcome === "refused" ? "failed"
    : dependency.outcome === "withdrawn" || dependency.outcome === "not_sent" ? "cancelled" : "unknown";
  const summary = `Result from ${dependency.targetLabel.slice(0, 160)}: ${dependency.outcome ?? "unknown"}`;
  const label = dependency.targetEnvironmentId === null ? "Receiving thread result (data)" : `Data from linked environment ${dependency.targetEnvironmentId}`;
  const target = dependency.targetEnvironmentId === null ? `[Receiving thread](t3-thread://v1/${dependency.targetThreadId})` : `Receiving thread ${dependency.targetLabel.slice(0, 160)} (${dependency.targetThreadId})`;
  const detail = `${label}:\n${(dependency.result ?? "No explicit result was recorded.").slice(0, 16000)}\n\nCorrelation: dependency ${dependency.dependencyId}; request ${dependency.requestId ?? "unbound"}; message ${dependency.messageId ?? "unbound"}; target ${target}; environment ${dependency.targetEnvironmentId ?? "local"}.`;
  return { source: { kind: "background_task" }, outcome, summary, detail };
}
