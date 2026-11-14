import type { OrchestrationV2RunStatus } from "@t3tools/contracts";

export function threadActivityLabel(
  status: OrchestrationV2RunStatus | null | undefined,
  runlessWorkActive = false,
): "Connecting" | "Starting" | "Thinking" {
  if (status === "preparing" || status === "queued" || status === "starting") return "Starting";
  if (status === "running" || status === "waiting" || runlessWorkActive) return "Thinking";
  return "Connecting";
}
