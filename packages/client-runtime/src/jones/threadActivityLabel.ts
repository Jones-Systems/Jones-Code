import type {
  OrchestrationV2RunStatus,
  OrchestrationV2TurnItem,
  ProviderTurnId,
  RunId,
} from "@t3tools/contracts";

export function hasThreadProviderActivity(
  items: ReadonlyArray<
    Pick<OrchestrationV2TurnItem, "runId" | "providerTurnId" | "nativeItemRef" | "type">
  >,
  runId: RunId,
  providerTurnId: ProviderTurnId | null,
): boolean {
  if (providerTurnId === null) return false;
  return (
    items.findLast(
      (item) =>
        item.runId === runId &&
        item.providerTurnId === providerTurnId &&
        item.nativeItemRef !== null &&
        item.type !== "user_message" &&
        item.type !== "system_notice" &&
        item.type !== "notification" &&
        item.type !== "error",
    ) !== undefined
  );
}

export function threadActivityLabel(
  status: OrchestrationV2RunStatus | null | undefined,
  runlessWorkActive = false,
  providerActivityStarted = false,
): "Connecting" | "Starting" | "Thinking" {
  if (status === "preparing" || status === "queued" || status === "starting") return "Starting";
  if (
    (status === "running" || status === "waiting") &&
    !providerActivityStarted &&
    !runlessWorkActive
  )
    return "Starting";
  if (status === "running" || status === "waiting" || runlessWorkActive) return "Thinking";
  return "Connecting";
}
