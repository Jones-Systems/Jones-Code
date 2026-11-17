import type { Item } from "@t3tools/client-runtime/jones/remoteAdmission/controller";
import { admissionStatus } from "./presentation";
import { requestHref } from "./navigation";

export function requestOutcomeLabel(item: Item) {
  const result = item.launchResult;
  const outcome = result?.outcome ?? item.outcome;
  switch (outcome) {
    case "created": {
      if (result && result.stages.creation !== "confirmed") return "Outcome unknown · do not resend";
      const availability = result?.stages.availability ?? item.availability;
      return availability === "available"
        ? "Created"
        : availability === "deleted"
          ? "Created then deleted"
          : "Creation confirmed · availability unknown";
    }
    case "not_created":
      return "Not created";
    case "refused":
    case "failed":
      return "Creation failed";
    case "unknown":
      return "Outcome unknown · do not resend";
    case "awaiting":
      return "Awaiting confirmation";
    default:
      return item.record.envelope.operation === "t3_thread_launch" && item.record.status === "dispatched"
        ? "Dispatch recorded · creation unconfirmed"
        : admissionStatus(item.record);
  }
}
export const requestOutcomeReason = (item: Item) =>
  item.launchResult?.reasonCode ?? item.reason ?? item.record.reason;

export function recoveryDetailsText(receiver: string, item: Item, baseUrl: string) {
  return [
    "Jones Code recovery details",
    `Receiver environment ID: ${receiver}`,
    `Admission ID: ${item.record.id}`,
    `Binding digest: ${item.record.bindingDigest}`,
    `Requests URL: ${new URL(requestHref(receiver, item.record.id, item.record.bindingDigest), baseUrl).href}`,
    `Observed outcome: ${requestOutcomeLabel(item)}`,
    `Reason: ${requestOutcomeReason(item) ?? "Not reported"}`,
    `Recorded launch result: ${JSON.stringify(item.launchResult ?? null)}`,
    `Original launch result: ${JSON.stringify(item.originalLaunchResult ?? null)}`,
    "Review this exact request in the receiving owner's Requests UI for current evidence and available owner actions.",
    "No original-request inspection locator was returned by this review API.",
    "Reading or copying these details executes nothing: it does not retry, create, adopt a thread, or grant access.",
  ].join("\n");
}
