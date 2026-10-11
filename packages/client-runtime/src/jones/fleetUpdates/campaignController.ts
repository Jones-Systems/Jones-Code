import type { EnvironmentId } from "@t3tools/contracts";
import type {
  FleetCampaignMember, FleetDesktopCampaign, FleetDesktopRequest, FleetDesktopState,
  FleetHostOperation, FleetHostStatus, FleetMemberPhase,
} from "@t3tools/contracts/jones/fleet-updates";
import type { FleetHostRequest } from "./hostBridge.ts";

export interface FleetCampaignDriver {
  readonly desktop: (request: FleetDesktopRequest) => Promise<FleetDesktopState>;
  readonly host: (environmentId: EnvironmentId, request: FleetHostRequest) => Promise<FleetHostStatus>;
  readonly cancelled: () => boolean;
}
const finished = new Set<FleetMemberPhase>(["current", "committed", "rolled-back", "blocked", "superseded"]);
const inflight = new Set<FleetMemberPhase>(["dispatching", "pending"]);

function matches(campaign: FleetDesktopCampaign, member: FleetCampaignMember, operation: FleetHostOperation) {
  return operation.input.operationId === member.operationId &&
    operation.input.enrollmentId === member.enrollment.enrollmentId &&
    operation.input.environmentId === member.enrollment.environmentId &&
    operation.input.targetSource === campaign.targetSource &&
    (member.expectedInstalledSource === undefined || operation.input.expectedInstalledSource === member.expectedInstalledSource);
}

async function advanceMember(driver: FleetCampaignDriver, campaign: FleetDesktopCampaign, original: FleetCampaignMember) {
  let member = original;
  const set = async (phase: FleetMemberPhase, reason?: string, source?: string) => {
    if (driver.cancelled()) return false;
    const state = await driver.desktop({ action: "updateMember", input: {
      campaignId: campaign.campaignId, operationId: member.operationId,
      expectedPhase: member.phase, phase,
      ...(reason === undefined ? {} : { reason }),
      ...(source === undefined ? {} : { expectedInstalledSource: source }),
    } });
    member = state.campaigns.find((entry) => entry.campaignId === campaign.campaignId)!
      .members.find((entry) => entry.operationId === member.operationId)!;
    return true;
  };
  try {
    let status = await driver.host(member.enrollment.environmentId, { action: "status", operationId: member.operationId });
    if (driver.cancelled()) return;
    if (status.environmentId !== member.enrollment.environmentId || (status.operation !== null && !matches(campaign, member, status.operation))) {
      await set("blocked", "The host operation identity changed. No update was repeated.");
      return;
    }
    if (status.operation !== null && (finished.has(status.operation.phase) || status.operation.phase === "pending")) {
      await set(status.operation.phase, status.operation.reason, status.operation.input.expectedInstalledSource);
      return;
    }
    if (status.enrollment?.enrollmentId !== member.enrollment.enrollmentId || !status.enrollment.enabled) {
      await set(inflight.has(member.phase) ? member.phase : "bootstrap-required", "This host needs its selected fleet enrollment restored explicitly.");
      return;
    }
    if (status.operation === null || status.operation.phase === "staging" || status.operation.phase === "stage-blocked") {
      if (inflight.has(member.phase)) {
        await set("blocked", "The host lost a previously accepted operation receipt; reconciliation is required.");
        return;
      }
      const source = member.expectedInstalledSource ?? status.update?.installedSource;
      if (status.operationProtocol !== 1 || source === undefined || !status.update?.capability.install || !status.update.capability.download) {
        await set("bootstrap-required", "This host needs source-qualified launcher setup before it can update automatically.");
        return;
      }
      if (!await set("staging", undefined, source)) return;
      status = await driver.host(member.enrollment.environmentId, { action: "stage", input: {
        operationId: member.operationId, enrollmentId: member.enrollment.enrollmentId,
        environmentId: member.enrollment.environmentId, targetSource: campaign.targetSource,
        expectedInstalledSource: source,
      } });
      if (driver.cancelled()) return;
    }
    const operation = status.operation;
    if (operation === null || !matches(campaign, member, operation)) {
      await set("blocked", "The host did not return the selected operation receipt.");
      return;
    }
    if (operation.phase !== "staged" && operation.phase !== "dispatching") {
      await set(operation.phase, operation.reason, operation.input.expectedInstalledSource);
      return;
    }
    if (campaign.phase !== "committed" || campaign.committedGeneration === undefined) {
      await set(operation.phase === "staged" ? "staged" : "blocked",
        operation.phase === "staged" ? "Ready; waiting for the laptop update to commit." : "A host dispatch exists without this laptop's commit proof.");
      return;
    }
    if (!await set("dispatching", undefined, operation.input.expectedInstalledSource)) return;
    status = await driver.host(member.enrollment.environmentId, { action: "activate", input: {
      operationId: member.operationId, enrollmentId: member.enrollment.enrollmentId,
      environmentId: member.enrollment.environmentId,
    } });
    if (driver.cancelled()) return;
    if (status.operation === null || !matches(campaign, member, status.operation)) {
      await set("blocked", "Activation did not return the selected operation receipt.");
    } else {
      await set(status.operation.phase, status.operation.reason);
    }
  } catch {
    // A lost activation response preserves dispatching. The next pass reads the exact
    // host receipt before considering any resend; unrelated hosts keep progressing.
    if (!driver.cancelled()) await set(inflight.has(member.phase) ? member.phase : "offline",
      "Host unavailable or authorization needs attention; this operation will be reconciled after reconnect.").catch(() => undefined);
  }
}

/** One bounded pass; the caller owns polling, cancellation, and native persistence. */
export async function advanceFleetCampaigns(driver: FleetCampaignDriver): Promise<void> {
  const state = await driver.desktop({ action: "read" });
  if (driver.cancelled()) return;
  const byEnvironment = new Map<EnvironmentId, { campaign: FleetDesktopCampaign; member: FleetCampaignMember }>();
  for (const campaign of state.campaigns) {
    for (const member of campaign.members) {
      if (finished.has(member.phase)) continue;
      if ((campaign.phase === "rolled-back" || campaign.phase === "blocked") && !inflight.has(member.phase)) continue;
      const enrolled = state.enrollments.some((entry) => entry.enabled && entry.environmentId === member.enrollment.environmentId && entry.enrollmentId === member.enrollment.enrollmentId);
      if (!enrolled && !inflight.has(member.phase)) continue;
      const previous = byEnvironment.get(member.enrollment.environmentId);
      if (previous === undefined || (!inflight.has(previous.member.phase) && inflight.has(member.phase))) {
        byEnvironment.set(member.enrollment.environmentId, { campaign, member });
      }
    }
  }
  await Promise.all([...byEnvironment.values()].map(({ campaign, member }) => advanceMember(driver, campaign, member)));
}
