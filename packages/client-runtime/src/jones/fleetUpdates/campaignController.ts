import type { EnvironmentId } from "@t3tools/contracts";
import type {
  FleetCampaignMember,
  FleetDesktopCampaign,
  FleetDesktopRequest,
  FleetDesktopState,
  FleetEnrollment,
  FleetFollowerStatus,
  FleetHostOperation,
  FleetHostStatus,
  FleetMemberPhase,
} from "@t3tools/contracts/jones/fleet-updates";
import type { JonesInstallPlanResult } from "@t3tools/contracts/jones/installPlan";
import type { FleetHostRequest } from "./hostBridge.ts";

export interface FleetCampaignDriver {
  readonly desktop: (request: FleetDesktopRequest) => Promise<FleetDesktopState>;
  readonly host: (
    environmentId: EnvironmentId,
    request: FleetHostRequest,
  ) => Promise<FleetHostStatus>;
  readonly cancelled: () => boolean;
  /**
   * Compatibility publication grant for drivers without the full native publication reader.
   * Installed-source label evidence must never supply this callback.
   */
  readonly committedSource?: () => Promise<FleetCommittedSource | undefined>;
  readonly publication?: () => Promise<FleetPublication>;
  /** Diagnostics only; a callback failure never changes dispatch decisions. */
  readonly onPublication?: (report: FleetPublicationReport) => void | Promise<void>;
}

export type FleetPublication = NonNullable<JonesInstallPlanResult["publication"]>;
export interface FleetPublicationReceipt {
  readonly enrollment: FleetEnrollment;
  readonly outcome: "following" | "directive-held" | "unknown" | "legacy" | "observed";
  readonly observedAt: string;
  readonly follower?: FleetFollowerStatus;
}
export interface FleetPublicationReport {
  readonly publication: FleetPublication;
  readonly hosts: ReadonlyArray<FleetPublicationReceipt>;
}

export interface FleetCommittedSource {
  readonly repository: string;
  readonly sha: string;
  readonly tree: string;
}
const finished = new Set<FleetMemberPhase>([
  "current",
  "committed",
  "rolled-back",
  "blocked",
  "superseded",
]);
const inflight = new Set<FleetMemberPhase>([
  "dispatching",
  "install-blocked",
  "reconciling",
  "pending",
]);
const occupied = new Set<FleetMemberPhase>([...inflight, "retiring"]);
const activationOccupied = new Set<FleetMemberPhase>(["dispatching", "reconciling", "pending"]);
/** Phases a following host's campaign member keeps until its accepted work settles. */
const handOffHeld = new Set<FleetMemberPhase>([...activationOccupied, "retiring"]);

function matches(
  campaign: FleetDesktopCampaign,
  member: FleetCampaignMember,
  operation: FleetHostOperation,
) {
  return (
    operation.input.operationId === member.operationId &&
    operation.input.enrollmentId === member.enrollment.enrollmentId &&
    operation.input.environmentId === member.enrollment.environmentId &&
    operation.input.targetSource === campaign.targetSource &&
    (member.expectedInstalledSource === undefined ||
      operation.input.expectedInstalledSource === member.expectedInstalledSource)
  );
}

async function advanceMember(
  driver: FleetCampaignDriver,
  campaign: FleetDesktopCampaign,
  original: FleetCampaignMember,
  observeOnly = false,
) {
  if (driver.cancelled()) return;
  let member = original;
  const mayDispatch = async () => {
    const current = await driver.desktop({ action: "read" });
    return (
      !driver.cancelled() &&
      current.enrollments.some((entry) => sameEnrollment(member.enrollment, entry))
    );
  };
  const set = async (phase: FleetMemberPhase, reason?: string, source?: string) => {
    if (driver.cancelled()) return false;
    const state = await driver.desktop({
      action: "updateMember",
      input: {
        campaignId: campaign.campaignId,
        operationId: member.operationId,
        expectedPhase: member.phase,
        phase,
        ...(reason === undefined ? {} : { reason }),
        ...(source === undefined ? {} : { expectedInstalledSource: source }),
      },
    });
    if (driver.cancelled()) return false;
    const next = state.campaigns
      .find((entry) => entry.campaignId === campaign.campaignId)
      ?.members.find((entry) => entry.operationId === member.operationId);
    if (next === undefined) return false;
    member = next;
    return (
      member.phase === phase && (source === undefined || member.expectedInstalledSource === source)
    );
  };
  try {
    let status = await driver.host(member.enrollment.environmentId, {
      action: "status",
      operationId: member.operationId,
    });
    if (driver.cancelled()) return;
    if (
      status.environmentId !== member.enrollment.environmentId ||
      (status.operation !== null && !matches(campaign, member, status.operation))
    ) {
      await set(
        activationOccupied.has(member.phase) ? "reconciling" : "blocked",
        "The host operation identity changed. No update was repeated.",
      );
      return;
    }
    if (observeOnly || status.follower?.planDirective !== undefined) {
      // Accepted work still settles under a publication/directive hold. No stage,
      // activation or retirement request can be inferred from a receipt alone.
      if (
        status.operation !== null &&
        (finished.has(status.operation.phase) ||
          ["pending", "reconciling"].includes(status.operation.phase))
      )
        await set(status.operation.phase, status.operation.reason);
      else if (activationOccupied.has(member.phase) && status.operation === null)
        await set("reconciling", "The host lost the in-flight receipt; activation remains held.");
      return;
    }
    if (member.phase === "retiring") {
      const source =
        member.expectedInstalledSource ?? status.operation?.input.expectedInstalledSource;
      if (source === undefined) {
        await set("superseded");
        return;
      }
      status = await driver.host(member.enrollment.environmentId, {
        action: "retire",
        input: {
          operationId: member.operationId,
          enrollmentId: member.enrollment.enrollmentId,
          environmentId: member.enrollment.environmentId,
          targetSource: campaign.targetSource,
          expectedInstalledSource: source,
        },
      });
      if (driver.cancelled()) return;
      if (status.operation === null || !matches(campaign, member, status.operation)) {
        await set(
          "retiring",
          "The exact retirement receipt could not be confirmed; the stage was preserved.",
        );
      } else if (status.operation.phase === "superseded" || finished.has(status.operation.phase)) {
        await set(status.operation.phase, status.operation.reason);
      } else if (
        ["reconciling", "dispatching", "install-blocked", "pending"].includes(
          status.operation.phase,
        )
      ) {
        await set(
          status.operation.phase === "dispatching" ? "reconciling" : status.operation.phase,
          status.operation.reason,
        );
      } else {
        await set(
          "retiring",
          status.operation.reason ?? "Waiting to retire the exact unaccepted host stage.",
        );
      }
      return;
    }
    if (
      status.operation !== null &&
      (finished.has(status.operation.phase) ||
        ["pending", "reconciling"].includes(status.operation.phase))
    ) {
      await set(
        status.operation.phase,
        status.operation.reason,
        status.operation.input.expectedInstalledSource,
      );
      return;
    }
    if (activationOccupied.has(member.phase)) {
      // A prior status response can still describe a preacceptance refusal while
      // this grant's activation request is in transit. Only its caller's response
      // or an authoritative terminal/pending receipt can settle the grant.
      if (status.operation === null)
        await set("reconciling", "The host lost the in-flight receipt; activation remains held.");
      return;
    }
    if (status.operation?.phase === "dispatching") {
      await set("reconciling", "An existing host dispatch still requires its exact outcome.");
      return;
    }
    if (
      status.enrollment?.enrollmentId !== member.enrollment.enrollmentId ||
      !status.enrollment.enabled
    ) {
      await set(
        inflight.has(member.phase) ? member.phase : "bootstrap-required",
        "This host needs its selected fleet enrollment restored explicitly.",
      );
      return;
    }
    if (
      status.operation === null ||
      status.operation.phase === "staging" ||
      status.operation.phase === "stage-blocked"
    ) {
      if (inflight.has(member.phase)) {
        await set(
          "blocked",
          "The host lost a previously accepted operation receipt; reconciliation is required.",
        );
        return;
      }
      const source = member.expectedInstalledSource ?? status.update?.installedSource?.sha;
      if (
        status.operationProtocol !== 1 ||
        source === undefined ||
        !status.update?.capability.install ||
        !status.update.capability.download
      ) {
        await set(
          "bootstrap-required",
          "This host needs source-qualified launcher setup before it can update automatically.",
        );
        return;
      }
      if (campaign.phase !== "committed" || campaign.committedGeneration === undefined) {
        await set("waiting", "Waits for the laptop update to commit.");
        return;
      }
      if (!(await set("staging", undefined, source))) return;
      if (!(await mayDispatch())) return;
      status = await driver.host(member.enrollment.environmentId, {
        action: "stage",
        input: {
          operationId: member.operationId,
          enrollmentId: member.enrollment.enrollmentId,
          environmentId: member.enrollment.environmentId,
          targetSource: campaign.targetSource,
          expectedInstalledSource: source,
        },
      });
      if (driver.cancelled()) return;
    }
    if (!boundStatus(status, member.enrollment) || status.follower?.planDirective !== undefined)
      return;
    const operation = status.operation;
    if (operation === null || !matches(campaign, member, operation)) {
      await set("blocked", "The host did not return the selected operation receipt.");
      return;
    }
    if (operation.phase === "dispatching") {
      await set("reconciling", "An existing host dispatch still requires its exact outcome.");
      return;
    }
    if (operation.phase !== "staged" && operation.phase !== "install-blocked") {
      await set(operation.phase, operation.reason, operation.input.expectedInstalledSource);
      return;
    }
    if (campaign.phase !== "committed" || campaign.committedGeneration === undefined) {
      await set(
        operation.phase === "staged" ? "staged" : "blocked",
        operation.phase === "staged"
          ? "Ready; waiting for the laptop update to commit."
          : "A host dispatch exists without this laptop's commit proof.",
      );
      return;
    }
    if (operation.phase === "staged") {
      if (
        !(await set(
          "staged",
          "Ready; waiting for the fleet activation slot.",
          operation.input.expectedInstalledSource,
        ))
      )
        return;
    }
    if (!(await mayDispatch())) return;
    if (!(await set("dispatching", undefined, operation.input.expectedInstalledSource))) return;
    status = await driver.host(member.enrollment.environmentId, {
      action: "activate",
      input: {
        operationId: member.operationId,
        enrollmentId: member.enrollment.enrollmentId,
        environmentId: member.enrollment.environmentId,
      },
    });
    if (driver.cancelled()) return;
    if (status.operation === null || !matches(campaign, member, status.operation)) {
      await set("reconciling", "Activation did not return the selected operation receipt.");
    } else {
      await set(
        status.operation.phase === "dispatching" ? "reconciling" : status.operation.phase,
        status.operation.reason,
      );
    }
  } catch {
    // A lost activation response preserves the outstanding grant. Later passes
    // observe its receipt; unrelated hosts can still stage and report status.
    if (!driver.cancelled())
      await set(
        occupied.has(member.phase) ? member.phase : "offline",
        "Host unavailable or authorization needs attention; this operation will be reconciled after reconnect.",
      ).catch(() => undefined);
  }
}

const unavailablePublication: FleetPublication = {
  kind: "withheld",
  code: "unavailable",
  message: "Native publication proof is unavailable.",
};

async function readPublication(driver: FleetCampaignDriver): Promise<FleetPublication> {
  try {
    if (driver.publication !== undefined) {
      const publication = await driver.publication();
      return publication.kind === "withheld" || publicationSource(publication) !== undefined
        ? publication
        : unavailablePublication;
    }
    const source = await driver.committedSource?.();
    return source === undefined ||
      source.repository !== "Jones-Systems/Jones-Code" ||
      !/^[a-f0-9]{40}$/.test(source.sha) ||
      !/^[a-f0-9]{40}$/.test(source.tree)
      ? unavailablePublication
      : { kind: "committed", source: { ...source, repository: "Jones-Systems/Jones-Code" } };
  } catch {
    return unavailablePublication;
  }
}

function sameEnrollment(left: FleetEnrollment, right: FleetEnrollment | null | undefined) {
  return (
    right !== null &&
    right !== undefined &&
    right.enabled &&
    left.environmentId === right.environmentId &&
    left.enrollmentId === right.enrollmentId &&
    left.continueRunningThreads === right.continueRunningThreads
  );
}

function boundStatus(status: FleetHostStatus, enrollment: FleetEnrollment) {
  return (
    status.environmentId === enrollment.environmentId &&
    sameEnrollment(enrollment, status.enrollment)
  );
}

function publicationSource(publication: FleetPublication) {
  if (publication.kind === "withheld") return undefined;
  const source = publication.source;
  return source.repository === "Jones-Systems/Jones-Code" &&
    /^[a-f0-9]{40}$/.test(source.sha) &&
    /^[a-f0-9]{40}$/.test(source.tree)
    ? source
    : undefined;
}

/** Observe receipts even without a native publication grant. Unknown effects hold dispatch. */
async function publishCommittedSource(driver: FleetCampaignDriver, state: FleetDesktopState) {
  const followers = new Set<EnvironmentId>();
  const held = new Set<EnvironmentId>();
  const publication = await readPublication(driver);
  if (driver.cancelled()) return { state, followers, held };
  const committed = publicationSource(publication);
  const hosts = await Promise.all(state.enrollments.filter((entry) => entry.enabled).map(
    async (enrollment): Promise<FleetPublicationReceipt> => {
      let follower: FleetFollowerStatus | undefined;
      let outcome: FleetPublicationReceipt["outcome"] = "unknown";
      if (driver.cancelled()) return { enrollment, outcome, observedAt: new Date().toISOString() };
      try {
        let status = await driver.host(enrollment.environmentId, { action: "status" });
        if (driver.cancelled()) return { enrollment, outcome, observedAt: new Date().toISOString() };
        if (boundStatus(status, enrollment)) {
          follower = status.follower?.protocol === 1 ? status.follower : undefined;
          if (follower?.planDirective !== undefined) outcome = "directive-held";
          else if (status.follower === undefined) outcome = "legacy";
          else if (follower !== undefined) {
            outcome = "observed";
            if (committed !== undefined) {
              if (follower.leaderSource !== committed.sha) {
                // Re-read local enrollment before sending a publication. The response is
                // not authority until its environment, enrollment and source all match.
                const current = await driver.desktop({ action: "read" });
                if (driver.cancelled()) return { enrollment, outcome: "unknown", observedAt: new Date().toISOString() };
                if (!current.enrollments.some((entry) => sameEnrollment(enrollment, entry)))
                  return { enrollment, outcome: "unknown", observedAt: new Date().toISOString() };
                const latestSource = publicationSource(await readPublication(driver));
                if (driver.cancelled() || latestSource?.sha !== committed.sha || latestSource?.tree !== committed.tree)
                  return { enrollment, outcome: "unknown", observedAt: new Date().toISOString() };
                outcome = "unknown";
                status = await driver.host(enrollment.environmentId, {
                  action: "leader",
                  input: {
                    enrollmentId: enrollment.enrollmentId,
                    environmentId: enrollment.environmentId,
                    repository: "Jones-Systems/Jones-Code",
                    committedSource: committed.sha,
                    committedTree: committed.tree,
                  },
                });
                if (driver.cancelled()) return { enrollment, outcome, observedAt: new Date().toISOString() };
                // Do not display an invalid acknowledgement as a host receipt.
                follower = boundStatus(status, enrollment) && status.follower?.protocol === 1
                  ? status.follower : undefined;
              }
              if (follower?.planDirective !== undefined) outcome = "directive-held";
              else if (follower?.leaderSource === committed.sha) outcome = "following";
              else outcome = "unknown";
            }
          }
        }
      } catch {
        // A leader request may have committed before its response was lost. Only
        // a subsequent authoritative status read can release this pass's hold.
        outcome = "unknown";
      }
      return {
        enrollment, outcome, observedAt: new Date().toISOString(),
        ...(follower === undefined ? {} : { follower }),
      };
    },
  ));
  if (driver.cancelled()) return { state, followers, held };
  const current = await driver.desktop({ action: "read" });
  if (driver.cancelled()) return { state: current, followers, held };
  const latestPublication = await readPublication(driver);
  if (driver.cancelled()) return { state: current, followers, held };
  const latestSource = publicationSource(latestPublication);
  const sourceChanged = committed?.sha !== latestSource?.sha || committed?.tree !== latestSource?.tree;
  const receipts = hosts.filter((host) => current.enrollments.some((entry) => sameEnrollment(host.enrollment, entry)));
  for (const host of hosts) {
    const stillEnrolled = receipts.includes(host);
    if (!stillEnrolled || sourceChanged || host.outcome === "unknown" || host.outcome === "directive-held")
      held.add(host.enrollment.environmentId);
    else if (host.outcome === "following") followers.add(host.enrollment.environmentId);
  }
  try {
    await driver.onPublication?.({ publication: latestPublication, hosts: sourceChanged ? [] : receipts });
  } catch {
    // Diagnostics cannot grant, revoke or interrupt update authority.
  }
  return { state: current, followers, held };
}

const passes = new WeakMap<FleetCampaignDriver["desktop"], number>();
/** One bounded pass; the caller owns polling, cancellation, and native persistence. */
export async function advanceFleetCampaigns(input: FleetCampaignDriver): Promise<void> {
  const pass = (passes.get(input.desktop) ?? 0) + 1;
  passes.set(input.desktop, pass);
  const driver: FleetCampaignDriver = {
    ...input,
    cancelled: () => input.cancelled() || passes.get(input.desktop) !== pass,
  };
  const initial = await driver.desktop({ action: "read" });
  if (driver.cancelled()) return;
  const { state, followers, held } = await publishCommittedSource(driver, initial);
  if (driver.cancelled()) return;
  const handOff: Array<{ campaign: FleetDesktopCampaign; member: FleetCampaignMember }> = [];
  const byEnvironment = new Map<
    EnvironmentId,
    { campaign: FleetDesktopCampaign; member: FleetCampaignMember }
  >();
  for (const campaign of state.campaigns) {
    for (const member of campaign.members) {
      if (finished.has(member.phase)) continue;
      if (
        followers.has(member.enrollment.environmentId) &&
        state.enrollments.some((entry) => sameEnrollment(member.enrollment, entry)) &&
        !handOffHeld.has(member.phase)
      ) {
        handOff.push({ campaign, member });
        continue;
      }
      if (
        (campaign.phase === "rolled-back" || campaign.phase === "blocked") &&
        !occupied.has(member.phase)
      )
        continue;
      const enrolled = state.enrollments.some(
        (entry) =>
          entry.enabled &&
          entry.environmentId === member.enrollment.environmentId &&
          entry.enrollmentId === member.enrollment.enrollmentId,
      );
      if (!enrolled && !occupied.has(member.phase)) continue;
      const previous = byEnvironment.get(member.enrollment.environmentId);
      if (
        previous === undefined ||
        (!occupied.has(previous.member.phase) && occupied.has(member.phase))
      ) {
        byEnvironment.set(member.enrollment.environmentId, { campaign, member });
      }
    }
  }
  for (const { campaign, member } of handOff) {
    if (driver.cancelled()) return;
    const current = await driver.desktop({ action: "read" });
    if (driver.cancelled()) return;
    if (!current.enrollments.some((entry) => sameEnrollment(member.enrollment, entry))) continue;
    await driver
      .desktop({
        action: "updateMember",
        input: {
          campaignId: campaign.campaignId,
          operationId: member.operationId,
          expectedPhase: member.phase,
          phase: "superseded",
          reason: "This host now installs the laptop's committed build by itself.",
        },
      })
      .catch(() => undefined);
  }
  const selected = [...byEnvironment.values()];
  // A host with a proven preacceptance refusal retries after other ready hosts
  // have had a chance to claim the durable slot, so it cannot starve the fleet.
  await Promise.all(
    selected
      .filter(({ member }) => member.phase !== "install-blocked")
      .map(({ campaign, member }) =>
        advanceMember(driver, campaign, member, held.has(member.enrollment.environmentId)),
      ),
  );
  await Promise.all(
    selected
      .filter(({ member }) => member.phase === "install-blocked")
      .map(({ campaign, member }) =>
        advanceMember(driver, campaign, member, held.has(member.enrollment.environmentId)),
      ),
  );
}
