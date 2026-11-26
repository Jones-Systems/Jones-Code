import { shortJonesSource } from "@t3tools/client-runtime/jones/updates";
import type {
  FleetPublication,
  FleetPublicationReceipt,
} from "@t3tools/client-runtime/jones/fleet-updates";
import type { FleetEnrollment, FleetHostStatus } from "@t3tools/contracts/jones/fleet-updates";

/** A late read from a retired enrollment cannot replace the selected host's report. */
export function selectPublicationReceipt(
  enrollment: FleetEnrollment | undefined,
  reported: ReadonlyArray<FleetPublicationReceipt> | undefined,
  local: {
    readonly status: FleetHostStatus | null;
    readonly observedAt: string | undefined;
    readonly unavailable: boolean;
  },
): FleetPublicationReceipt | undefined {
  if (enrollment?.enabled !== true) return undefined;
  const receipt = reported?.find(
    (host) =>
      host.enrollment.environmentId === enrollment.environmentId &&
      host.enrollment.enrollmentId === enrollment.enrollmentId &&
      host.enrollment.continueRunningThreads === enrollment.continueRunningThreads,
  );
  if (
    local.observedAt === undefined ||
    (receipt !== undefined && receipt.observedAt >= local.observedAt)
  )
    return receipt;
  const status = local.status;
  const bound =
    status !== null &&
    status.environmentId === enrollment.environmentId &&
    status.enrollment?.enabled === true &&
    status.enrollment.enrollmentId === enrollment.enrollmentId &&
    status.enrollment.continueRunningThreads === enrollment.continueRunningThreads;
  const follower = bound ? status.follower : undefined;
  const earlierFollower = receipt?.follower;
  const acknowledged =
    !local.unavailable &&
    follower !== undefined &&
    earlierFollower !== undefined &&
    receipt?.publicationAcknowledged === true &&
    follower.leaderSource === earlierFollower.leaderSource &&
    follower.leaderPublishedAt === earlierFollower.leaderPublishedAt;
  return {
    enrollment,
    observedAt: local.observedAt,
    outcome:
      local.unavailable || !bound
        ? "unknown"
        : follower?.planDirective !== undefined
          ? "directive-held"
          : follower === undefined
            ? "legacy"
            : acknowledged
              ? "following"
              : "observed",
    ...(local.unavailable || !bound
      ? {
          failure: local.unavailable ? "unreachable" : "unconfirmed",
          ...(earlierFollower === undefined ? {} : { follower: earlierFollower }),
        }
      : follower === undefined
        ? {}
        : { follower }),
    ...(acknowledged ? { publicationAcknowledged: true } : {}),
  };
}

export function formatFleetTime(iso: string): string {
  const date = new Date(iso);
  return Number.isNaN(date.getTime())
    ? "time unknown"
    : date.toLocaleString("en-US", {
        timeZone: "America/New_York",
        year: "numeric",
        month: "short",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
        timeZoneName: "short",
      });
}

export function describePublication(
  publication: FleetPublication | undefined,
  runningSource?: string,
): string {
  const running =
    runningSource === undefined
      ? "Laptop running build verification is unavailable."
      : `Verified running laptop build ${shortJonesSource(runningSource)}.`;
  if (publication === undefined) return `Laptop publication status unknown. ${running}`;
  if (publication.kind !== "withheld") {
    const published = shortJonesSource(publication.source.sha);
    return runningSource === publication.source.sha
      ? `${running} Publication authorized for ${published}.`
      : `${running} Publication was last authorized for ${published}.`;
  }
  const reason = {
    "plan-managed": "update plan",
    unproved: "native proof missing",
    blocked: "native verification blocked",
    unavailable: "native proof unavailable",
  }[publication.code];
  return `Automatic publication withheld (${reason}). ${running}${
    publication.code === "unproved" ? " Open Install status to check laptop verification." : ""
  }`;
}

export function describePublicationReceipt(
  label: string,
  receipt: FleetPublicationReceipt | undefined,
  runningSource?: string,
  formatTime: (iso: string) => string = formatFleetTime,
): string {
  if (receipt === undefined) return `${label}: host report unknown; not reached yet.`;
  const follower = receipt.follower;
  const reported =
    follower?.leaderSource == null
      ? "last reported laptop build unknown"
      : `last reported laptop build ${shortJonesSource(follower.leaderSource)}, published ${
          follower.leaderPublishedAt === undefined
            ? "time unknown"
            : formatTime(follower.leaderPublishedAt)
        }`;
  const outcome =
    receipt.outcome === "unknown"
      ? receipt.failure === "unreachable"
        ? "host status could not be read"
        : "latest host acknowledgement could not be confirmed"
      : {
          following: "publication acknowledged",
          "directive-held": "manual review directive retained",
          legacy: "host uses laptop campaigns",
          observed: "host status observed",
        }[receipt.outcome];
  const mismatch =
    receipt.outcome !== "unknown" &&
    runningSource !== undefined &&
    follower?.leaderSource != null &&
    follower.leaderSource !== runningSource
      ? " · report is stale"
      : "";
  const acknowledged =
    receipt.publicationAcknowledged === true && receipt.outcome === "directive-held"
      ? " · publication acknowledged"
      : "";
  return `${label}: ${reported}${mismatch}${acknowledged} · ${outcome}.`;
}
