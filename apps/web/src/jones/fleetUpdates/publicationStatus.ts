import { shortJonesSource } from "@t3tools/client-runtime/jones/updates";
import type {
  FleetCommittedSource,
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
            : "observed",
    ...(follower === undefined ? {} : { follower }),
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
  installedSource?: FleetCommittedSource | null,
): string {
  const installed =
    installedSource == null
      ? ""
      : ` Laptop last observed installed build: ${shortJonesSource(installedSource.sha)}.`;
  if (publication === undefined) return `Laptop publication status unknown.${installed}`;
  if (publication.kind !== "withheld")
    return `Laptop publication authorized for ${shortJonesSource(publication.source.sha)}.${installed}`;
  const reason = {
    "plan-managed": "update plan",
    unproved: "native proof missing",
    blocked: "native verification blocked",
    unavailable: "native proof unavailable",
  }[publication.code];
  return `Automatic publication withheld (${reason}).${installed}`;
}

export function describePublicationReceipt(
  label: string,
  receipt: FleetPublicationReceipt | undefined,
  formatTime: (iso: string) => string = formatFleetTime,
): string {
  if (receipt === undefined) return `${label}: host report unknown; not reached yet.`;
  const follower = receipt.follower;
  const reported =
    follower?.leaderSource == null
      ? "last reported laptop build unknown"
      : `last reported laptop build ${shortJonesSource(follower.leaderSource)} at ${
          follower.leaderPublishedAt === undefined
            ? "time unknown"
            : formatTime(follower.leaderPublishedAt)
        }`;
  const outcome = {
    following: "publication acknowledged",
    "directive-held": "update plan holds automatic following",
    unknown: "latest acknowledgement unknown; host not reached or response not confirmed",
    legacy: "host uses laptop campaigns",
    observed: "host status observed",
  }[receipt.outcome];
  return `${label}: ${reported} · ${outcome}.`;
}
