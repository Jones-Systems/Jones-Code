import { shortJonesSource } from "@t3tools/client-runtime/jones/updates";
import type { FleetFollowerStatus } from "@t3tools/contracts/jones/fleet-updates";

const clockTime = (iso: string) =>
  new Date(iso).toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" });

/** One line for a host that prepares and installs this laptop's build by itself. */
export function describeFollower(
  follower: FleetFollowerStatus,
  formatTime: (iso: string) => string = clockTime,
): string {
  const build = follower.leaderSource === null ? "" : ` ${shortJonesSource(follower.leaderSource)}`;
  const reason = follower.reason === undefined ? "" : ` · ${follower.reason}`;
  const after = (label: string) =>
    follower.notBefore === undefined ? "" : ` · ${label} ${formatTime(follower.notBefore)}`;
  switch (follower.state) {
    case "unavailable":
      return `Unavailable${reason}`;
    case "waiting-for-leader":
      return "Waiting for this laptop's installed build";
    case "current":
      return `On this laptop's build${build}`;
    case "preparing":
      return `Preparing this laptop's build${build}`;
    case "prepare-blocked":
      return `Couldn't prepare${build}${reason}${after("retries after")}`;
    case "waiting-for-quiet":
      return `Ready to install${build} when the host is quiet${reason}${after("not before")}`;
    case "activating":
      return `Installing${build}`;
    case "reconciling":
      return `Checking the last update${reason}`;
  }
}
