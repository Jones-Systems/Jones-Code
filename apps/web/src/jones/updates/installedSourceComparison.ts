import {
  resolveJonesSourceCurrency,
  type JonesSourceComparison as JonesSourceAncestry,
} from "../fleetUpdates/sourceCurrency";

/** Where a host's installed build stands against the source installed on this laptop. */
export type JonesInstalledComparison =
  | "matches"
  | "older"
  | "newer"
  | "different"
  | "diverged"
  | "unavailable";

interface InstalledSource {
  readonly repository: string;
  readonly sha: string;
}

const FULL_SOURCE_COMMIT = /^[a-f0-9]{40}$/;

/**
 * Places a host's installed source against this laptop's. Equality needs only full commits from
 * one repository. Older, newer and diverged need ancestry evidence for exactly these two commits,
 * with the host's commit as `base` and the laptop's as `head`; without it, distinct commits stay
 * "different". Versions, run numbers and dates never order sources.
 */
export function compareInstalledSource(input: {
  readonly host?: InstalledSource | undefined;
  readonly laptop?: InstalledSource | undefined;
  readonly ancestry?: JonesSourceAncestry | undefined;
}): JonesInstalledComparison {
  const { host, laptop, ancestry } = input;
  if (
    host === undefined ||
    laptop === undefined ||
    host.repository !== laptop.repository ||
    !FULL_SOURCE_COMMIT.test(host.sha) ||
    !FULL_SOURCE_COMMIT.test(laptop.sha)
  )
    return "unavailable";
  switch (
    resolveJonesSourceCurrency({
      installedSource: host.sha,
      targetSource: laptop.sha,
      comparison: ancestry,
    })
  ) {
    case "current":
      return "matches";
    case "behind":
      return "older";
    case "ahead":
      return "newer";
    case "diverged":
      return "diverged";
    case "unknown":
      return "different";
  }
}
