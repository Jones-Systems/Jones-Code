import {
  executeAuthenticatedEnvironmentHttpRequest,
  RemoteEnvironmentAuthorization,
} from "@t3tools/client-runtime/authorization";
import type { PreparedConnection } from "@t3tools/client-runtime/connection";
import * as ManagedRelay from "@t3tools/client-runtime/relay";
import * as Effect from "effect/Effect";

import type { JonesSourceComparison } from "../fleetUpdates/sourceCurrency";
import { compareInstalledSource, type JonesInstalledComparison } from "./installedSourceComparison";

const REPOSITORY = "Jones-Systems/Jones-Code";
const FULL_SOURCE_COMMIT = /^[a-f0-9]{40}$/;

interface InstalledSource {
  readonly repository: string;
  readonly sha: string;
}

/** `base` is the host's installed commit and `head` this laptop's, matching the server route. */
export interface InstalledSourcePair {
  readonly base: string;
  readonly head: string;
}

interface SourceRelationReport {
  readonly repository: string;
  readonly base: string;
  readonly head: string;
  readonly relation: "ahead" | "behind" | "identical" | "diverged" | "unknown";
}

/** The pair that needs ancestry, or undefined when absence or equality already decides it. */
export function installedSourcePair(
  host: InstalledSource | undefined,
  laptop: InstalledSource | undefined,
): InstalledSourcePair | undefined {
  if (
    host === undefined ||
    laptop === undefined ||
    host.repository !== REPOSITORY ||
    laptop.repository !== REPOSITORY ||
    !FULL_SOURCE_COMMIT.test(host.sha) ||
    !FULL_SOURCE_COMMIT.test(laptop.sha) ||
    host.sha === laptop.sha
  )
    return undefined;
  return { base: host.sha, head: laptop.sha };
}

/** Accepts only an ordering the server proved for exactly this pair. */
export function installedSourceAncestry(
  pair: InstalledSourcePair,
  report: SourceRelationReport | undefined,
): JonesSourceComparison | undefined {
  if (
    report === undefined ||
    report.repository !== REPOSITORY ||
    report.base !== pair.base ||
    report.head !== pair.head
  )
    return undefined;
  switch (report.relation) {
    case "ahead":
    case "behind":
    case "diverged":
      return { base: pair.base, head: pair.head, relation: report.relation };
    case "identical":
    case "unknown":
      return undefined;
  }
}

/**
 * The installed badge for one host. While the ancestry of distinct commits is still loading the
 * badge stays absent, so "Different source" never shows before a known order replaces it.
 */
export function installedComparisonFor(input: {
  readonly host: InstalledSource | undefined;
  readonly laptop: InstalledSource | undefined;
  readonly ancestry: JonesSourceComparison | undefined | "loading";
}): JonesInstalledComparison | null {
  const { host, laptop, ancestry } = input;
  if (ancestry === "loading" && installedSourcePair(host, laptop) !== undefined) return null;
  return compareInstalledSource({
    host,
    laptop,
    ancestry: ancestry === "loading" ? undefined : ancestry,
  });
}

/**
 * Asks this laptop's own server, which reads GitHub. A server that predates the route, a timeout,
 * or any other failure leaves the order unknown rather than failing the update controls.
 */
export const readInstalledSourceAncestry = Effect.fn("readInstalledSourceAncestry")(
  function* (prepared: PreparedConnection, pair: InstalledSourcePair) {
    const report = yield* executeAuthenticatedEnvironmentHttpRequest({
      prepared,
      signer: yield* Effect.serviceOption(ManagedRelay.ManagedRelay.ManagedRelayDpopSigner),
      remoteAuthorization: yield* Effect.serviceOption(RemoteEnvironmentAuthorization),
      group: "jonesUpdates",
      method: "GET",
      url: (urls) => urls.sourceRelation({ query: pair }),
      // Outlasts the server's own GitHub timeout so its "unknown" arrives as an answer.
      timeoutMs: 20_000,
      request: ({ client, headers }) => client.sourceRelation({ headers, query: pair }),
    });
    return installedSourceAncestry(pair, report);
  },
  (effect) => effect.pipe(Effect.catch(() => Effect.succeed(undefined))),
);
