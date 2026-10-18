import { TrustedT3PlacementEnvironment, type ServerConfig } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import type { TrustedT3EnvironmentAttestation } from "./nativeThreadGrouping.ts";

export interface ConnectedEnvironmentTrustInput {
  readonly environmentId: string;
  readonly config: {
    readonly environment: { readonly environmentId: string };
    readonly nativeStoreAuthority?: ServerConfig["nativeStoreAuthority"];
  } | null;
  /**
   * True only when `config` is the snapshot from the current live connection.
   * A cached or restored config may carry a descriptor the server no longer
   * reports, so it never contributes trust or conflicts.
   */
  readonly live: boolean;
}

export interface ConnectedEnvironmentTrust {
  readonly trusted: ReadonlyMap<string, TrustedT3EnvironmentAttestation>;
  readonly conflicts: ReadonlySet<string>;
}

const isTrustedEnvironment = Schema.is(TrustedT3PlacementEnvironment);

type Candidate = TrustedT3EnvironmentAttestation | "conflict";

const sameAttestation = (
  left: TrustedT3EnvironmentAttestation,
  right: TrustedT3EnvironmentAttestation,
): boolean =>
  left.authorityNamespace === right.authorityNamespace &&
  left.storeGeneration === right.storeGeneration;

function collect(
  into: Map<string, Candidate>,
  environmentId: string,
  attestation: TrustedT3EnvironmentAttestation,
): void {
  const prior = into.get(environmentId);
  if (prior === undefined) {
    into.set(environmentId, {
      authorityNamespace: attestation.authorityNamespace,
      storeGeneration: attestation.storeGeneration,
    });
  } else if (prior !== "conflict" && !sameAttestation(prior, attestation)) {
    into.set(environmentId, "conflict");
  }
}

/**
 * Builds Workstream grouping trust from the primary's placement trust plus
 * each connected environment's own live config descriptor. A descriptor
 * applies only to the environment it names, and any disagreement for one
 * environment excludes it from trust and reports it as a conflict.
 */
export function buildConnectedEnvironmentTrust(input: {
  readonly environments: readonly ConnectedEnvironmentTrustInput[];
  readonly placementTrustedEnvironments?: readonly TrustedT3PlacementEnvironment[] | undefined;
}): ConnectedEnvironmentTrust {
  const placement = new Map<string, Candidate>();
  for (const entry of input.placementTrustedEnvironments ?? []) {
    if (isTrustedEnvironment(entry)) collect(placement, entry.environmentId, entry);
  }
  const described = new Map<string, Candidate>();
  for (const { environmentId, config, live } of input.environments) {
    const descriptor = config?.nativeStoreAuthority;
    if (
      !live ||
      config === null ||
      config.environment.environmentId !== environmentId ||
      !isTrustedEnvironment(descriptor) ||
      descriptor.environmentId !== environmentId
    )
      continue;
    collect(described, environmentId, descriptor);
  }

  const trusted = new Map<string, TrustedT3EnvironmentAttestation>();
  const conflicts = new Set<string>();
  const environmentIds = [...new Set([...placement.keys(), ...described.keys()])].sort();
  for (const environmentId of environmentIds) {
    const fromPlacement = placement.get(environmentId);
    const fromConfig = described.get(environmentId);
    if (
      fromPlacement === "conflict" ||
      fromConfig === "conflict" ||
      (fromPlacement !== undefined &&
        fromConfig !== undefined &&
        !sameAttestation(fromPlacement, fromConfig))
    ) {
      conflicts.add(environmentId);
      continue;
    }
    const attestation = fromPlacement ?? fromConfig;
    if (attestation !== undefined) trusted.set(environmentId, attestation);
  }
  return { trusted, conflicts };
}
