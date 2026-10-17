import type {
  MembershipEpisode,
  NativeReference,
  T3ThreadPlacement,
  T3WorkstreamMetadata,
} from "@t3tools/contracts";
import type { WorkstreamAssignmentIndex } from "./assignmentIndex.ts";
import { currentT3Placement } from "./placements.ts";

export interface WorkstreamThreadLike {
  readonly environmentId: string;
  readonly id: string;
}

export interface NativeWorkstreamThreadGroup<Thread extends WorkstreamThreadLike> {
  readonly workstream: T3WorkstreamMetadata;
  readonly threads: readonly Thread[];
}

/**
 * Display assurance for a visible native thread grouped from an assignment index.
 * `verified` has a current matching proof; `recorded` is a stored assignment without one;
 * `unassigned` is confirmed only by a complete index; `unknown` means coverage cannot tell.
 */
export type NativeWorkstreamThreadAssurance =
  | "verified"
  | "recorded"
  | "conflict"
  | "unassigned"
  | "unknown";

export interface NativeWorkstreamThreadGrouping<Thread extends WorkstreamThreadLike> {
  readonly groups: readonly NativeWorkstreamThreadGroup<Thread>[];
  readonly ungrouped: readonly Thread[];
  readonly ordered: readonly Thread[];
  readonly groupedKeys: ReadonlySet<string>;
  readonly secondaryWorkstreamIdsByKey: ReadonlyMap<string, readonly string[]>;
  readonly secondaryWorkstreamLabelsByKey: ReadonlyMap<string, readonly string[]>;
  readonly conflictingKeys: ReadonlySet<string>;
  /** Present only when grouping from an assignment index; one value per visible thread. */
  readonly assuranceByKey?: ReadonlyMap<string, NativeWorkstreamThreadAssurance>;
}

export const nativeWorkstreamThreadKey = (environmentId: string, threadId: string) =>
  JSON.stringify([environmentId, threadId]);

export const secondaryNativeWorkstreamLabels = (
  grouping: Pick<
    NativeWorkstreamThreadGrouping<WorkstreamThreadLike>,
    "secondaryWorkstreamLabelsByKey"
  >,
  thread: WorkstreamThreadLike,
): readonly string[] =>
  grouping.secondaryWorkstreamLabelsByKey.get(
    nativeWorkstreamThreadKey(thread.environmentId, thread.id),
  ) ?? [];

// This is the sole T3 activation convention. Other provider/resource identities remain unassigned.
export interface TrustedT3EnvironmentAttestation {
  readonly authorityNamespace: string;
  readonly storeGeneration: number;
}

export function attestedNativeThreadKey(
  reference: NativeReference,
  trustedNowMs: number,
  trustedEnvironments: ReadonlyMap<string, TrustedT3EnvironmentAttestation>,
): string | null {
  const identity = reference.identity;
  const registration = reference.registration;
  const evidence = registration.evidence;
  const trustedEnvironment = trustedEnvironments.get(identity.source_instance_id);
  if (
    identity.provider !== "t3" ||
    identity.resource_kind !== "thread" ||
    identity.id_kind !== "internal" ||
    identity.account_provenance.kind !== "not_account_scoped" ||
    registration.state !== "attested" ||
    !Number.isSafeInteger(registration.attestation_version) ||
    registration.attestation_version < 1 ||
    registration.attested_at === null ||
    !Number.isFinite(Date.parse(registration.attested_at)) ||
    Date.parse(registration.attested_at) > trustedNowMs ||
    registration.expires_at === null ||
    !Number.isFinite(Date.parse(registration.expires_at)) ||
    Date.parse(registration.expires_at) <= trustedNowMs ||
    evidence === null ||
    !/^[a-f0-9]{64}$/.test(evidence.evidence_sha256) ||
    trustedEnvironment === undefined ||
    evidence.provider !== identity.provider ||
    evidence.source_instance_id !== identity.source_instance_id ||
    evidence.authority_namespace !== trustedEnvironment.authorityNamespace ||
    evidence.native_id !== identity.native_id ||
    evidence.store_generation !== trustedEnvironment.storeGeneration
  )
    return null;
  return nativeWorkstreamThreadKey(identity.source_instance_id, identity.native_id);
}

const trustedCurrentPlacement = (
  placement: T3ThreadPlacement,
  trustedNowMs: number,
  trustedEnvironments: ReadonlyMap<string, TrustedT3EnvironmentAttestation>,
): boolean => {
  const trust = trustedEnvironments.get(placement.source_instance_id);
  return (
    currentT3Placement(placement, trustedNowMs) &&
    trust !== undefined &&
    trust.authorityNamespace === placement.authority_namespace &&
    trust.storeGeneration === placement.store_generation &&
    Number.isSafeInteger(placement.source_binding_version) &&
    placement.source_binding_version >= 1 &&
    Number.isSafeInteger(placement.attestation_version) &&
    placement.attestation_version >= 1 &&
    /^[a-f0-9]{64}$/.test(placement.evidence_sha256)
  );
};

interface NativeThreadGroupingInput<Thread extends WorkstreamThreadLike> {
  readonly workstreams: readonly T3WorkstreamMetadata[];
  readonly memberships?: readonly MembershipEpisode[];
  readonly references?: readonly NativeReference[];
  readonly placements?: readonly T3ThreadPlacement[];
  readonly threads: readonly Thread[];
  readonly trustedNow: string;
  readonly trustedEnvironments: ReadonlyMap<string, TrustedT3EnvironmentAttestation>;
  /**
   * Groups from stored assignments instead of fresh proof. Placements then only upgrade
   * matching primaries to `verified`; `memberships` and `references` are not consulted.
   */
  readonly assignmentIndex?: WorkstreamAssignmentIndex;
  /**
   * Clock for index-path `verified` assurance only; defaults to `trustedNow`. Proof attested
   * after it is not verified, but grouping from the index never depends on any clock.
   */
  readonly verificationNow?: string;
}

export function groupNativeThreadsByWorkstream<Thread extends WorkstreamThreadLike>(
  input: NativeThreadGroupingInput<Thread>,
): NativeWorkstreamThreadGrouping<Thread> {
  if (input.assignmentIndex !== undefined)
    return groupNativeThreadsByAssignmentIndex(input, input.assignmentIndex);
  const trustedNowMs = Date.parse(input.trustedNow);
  const referenceKeys = new Map<string, string>();
  for (const reference of input.references ?? []) {
    const key = Number.isFinite(trustedNowMs)
      ? attestedNativeThreadKey(reference, trustedNowMs, input.trustedEnvironments)
      : null;
    if (key !== null) referenceKeys.set(reference.native_reference_id, key);
  }
  const invalidReferences = new Set<string>();
  for (const placement of input.placements ?? []) {
    if (!trustedCurrentPlacement(placement, trustedNowMs, input.trustedEnvironments)) {
      invalidReferences.add(placement.native_reference_id);
      continue;
    }
    const key = nativeWorkstreamThreadKey(placement.source_instance_id, placement.native_thread_id);
    const prior = referenceKeys.get(placement.native_reference_id);
    if (prior !== undefined && prior !== key) invalidReferences.add(placement.native_reference_id);
    referenceKeys.set(placement.native_reference_id, key);
  }
  for (const id of invalidReferences) referenceKeys.delete(id);

  const primaryByKey = new Map<string, string>();
  const secondaryByKey = new Map<string, Set<string>>();
  const conflictingKeys = new Set<string>();
  for (const membership of [...(input.memberships ?? []), ...(input.placements ?? [])]) {
    if ("closed" in membership && membership.closed !== null) continue;
    const key = referenceKeys.get(membership.native_reference_id);
    if (key === undefined) continue;
    if (membership.kind === "secondary") {
      const current = secondaryByKey.get(key) ?? new Set<string>();
      current.add(membership.workstream_id);
      secondaryByKey.set(key, current);
      continue;
    }
    const existing = primaryByKey.get(key);
    if (existing !== undefined && existing !== membership.workstream_id) {
      conflictingKeys.add(key);
      primaryByKey.delete(key);
    } else if (!conflictingKeys.has(key)) {
      primaryByKey.set(key, membership.workstream_id);
    }
  }

  const threadsByWorkstream = new Map<string, Thread[]>();
  const groupedKeys = new Set<string>();
  const ungrouped: Thread[] = [];
  const availableWorkstreams = new Set(input.workstreams.map((item) => item.workstreamId));
  for (const thread of input.threads) {
    const key = nativeWorkstreamThreadKey(thread.environmentId, thread.id);
    const workstreamId = primaryByKey.get(key);
    if (
      workstreamId === undefined ||
      !availableWorkstreams.has(workstreamId) ||
      conflictingKeys.has(key)
    ) {
      ungrouped.push(thread);
      continue;
    }
    const bucket = threadsByWorkstream.get(workstreamId);
    if (bucket === undefined) threadsByWorkstream.set(workstreamId, [thread]);
    else bucket.push(thread);
    groupedKeys.add(key);
  }

  const groups = input.workstreams.flatMap((workstream) => {
    const threads = threadsByWorkstream.get(workstream.workstreamId);
    return threads === undefined || threads.length === 0 ? [] : [{ workstream, threads }];
  });
  const workstreamNames = new Map(input.workstreams.map((item) => [item.workstreamId, item.name]));
  const secondaryWorkstreamIdsByKey = new Map(
    [...secondaryByKey].map(([key, ids]) => [key, [...ids]]),
  );
  return {
    groups,
    ungrouped,
    ordered: [...groups.flatMap((group) => group.threads), ...ungrouped],
    groupedKeys,
    secondaryWorkstreamIdsByKey,
    secondaryWorkstreamLabelsByKey: new Map(
      [...secondaryWorkstreamIdsByKey].map(([key, ids]) => [
        key,
        ids.map((id) => workstreamNames.get(id) ?? id),
      ]),
    ),
    conflictingKeys,
  };
}

function groupNativeThreadsByAssignmentIndex<Thread extends WorkstreamThreadLike>(
  input: NativeThreadGroupingInput<Thread>,
  index: WorkstreamAssignmentIndex,
): NativeWorkstreamThreadGrouping<Thread> {
  const trustedNowMs = Date.parse(input.verificationNow ?? input.trustedNow);
  const provenMemberships = new Set<string>();
  for (const placement of input.placements ?? [])
    if (
      placement.kind === "primary" &&
      trustedCurrentPlacement(placement, trustedNowMs, input.trustedEnvironments)
    )
      provenMemberships.add(
        JSON.stringify([
          placement.membership_id,
          placement.native_reference_id,
          placement.workstream_id,
          nativeWorkstreamThreadKey(placement.source_instance_id, placement.native_thread_id),
        ]),
      );
  const availableWorkstreams = new Set(input.workstreams.map((item) => item.workstreamId));
  const assurance = (key: string): NativeWorkstreamThreadAssurance => {
    const entry = index.entries.get(key);
    if (entry === undefined || entry.state === "secondary-only")
      return index.complete ? "unassigned" : "unknown";
    const primary = entry.primary;
    if (entry.state === "conflict" || primary === null) return "conflict";
    if (!availableWorkstreams.has(primary.workstreamId)) return "unknown";
    if (
      provenMemberships.has(
        JSON.stringify([primary.membershipId, primary.referenceId, primary.workstreamId, key]),
      )
    )
      return "verified";
    // A known namespace or generation mismatch blocks changes until the reference is re-verified.
    const trust = input.trustedEnvironments.get(entry.sourceInstanceId);
    const evidence = primary.reference.registration.evidence;
    if (
      trust !== undefined &&
      evidence !== null &&
      (evidence.authority_namespace !== trust.authorityNamespace ||
        evidence.store_generation !== trust.storeGeneration)
    )
      return "conflict";
    return Number.isFinite(trustedNowMs) &&
      attestedNativeThreadKey(primary.reference, trustedNowMs, input.trustedEnvironments) === key
      ? "verified"
      : "recorded";
  };

  const threadsByWorkstream = new Map<string, Thread[]>();
  const groupedKeys = new Set<string>();
  const conflictingKeys = new Set<string>();
  const assuranceByKey = new Map<string, NativeWorkstreamThreadAssurance>();
  const secondaryWorkstreamIdsByKey = new Map<string, readonly string[]>();
  const ungrouped: Thread[] = [];
  for (const thread of input.threads) {
    const key = nativeWorkstreamThreadKey(thread.environmentId, thread.id);
    const value = assurance(key);
    assuranceByKey.set(key, value);
    const entry = index.entries.get(key);
    if (entry !== undefined && entry.secondaries.length > 0)
      secondaryWorkstreamIdsByKey.set(key, [
        ...new Set(entry.secondaries.map((item) => item.workstreamId)),
      ]);
    if (value === "conflict") conflictingKeys.add(key);
    const workstreamId = entry?.primary?.workstreamId;
    if ((value !== "verified" && value !== "recorded") || workstreamId === undefined) {
      ungrouped.push(thread);
      continue;
    }
    const bucket = threadsByWorkstream.get(workstreamId);
    if (bucket === undefined) threadsByWorkstream.set(workstreamId, [thread]);
    else bucket.push(thread);
    groupedKeys.add(key);
  }

  const groups = input.workstreams.flatMap((workstream) => {
    const threads = threadsByWorkstream.get(workstream.workstreamId);
    return threads === undefined || threads.length === 0 ? [] : [{ workstream, threads }];
  });
  const workstreamNames = new Map(input.workstreams.map((item) => [item.workstreamId, item.name]));
  return {
    groups,
    ungrouped,
    ordered: [...groups.flatMap((group) => group.threads), ...ungrouped],
    groupedKeys,
    secondaryWorkstreamIdsByKey,
    secondaryWorkstreamLabelsByKey: new Map(
      [...secondaryWorkstreamIdsByKey].map(([key, ids]) => [
        key,
        ids.map((id) => workstreamNames.get(id) ?? id),
      ]),
    ),
    conflictingKeys,
    assuranceByKey,
  };
}
