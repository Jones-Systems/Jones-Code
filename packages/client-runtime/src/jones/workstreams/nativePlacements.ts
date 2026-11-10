import {
  TrustedT3PlacementEnvironment,
  t3PlacementIdentityKey,
  type T3PlacementIdentity,
  type T3WorkstreamListResult,
} from "@t3tools/contracts";
import {
  WorkstreamsNativePlacementPage,
  WorkstreamsNativePlacementResult,
} from "@t3tools/contracts/jones/workstreamsNativePlacement";
import type { PreparedConnection } from "../../connection/model.ts";
import {
  nativePlacementWitness,
  type LiveT3Placements,
} from "../../state/workstreams/placements.ts";
import {
  NativeReadError,
  nativeReadRequest,
  validateNativeReadScope,
  validateNativeReadResult,
  assertNativeReadSnapshot,
  type NativeCompleteRead,
  type NativeReadScope,
  type NativeReadSha256,
} from "../../state/workstreams/nativeRead.ts";
import * as Schema from "effect/Schema";

export interface NativePlacementConnection {
  readonly prepared: PreparedConnection;
  readonly generation: number;
  readonly session: { readonly authenticated: boolean };
  readonly config: {
    readonly environment: { readonly environmentId: string };
    readonly nativeStoreAuthority?: TrustedT3PlacementEnvironment;
  };
}
export type NativePlacementConnectionLookup = (
  environmentId: string,
) => NativePlacementConnection | null;

interface Trust {
  readonly environments: readonly TrustedT3PlacementEnvironment[];
}
const captures = new WeakMap<Trust, () => void>();
const decodeTrust = Schema.decodeUnknownSync(TrustedT3PlacementEnvironment);
const decodePage = Schema.decodeUnknownSync(WorkstreamsNativePlacementPage);
const decodeResult = Schema.decodeUnknownSync(WorkstreamsNativePlacementResult);
function refuse(message: string): never {
  throw new NativeReadError("scope_changed", message);
}
const trustKey = (value: TrustedT3PlacementEnvironment) =>
  JSON.stringify([value.environmentId, value.authorityNamespace, value.storeGeneration]);

/** Only the application's original live source connections supply trust, never a placement response. */
function captureNativePlacementTrust(
  identities: readonly T3PlacementIdentity[],
  lookup: NativePlacementConnectionLookup,
): Trust {
  const ids = [...new Set(identities.map((identity) => identity.source_instance_id))].sort();
  if (ids.length > 100)
    throw new NativeReadError("bounds", "Too many native placement authorities.");
  const sources = ids.map((id) => {
    const original = lookup(id);
    if (
      !original?.session.authenticated ||
      original.prepared.environmentId !== id ||
      original.config.environment.environmentId !== id ||
      !Number.isSafeInteger(original.generation) ||
      original.generation < 1 ||
      !original.config.nativeStoreAuthority
    )
      refuse("A live native source connection and authority descriptor are required.");
    const descriptor = decodeTrust(original.config.nativeStoreAuthority, {
      onExcessProperty: "error",
    });
    if (descriptor.environmentId !== id)
      refuse("Native source descriptor names another environment.");
    const copy = Object.freeze({ ...descriptor });
    const key = trustKey(copy);
    return { id, original, descriptor: copy, key };
  });
  const trust: Trust = Object.freeze({
    environments: Object.freeze(sources.map((source) => source.descriptor)),
  });
  captures.set(trust, () => {
    for (const { id, original, key } of sources) {
      const current = lookup(id);
      if (
        !current ||
        current.prepared !== original.prepared ||
        current.session !== original.session ||
        current.config !== original.config ||
        current.generation !== original.generation ||
        !current.session.authenticated ||
        current.prepared.environmentId !== id ||
        current.config.environment.environmentId !== id ||
        !current.config.nativeStoreAuthority ||
        trustKey(current.config.nativeStoreAuthority) !== key
      )
        refuse("The original native source connection or authority changed.");
    }
  });
  return trust;
}

const bindingKey = (binding: NativeReadScope["binding"]) =>
  JSON.stringify([
    binding.registryId,
    binding.ownerId,
    binding.principalId,
    binding.authorizationRevision,
    binding.serverGeneration,
    binding.registryVersion,
    binding.contractVersion,
    binding.contractManifest,
    [...binding.permissions].sort(),
  ]);

export async function loadNativePlacements(input: {
  readonly scope: NativeReadScope;
  readonly metadata: T3WorkstreamListResult;
  readonly identities: readonly T3PlacementIdentity[];
  readonly lookup: NativePlacementConnectionLookup;
  readonly servedByEnvironmentId: string;
  readonly sha256: NativeReadSha256;
  readonly checkCurrent: () => void;
  readonly complete: (
    request: ReturnType<
      typeof nativeReadRequest<{
        readonly route: "placements";
        readonly params: { readonly identities: readonly T3PlacementIdentity[] };
      }>
    >,
    signal?: AbortSignal,
  ) => Promise<NativeCompleteRead<"placements">>;
  readonly signal?: AbortSignal;
  readonly now?: () => number;
}): Promise<LiveT3Placements> {
  const scope = validateNativeReadScope(input.scope);
  if (!scope.capability.placement_proofs.includes("workstreams-t3-placement/2.0.0"))
    throw new NativeReadError("incomplete", "Native placement v2 was not negotiated.");
  if (
    input.metadata.source !== "live" ||
    input.metadata.stale ||
    input.metadata.nextCursor !== null ||
    bindingKey(input.metadata.binding) !== bindingKey(scope.binding)
  )
    refuse("Native placement metadata no longer matches the reader's original binding.");
  const request = nativeReadRequest(
    { route: "placements", params: { identities: input.identities } },
    "workstreams-t3-placement/2.0.0",
  );
  const trust = captureNativePlacementTrust(input.identities, input.lookup);
  const check = () => {
    input.checkCurrent();
    input.signal?.throwIfAborted();
    const current = captures.get(trust);
    if (!current) refuse("Native source trust was not captured by this invocation.");
    current();
  };
  check();
  const snapshot = await input.complete(request, input.signal);
  check();
  if (
    snapshot.stale ||
    snapshot.readOnly ||
    snapshot.pages.length === 0 ||
    snapshot.authorization_revision !== scope.binding.authorizationRevision
  )
    throw new NativeReadError(
      "incomplete",
      "Native placements require a complete current home snapshot.",
    );
  const pages: WorkstreamsNativePlacementPage[] = [];
  const seen = new Set<string>();
  const routing = new Map<string, string>();
  let previous: readonly [string, string] | undefined;
  const descriptors = new Map(trust.environments.map((value) => [value.environmentId, value]));
  const requested = new Set(input.identities.map(t3PlacementIdentityKey));
  for (const page of snapshot.pages) {
    check();
    const validated = await validateNativeReadResult(page, request, scope, input.sha256);
    check();
    assertNativeReadSnapshot(snapshot.envelope, validated.envelope);
    if (
      validated.route !== "placements" ||
      validated.placement_contract !== "workstreams-t3-placement/2.0.0"
    )
      throw new NativeReadError("invalid_response", "Native placement contract changed.");
    const value = decodePage(validated.value, {
      onExcessProperty: "error",
    });
    if (
      value.provenance.served_by_environment_id !== input.servedByEnvironmentId &&
      value.provenance.served_by_environment_id !== scope.capability.registry!.home_environment_id
    )
      throw new NativeReadError(
        "invalid_response",
        "Native placement serving environment changed.",
      );
    for (const source of trust.environments) {
      const coverage = validated.envelope.coverage.sources.filter(
        (entry) => entry.source_instance_id === source.environmentId,
      );
      if (
        coverage.length !== 1 ||
        coverage[0]!.mapping !== "mapped" ||
        coverage[0]!.attestation !== "current"
      )
        throw new NativeReadError("incomplete", "Native placement source coverage is unavailable.");
    }
    for (const item of value.items) {
      const source = descriptors.get(item.source_instance_id);
      const coverage = validated.envelope.coverage.sources.filter(
        (entry) => entry.source_instance_id === item.source_instance_id,
      );
      const tuple = [item.native_reference_id, item.membership_id] as const;
      const {
        membership_id: _membership,
        workstream_id: _workstream,
        kind: _kind,
        ...reference
      } = item;
      const retained = JSON.stringify(reference);
      if (
        !source ||
        coverage.length !== 1 ||
        coverage[0]!.mapping !== "mapped" ||
        coverage[0]!.attestation !== "current" ||
        source.authorityNamespace !== item.authority_namespace ||
        source.storeGeneration !== item.store_generation ||
        seen.has(item.membership_id) ||
        !requested.has(t3PlacementIdentityKey(item)) ||
        (previous &&
          (tuple[0] < previous[0] || (tuple[0] === previous[0] && tuple[1] <= previous[1]))) ||
        (routing.has(item.native_reference_id) &&
          routing.get(item.native_reference_id) !== retained)
      )
        throw new NativeReadError(
          "invalid_response",
          "Native placement identity, order or local source authority changed.",
        );
      previous = tuple;
      seen.add(item.membership_id);
      routing.set(item.native_reference_id, retained);
    }
    pages.push(value);
  }
  if (pages.at(-1)!.next_cursor !== null)
    throw new NativeReadError("incomplete", "Native placement continuation did not finish.");
  const result = decodeResult(
    {
      page: { ...pages[0]!, items: pages.flatMap((page) => page.items), next_cursor: null },
      trustedEnvironments: trust.environments,
      readiness: "ready",
    },
    { onExcessProperty: "error" },
  );
  const now = (input.now ?? Date.now)();
  if (
    !Number.isFinite(now) ||
    pages.some((page) => Date.parse(page.provenance.observed_at) > now) ||
    result.page.items.some((item) => Date.parse(item.attested_at) > now)
  )
    throw new NativeReadError(
      "invalid_response",
      "Native placement evidence is observed in the future.",
    );
  check();
  return {
    context: result.page.context,
    items: result.page.items.filter((item) => Date.parse(item.expires_at) > now),
    trustedEnvironments: result.trustedEnvironments,
    readiness: result.readiness,
    native: nativePlacementWitness(
      pages.map((page) => page.provenance),
      () => {
        try {
          check();
          return true;
        } catch {
          return false;
        }
      },
    ),
  };
}
