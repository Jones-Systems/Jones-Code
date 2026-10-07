import {
  WorkstreamPrLocator,
  type NativeReference,
  type WorkstreamCommand,
  type WorkstreamReceipt,
  type WorkstreamPrObservation,
  type WorkstreamsRegistrationContextSource,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { workstreamBindingKey } from "@t3tools/client-runtime/state/workstreams";
import {
  assertWorkstreamActionSnapshot,
  assertWorkstreamReadContext,
  WorkstreamActionError,
  type WorkstreamActionSnapshot,
  type WorkstreamReferenceController,
} from "./workstreamActionSnapshot";
import type { WorkstreamThreadLike } from "../../components/workstreams/nativeThreadGrouping";

function qualifiedRegistrationSource(
  snapshot: WorkstreamActionSnapshot,
  provider: "t3" | "github",
  thread?: WorkstreamThreadLike,
): WorkstreamsRegistrationContextSource {
  assertWorkstreamActionSnapshot(snapshot);
  const sources =
    snapshot.registrationContext?.sources.filter((source) => source.provider === provider) ?? [];
  if (sources.length !== 1) throw new WorkstreamActionError("activation");
  const source = sources[0]!;
  if (provider === "t3") {
    const trusted = snapshot.placements?.trustedEnvironments.find(
      (entry) => entry.environmentId === thread?.environmentId,
    );
    if (
      !thread ||
      source.source_instance_id !== thread.environmentId ||
      !trusted ||
      source.authority_namespace !== trusted.authorityNamespace ||
      source.store_generation !== trusted.storeGeneration
    )
      throw new WorkstreamActionError("activation");
  }
  return source;
}

function committedWorkstreamReceipt(
  receipt: WorkstreamReceipt,
): Extract<WorkstreamReceipt, { readonly state: "committed" }> {
  if (receipt.state !== "committed")
    throw new WorkstreamActionError(receipt.state === "rejected" ? "denied" : "unknown");
  return receipt;
}

export function createSnapshotSequence(
  controller: WorkstreamReferenceController,
  options: { readonly signal?: AbortSignal } = {},
) {
  const accepted = new Set<string>();
  if (controller.data) accepted.add(workstreamBindingKey(controller.data.binding));
  let current: WorkstreamActionSnapshot | null = null;
  return {
    async load() {
      options.signal?.throwIfAborted();
      const snapshot = await controller.loadActionSnapshot(options);
      options.signal?.throwIfAborted();
      assertWorkstreamActionSnapshot(snapshot);
      const key = workstreamBindingKey(snapshot.data.binding);
      if (!accepted.has(key)) throw new WorkstreamActionError("stale");
      current = snapshot;
      return snapshot;
    },
    accept(receipt: WorkstreamReceipt) {
      options.signal?.throwIfAborted();
      const committed = committedWorkstreamReceipt(receipt);
      if (!current) throw new WorkstreamActionError("stale");
      accepted.add(
        workstreamBindingKey({
          ...current.data.binding,
          registryVersion: committed.registry_version,
        }),
      );
      return committed;
    },
  };
}

export function parseWorkstreamPrUrl(input: string): WorkstreamPrLocator {
  try {
    const url = new URL(input.trim());
    if (
      url.protocol !== "https:" ||
      url.hostname.toLowerCase() !== "github.com" ||
      url.port ||
      url.username ||
      url.password ||
      url.search ||
      url.hash
    )
      throw new WorkstreamActionError("invalid-pr");
    const match = /^\/([^/]+)\/([^/]+)\/pull\/([1-9][0-9]*)\/?$/.exec(url.pathname);
    if (!match) throw new WorkstreamActionError("invalid-pr");
    return Schema.decodeSync(WorkstreamPrLocator)({
      host: "github.com",
      repository_owner: match[1]!.toLowerCase(),
      repository_name: match[2]!.toLowerCase(),
      number: Number(match[3]),
    });
  } catch {
    throw new WorkstreamActionError("invalid-pr");
  }
}

export const canonicalWorkstreamPrId = (locator: WorkstreamPrLocator): string =>
  `${locator.repository_owner}/${locator.repository_name}#${locator.number}`;
const sameLocator = (a: WorkstreamPrLocator | null, b: WorkstreamPrLocator): boolean =>
  a !== null &&
  a.host === b.host &&
  a.repository_owner === b.repository_owner &&
  a.repository_name === b.repository_name &&
  a.number === b.number;

function exactPrReference(
  snapshot: WorkstreamActionSnapshot,
  locator: WorkstreamPrLocator,
  source: WorkstreamsRegistrationContextSource,
): NativeReference | null {
  const nativeId = canonicalWorkstreamPrId(locator);
  const matches = snapshot.references.items.filter(
    (reference) =>
      reference.owner_id === snapshot.data.binding.ownerId &&
      ((reference.identity.provider === "github" &&
        reference.identity.source_instance_id === source.source_instance_id &&
        reference.identity.native_id === nativeId) ||
        sameLocator(reference.pr_locator, locator)),
  );
  if (matches.length > 1) throw new WorkstreamActionError("ambiguous");
  const reference = matches[0];
  if (!reference) return null;
  if (
    reference.identity.provider !== "github" ||
    reference.identity.source_instance_id !== source.source_instance_id ||
    reference.identity.native_id !== nativeId ||
    reference.identity.resource_kind !== "pull_request" ||
    reference.identity.id_kind !== "external" ||
    reference.identity.account_provenance.kind !== "not_account_scoped" ||
    !sameLocator(reference.pr_locator, locator)
  )
    throw new WorkstreamActionError("ambiguous");
  return reference;
}

export async function prepareWorkstreamPr(input: {
  readonly controller: WorkstreamReferenceController;
  readonly url: string;
  readonly workstreamId: string;
  readonly commandId: () => Promise<string>;
  readonly verify: boolean;
  readonly signal?: AbortSignal;
}): Promise<NativeReference> {
  const locator = parseWorkstreamPrUrl(input.url);
  return input.controller.runBindingOperation(async (submit) => {
    const sequence = createSnapshotSequence(
      input.controller,
      input.signal ? { signal: input.signal } : {},
    );
    let snapshot = await sequence.load();
    let source = qualifiedRegistrationSource(snapshot, "github");
    let reference = exactPrReference(snapshot, locator, source);
    const send = async (action: WorkstreamCommand["action"]) => {
      const commandId = await input.commandId();
      input.signal?.throwIfAborted();
      return sequence.accept(
        await submit({
          command_id: commandId,
          expected_server_generation: snapshot.data.binding.serverGeneration,
          expected_registry_version: snapshot.data.binding.registryVersion,
          action,
        }),
      );
    };
    if (!reference) {
      const receipt = await send({
        operation: "register_reference",
        identity: {
          provider: "github",
          source_instance_id: source.source_instance_id,
          resource_kind: "pull_request",
          id_kind: "external",
          native_id: canonicalWorkstreamPrId(locator),
          account_provenance: { kind: "not_account_scoped" },
        },
        pr_locator: locator,
      });
      snapshot = await sequence.load();
      source = qualifiedRegistrationSource(snapshot, "github");
      reference = exactPrReference(snapshot, locator, source);
      if (!reference || reference.native_reference_id !== receipt.effects.native_reference_id)
        throw new WorkstreamActionError("unknown");
    }
    if (!input.verify) return reference;
    await send({
      operation: "verify_reference",
      native_reference_id: reference.native_reference_id,
      expected_attestation_version: reference.registration.attestation_version,
    });
    snapshot = await sequence.load();
    source = qualifiedRegistrationSource(snapshot, "github");
    reference = exactPrReference(snapshot, locator, source);
    const evidence = reference?.registration.evidence;
    if (
      !reference ||
      reference.registration.state !== "attested" ||
      !reference.registration.expires_at ||
      Date.parse(reference.registration.expires_at) <= Date.now() ||
      evidence?.provider !== "github" ||
      evidence.source_instance_id !== source.source_instance_id ||
      evidence.native_id !== canonicalWorkstreamPrId(locator) ||
      evidence.authority_namespace !== source.authority_namespace ||
      evidence.store_generation !== source.store_generation
    )
      throw new WorkstreamActionError("stale");
    input.signal?.throwIfAborted();
    const detail = await input.controller.loadDetail(
      input.workstreamId,
      input.signal ? { signal: input.signal } : {},
    );
    input.signal?.throwIfAborted();
    assertWorkstreamReadContext(snapshot.data, detail.detail.context);
    const linked = detail.memberships.items.filter(
      (membership) =>
        membership.native_reference_id === reference.native_reference_id &&
        membership.closed === null,
    );
    if (linked.length > 1 || linked.some((membership) => membership.kind !== "secondary"))
      throw new WorkstreamActionError("ambiguous");
    if (!linked.length)
      await send({
        operation: "link_secondary",
        workstream_id: input.workstreamId,
        expected_version: detail.detail.workstream.version,
        native_reference_id: reference.native_reference_id,
      });
    return reference;
  });
}

export function workstreamPrObservationLabel(
  observation: typeof WorkstreamPrObservation.Type | null,
): string {
  if (!observation) return "Not refreshed";
  const state = observation.last_success?.state ?? "No successful observation";
  const freshness =
    observation.outcome === "observed" || observation.outcome === "not_modified"
      ? "current"
      : `refresh ${observation.outcome.replaceAll("_", " ")}`;
  return `${state} · ${freshness}`;
}

export async function refreshWorkstreamPr(input: {
  readonly controller: WorkstreamReferenceController;
  readonly workstreamId: string;
  readonly membershipId: string;
  readonly referenceId: string;
  readonly commandId: () => Promise<string>;
  readonly signal?: AbortSignal;
}): Promise<typeof WorkstreamPrObservation.Type | null> {
  return input.controller.runBindingOperation(async (submit) => {
    const options = input.signal ? { signal: input.signal } : {};
    const sequence = createSnapshotSequence(input.controller, options);
    const snapshot = await sequence.load();
    const detail = await input.controller.loadDetail(input.workstreamId, options);
    input.signal?.throwIfAborted();
    const reference = await input.controller.loadReference(input.referenceId, options);
    input.signal?.throwIfAborted();
    assertWorkstreamReadContext(snapshot.data, detail.detail.context);
    assertWorkstreamReadContext(snapshot.data, reference.context);
    if (!reference.latest_observation) throw new WorkstreamActionError("stale");
    const commandId = await input.commandId();
    input.signal?.throwIfAborted();
    sequence.accept(
      await submit({
        command_id: commandId,
        expected_server_generation: snapshot.data.binding.serverGeneration,
        expected_registry_version: snapshot.data.binding.registryVersion,
        action: {
          operation: "refresh_linked_pr",
          workstream_id: input.workstreamId,
          expected_version: detail.detail.workstream.version,
          membership_id: input.membershipId,
          expected_observation_version: reference.latest_observation.observation_version,
        },
      }),
    );
    const fresh = await sequence.load();
    const result = await input.controller.loadReference(input.referenceId, options);
    input.signal?.throwIfAborted();
    assertWorkstreamReadContext(fresh.data, result.context);
    return result.latest_observation;
  });
}
