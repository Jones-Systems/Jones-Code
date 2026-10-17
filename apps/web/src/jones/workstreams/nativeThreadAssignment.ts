import type {
  NativeReference,
  T3WorkstreamListResult,
  WorkstreamCommand,
  WorkstreamMembershipPage,
  WorkstreamReceipt,
  WorkstreamReferenceDetail,
  WorkstreamsRegistrationContextAcceptedResponse,
} from "@t3tools/contracts";
import {
  workstreamBindingKey,
  type LiveT3Placements,
} from "@t3tools/client-runtime/state/workstreams";
import {
  attestedNativeThreadKey,
  nativeWorkstreamThreadKey,
  type WorkstreamThreadLike,
} from "../../components/workstreams/nativeThreadGrouping";
import {
  canEditWorkstreams,
  planNativeMembership,
} from "../../components/workstreams/nativeWorkstreamActions";
import { assertWorkstreamReadContext, WorkstreamActionError } from "./workstreamActionSnapshot";

export interface NativeThreadAssignmentSnapshot {
  readonly data: T3WorkstreamListResult;
  readonly registrationContext: WorkstreamsRegistrationContextAcceptedResponse;
  readonly placements: LiveT3Placements;
  readonly reference: WorkstreamReferenceDetail | null;
  readonly destinationMemberships: WorkstreamMembershipPage | null;
}

export interface NativeThreadAssignmentOptions {
  readonly signal?: AbortSignal;
  readonly commandId: () => Promise<string>;
}

export function nativeThreadSourceAvailable(
  context: WorkstreamsRegistrationContextAcceptedResponse | null | undefined,
  thread: WorkstreamThreadLike,
): boolean {
  return (
    context?.state === "ready" &&
    context.sources.filter(
      (source) => source.provider === "t3" && source.source_instance_id === thread.environmentId,
    ).length === 1
  );
}

export async function assignNativeThread(
  input: NativeThreadAssignmentOptions & {
    readonly data: T3WorkstreamListResult;
    readonly thread: WorkstreamThreadLike;
    readonly destination: string | null;
    readonly loadSnapshot: (referenceId?: string) => Promise<NativeThreadAssignmentSnapshot>;
    readonly runBindingOperation: <A>(
      operation: (submit: (command: WorkstreamCommand) => Promise<WorkstreamReceipt>) => Promise<A>,
    ) => Promise<A>;
    readonly now?: () => number;
  },
): Promise<void> {
  const now = input.now ?? Date.now;
  const key = nativeWorkstreamThreadKey(input.thread.environmentId, input.thread.id);
  const authority = workstreamBindingKey({ ...input.data.binding, registryVersion: 0 });
  const permissions = JSON.stringify([...input.data.binding.permissions].sort());
  let expectedBinding: string | undefined;
  let sourceBinding: string | undefined;
  let snapshot: NativeThreadAssignmentSnapshot;
  const load = async (referenceId?: string) => {
    input.signal?.throwIfAborted();
    const fresh = await input.loadSnapshot(referenceId);
    input.signal?.throwIfAborted();
    if (
      !canEditWorkstreams(fresh.data) ||
      authority !== workstreamBindingKey({ ...fresh.data.binding, registryVersion: 0 }) ||
      permissions !== JSON.stringify([...fresh.data.binding.permissions].sort()) ||
      (expectedBinding !== undefined &&
        expectedBinding !== workstreamBindingKey(fresh.data.binding))
    )
      throw new WorkstreamActionError("stale");
    for (const context of [fresh.registrationContext, fresh.placements.context]) {
      assertWorkstreamReadContext(fresh.data, context);
      if (
        context.principal_id !== fresh.data.binding.principalId ||
        context.authorization_revision !== fresh.data.binding.authorizationRevision
      )
        throw new WorkstreamActionError("stale");
    }
    const sources = fresh.registrationContext.sources.filter(
      (source) =>
        source.provider === "t3" && source.source_instance_id === input.thread.environmentId,
    );
    const trust = fresh.placements.trustedEnvironments.find(
      (entry) => entry.environmentId === input.thread.environmentId,
    );
    if (
      fresh.registrationContext.state !== "ready" ||
      sources.length !== 1 ||
      fresh.placements.readiness !== "ready" ||
      !trust ||
      sources[0]?.authority_namespace !== trust.authorityNamespace ||
      sources[0]?.store_generation !== trust.storeGeneration
    )
      throw new WorkstreamActionError("activation");
    const selectedSource = JSON.stringify(sources[0]);
    if (sourceBinding !== undefined && sourceBinding !== selectedSource)
      throw new WorkstreamActionError("stale");
    sourceBinding = selectedSource;
    if (fresh.reference) {
      assertWorkstreamReadContext(fresh.data, fresh.reference.context);
      const reference = fresh.reference.reference;
      const identity = reference.identity;
      if (
        reference.owner_id !== fresh.data.binding.ownerId ||
        (referenceId !== undefined && reference.native_reference_id !== referenceId) ||
        identity.provider !== "t3" ||
        identity.source_instance_id !== input.thread.environmentId ||
        identity.native_id !== input.thread.id ||
        identity.resource_kind !== "thread" ||
        identity.id_kind !== "internal" ||
        identity.account_provenance.kind !== "not_account_scoped" ||
        reference.pr_locator !== null
      )
        throw new WorkstreamActionError("ambiguous");
    } else if (referenceId !== undefined) throw new WorkstreamActionError("unknown");
    if (fresh.destinationMemberships) {
      assertWorkstreamReadContext(fresh.data, fresh.destinationMemberships.context);
      if (
        fresh.destinationMemberships.next_cursor !== null ||
        fresh.destinationMemberships.items.some(
          (membership) => membership.workstream_id !== input.destination,
        )
      )
        throw new WorkstreamActionError("stale");
    } else if (input.destination !== null) throw new WorkstreamActionError("stale");
    expectedBinding = workstreamBindingKey(fresh.data.binding);
    snapshot = fresh;
    return fresh;
  };
  const currentReference = (reference: NativeReference) =>
    attestedNativeThreadKey(
      reference,
      now(),
      new Map(snapshot.placements.trustedEnvironments.map((entry) => [entry.environmentId, entry])),
    ) === key;
  await input.runBindingOperation(async (submit) => {
    await load();
    const send = async (action: WorkstreamCommand["action"], beforeSubmit?: () => void) => {
      const commandId = await input.commandId();
      input.signal?.throwIfAborted();
      beforeSubmit?.();
      const receipt = await submit({
        command_id: commandId,
        expected_server_generation: snapshot.data.binding.serverGeneration,
        expected_registry_version: snapshot.data.binding.registryVersion,
        action,
      });
      input.signal?.throwIfAborted();
      if (
        receipt.command_id !== commandId ||
        receipt.owner_id !== snapshot.data.binding.ownerId ||
        receipt.server_generation !== snapshot.data.binding.serverGeneration ||
        receipt.operation !== action.operation
      )
        throw new WorkstreamActionError("unknown");
      if (receipt.state !== "committed")
        throw new WorkstreamActionError(receipt.state === "rejected" ? "denied" : "unknown");
      expectedBinding = workstreamBindingKey({
        ...snapshot.data.binding,
        registryVersion: receipt.registry_version,
      });
      return receipt;
    };
    let reference = snapshot.reference?.reference;
    if (!reference) {
      if (input.destination === null) return;
      const receipt = await send({
        operation: "register_reference",
        identity: {
          provider: "t3",
          source_instance_id: input.thread.environmentId,
          resource_kind: "thread",
          id_kind: "internal",
          native_id: input.thread.id,
          account_provenance: { kind: "not_account_scoped" },
        },
        pr_locator: null,
      });
      const referenceId = receipt.effects.native_reference_id;
      if (!referenceId) throw new WorkstreamActionError("unknown");
      reference = (await load(referenceId)).reference?.reference;
      if (!reference) throw new WorkstreamActionError("unknown");
    }
    if (!currentReference(reference)) {
      await send({
        operation: "verify_reference",
        native_reference_id: reference.native_reference_id,
        expected_attestation_version: reference.registration.attestation_version,
      });
      reference = (await load(reference.native_reference_id)).reference?.reference;
      if (!reference || !currentReference(reference)) throw new WorkstreamActionError("stale");
    }
    const action = planNativeMembership({
      data: snapshot.data,
      placements: snapshot.placements,
      references: [reference],
      destinationMemberships: snapshot.destinationMemberships?.items ?? [],
      thread: input.thread,
      destination: input.destination,
      now: now(),
    });
    if (action)
      await send(action, () => {
        if (!currentReference(reference)) throw new WorkstreamActionError("stale");
      });
  });
}
