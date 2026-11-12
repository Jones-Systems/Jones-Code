import type {
  CommittedReceipt,
  DeclarationRevision,
  Edge,
  Effects,
  LifecycleDeclaration,
  MembershipEpisode,
  NativeReference,
  RegistryCommandInput,
  RegistryExternalResult,
  RegistrySourceBinding,
  RejectedReceipt,
  RegistrationVerification,
  ResourceVersion,
  Workstream,
  WorkstreamAction,
} from "./state.ts";
import { requireDomain, WorkstreamDomainError, registryError } from "./errors.ts";
import {
  canonicalJson,
  validateCommand,
  validText,
  validatePrIdentity,
  validTimestamp,
  validateExternalValue,
} from "./validation.ts";
import { requireNativeAdmission, requireNativeResult } from "./externalIntents.ts";
import { matchingPrCache, observationCache, publicPrObservation } from "./externalIntents.ts";

import type { WorkstreamDomainState, DomainCommandResult, ExternalIntent } from "./state.ts";
import type { DomainIdSource } from "./ids.ts";
import { requireAuthorization } from "./validation.ts";
function emptyEffects(): Effects {
  return {
    workstream_versions: [],
    native_reference_id: null,
    membership_ids: [],
    declaration_id: null,
    declaration_revision: null,
    edge_id: null,
    observation: null,
    registration: null,
    lifecycle_declaration: null,
    coordination_disposition: null,
    native_settlement: null,
  };
}

function activeMemberships(state: WorkstreamDomainState, referenceId: string): MembershipEpisode[] {
  return [...state.memberships.values()].filter(
    (item) => item.native_reference_id === referenceId && item.closed === null,
  );
}

function getWorkstream(
  state: WorkstreamDomainState,
  id: string,
  ownerId: string,
  expectedVersion?: number,
): Workstream {
  const value = state.workstreams.get(id);
  requireDomain(value?.owner_id === ownerId, "not_found");
  if (expectedVersion !== undefined)
    requireDomain(value.version === expectedVersion, "version_conflict");
  return value;
}

function getReference(state: WorkstreamDomainState, id: string, ownerId: string): NativeReference {
  const value = state.references.get(id);
  requireDomain(value?.owner_id === ownerId, "not_found");
  return value;
}

export function requireAttested(
  reference: NativeReference,
  now: string,
  binding: RegistrySourceBinding | undefined,
): void {
  requireDomain(reference.registration.state === "attested", "registration_unverified");
  const registration = reference.registration;
  requireDomain(
    validTimestamp(now) &&
      validTimestamp(registration.attested_at) &&
      validTimestamp(registration.expires_at) &&
      Date.parse(registration.attested_at) <= Date.parse(now) &&
      Date.parse(registration.expires_at) > Date.parse(now),
    "registration_unverified",
  );
  requireDomain(
    binding?.state === "active" &&
      registration.evidence !== null &&
      binding.provider === reference.identity.provider &&
      binding.source_instance_id === reference.identity.source_instance_id &&
      registration.evidence.provider === binding.provider &&
      registration.evidence.source_instance_id === binding.source_instance_id &&
      registration.evidence.native_id === reference.identity.native_id &&
      registration.evidence.authority_namespace === binding.authority_namespace &&
      registration.evidence.store_generation === binding.store_generation,
    "registration_unverified",
  );
}

function bump(workstream: Workstream, at: string, actor: Workstream["updated_by"]): Workstream {
  requireDomain(workstream.version < Number.MAX_SAFE_INTEGER, "version_conflict");
  return { ...workstream, version: workstream.version + 1, updated_at: at, updated_by: actor };
}

function resource(value: Workstream): ResourceVersion {
  return { workstream_id: value.workstream_id, version: value.version };
}

function validateName(name: string): void {
  requireDomain(validText(name, 160), "invalid_request");
}

function validateSortOrder(sortOrder: number): void {
  requireDomain(
    Number.isSafeInteger(sortOrder) && sortOrder >= 0 && sortOrder <= 2_147_483_647,
    "invalid_request",
  );
}

function sameIdentity(
  left: NativeReference["identity"],
  right: NativeReference["identity"],
): boolean {
  if (
    left.provider !== right.provider ||
    left.source_instance_id !== right.source_instance_id ||
    left.resource_kind !== right.resource_kind ||
    left.id_kind !== right.id_kind ||
    left.native_id !== right.native_id ||
    left.account_provenance.kind !== right.account_provenance.kind
  )
    return false;
  return (
    left.account_provenance.kind === "not_account_scoped" ||
    (right.account_provenance.kind === "account" &&
      left.account_provenance.account_id === right.account_provenance.account_id &&
      left.account_provenance.account_namespace === right.account_provenance.account_namespace)
  );
}

function validateProgress(action: WorkstreamAction & { progress?: Workstream["progress"] }): void {
  if (!action.progress) return;
  if (action.progress.state === "waiting")
    requireDomain(validText(action.progress.condition, 4096), "invalid_request");
  if (action.progress.state === "blocked")
    requireDomain(validText(action.progress.impediment, 4096), "invalid_request");
}

function validateExplicitPr(reference: NativeReference): void {
  validatePrIdentity(reference.identity, reference.pr_locator);
}

function appendLifecycle(
  state: WorkstreamDomainState,
  ids: DomainIdSource,
  input: RegistryCommandInput,
  workstream: Workstream,
  prior: Workstream["lifecycle"] | null,
): LifecycleDeclaration {
  const history = state.lifecycle_declarations.get(workstream.workstream_id) ?? [];
  const previous = history.at(-1) ?? null;
  requireDomain((previous?.revision ?? 0) < Number.MAX_SAFE_INTEGER, "version_conflict");
  const declaration: LifecycleDeclaration = {
    lifecycle_declaration_id: ids.next("declaration"),
    workstream_id: workstream.workstream_id,
    revision: (previous?.revision ?? 0) + 1,
    prior_lifecycle: prior,
    new_lifecycle: workstream.lifecycle,
    recorded_at: input.accepted_at,
    actor: input.authorization.actor,
    command_id: input.command.command_id,
    supersedes_lifecycle_declaration_id: previous?.lifecycle_declaration_id ?? null,
    registry_version: state.registry_version + 1,
  };
  state.lifecycle_declarations.set(workstream.workstream_id, [...history, declaration]);
  return declaration;
}

function hasPath(state: WorkstreamDomainState, from: string, to: string): boolean {
  if (state.edge_path_exists) return true;
  const seen = new Set<string>();
  const visit = (current: string): boolean => {
    if (current === to) return true;
    if (seen.has(current)) return false;
    seen.add(current);
    return [...state.edges.values()]
      .filter((edge) => edge.closed === null && edge.from_workstream_id === current)
      .some((edge) => visit(edge.to_workstream_id));
  };
  return visit(from);
}

function makeReceipt(
  input: RegistryCommandInput,
  state: WorkstreamDomainState,
  effects: Effects,
  changed = true,
): CommittedReceipt {
  return {
    state: "committed",
    command_id: input.command.command_id,
    owner_id: input.authorization.owner_id,
    actor: input.authorization.actor,
    operation: input.command.action.operation,
    request_sha256: input.request_sha256,
    server_generation: state.server_generation,
    accepted_at: input.accepted_at,
    completed_at: input.accepted_at,
    registry_version: state.registry_version,
    changed,
    effects,
  };
}

function appendAudit(
  state: WorkstreamDomainState,
  ids: DomainIdSource,
  receipt: CommittedReceipt,
): void {
  state.audit.push({
    event_id: ids.next("event"),
    command_id: receipt.command_id,
    actor: receipt.actor,
    operation: receipt.operation,
    occurred_at: receipt.completed_at,
    registry_version: receipt.registry_version,
    changed: receipt.changed,
    workstream_versions: receipt.effects.workstream_versions,
    native_reference_id: receipt.effects.native_reference_id,
    membership_ids: receipt.effects.membership_ids,
    declaration_id: receipt.effects.declaration_id,
    declaration_revision: receipt.effects.declaration_revision,
    edge_id: receipt.effects.edge_id,
    lifecycle_declaration: receipt.effects.lifecycle_declaration,
  });
}

function admitDomainCommand(
  state: WorkstreamDomainState,
  input: RegistryCommandInput,
  ids: DomainIdSource,
  now: string,
): DomainCommandResult {
  requireAuthorization(
    input.authorization,
    input.current_grant,
    "workstreams:write",
    state.owner_id,
  );
  validateCommand(input.command);
  requireDomain(
    /^[a-f0-9]{64}$/.test(input.request_sha256) &&
      validTimestamp(input.accepted_at) &&
      validTimestamp(now),
    "invalid_request",
  );
  requireDomain(input.authorization.owner_id.length > 0, "forbidden");
  requireDomain(
    input.authorization.server_generation === state.server_generation,
    "generation_conflict",
  );
  requireDomain(
    input.command.expected_server_generation === state.server_generation,
    "generation_conflict",
  );
  const replay =
    state.receipts.get(input.command.command_id) ??
    state.legacy_receipts.get(input.command.command_id);
  if (replay) {
    requireDomain(
      replay.owner_id === input.authorization.owner_id &&
        replay.actor.principal_id === input.authorization.actor.principal_id &&
        replay.request_sha256 === input.request_sha256,
      "idempotency_conflict",
    );
    if (replay.state === "committed")
      return { state: replay.changed ? "committed_change" : "committed_noop", receipt: replay };
    if (replay.state === "rejected") return { state: "rejected", receipt: replay };
    if (replay.state === "pending" || replay.state === "unresolved")
      return { state: "pending", receipt: replay };
    throw new WorkstreamDomainError("idempotency_conflict");
  }
  requireDomain(
    state.receipt_archive_state === "native_only" || state.receipt_archive_state === "complete",
    "unknown_effect",
  );
  const before = structuredClone(state);
  try {
    return decideCommand(state, input, ids, now);
  } catch (error) {
    if (!(error instanceof WorkstreamDomainError)) throw error;
    Object.assign(state, before);
    return { state: "rejected", receipt: rejectCommand(state, input, ids, error.code) };
  }
}

function rejectCommand(
  state: WorkstreamDomainState,
  input: RegistryCommandInput,
  ids: DomainIdSource,
  code: WorkstreamDomainError["code"],
): RejectedReceipt {
  const base = makeReceipt(input, state, emptyEffects(), false);
  const { registry_version: _version, changed: _changed, effects: _effects, ...receiptBase } = base;
  const receipt: RejectedReceipt = {
    ...receiptBase,
    state: "rejected",
    error: registryError(code, input.command.command_id),
  };
  state.receipts.set(input.command.command_id, receipt);
  appendAudit(state, ids, base);
  return receipt;
}

function decideCommand(
  state: WorkstreamDomainState,
  input: RegistryCommandInput,
  ids: DomainIdSource,
  now: string,
): DomainCommandResult {
  requireDomain(
    input.command.expected_registry_version === state.registry_version,
    "version_conflict",
  );

  const action = input.command.action;
  let effects = emptyEffects();
  let lifecycleDeclaration: LifecycleDeclaration | null = null;
  let changed = true;
  const attest = (reference: NativeReference) =>
    requireAttested(
      reference,
      now,
      state.source_bindings.get(reference.identity.source_instance_id),
    );

  if (
    action.operation === "verify_reference" ||
    action.operation === "request_native_t3_settlement" ||
    action.operation === "refresh_linked_pr"
  ) {
    const refreshMembership =
      action.operation === "refresh_linked_pr"
        ? (state.memberships.get(action.membership_id) ?? null)
        : null;
    const reference = getReference(
      state,
      action.operation === "refresh_linked_pr"
        ? (refreshMembership?.native_reference_id ?? "")
        : action.native_reference_id,
      input.authorization.owner_id,
    );
    if (action.operation !== "refresh_linked_pr") {
      requireDomain(
        reference.registration.attestation_version === action.expected_attestation_version,
        "version_conflict",
      );
    }
    let membership: MembershipEpisode | null = null;
    requireDomain(
      state.active_reservation === null &&
        ![...state.external_intents.values()].some(
          (item) =>
            item.reference.native_reference_id === reference.native_reference_id &&
            ["pending", "unresolved"].includes(
              state.receipts.get(item.command.command_id)?.state ?? "",
            ),
        ),
      "backpressure",
    );
    const sourceBinding = state.source_bindings.get(reference.identity.source_instance_id);
    requireDomain(sourceBinding?.state === "active", "registration_unverified");
    if (action.operation === "request_native_t3_settlement") {
      attest(reference);
      requireDomain(
        reference.identity.provider === "t3" && reference.identity.resource_kind === "thread",
        "identity_conflict",
      );
    }
    if (action.operation === "refresh_linked_pr") {
      attest(reference);
      requireDomain(reference.pr_locator !== null, "identity_conflict");
      membership = refreshMembership;
      requireDomain(
        membership?.workstream_id === action.workstream_id &&
          membership.native_reference_id === reference.native_reference_id &&
          membership.closed === null,
        "membership_conflict",
      );
      getWorkstream(
        state,
        action.workstream_id,
        input.authorization.owner_id,
        action.expected_version,
      );
      const previous = state.observations.get(reference.native_reference_id)?.at(-1) ?? null;
      requireDomain(
        (previous?.observation_version ?? 0) === action.expected_observation_version,
        "version_conflict",
      );
    }
    const intent: ExternalIntent = {
      kind:
        action.operation === "verify_reference"
          ? "verify_reference"
          : action.operation === "request_native_t3_settlement"
            ? "native_t3_settlement"
            : "refresh_linked_pr",
      command: input.command,
      owner_id: input.authorization.owner_id,
      actor: input.authorization.actor,
      authorization: input.authorization,
      source_binding: sourceBinding,
      request_sha256: input.request_sha256,
      accepted_at: input.accepted_at,
      admitted_registry_version: state.registry_version,
      reference,
      membership,
      previous_observation: state.observations.get(reference.native_reference_id)?.at(-1) ?? null,
    };
    state.external_intents.set(input.command.command_id, intent);
    state.receipts.set(input.command.command_id, {
      state: "pending",
      command_id: input.command.command_id,
      owner_id: input.authorization.owner_id,
      actor: input.authorization.actor,
      operation: action.operation,
      request_sha256: input.request_sha256,
      server_generation: state.server_generation,
      accepted_at: input.accepted_at,
      retry_after_seconds: 1,
    });
    return {
      state: "external",
      intent,
    };
  }

  switch (action.operation) {
    case "create_workstream": {
      validateName(action.name);
      validateSortOrder(action.sort_order);
      validateProgress(action);
      const id = ids.next("workstream");
      const value: Workstream = {
        workstream_id: id,
        owner_id: input.authorization.owner_id,
        name: action.name,
        lifecycle: action.lifecycle,
        progress: action.progress,
        delivery: "none-observed",
        freshness: "unknown",
        sort_order: action.sort_order,
        version: 1,
        created_at: input.accepted_at,
        updated_at: input.accepted_at,
        created_by: input.authorization.actor,
        updated_by: input.authorization.actor,
      };
      state.workstreams.set(id, value);
      lifecycleDeclaration = appendLifecycle(state, ids, input, value, null);
      effects = {
        ...effects,
        workstream_versions: [resource(value)],
        lifecycle_declaration: lifecycleDeclaration,
      };
      break;
    }
    case "update_workstream": {
      validateName(action.name);
      validateSortOrder(action.sort_order);
      validateProgress(action);
      const prior = getWorkstream(
        state,
        action.workstream_id,
        input.authorization.owner_id,
        action.expected_version,
      );
      if (
        prior.name === action.name &&
        prior.lifecycle === action.lifecycle &&
        canonicalJson(prior.progress) === canonicalJson(action.progress) &&
        prior.sort_order === action.sort_order
      ) {
        effects = { ...effects, workstream_versions: [resource(prior)] };
        changed = false;
        break;
      }
      const value: Workstream = {
        ...bump(prior, input.accepted_at, input.authorization.actor),
        name: action.name,
        lifecycle: action.lifecycle,
        progress: action.progress,
        sort_order: action.sort_order,
      };
      state.workstreams.set(value.workstream_id, value);
      if (prior.lifecycle !== value.lifecycle)
        lifecycleDeclaration = appendLifecycle(state, ids, input, value, prior.lifecycle);
      effects = {
        ...effects,
        workstream_versions: [resource(value)],
        lifecycle_declaration: lifecycleDeclaration,
      };
      break;
    }
    case "register_reference": {
      const existing = [...state.references.values()].find((reference) =>
        sameIdentity(reference.identity, action.identity),
      );
      if (existing) {
        requireDomain(
          existing.registration.state === "attested" ||
            existing.created_by.principal_id === input.authorization.actor.principal_id,
          "identity_conflict",
        );
        requireDomain(
          canonicalJson(existing.pr_locator) === canonicalJson(action.pr_locator),
          "identity_conflict",
        );
        effects = {
          ...effects,
          native_reference_id: existing.native_reference_id,
          registration: existing.registration,
        };
        changed = false;
        break;
      }
      const id = ids.next("native_reference");
      const registration: RegistrationVerification = {
        state: "quarantined",
        attestation_version: 0,
        attested_at: null,
        expires_at: null,
        evidence: null,
      };
      const reference: NativeReference = {
        native_reference_id: id,
        owner_id: input.authorization.owner_id,
        identity: action.identity,
        pr_locator: action.pr_locator,
        registration,
        created_at: input.accepted_at,
        created_by: input.authorization.actor,
        created_registry_version: state.registry_version + 1,
      };
      validateExplicitPr(reference);
      state.references.set(id, reference);
      effects = { ...effects, native_reference_id: id, registration };
      break;
    }
    case "attach_primary":
    case "reattach_primary":
    case "link_secondary": {
      const workstream = getWorkstream(
        state,
        action.workstream_id,
        input.authorization.owner_id,
        action.expected_version,
      );
      const reference = getReference(
        state,
        action.native_reference_id,
        input.authorization.owner_id,
      );
      attest(reference);
      const active = activeMemberships(state, reference.native_reference_id);
      const kind = action.operation === "link_secondary" ? "secondary" : "primary";
      const existingSecondary = active.find(
        (item) => item.workstream_id === workstream.workstream_id && item.kind === "secondary",
      );
      if (kind === "secondary" && existingSecondary) {
        effects = {
          ...effects,
          workstream_versions: [resource(workstream)],
          native_reference_id: reference.native_reference_id,
          membership_ids: [existingSecondary.membership_id],
        };
        changed = false;
        break;
      }
      const priorPrimary = [...state.memberships.values()].some(
        (item) =>
          item.workstream_id === workstream.workstream_id &&
          item.native_reference_id === reference.native_reference_id &&
          item.kind === "primary" &&
          item.closed !== null,
      );
      if (action.operation === "reattach_primary")
        requireDomain(priorPrimary, "membership_conflict");
      if (action.operation === "attach_primary")
        requireDomain(!priorPrimary, "membership_conflict");
      if (kind === "primary")
        requireDomain(
          !active.some((membership) => membership.kind === "primary"),
          "membership_conflict",
        );
      requireDomain(
        !active.some((membership) => membership.workstream_id === workstream.workstream_id),
        "membership_conflict",
      );
      const membership: MembershipEpisode = {
        membership_id: ids.next("membership"),
        workstream_id: workstream.workstream_id,
        native_reference_id: reference.native_reference_id,
        kind,
        opened: {
          at: input.accepted_at,
          actor: input.authorization.actor,
          command_id: input.command.command_id,
          registry_version: state.registry_version + 1,
        },
        closed: null,
      };
      state.memberships.set(membership.membership_id, membership);
      const updated = bump(workstream, input.accepted_at, input.authorization.actor);
      state.workstreams.set(updated.workstream_id, updated);
      effects = {
        ...effects,
        workstream_versions: [resource(updated)],
        native_reference_id: reference.native_reference_id,
        membership_ids: [membership.membership_id],
      };
      break;
    }
    case "remove_membership":
    case "set_coordination_disposition": {
      const workstream = getWorkstream(
        state,
        action.workstream_id,
        input.authorization.owner_id,
        action.expected_version,
      );
      const membership = state.memberships.get(action.membership_id);
      requireDomain(
        membership?.workstream_id === workstream.workstream_id && membership.closed === null,
        "membership_conflict",
      );
      attest(getReference(state, membership.native_reference_id, input.authorization.owner_id));
      const reason = action.operation === "remove_membership" ? "removed" : action.disposition;
      const other =
        action.operation === "set_coordination_disposition" ? action.other_disposition : null;
      requireDomain(
        (reason === "other") === (typeof other === "string" && other.trim().length > 0),
        "invalid_request",
      );
      requireDomain(other === null || validText(other, 4096), "invalid_request");
      const closed: MembershipEpisode = {
        ...membership,
        closed: {
          at: input.accepted_at,
          actor: input.authorization.actor,
          command_id: input.command.command_id,
          registry_version: state.registry_version + 1,
          reason,
          other_reason: other,
        },
      };
      state.memberships.set(closed.membership_id, closed);
      const updated = bump(workstream, input.accepted_at, input.authorization.actor);
      state.workstreams.set(updated.workstream_id, updated);
      effects = {
        ...effects,
        workstream_versions: [resource(updated)],
        native_reference_id: membership.native_reference_id,
        membership_ids: [membership.membership_id],
        coordination_disposition:
          action.operation === "set_coordination_disposition"
            ? { membership_id: membership.membership_id, disposition: action.disposition }
            : null,
      };
      break;
    }
    case "move_primary": {
      requireDomain(
        action.source_workstream_id !== action.destination_workstream_id,
        "membership_conflict",
      );
      const source = getWorkstream(
        state,
        action.source_workstream_id,
        input.authorization.owner_id,
        action.expected_source_version,
      );
      const destination = getWorkstream(
        state,
        action.destination_workstream_id,
        input.authorization.owner_id,
        action.expected_destination_version,
      );
      const prior = state.memberships.get(action.source_membership_id);
      requireDomain(
        prior?.workstream_id === source.workstream_id &&
          prior.kind === "primary" &&
          prior.closed === null,
        "membership_conflict",
      );
      attest(getReference(state, prior.native_reference_id, input.authorization.owner_id));
      requireDomain(
        !activeMemberships(state, prior.native_reference_id).some(
          (item) => item.kind === "primary" && item.membership_id !== prior.membership_id,
        ),
        "membership_conflict",
      );
      requireDomain(
        !activeMemberships(state, prior.native_reference_id).some(
          (item) => item.workstream_id === destination.workstream_id,
        ),
        "membership_conflict",
      );
      state.memberships.set(prior.membership_id, {
        ...prior,
        closed: {
          at: input.accepted_at,
          actor: input.authorization.actor,
          command_id: input.command.command_id,
          registry_version: state.registry_version + 1,
          reason: "continued-elsewhere",
          other_reason: null,
        },
      });
      const next: MembershipEpisode = {
        membership_id: ids.next("membership"),
        workstream_id: destination.workstream_id,
        native_reference_id: prior.native_reference_id,
        kind: "primary",
        opened: {
          at: input.accepted_at,
          actor: input.authorization.actor,
          command_id: input.command.command_id,
          registry_version: state.registry_version + 1,
        },
        closed: null,
      };
      state.memberships.set(next.membership_id, next);
      const updatedSource = bump(source, input.accepted_at, input.authorization.actor);
      const updatedDestination = bump(destination, input.accepted_at, input.authorization.actor);
      state.workstreams.set(source.workstream_id, updatedSource);
      state.workstreams.set(destination.workstream_id, updatedDestination);
      effects = {
        ...effects,
        workstream_versions: [resource(updatedSource), resource(updatedDestination)],
        native_reference_id: prior.native_reference_id,
        membership_ids: [prior.membership_id, next.membership_id],
      };
      break;
    }
    case "set_declaration": {
      const workstream = getWorkstream(
        state,
        action.workstream_id,
        input.authorization.owner_id,
        action.expected_version,
      );
      requireDomain(validText(action.text, 4096), "invalid_request");
      const id = action.declaration_id ?? ids.next("declaration");
      const history = state.declarations.get(id) ?? [];
      requireDomain(
        (history.at(-1)?.revision ?? 0) === action.expected_revision,
        "version_conflict",
      );
      requireDomain(
        history.length === 0 || history[0]?.workstream_id === workstream.workstream_id,
        "not_found",
      );
      const previous = history.at(-1);
      if (previous?.state === "active" && previous.text === action.text) {
        effects = {
          ...effects,
          workstream_versions: [resource(workstream)],
          declaration_id: id,
          declaration_revision: previous.revision,
        };
        changed = false;
        break;
      }
      requireDomain((previous?.revision ?? 0) < Number.MAX_SAFE_INTEGER, "version_conflict");
      const revision: DeclarationRevision = {
        declaration_id: id,
        workstream_id: workstream.workstream_id,
        revision: (previous?.revision ?? 0) + 1,
        text: action.text,
        state: "active",
        recorded_at: input.accepted_at,
        actor: input.authorization.actor,
        command_id: input.command.command_id,
        registry_version: state.registry_version + 1,
      };
      state.declarations.set(id, [...history, revision]);
      const updated = bump(workstream, input.accepted_at, input.authorization.actor);
      state.workstreams.set(updated.workstream_id, updated);
      effects = {
        ...effects,
        workstream_versions: [resource(updated)],
        declaration_id: id,
        declaration_revision: revision.revision,
      };
      break;
    }
    case "withdraw_declaration": {
      const workstream = getWorkstream(
        state,
        action.workstream_id,
        input.authorization.owner_id,
        action.expected_version,
      );
      const history = state.declarations.get(action.declaration_id) ?? [];
      requireDomain(history.at(-1)?.revision === action.expected_revision, "version_conflict");
      const previous = history.at(-1);
      requireDomain(previous?.workstream_id === workstream.workstream_id, "not_found");
      if (previous.state === "withdrawn") {
        effects = {
          ...effects,
          workstream_versions: [resource(workstream)],
          declaration_id: previous.declaration_id,
          declaration_revision: previous.revision,
        };
        changed = false;
        break;
      }
      requireDomain(previous.revision < Number.MAX_SAFE_INTEGER, "version_conflict");
      const revision: DeclarationRevision = {
        ...previous,
        revision: previous.revision + 1,
        state: "withdrawn",
        recorded_at: input.accepted_at,
        actor: input.authorization.actor,
        command_id: input.command.command_id,
        registry_version: state.registry_version + 1,
      };
      state.declarations.set(action.declaration_id, [...history, revision]);
      const updated = bump(workstream, input.accepted_at, input.authorization.actor);
      state.workstreams.set(updated.workstream_id, updated);
      effects = {
        ...effects,
        workstream_versions: [resource(updated)],
        declaration_id: action.declaration_id,
        declaration_revision: revision.revision,
      };
      break;
    }
    case "add_edge": {
      requireDomain(action.from_workstream_id !== action.to_workstream_id, "relationship_conflict");
      const from = getWorkstream(
        state,
        action.from_workstream_id,
        input.authorization.owner_id,
        action.expected_from_version,
      );
      const to = getWorkstream(
        state,
        action.to_workstream_id,
        input.authorization.owner_id,
        action.expected_to_version,
      );
      const existing = [...state.edges.values()].find(
        (edge) =>
          edge.closed === null &&
          edge.from_workstream_id === from.workstream_id &&
          edge.to_workstream_id === to.workstream_id &&
          edge.relation === action.relation,
      );
      if (existing) {
        effects = {
          ...effects,
          workstream_versions: [resource(from), resource(to)],
          edge_id: existing.edge_id,
        };
        changed = false;
        break;
      }
      requireDomain(!hasPath(state, to.workstream_id, from.workstream_id), "cycle_conflict");
      const edge: Edge = {
        edge_id: ids.next("edge"),
        from_workstream_id: from.workstream_id,
        to_workstream_id: to.workstream_id,
        relation: action.relation,
        opened: {
          at: input.accepted_at,
          actor: input.authorization.actor,
          command_id: input.command.command_id,
          registry_version: state.registry_version + 1,
        },
        closed: null,
      };
      state.edges.set(edge.edge_id, edge);
      const updatedFrom = bump(from, input.accepted_at, input.authorization.actor);
      const updatedTo = bump(to, input.accepted_at, input.authorization.actor);
      state.workstreams.set(from.workstream_id, updatedFrom);
      state.workstreams.set(to.workstream_id, updatedTo);
      effects = {
        ...effects,
        workstream_versions: [resource(updatedFrom), resource(updatedTo)],
        edge_id: edge.edge_id,
      };
      break;
    }
    case "remove_edge": {
      const from = getWorkstream(
        state,
        action.from_workstream_id,
        input.authorization.owner_id,
        action.expected_from_version,
      );
      const to = getWorkstream(
        state,
        action.to_workstream_id,
        input.authorization.owner_id,
        action.expected_to_version,
      );
      const edge = state.edges.get(action.edge_id);
      requireDomain(
        edge?.closed === null &&
          edge.from_workstream_id === from.workstream_id &&
          edge.to_workstream_id === to.workstream_id,
        "relationship_conflict",
      );
      state.edges.set(edge.edge_id, {
        ...edge,
        closed: {
          at: input.accepted_at,
          actor: input.authorization.actor,
          command_id: input.command.command_id,
          registry_version: state.registry_version + 1,
        },
      });
      const updatedFrom = bump(from, input.accepted_at, input.authorization.actor);
      const updatedTo = bump(to, input.accepted_at, input.authorization.actor);
      state.workstreams.set(from.workstream_id, updatedFrom);
      state.workstreams.set(to.workstream_id, updatedTo);
      effects = {
        ...effects,
        workstream_versions: [resource(updatedFrom), resource(updatedTo)],
        edge_id: edge.edge_id,
      };
      break;
    }
    default:
      throw new WorkstreamDomainError("invalid_request");
  }

  if (changed) {
    requireDomain(state.registry_version < Number.MAX_SAFE_INTEGER, "version_conflict");
    state.registry_version += 1;
  }
  if (lifecycleDeclaration) {
    const history = state.lifecycle_declarations.get(lifecycleDeclaration.workstream_id) ?? [];
    const corrected = { ...lifecycleDeclaration, registry_version: state.registry_version };
    state.lifecycle_declarations.set(lifecycleDeclaration.workstream_id, [
      ...history.slice(0, -1),
      corrected,
    ]);
    effects = { ...effects, lifecycle_declaration: corrected };
  }
  const receipt = makeReceipt(input, state, effects, changed);
  state.receipts.set(input.command.command_id, receipt);
  appendAudit(state, ids, receipt);
  return { state: changed ? "committed_change" : "committed_noop", receipt };
}

function completeExternalIntent(
  state: WorkstreamDomainState,
  intent: ExternalIntent,
  ids: DomainIdSource,
  result: RegistryExternalResult,
  completedAt: string,
): CommittedReceipt {
  const existing = state.receipts.get(intent.command.command_id);
  if (existing?.state === "committed") return existing;
  requireDomain(
    state.server_generation === intent.authorization.server_generation,
    "generation_conflict",
  );
  requireDomain(state.registry_version === intent.admitted_registry_version, "version_conflict");
  const action = intent.command.action;
  let effects = emptyEffects();
  const currentReference = getReference(
    state,
    intent.reference.native_reference_id,
    intent.owner_id,
  );
  requireDomain(
    canonicalJson(currentReference) === canonicalJson(intent.reference),
    "version_conflict",
  );
  const binding = state.source_bindings.get(currentReference.identity.source_instance_id);
  requireDomain(
    canonicalJson(binding) === canonicalJson(intent.source_binding) && binding?.state === "active",
    "registration_unverified",
  );
  if (intent.kind !== "verify_reference") requireAttested(currentReference, completedAt, binding);
  if (intent.kind === "verify_reference" && action.operation === "verify_reference") {
    const current = getReference(state, intent.reference.native_reference_id, intent.owner_id);
    requireDomain(
      current.registration.attestation_version === action.expected_attestation_version,
      "version_conflict",
    );
    requireDomain(result.registration, "unknown_effect");
    validateExternalValue("registration", result.registration);
    if (result.registration.state === "attested") {
      requireDomain(
        result.registration.evidence?.provider === current.identity.provider,
        "identity_conflict",
      );
      requireDomain(
        result.registration.evidence.source_instance_id === current.identity.source_instance_id,
        "identity_conflict",
      );
      requireDomain(
        result.registration.evidence.native_id === current.identity.native_id,
        "identity_conflict",
      );
      requireAttested({ ...current, registration: result.registration }, completedAt, binding);
    }
    requireDomain(
      result.registration.attestation_version === action.expected_attestation_version + 1,
      "version_conflict",
    );
    state.references.set(current.native_reference_id, {
      ...current,
      registration: result.registration,
    });
    effects = {
      ...effects,
      native_reference_id: current.native_reference_id,
      registration: result.registration,
    };
  } else if (intent.kind === "refresh_linked_pr" && action.operation === "refresh_linked_pr") {
    const currentMembership = state.memberships.get(action.membership_id);
    requireDomain(
      currentMembership?.closed === null &&
        currentMembership.workstream_id === action.workstream_id &&
        canonicalJson(currentMembership) === canonicalJson(intent.membership),
      "membership_conflict",
    );
    const workstream = getWorkstream(
      state,
      action.workstream_id,
      intent.owner_id,
      action.expected_version,
    );
    requireDomain(result.observation, "unknown_effect");
    validateExternalValue("observation", result.observation);
    const history = state.observations.get(intent.reference.native_reference_id) ?? [];
    requireDomain(
      (history.at(-1)?.observation_version ?? 0) === action.expected_observation_version,
      "version_conflict",
    );
    requireDomain(
      result.observation.native_reference_id === currentReference.native_reference_id &&
        result.observation.command_id === intent.command.command_id &&
        result.observation.observation_version === action.expected_observation_version + 1,
      "identity_conflict",
    );
    const observation = publicPrObservation(result.observation);
    const previous = matchingPrCache(
      intent.previous_observation,
      currentReference,
      intent.authorization.server_generation,
      intent.source_binding,
    );
    const previousCache =
      previous?.observation_version === action.expected_observation_version ? previous : null;
    let cache = matchingPrCache(
      { ...observation, observation_cache: result.observation_cache ?? null },
      currentReference,
      intent.authorization.server_generation,
      intent.source_binding,
    );
    if (observation.outcome === "not_modified") {
      requireDomain(
        previousCache !== null &&
          observation.last_success !== null &&
          canonicalJson({
            ...observation.last_success,
            observed_at: previousCache.representation.observed_at,
          }) === canonicalJson(previousCache.representation),
        "unknown_effect",
      );
      cache ??= observationCache(intent, observation, previousCache.etag);
    } else if (observation.outcome !== "observed") {
      cache =
        previousCache &&
        canonicalJson(observation.last_success) === canonicalJson(previousCache.representation)
          ? observationCache(intent, observation, previousCache.etag)
          : null;
    }
    state.observations.set(intent.reference.native_reference_id, [
      ...history,
      { ...observation, observation_cache: cache },
    ]);
    effects = {
      ...effects,
      workstream_versions: [resource(workstream)],
      native_reference_id: intent.reference.native_reference_id,
      membership_ids: [action.membership_id],
      observation,
    };
  } else if (
    intent.kind === "native_t3_settlement" &&
    action.operation === "request_native_t3_settlement"
  ) {
    const current = getReference(state, action.native_reference_id, intent.owner_id);
    requireDomain(
      current.registration.attestation_version === action.expected_attestation_version,
      "version_conflict",
    );
    effects = {
      ...effects,
      native_reference_id: current.native_reference_id,
      native_settlement: requireNativeResult(intent, result),
    };
  } else throw new WorkstreamDomainError("unknown_effect");
  const changed = intent.kind !== "native_t3_settlement" || result.native_outcome === "committed";
  if (changed) {
    requireDomain(state.registry_version < Number.MAX_SAFE_INTEGER, "version_conflict");
    state.registry_version += 1;
  }
  const input: RegistryCommandInput = {
    authorization: intent.authorization,
    current_grant: intent.authorization.grant,
    command: intent.command,
    request_sha256: intent.request_sha256,
    accepted_at: intent.accepted_at,
  };
  const receipt = { ...makeReceipt(input, state, effects, changed), completed_at: completedAt };
  state.receipts.set(intent.command.command_id, receipt);
  appendAudit(state, ids, receipt);
  return receipt;
}

function reconcileNativeIntent(
  state: WorkstreamDomainState,
  intent: ExternalIntent,
  ids: DomainIdSource,
  result: RegistryExternalResult,
  completedAt: string,
): CommittedReceipt {
  requireNativeAdmission(state.receipts.get(intent.command.command_id), intent);
  const effect = requireNativeResult(intent, result);
  const changed = effect.outcome === "committed";
  if (changed) {
    requireDomain(state.registry_version < Number.MAX_SAFE_INTEGER, "version_conflict");
    state.registry_version += 1;
  }
  // Recovery records an admitted attempt's observed outcome, not a new action on the current reference or binding.
  const input: RegistryCommandInput = {
    authorization: intent.authorization,
    current_grant: intent.authorization.grant,
    command: intent.command,
    request_sha256: intent.request_sha256,
    accepted_at: intent.accepted_at,
  };
  const receipt = {
    ...makeReceipt(
      input,
      state,
      {
        ...emptyEffects(),
        native_reference_id: effect.native_reference_id,
        native_settlement: effect,
      },
      changed,
    ),
    completed_at: completedAt,
  };
  state.receipts.set(intent.command.command_id, receipt);
  appendAudit(state, ids, receipt);
  return receipt;
}

/** Decide against a transaction snapshot; callers persist the returned state atomically before running any intent. */
export function decideNativeCommand(
  snapshot: WorkstreamDomainState,
  input: RegistryCommandInput,
  ids: DomainIdSource,
  now: string,
): { readonly state: WorkstreamDomainState; readonly result: DomainCommandResult } {
  const state = structuredClone(snapshot);
  const result = admitDomainCommand(state, structuredClone(input), ids, now);
  return { state, result };
}

/** Completion has no I/O; an unknown or stale result leaves the caller snapshot untouched. */
export function completeNativeExternalIntent(
  snapshot: WorkstreamDomainState,
  intent: ExternalIntent,
  ids: DomainIdSource,
  result: RegistryExternalResult,
  completedAt: string,
  recovery = false,
): { readonly state: WorkstreamDomainState; readonly receipt: CommittedReceipt } {
  const state = structuredClone(snapshot);
  requireDomain(
    intent.owner_id === state.owner_id &&
      validTimestamp(completedAt) &&
      Date.parse(completedAt) >= Date.parse(intent.accepted_at),
    "unknown_effect",
  );
  const prior = state.receipts.get(intent.command.command_id);
  requireDomain(
    prior &&
      prior.owner_id === intent.owner_id &&
      prior.request_sha256 === intent.request_sha256 &&
      prior.actor.principal_id === intent.actor.principal_id &&
      prior.operation === intent.command.action.operation &&
      prior.accepted_at === intent.accepted_at,
    "unknown_effect",
  );
  requireDomain(
    canonicalJson(state.external_intents.get(intent.command.command_id)) === canonicalJson(intent),
    "unknown_effect",
  );
  if (prior.state === "committed") return { state, receipt: prior };
  requireDomain(prior.state === "pending" || prior.state === "unresolved", "unknown_effect");
  const recorded = state.external_results.get(intent.command.command_id);
  requireDomain(!recorded || canonicalJson(recorded) === canonicalJson(result), "unknown_effect");
  const receipt = recovery
    ? reconcileNativeIntent(state, intent, ids, result, completedAt)
    : completeExternalIntent(state, intent, ids, result, completedAt);
  state.external_results.set(intent.command.command_id, structuredClone(result));
  if (state.active_reservation?.command.command_id === intent.command.command_id)
    state.active_reservation = null;
  return { state, receipt };
}
