import type {
  DeclarationRevision,
  LifecycleDeclaration,
  MembershipEpisode,
  WorkstreamEdge,
  WorkstreamReadContext,
  WorkstreamReceipt,
  WorkstreamState,
  WorkstreamStateUpdate,
} from "./model.ts";

export type WorkstreamUpdateResult =
  | { readonly kind: "updated"; readonly state: WorkstreamState }
  | { readonly kind: "unchanged"; readonly state: WorkstreamState }
  | {
      readonly kind: "reset-required";
      readonly reason: "binding-changed" | "server-generation-changed";
      readonly state: WorkstreamState;
    };

function emptyRecord<T>(): Readonly<Record<string, T>> {
  return {};
}

export function createWorkstreamState(context: WorkstreamReadContext): WorkstreamState {
  return {
    context,
    workstreams: emptyRecord(),
    nativeReferences: emptyRecord(),
    memberships: emptyRecord(),
    lifecycleDeclarations: emptyRecord(),
    declarations: emptyRecord(),
    edges: emptyRecord(),
    receipts: emptyRecord(),
  };
}

function hasSameBinding(left: WorkstreamReadContext, right: WorkstreamReadContext): boolean {
  return (
    left.registryId === right.registryId &&
    left.ownerScopeId === right.ownerScopeId &&
    left.principalId === right.principalId &&
    left.authorizationRevision === right.authorizationRevision &&
    left.contractVersion === right.contractVersion &&
    left.contractManifest === right.contractManifest
  );
}

function mergeByKey<T>(
  current: Readonly<Record<string, T>>,
  values: ReadonlyArray<T> | undefined,
  keyOf: (value: T) => string,
  merge: (existing: T | undefined, incoming: T) => T = (_existing, incoming) => incoming,
): Readonly<Record<string, T>> {
  if (!values || values.length === 0) {
    return current;
  }
  const next = { ...current };
  for (const value of values) {
    const key = keyOf(value);
    next[key] = merge(current[key], value);
  }
  return next;
}

function declarationKey(value: DeclarationRevision): string {
  return `${value.declarationId}:${value.revision}`;
}

function preserveImmutable<T>(existing: T | undefined, incoming: T): T {
  return existing ?? incoming;
}

function preserveClosedEpisode(
  existing: MembershipEpisode | undefined,
  incoming: MembershipEpisode,
): MembershipEpisode {
  return existing?.closed && !incoming.closed ? existing : incoming;
}

/**
 * Merges one registry-version projection without deriving or mutating native
 * T3 state. Closed membership episodes and declaration revisions are only
 * replaced by the same identity; absence from a later page never deletes
 * history.
 */
export function applyWorkstreamUpdate(
  state: WorkstreamState,
  update: WorkstreamStateUpdate,
): WorkstreamUpdateResult {
  if (!hasSameBinding(state.context, update.context)) {
    return { kind: "reset-required", reason: "binding-changed", state };
  }
  if (state.context.serverGeneration !== update.context.serverGeneration) {
    return { kind: "reset-required", reason: "server-generation-changed", state };
  }
  if (update.context.registryVersion < state.context.registryVersion) {
    return { kind: "unchanged", state };
  }

  const next: WorkstreamState = {
    context: update.context,
    workstreams: mergeByKey(state.workstreams, update.workstreams, (item) => item.workstreamId),
    nativeReferences: mergeByKey(
      state.nativeReferences,
      update.nativeReferences,
      (item) => item.nativeReferenceId,
    ),
    memberships: mergeByKey(
      state.memberships,
      update.memberships,
      (item: MembershipEpisode) => item.membershipId,
      preserveClosedEpisode,
    ),
    lifecycleDeclarations: mergeByKey(
      state.lifecycleDeclarations,
      update.lifecycleDeclarations,
      (item: LifecycleDeclaration) => item.lifecycleDeclarationId,
      preserveImmutable,
    ),
    declarations: mergeByKey(
      state.declarations,
      update.declarations,
      declarationKey,
      preserveImmutable,
    ),
    edges: mergeByKey(state.edges, update.edges, (item: WorkstreamEdge) => item.edgeId),
    receipts: mergeByKey(
      state.receipts,
      update.receipts,
      (item: WorkstreamReceipt) => item.commandId,
    ),
  };
  return { kind: "updated", state: next };
}
