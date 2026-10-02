import type {
  DeclarationRevision,
  LifecycleDeclaration,
  MembershipEpisode,
  NativeReferenceRecord,
  WorkstreamEdge,
  WorkstreamReceipt,
  WorkstreamRecord,
  WorkstreamState,
} from "./model.ts";

export interface WorkstreamMemberProjection {
  readonly episode: MembershipEpisode;
  readonly reference: NativeReferenceRecord | null;
}

export interface WorkstreamDetailProjection {
  readonly workstream: WorkstreamRecord;
  readonly activePrimary: ReadonlyArray<WorkstreamMemberProjection>;
  readonly activeSecondary: ReadonlyArray<WorkstreamMemberProjection>;
  readonly membershipHistory: ReadonlyArray<WorkstreamMemberProjection>;
  readonly lifecycleHistory: ReadonlyArray<LifecycleDeclaration>;
  readonly declarationHistory: ReadonlyArray<DeclarationRevision>;
  readonly relationships: ReadonlyArray<WorkstreamEdge>;
  readonly coordinationDispositionReceipts: ReadonlyArray<WorkstreamReceipt>;
  readonly nativeT3SettlementReceipts: ReadonlyArray<WorkstreamReceipt>;
}

function compareText(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

export function selectOrderedWorkstreams(state: WorkstreamState): ReadonlyArray<WorkstreamRecord> {
  return Object.values(state.workstreams).sort(
    (left, right) =>
      left.sortOrder - right.sortOrder || compareText(left.workstreamId, right.workstreamId),
  );
}

function compareEpisodes(left: MembershipEpisode, right: MembershipEpisode): number {
  return (
    left.opened.registryVersion - right.opened.registryVersion ||
    compareText(left.membershipId, right.membershipId)
  );
}

function compareReceipts(left: WorkstreamReceipt, right: WorkstreamReceipt): number {
  return (
    compareText(left.acceptedAt, right.acceptedAt) || compareText(left.commandId, right.commandId)
  );
}

export function projectWorkstreamDetail(
  state: WorkstreamState,
  workstreamId: string,
): WorkstreamDetailProjection | null {
  const workstream = state.workstreams[workstreamId];
  if (!workstream) {
    return null;
  }

  const membershipHistory = Object.values(state.memberships)
    .filter((episode) => episode.workstreamId === workstreamId)
    .sort(compareEpisodes)
    .map((episode) => ({
      episode,
      reference: state.nativeReferences[episode.nativeReferenceId] ?? null,
    }));
  const active = membershipHistory.filter(({ episode }) => episode.closed === null);

  return {
    workstream,
    activePrimary: active.filter(({ episode }) => episode.kind === "primary"),
    activeSecondary: active.filter(({ episode }) => episode.kind === "secondary"),
    membershipHistory,
    lifecycleHistory: Object.values(state.lifecycleDeclarations)
      .filter((declaration) => declaration.workstreamId === workstreamId)
      .sort(
        (left, right) =>
          left.revision - right.revision ||
          compareText(left.lifecycleDeclarationId, right.lifecycleDeclarationId),
      ),
    declarationHistory: Object.values(state.declarations)
      .filter((declaration) => declaration.workstreamId === workstreamId)
      .sort(
        (left, right) =>
          left.revision - right.revision || compareText(left.declarationId, right.declarationId),
      ),
    relationships: Object.values(state.edges)
      .filter(
        (edge) => edge.fromWorkstreamId === workstreamId || edge.toWorkstreamId === workstreamId,
      )
      .sort((left, right) => compareText(left.edgeId, right.edgeId)),
    coordinationDispositionReceipts: Object.values(state.receipts)
      .filter(
        (receipt) =>
          receipt.operation === "set_coordination_disposition" &&
          receipt.workstreamIds.includes(workstreamId),
      )
      .sort(compareReceipts),
    nativeT3SettlementReceipts: Object.values(state.receipts)
      .filter(
        (receipt) =>
          receipt.operation === "request_native_t3_settlement" &&
          receipt.workstreamIds.includes(workstreamId),
      )
      .sort(compareReceipts),
  };
}
