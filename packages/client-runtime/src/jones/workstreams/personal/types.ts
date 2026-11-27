import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type {
  PersonalWorkstreamLifecycle,
  PersonalWorkstreamAssignment,
  PersonalWorkstreamFleet,
  PersonalWorkstreamFleetProposal,
  PersonalWorkstreamMembers,
  PersonalWorkstreamOperationResult,
} from "@t3tools/contracts/jones/workstreamsPersonal";
import type { ManagePersonalWorkstreamFleetInput } from "@t3tools/contracts/jones/workstreamsPersonalRpc";
import type { Atom } from "effect/reactivity";

export type PersonalThreadRef = { readonly environmentId: EnvironmentId; readonly id: ThreadId };

export interface PersonalWorkstreamView {
  readonly id: string;
  readonly name: string;
  readonly lifecycle: PersonalWorkstreamLifecycle;
  readonly color: string | null;
  readonly nameIcon?: { readonly projectKey: string | null; readonly updatedAt: string };
  readonly orderKey: string;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly snoozedAt?: string | null;
  readonly snoozedUntil?: string | null;
}

export type PersonalEnvironmentStatus =
  | "ready"
  | "offline"
  | "update_required"
  | "read_only"
  | "sync_failed";

export interface PersonalEnvironmentState {
  readonly environmentId: EnvironmentId;
  readonly status: PersonalEnvironmentStatus;
  readonly confirmed: boolean; // false while showing a cached snapshot
  readonly workstreamCount: number; // live definitions on that environment
  /** Live definitions reported by this environment, including its cached snapshot when offline. */
  readonly workstreams?: readonly PersonalWorkstreamView[];
  /** Approval offers from the current session. Cached offers are omitted. */
  readonly fleetProposals?: readonly PersonalWorkstreamFleetProposal[];
  readonly followsAuthority?: EnvironmentId;
}

export interface PersonalWorkstreamsState {
  readonly workstreams: readonly PersonalWorkstreamView[]; // live definitions, sorted by (orderKey, id)
  readonly environments: readonly PersonalEnvironmentState[];
  readonly assignmentsByThreadKey: ReadonlyMap<string, PersonalWorkstreamAssignment>;
  readonly pendingThreadKeys: ReadonlySet<string>; // local actions in flight
  /**
   * Threads the server assigned automatically whose effective assignment (including a local
   * optimistic move) is still that exact assignment. Optional so hand-built states stay valid.
   */
  readonly autoAssignedThreadKeys?: ReadonlySet<string>;
  readonly canCreate: boolean; // some environment is writable
  readonly canSetNameIcon?: boolean;
  readonly canSnooze?: boolean;
  readonly fleet?: PersonalWorkstreamFleet;
  readonly fleetAuthorityStatus?: PersonalWorkstreamFleetAuthorityStatus;
  readonly pendingCloseWorkstreamIds?: ReadonlySet<string>;
}

export type PersonalWorkstreamFleetAuthorityStatus =
  | "ready"
  | "offline"
  | "read_only"
  | "update_required"
  | "unconfirmed"
  | "changed_elsewhere";

export interface PersonalWorkstreamsGrouping<T extends PersonalThreadRef> {
  readonly groups: readonly {
    readonly workstream: PersonalWorkstreamView;
    readonly threads: readonly T[];
  }[];
  readonly ungrouped: readonly T[];
  readonly workstreamIdByThreadKey: ReadonlyMap<string, string>;
}

export type PersonalWorkstreamMember = PersonalWorkstreamMembers[number] & PersonalThreadRef;

export interface PersonalWorkstreamHistory {
  readonly members: readonly PersonalWorkstreamMember[];
  readonly complete: boolean;
}

export type PersonalWorkstreamsAction =
  | "create"
  | "rename"
  | "recolor"
  | "nameIcon"
  | "snooze"
  | "unsnooze"
  | "lifecycle"
  | "reorder"
  | "delete"
  | "assign"
  | "unassign"
  | "cancelOperation"
  | "manageFleet"
  | "resolveOperation";
export type PersonalWorkstreamsFailureReason =
  | "offline"
  | "read_only"
  | "update_required"
  | "thread_not_found"
  | "workstream_deleted"
  | "workstream_closed"
  | "unsettled_threads"
  | "closure_unavailable"
  | "changed_elsewhere"
  | "unconfirmed"
  | "failed";
export interface PersonalWorkstreamsFailure {
  readonly action: PersonalWorkstreamsAction;
  readonly reason: PersonalWorkstreamsFailureReason;
  readonly environmentId: EnvironmentId | null;
  readonly dedupeKey: string; // `${action}:${target}:${reason}`
}
export type PersonalResult<A> =
  | { readonly ok: true; readonly value: A }
  | { readonly ok: false; readonly failure: PersonalWorkstreamsFailure };

export interface PersonalWorkstreamConflictChoice {
  readonly expectedObservationKey: string;
  readonly requestId: string;
  readonly environmentId: EnvironmentId;
  readonly choice: "use_authority" | "use_peer";
}

export interface PersonalWorkstreamsController {
  readonly state: Atom.Atom<PersonalWorkstreamsState>;
  readHistory?(id: string): Promise<PersonalWorkstreamHistory>;
  canAssign(thread: PersonalThreadRef): boolean;
  create(input: {
    name: string;
    nameIconProjectKey?: string;
  }): Promise<PersonalResult<PersonalWorkstreamView>>;
  rename(id: string, name: string): Promise<PersonalResult<PersonalWorkstreamView>>;
  setColor(id: string, color: string | null): Promise<PersonalResult<PersonalWorkstreamView>>;
  setNameIcon?(
    id: string,
    projectKey: string | null,
  ): Promise<PersonalResult<PersonalWorkstreamView>>;
  setLifecycle(
    id: string,
    lifecycle: PersonalWorkstreamLifecycle,
  ): Promise<PersonalResult<PersonalWorkstreamView>>;
  setSnoozedUntil?(
    id: string,
    until: string | null,
  ): Promise<PersonalResult<PersonalWorkstreamView>>;
  reorder(
    id: string,
    neighbors: { beforeId: string | null; afterId: string | null },
  ): Promise<PersonalResult<PersonalWorkstreamView>>;
  remove(id: string): Promise<PersonalResult<void>>;
  assign(thread: PersonalThreadRef, workstreamId: string | null): Promise<PersonalResult<void>>;
  createAndAssign(
    thread: PersonalThreadRef,
    name: string,
  ): Promise<PersonalResult<PersonalWorkstreamView>>;
  cancelOperation?(requestId: string): Promise<PersonalResult<PersonalWorkstreamOperationResult>>;
  manageFleet?(
    environmentId: EnvironmentId,
    input: ManagePersonalWorkstreamFleetInput,
  ): Promise<PersonalResult<PersonalWorkstreamFleet>>;
  releaseFollower?(
    environmentId: EnvironmentId,
    authorityId: EnvironmentId,
  ): Promise<PersonalResult<void>>;
  resolveOperation?(
    input: PersonalWorkstreamConflictChoice,
  ): Promise<PersonalResult<PersonalWorkstreamOperationResult>>;
}
