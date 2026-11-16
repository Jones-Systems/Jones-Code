import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type {
  PersonalWorkstreamLifecycle,
  PersonalWorkstreamAssignment,
} from "@t3tools/contracts/jones/workstreamsPersonal";
import type { Atom } from "effect/reactivity";

export type PersonalThreadRef = { readonly environmentId: EnvironmentId; readonly id: ThreadId };

export interface PersonalWorkstreamView {
  readonly id: string;
  readonly name: string;
  readonly lifecycle: PersonalWorkstreamLifecycle;
  readonly color: string | null;
  readonly orderKey: string;
  readonly createdAt: string;
  readonly updatedAt: string;
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
}

export interface PersonalWorkstreamsState {
  readonly workstreams: readonly PersonalWorkstreamView[]; // merged, live, (orderKey, id)
  readonly environments: readonly PersonalEnvironmentState[];
  readonly assignmentsByThreadKey: ReadonlyMap<string, PersonalWorkstreamAssignment>;
  readonly pendingThreadKeys: ReadonlySet<string>; // local actions in flight
  readonly canCreate: boolean; // some environment is writable
}

export interface PersonalWorkstreamsGrouping<T extends PersonalThreadRef> {
  readonly groups: readonly {
    readonly workstream: PersonalWorkstreamView;
    readonly threads: readonly T[];
  }[];
  readonly ungrouped: readonly T[];
  readonly workstreamIdByThreadKey: ReadonlyMap<string, string>;
}

export type PersonalWorkstreamsAction =
  | "create"
  | "rename"
  | "recolor"
  | "lifecycle"
  | "reorder"
  | "delete"
  | "assign"
  | "unassign";
export type PersonalWorkstreamsFailureReason =
  | "offline"
  | "read_only"
  | "update_required"
  | "thread_not_found"
  | "workstream_deleted"
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

export interface PersonalWorkstreamsController {
  readonly state: Atom.Atom<PersonalWorkstreamsState>;
  canAssign(thread: PersonalThreadRef): boolean;
  create(input: { name: string }): Promise<PersonalResult<PersonalWorkstreamView>>;
  rename(id: string, name: string): Promise<PersonalResult<PersonalWorkstreamView>>;
  setColor(id: string, color: string | null): Promise<PersonalResult<PersonalWorkstreamView>>;
  setLifecycle(
    id: string,
    lifecycle: PersonalWorkstreamLifecycle,
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
}
