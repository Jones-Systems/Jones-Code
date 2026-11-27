import type { EnvironmentId } from "@t3tools/contracts";
import {
  canonicalPersonalWorkstream,
  comparePersonalWorkstreams,
  isClosedPersonalWorkstream,
  mergePersonalWorkstreams,
  newPersonalWorkstream,
  editPersonalWorkstream,
  deletePersonalWorkstream,
  type PersonalWorkstream,
  type PersonalWorkstreamAssignment,
  type PersonalWorkstreamsSnapshot,
  type PersonalWorkstreamMembers,
  type PutResult,
  type AssignResult,
  type AssignPersonalWorkstreamInput,
  type PersonalWorkstreamFleet,
  type PersonalWorkstreamFleetAction,
  type PersonalWorkstreamOperationResult,
  type RequestPersonalWorkstreamOperation,
} from "@t3tools/contracts/jones/workstreamsPersonal";
import type { ManagePersonalWorkstreamFleetInput } from "@t3tools/contracts/jones/workstreamsPersonalRpc";
import { pinOrderKeyBetween } from "@t3tools/shared/jones/pinOrder";
import * as DateTime from "effect/DateTime";
import { Atom, type AtomRegistry } from "effect/reactivity";
import type {
  PersonalThreadRef,
  PersonalWorkstreamsState,
  PersonalWorkstreamsGrouping,
  PersonalWorkstreamsController,
  PersonalWorkstreamsAction,
  PersonalResult,
  PersonalWorkstreamsFailureReason,
  PersonalEnvironmentState,
  PersonalWorkstreamHistory,
  PersonalWorkstreamView,
  PersonalWorkstreamConflictChoice,
  PersonalWorkstreamFleetAuthorityStatus,
} from "./types.ts";

export const personalThreadKey = (t: PersonalThreadRef): string =>
  JSON.stringify([t.environmentId, t.id]);
export const openPersonalWorkstreams = (state: PersonalWorkstreamsState) =>
  state.workstreams.filter((workstream) => !isClosedPersonalWorkstream(workstream.lifecycle));

export function visiblePersonalWorkstreamId(
  state: PersonalWorkstreamsState,
  thread: PersonalThreadRef,
): string | null {
  const id = state.assignmentsByThreadKey.get(personalThreadKey(thread))?.workstreamId;
  return id &&
    state.workstreams.some(
      (workstream) => workstream.id === id && !isClosedPersonalWorkstream(workstream.lifecycle),
    )
    ? id
    : null;
}

export function groupPersonalThreads<T extends PersonalThreadRef>(
  state: PersonalWorkstreamsState,
  threads: readonly T[],
): PersonalWorkstreamsGrouping<T> {
  const groups = openPersonalWorkstreams(state).map((workstream) => ({
    workstream,
    threads: [] as T[],
  }));
  const byId = new Map(groups.map((g) => [g.workstream.id, g]));
  const ungrouped: T[] = [];
  const workstreamIdByThreadKey = new Map<string, string>();
  for (const thread of threads) {
    const key = personalThreadKey(thread);
    const id = state.assignmentsByThreadKey.get(key)?.workstreamId;
    const group = id ? byId.get(id) : undefined;
    if (group) {
      group.threads.push(thread);
      workstreamIdByThreadKey.set(key, group.workstream.id);
    } else ungrouped.push(thread);
  }
  return { groups, ungrouped, workstreamIdByThreadKey };
}
export interface PersonalConnection {
  readonly environmentId: EnvironmentId;
  /** Change when the owning RPC session is replaced. */
  readonly generation: string | number;
  readonly connected: boolean;
  readonly capable: boolean;
  readonly fleetCapable?: boolean;
  readonly writable: boolean;
}
export type PersonalPutResult = PutResult;
export type PersonalAssignInput = AssignPersonalWorkstreamInput;
export type PersonalAssignResult = AssignResult;
/** The host binds these single attempts to the captured session, never to a replacement. */
export interface PersonalWorkstreamsTransport {
  subscribe(
    connection: PersonalConnection,
    onSnapshot: (snapshot: PersonalWorkstreamsSnapshot) => void,
    onError: (error: unknown) => void,
  ): () => void;
  members?(connection: PersonalConnection, id: string): Promise<PersonalWorkstreamMembers>;
  put(
    connection: PersonalConnection,
    workstream: PersonalWorkstream,
    expected?: PersonalWorkstream,
  ): Promise<PersonalPutResult>;
  assign(connection: PersonalConnection, input: PersonalAssignInput): Promise<PersonalAssignResult>;
  request?(
    connection: PersonalConnection,
    input: RequestPersonalWorkstreamOperation,
  ): Promise<PersonalWorkstreamOperationResult>;
  cancel?(
    connection: PersonalConnection,
    input: { readonly requestId: string; readonly cancellationId: string },
  ): Promise<PersonalWorkstreamOperationResult>;
  manageFleet?(
    connection: PersonalConnection,
    input: ManagePersonalWorkstreamFleetInput,
  ): Promise<PersonalWorkstreamFleet>;
  releaseFollower?(
    connection: PersonalConnection,
    authorityId: EnvironmentId,
  ): Promise<{ released: true }>;
  resolve?(
    connection: PersonalConnection,
    input: PersonalWorkstreamConflictChoice & { readonly resolutionId: string },
  ): Promise<PersonalWorkstreamOperationResult>;
}
type FleetSnapshot = PersonalWorkstreamsSnapshot & { readonly fleet: PersonalWorkstreamFleet };
interface FleetObservation {
  readonly reply: PersonalWorkstreamOperationResult;
  readonly replacesRequestId?: string;
}
interface Entry {
  connection: PersonalConnection;
  snapshot?: PersonalWorkstreamsSnapshot;
  fleetSnapshot?: FleetSnapshot;
  confirmed: boolean;
  sequence: number;
  observations: Map<string, { sequence: number; reply: PersonalPutResult | PersonalAssignResult }>;
  fleetReplies: Map<string, FleetObservation>;
  stop?: () => void;
  failed: boolean;
}
interface AssignmentLaneResult {
  readonly result: PersonalResult<void>;
  readonly entry: Entry | undefined;
  readonly confirmedRevision: number | undefined;
}
interface Overlay {
  definition?: PersonalWorkstream;
  thread?: PersonalThreadRef;
  target?: string | null;
}
const empty = (): PersonalWorkstreamsState => ({
  workstreams: [],
  environments: [],
  assignmentsByThreadKey: new Map(),
  pendingThreadKeys: new Set(),
  autoAssignedThreadKeys: new Set(),
  canCreate: false,
});
const compareString = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);
const errorReason = (error: unknown): PersonalWorkstreamsFailureReason => {
  const code = error && typeof error === "object" && "code" in error ? error.code : undefined;
  if (code === "precondition_failed") return "changed_elsewhere";
  if (
    code === "thread_not_found" ||
    code === "workstream_deleted" ||
    code === "workstream_closed" ||
    code === "unsettled_threads"
  )
    return code;
  return "unconfirmed";
};
const fleetErrorReason = (error: unknown): PersonalWorkstreamsFailureReason => {
  if (error && typeof error === "object") {
    if ("_tag" in error && error._tag === "EnvironmentAuthorizationError") return "read_only";
    if ("code" in error) {
      switch (error.code) {
        case "authority_required":
        case "operation_pending":
          return "changed_elsewhere";
        case "closure_unavailable":
          return "closure_unavailable";
        case "workstream_not_found":
          return "workstream_deleted";
        case "invalid_request":
          return "failed";
      }
    }
  }
  return errorReason(error);
};
function matchesFleetAction(
  action: PersonalWorkstreamFleetAction,
  workstream: PersonalWorkstream,
): boolean {
  if (action.kind === "delete") return workstream.deletedAt !== null;
  if (workstream.deletedAt !== null) return false;
  const patch =
    action.kind === "create"
      ? {
          name: action.name,
          orderKey: action.orderKey,
          nameIconProjectKey: action.nameIconProjectKey,
        }
      : action.patch;
  return (
    (patch.name === undefined || patch.name === workstream.name) &&
    (patch.orderKey === undefined || patch.orderKey === workstream.orderKey) &&
    (patch.nameIconProjectKey === undefined ||
      patch.nameIconProjectKey === workstream.nameIcon?.projectKey) &&
    (action.kind === "create" ||
      ((action.patch.lifecycle === undefined || action.patch.lifecycle === workstream.lifecycle) &&
        (action.patch.color === undefined || action.patch.color === workstream.color) &&
        (action.patch.snoozedAt === undefined || action.patch.snoozedAt === workstream.snoozedAt) &&
        (action.patch.snoozedUntil === undefined ||
          action.patch.snoozedUntil === workstream.snoozedUntil)))
  );
}
export function createPersonalWorkstreamsController(options: {
  readonly registry: AtomRegistry.AtomRegistry;
  /** Platform-persisted UUID; not an authority identity. */
  readonly origin: string;
  readonly transport: PersonalWorkstreamsTransport;
  readonly now?: () => string;
  readonly randomUUID?: () => string;
}): PersonalWorkstreamsController & {
  setEnvironments(connections: readonly PersonalConnection[]): void;
  loadCache(snapshot: PersonalWorkstreamsSnapshot): void;
  dispose(): void;
} {
  const state = Atom.make(empty());
  const entries = new Map<EnvironmentId, Entry>();
  const overlays = new Map<number, Overlay>();
  // Keep the authority binding across disconnects and version skew within this controller.
  const observedFleets = new Map<EnvironmentId, PersonalWorkstreamFleet>();
  const fleetCloses = new Map<string, Entry>();
  let designating: EnvironmentId | undefined;
  const pendingClosures = new Map<
    string,
    { base: PersonalWorkstream; candidate: PersonalWorkstream }
  >();
  const lanes = new Map<string, Promise<AssignmentLaneResult>>();
  const repairInFlight = new Set<string>();
  // A failed candidate is retried only after a connection/permission change.
  const repairFailures = new Set<string>();
  let activeRepairs = 0;
  let actionId = 0;
  let disposed = false;
  const now = options.now ?? (() => DateTime.formatIso(DateTime.nowUnsafe()));
  // @effect-diagnostics-next-line cryptoRandomUUID:off - Promise controller runs outside Effect; platforms can inject their UUID generator.
  const uuid = options.randomUUID ?? (() => crypto.randomUUID());
  const current = (e: Entry) => !disposed && entries.get(e.connection.environmentId) === e;
  const writable = (e: Entry) =>
    current(e) &&
    e.connection.connected &&
    e.connection.capable &&
    e.connection.writable &&
    e.confirmed;
  const fleetMode = () => observedFleets.size > 0 || designating !== undefined;
  const observeFleet = (entry: Entry, snapshot: PersonalWorkstreamsSnapshot) => {
    if (!snapshot.fleet) return;
    entry.fleetSnapshot = { ...snapshot, fleet: snapshot.fleet };
    observedFleets.set(snapshot.fleet.authorityEnvironmentId, snapshot.fleet);
    if (designating === snapshot.fleet.authorityEnvironmentId) designating = undefined;
  };
  const fleetSessionStatus = (e: Entry | undefined): PersonalWorkstreamFleetAuthorityStatus => {
    if (!e || !current(e) || !e.connection.connected) return "offline";
    if (!e.connection.capable || !e.connection.fleetCapable) return "update_required";
    if (!e.connection.writable) return "read_only";
    if (!e.confirmed || e.failed) return "unconfirmed";
    return "ready";
  };
  const fleetAuthority = () => {
    const observed = observedFleets.values().next().value;
    const id = observed?.authorityEnvironmentId ?? designating;
    const entry = id === undefined ? undefined : entries.get(id);
    const snapshot = entry?.fleetSnapshot;
    const exact =
      snapshot?.fleet.role === "authority" && snapshot.fleet.authorityEnvironmentId === id
        ? snapshot
        : undefined;
    const live = entry?.snapshot?.fleet;
    const sessionStatus = fleetSessionStatus(entry);
    const conflict = observedFleets.size > 1;
    const status: PersonalWorkstreamFleetAuthorityStatus = conflict
      ? "changed_elsewhere"
      : sessionStatus !== "ready"
        ? sessionStatus
        : live?.role === "authority" && live.authorityEnvironmentId === id
          ? "ready"
          : observed && designating !== id
            ? "changed_elsewhere"
            : "unconfirmed";
    const operations = new Map(
      exact?.fleet.operations.map((operation) => [operation.requestId, operation]),
    );
    if (entry?.confirmed && live === exact?.fleet) {
      const observations = [...entry.fleetReplies.values()].sort(
        (a, b) => a.reply.sequence - b.reply.sequence,
      );
      for (const { reply, replacesRequestId } of observations) {
        if (replacesRequestId) operations.delete(replacesRequestId);
        operations.set(reply.operation.requestId, reply.operation);
      }
    }
    return {
      id,
      entry,
      status,
      snapshot: conflict ? undefined : exact,
      fleet: conflict
        ? undefined
        : exact
          ? { ...exact.fleet, operations: [...operations.values()] }
          : observed,
    };
  };
  const pendingFleetCloses = (authority: ReturnType<typeof fleetAuthority>) => {
    const pending = new Set(
      authority.fleet?.operations
        .filter(
          ({ kind, requestId, status }) =>
            kind === "close" &&
            (status === "pending" ||
              status === "stale" ||
              (status === "applied" && authority.entry?.fleetReplies.has(requestId))),
        )
        .map(({ workstreamId }) => workstreamId),
    );
    for (const [id, entry] of fleetCloses) if (current(entry)) pending.add(id);
    return pending;
  };
  const winners = (confirmedOnly = false) => {
    const result = new Map<string, PersonalWorkstream>();
    for (const e of entries.values()) {
      if (!e.connection.capable || (confirmedOnly && !e.confirmed)) continue;
      for (const w of e.snapshot?.workstreams ?? []) {
        const old = result.get(w.id);
        result.set(w.id, old ? mergePersonalWorkstreams([old, w]) : w);
      }
    }
    return result;
  };
  const editableWinners = () => {
    if (fleetMode()) {
      const authority = fleetAuthority();
      return new Map(
        authority.status === "ready"
          ? authority.snapshot?.workstreams.map((w) => [w.id, w])
          : undefined,
      );
    }
    const merged = winners(true);
    for (const overlay of overlays.values())
      if (overlay.definition) {
        const old = merged.get(overlay.definition.id);
        if (!old || comparePersonalWorkstreams(overlay.definition, old) > 0)
          merged.set(overlay.definition.id, overlay.definition);
      }
    return merged;
  };
  const publish = () => {
    if (disposed) return;
    const authority = fleetMode() ? fleetAuthority() : undefined;
    const merged =
      authority && observedFleets.size > 0
        ? new Map(authority.snapshot?.workstreams.map((w) => [w.id, w]))
        : winners();
    const assignments = new Map<string, PersonalWorkstreamAssignment>();
    const pending = new Set<string>();
    for (const e of entries.values())
      if (e.connection.capable) {
        for (const a of e.snapshot?.assignments ?? [])
          assignments.set(
            personalThreadKey({ environmentId: e.connection.environmentId, id: a.threadId }),
            a,
          );
      }
    for (const overlay of overlays.values()) {
      if (!authority && overlay.definition) {
        const old = merged.get(overlay.definition.id);
        if (!old || comparePersonalWorkstreams(overlay.definition, old) > 0)
          merged.set(overlay.definition.id, overlay.definition);
      }
      if (overlay.thread) {
        const key = personalThreadKey(overlay.thread);
        pending.add(key);
        assignments.set(key, {
          threadId: overlay.thread.id,
          workstreamId: overlay.target ?? null,
          revision: assignments.get(key)?.revision ?? 0,
          updatedAt: null,
        });
      }
    }
    if (!authority)
      for (const [id, { base, candidate }] of pendingClosures) {
        const copy = merged.get(id);
        if (copy && comparePersonalWorkstreams(copy, candidate) === 0) merged.set(id, base);
      }
    for (const [key, a] of assignments)
      if (a.workstreamId && merged.get(a.workstreamId)?.deletedAt !== null)
        assignments.set(key, { ...a, workstreamId: null });
    const autoAssigned = new Set<string>();
    for (const e of entries.values())
      if (e.connection.capable)
        for (const auto of e.snapshot?.autoAssignments ?? []) {
          const key = personalThreadKey({
            environmentId: e.connection.environmentId,
            id: auto.threadId,
          });
          const effective = assignments.get(key);
          if (
            !pending.has(key) &&
            effective?.workstreamId === auto.workstreamId &&
            effective.revision === auto.revision
          )
            autoAssigned.add(key);
        }
    options.registry.set(state, {
      workstreams: [...merged.values()]
        .filter((w) => w.deletedAt === null)
        .sort((a, b) => compareString(a.orderKey, b.orderKey) || compareString(a.id, b.id)),
      assignmentsByThreadKey: assignments,
      pendingThreadKeys: pending,
      autoAssignedThreadKeys: autoAssigned,
      ...(authority
        ? {
            canCreate: authority.status === "ready" && !!options.transport.request,
            canSetNameIcon:
              authority.status === "ready" &&
              !!options.transport.request &&
              authority.snapshot?.supportsNameIcons === true,
            canSnooze:
              authority.status === "ready" &&
              !!options.transport.request &&
              authority.snapshot?.supportsSnooze === true,
            fleetAuthorityStatus: authority.status,
            ...(authority.fleet ? { fleet: authority.fleet } : {}),
            pendingCloseWorkstreamIds: pendingFleetCloses(authority),
          }
        : {
            canCreate: [...entries.values()].some(writable),
            canSetNameIcon: [...entries.values()].some(
              (e) => writable(e) && e.snapshot?.supportsNameIcons,
            ),
            canSnooze: [...entries.values()].some((e) => writable(e) && e.snapshot?.supportsSnooze),
          }),
      environments: [...entries.values()].map((e): PersonalEnvironmentState => {
        const workstreams = e.connection.capable
          ? (e.snapshot?.workstreams.filter((w) => w.deletedAt === null) ?? [])
          : [];
        return {
          environmentId: e.connection.environmentId,
          status: !e.connection.capable
            ? "update_required"
            : !e.connection.connected
              ? "offline"
              : !e.connection.writable
                ? "read_only"
                : e.failed || e.snapshot?.importIssue
                  ? "sync_failed"
                  : "ready",
          confirmed: e.confirmed,
          workstreamCount: workstreams.length,
          workstreams,
          ...(e.confirmed && !e.failed && e.connection.fleetCapable && e.snapshot?.fleetProposals
            ? { fleetProposals: e.snapshot.fleetProposals }
            : {}),
          ...(e.confirmed && !e.failed && e.snapshot?.fleet?.role === "follower"
            ? { followsAuthority: e.snapshot.fleet.authorityEnvironmentId }
            : {}),
        };
      }),
    });
  };
  // Mutation replies are partial observations, so do not advance the full-snapshot
  // watermark: the subscription still has to deliver all other rows at that sequence.
  const mergeReply = (e: Entry, reply: PersonalPutResult | PersonalAssignResult) => {
    if (!current(e) || !e.confirmed || reply.sequence < e.sequence || !e.snapshot) return;
    const observationKey =
      "assignment" in reply
        ? `assignment:${reply.assignment.threadId}`
        : `definition:${reply.workstream.id}`;
    const prior = e.observations.get(observationKey);
    if (prior && prior.sequence > reply.sequence) return;
    e.observations.set(observationKey, { sequence: reply.sequence, reply });
    const defs = new Map(e.snapshot.workstreams.map((w) => [w.id, w]));
    if (reply.workstream && !fleetMode()) {
      const old = defs.get(reply.workstream.id);
      if (!old || comparePersonalWorkstreams(reply.workstream, old) > 0)
        defs.set(reply.workstream.id, reply.workstream);
    }
    const assignments = new Map(e.snapshot.assignments.map((a) => [a.threadId, a]));
    if ("assignment" in reply) {
      const old = assignments.get(reply.assignment.threadId);
      if (!old || reply.assignment.revision >= old.revision)
        assignments.set(reply.assignment.threadId, reply.assignment);
    }
    e.snapshot = {
      ...e.snapshot,
      workstreams: [...defs.values()],
      assignments: [...assignments.values()],
    };
  };
  const definitionFor = (entry: Entry, workstream: PersonalWorkstream): PersonalWorkstream => {
    let local = workstream;
    if (!entry.snapshot?.supportsNameIcons && local.nameIcon !== undefined) {
      const { nameIcon: _nameIcon, ...legacy } = local;
      local = legacy;
    }
    if (
      entry.snapshot?.supportsSnooze ||
      (local.snoozedAt === undefined && local.snoozedUntil === undefined)
    )
      return local;
    const { snoozedAt: _started, snoozedUntil: _until, ...legacy } = local;
    return legacy;
  };
  const repair = () => {
    if (disposed || fleetMode()) return;
    const merged = winners(true);
    for (const e of entries.values()) {
      if (!writable(e)) continue;
      for (const winner of merged.values()) {
        if (activeRepairs >= 4) return;
        const candidate = definitionFor(e, winner);
        const key = JSON.stringify([e.connection.environmentId, winner.id]);
        const old = e.snapshot?.workstreams.find((d) => d.id === winner.id);
        if (
          pendingClosures.has(winner.id) ||
          (old && comparePersonalWorkstreams(old, candidate) >= 0) ||
          repairInFlight.has(key) ||
          repairFailures.has(key)
        )
          continue;
        repairInFlight.add(key);
        activeRepairs++;
        void Promise.resolve()
          .then(() => (fleetMode() ? undefined : options.transport.put(e.connection, candidate)))
          .then((reply) => {
            if (!reply || !current(e) || fleetMode()) return;
            mergeReply(e, reply);
            if (comparePersonalWorkstreams(reply.workstream, candidate) < 0)
              throw new Error("Invalid canonical reply");
          })
          .catch(() => {
            if (current(e) && !fleetMode()) {
              e.failed = true;
              repairFailures.add(key);
            }
          })
          .finally(() => {
            repairInFlight.delete(key);
            activeRepairs--;
            publish();
            repair();
          });
      }
    }
  };
  const fail = <A>(
    action: PersonalWorkstreamsAction,
    target: string,
    reason: PersonalWorkstreamsFailureReason,
    environmentId: EnvironmentId | null = null,
  ): PersonalResult<A> => ({
    ok: false,
    failure: { action, reason, environmentId, dedupeKey: `${action}:${target}:${reason}` },
  });
  const unavailable = (e: Entry | undefined): PersonalWorkstreamsFailureReason =>
    !e || !e.connection.connected
      ? "offline"
      : !e.connection.capable
        ? "update_required"
        : !e.connection.writable
          ? "read_only"
          : "unconfirmed";
  const observeFleetReply = (
    entry: Entry,
    reply: PersonalWorkstreamOperationResult,
    replacesRequestId?: string,
  ) => {
    if (reply.sequence > entry.sequence) {
      const prior = entry.fleetReplies.get(reply.operation.requestId);
      if (!prior || prior.reply.sequence <= reply.sequence)
        entry.fleetReplies.set(reply.operation.requestId, {
          reply,
          ...(replacesRequestId === undefined ? {} : { replacesRequestId }),
        });
    }
    publish();
  };
  const stillAuthority = (entry: Entry) =>
    current(entry) &&
    observedFleets.size === 1 &&
    entry.snapshot?.fleet?.role === "authority" &&
    entry.snapshot.fleet.authorityEnvironmentId === entry.connection.environmentId;
  const requestFleet = async (
    action: PersonalWorkstreamsAction,
    id: string,
    change: PersonalWorkstreamFleetAction,
    expected: PersonalWorkstream | null,
  ): Promise<PersonalResult<PersonalWorkstream>> => {
    const authority = fleetAuthority();
    const rejected = (reason: PersonalWorkstreamsFailureReason) =>
      fail<PersonalWorkstream>(action, id, reason, authority.id);
    const entry = authority.entry;
    if (authority.status !== "ready" || !entry)
      return rejected(authority.status === "ready" ? "unconfirmed" : authority.status);
    const request = options.transport.request;
    if (
      !request ||
      ((action === "nameIcon" ||
        (change.kind === "create" && change.nameIconProjectKey !== undefined)) &&
        !authority.snapshot?.supportsNameIcons) ||
      ((action === "snooze" || action === "unsnooze") && !authority.snapshot?.supportsSnooze)
    )
      return rejected("update_required");
    if (pendingFleetCloses(authority).has(id)) return rejected("changed_elsewhere");
    const lifecycle = change.kind === "edit" ? change.patch.lifecycle : undefined;
    const closing =
      lifecycle !== undefined &&
      lifecycle !== expected?.lifecycle &&
      isClosedPersonalWorkstream(lifecycle);
    const kind = closing ? "close" : change.kind === "delete" ? "delete" : "put";
    if (closing) {
      fleetCloses.set(id, entry);
      publish();
    }
    try {
      if (!stillAuthority(entry)) return rejected("unconfirmed");
      const requestId = uuid();
      const reply = await request(entry.connection, {
        requestId,
        workstreamId: id,
        expected,
        action: change,
      });
      if (!stillAuthority(entry)) return rejected("unconfirmed");
      const { operation, workstream } = reply;
      if (
        operation.requestId !== requestId ||
        operation.workstreamId !== id ||
        operation.kind !== kind ||
        (closing && operation.requestedLifecycle !== lifecycle)
      )
        return rejected("unconfirmed");
      observeFleetReply(entry, reply);
      if (operation.status !== "pending" && operation.status !== "applied")
        return rejected("changed_elsewhere");
      if (!workstream || workstream.id !== id) return rejected("unconfirmed");
      const matches =
        closing && operation.status === "pending"
          ? expected !== null &&
            canonicalPersonalWorkstream(workstream) === canonicalPersonalWorkstream(expected)
          : matchesFleetAction(change, workstream);
      return matches ? { ok: true, value: workstream } : rejected("unconfirmed");
    } catch (error) {
      return rejected(stillAuthority(entry) ? fleetErrorReason(error) : "unconfirmed");
    } finally {
      if (closing && fleetCloses.get(id) === entry) {
        fleetCloses.delete(id);
        publish();
      }
    }
  };
  const put = async (
    action: PersonalWorkstreamsAction,
    w: PersonalWorkstream,
  ): Promise<PersonalResult<PersonalWorkstream>> => {
    if (fleetMode()) return fail(action, w.id, "changed_elsewhere");
    const requireNameIcons =
      action === "nameIcon" || (action === "create" && w.nameIcon !== undefined);
    const requireSnooze = action === "snooze" || action === "unsnooze";
    const available = [...entries.values()].filter(writable);
    const targets = available.filter(
      (e) =>
        (!requireNameIcons || e.snapshot?.supportsNameIcons) &&
        (!requireSnooze || e.snapshot?.supportsSnooze),
    );
    if (!targets.length)
      return fail(
        action,
        w.id,
        available.length ? "update_required" : unavailable(entries.values().next().value),
      );
    const id = ++actionId;
    overlays.set(id, { definition: w });
    publish();
    let confirmed = false;
    let replied = false;
    await Promise.all(
      targets.map(async (e) => {
        try {
          if (fleetMode()) return;
          const candidate = definitionFor(e, w);
          const reply = await options.transport.put(e.connection, candidate);
          if (!current(e) || fleetMode()) return;
          replied = true;
          mergeReply(e, reply);
          if (
            reply.outcome !== "superseded" &&
            comparePersonalWorkstreams(reply.workstream, candidate) === 0
          )
            confirmed = true;
        } catch {
          /* Other destinations can still confirm this exact candidate. */
        }
      }),
    );
    overlays.delete(id);
    publish();
    repair();
    return confirmed
      ? { ok: true, value: w }
      : fail(action, w.id, replied ? "changed_elsewhere" : "unconfirmed");
  };
  const close = async (
    base: PersonalWorkstream,
    candidate: PersonalWorkstream,
  ): Promise<PersonalResult<PersonalWorkstream>> => {
    const id = candidate.id;
    if (pendingClosures.has(id)) return fail("lifecycle", id, "changed_elsewhere");
    const targets = [...entries.values()];
    if (!targets.length || !targets.every(writable) || !options.transport.members)
      return fail("lifecycle", id, "closure_unavailable");

    pendingClosures.set(id, { base, candidate });
    try {
      const history = await readHistory(id);
      if (
        fleetMode() ||
        !history.complete ||
        entries.size !== targets.length ||
        !targets.every(writable)
      )
        return fail("lifecycle", id, "closure_unavailable");
      if (history.members.some(({ settled }) => !settled))
        return fail("lifecycle", id, "unsettled_threads");
      const latest = editableWinners().get(id);
      if (!latest || comparePersonalWorkstreams(base, latest) !== 0)
        return fail("lifecycle", id, "changed_elsewhere");
      if (
        !targets.every((entry) => {
          const copy = entry.snapshot?.workstreams.find((workstream) => workstream.id === id);
          return copy && comparePersonalWorkstreams(copy, definitionFor(entry, base)) === 0;
        })
      )
        return fail("lifecycle", id, "closure_unavailable");

      const results = await Promise.all(
        targets.map(async (entry) => {
          try {
            const local = definitionFor(entry, candidate);
            const reply = await options.transport.put(
              entry.connection,
              local,
              definitionFor(entry, base),
            );
            if (!current(entry) || fleetMode()) return { entry, reason: "unconfirmed" } as const;
            mergeReply(entry, reply);
            return comparePersonalWorkstreams(reply.workstream, local) === 0
              ? ({ entry, applied: true } as const)
              : ({ entry, reason: "changed_elsewhere" } as const);
          } catch (error) {
            return { entry, reason: errorReason(error) };
          }
        }),
      );
      const failed = results.filter((result) => "reason" in result);
      if (!failed.length) return { ok: true, value: candidate };

      // Restore only copies that still hold our exact candidate. A newer edit
      // stays intact, even if it arrived before all close replies came back.
      const restore = editPersonalWorkstream(
        candidate,
        { lifecycle: base.lifecycle },
        now(),
        options.origin,
      );
      const restored = await Promise.all(
        results
          .filter((result) => "applied" in result)
          .map(async ({ entry }) => {
            try {
              if (!current(entry) || fleetMode()) return false;
              const local = definitionFor(entry, restore);
              const reply = await options.transport.put(
                entry.connection,
                local,
                definitionFor(entry, candidate),
              );
              if (!current(entry) || fleetMode()) return false;
              mergeReply(entry, reply);
              return comparePersonalWorkstreams(reply.workstream, local) === 0;
            } catch {
              return false;
            }
          }),
      );
      const reason =
        failed.some((result) => result.reason === "unconfirmed") || !restored.every(Boolean)
          ? "unconfirmed"
          : failed.some((result) => result.reason === "unsettled_threads")
            ? "unsettled_threads"
            : "changed_elsewhere";
      return fail("lifecycle", id, reason);
    } finally {
      pendingClosures.delete(id);
      publish();
      repair();
    }
  };
  const edit = async (
    action: PersonalWorkstreamsAction,
    id: string,
    patch: Parameters<typeof editPersonalWorkstream>[1],
  ) => {
    if (fleetMode()) {
      const authority = fleetAuthority();
      if (authority.status !== "ready")
        return fail<PersonalWorkstream>(action, id, authority.status, authority.id);
      const base = authority.snapshot?.workstreams.find((w) => w.id === id);
      if (!base || base.deletedAt !== null)
        return fail<PersonalWorkstream>(action, id, "workstream_deleted", authority.id);
      return requestFleet(action, id, { kind: "edit", patch }, base);
    }
    const base = editableWinners().get(id);
    if (!base || base.deletedAt !== null)
      return fail<PersonalWorkstream>(action, id, "workstream_deleted");
    try {
      const candidate = editPersonalWorkstream(base, patch, now(), options.origin);
      return action === "lifecycle" &&
        isClosedPersonalWorkstream(candidate.lifecycle) &&
        candidate.lifecycle !== base.lifecycle
        ? await close(base, candidate)
        : await put(action, candidate);
    } catch {
      return fail<PersonalWorkstream>(action, id, "failed");
    }
  };
  const fleetOwnerFailure = (
    e: Entry | undefined,
  ): PersonalWorkstreamsFailureReason | undefined => {
    const status = fleetSessionStatus(e);
    if (status !== "ready") return status;
    const authority = fleetAuthority();
    const fleet = e?.snapshot?.fleet;
    const isAuthority = e?.connection.environmentId === authority.id;
    if (
      authority.status === "changed_elsewhere" ||
      !fleet ||
      fleet.authorityEnvironmentId !== authority.id ||
      fleet.role !== (isAuthority ? "authority" : "follower")
    )
      return "changed_elsewhere";
  };
  const assign = (
    thread: PersonalThreadRef,
    target: string | null,
    created?: PersonalWorkstreamView,
  ): Promise<PersonalResult<void>> => {
    const key = personalThreadKey(thread);
    const action = target === null ? "unassign" : "assign";
    const isFleet = fleetMode();
    const id = ++actionId;
    const predecessor = lanes.get(key);
    const submittedEntry = entries.get(thread.environmentId);
    if (isFleet) {
      const reason = fleetOwnerFailure(submittedEntry);
      if (reason) return Promise.resolve(fail(action, key, reason, thread.environmentId));
    }
    const submittedRevision =
      submittedEntry?.snapshot?.assignments.find((a) => a.threadId === thread.id)?.revision ?? 0;
    let confirmedRevision: number | undefined;
    overlays.set(id, { thread, target });
    publish();
    const run = async (previous?: AssignmentLaneResult): Promise<PersonalResult<void>> => {
      // A queued intent depends on its predecessor's confirmed result, not on a
      // newer snapshot or conflict revision observed while that result was pending.
      if (previous && (!previous.result.ok || previous.confirmedRevision === undefined))
        return fail(action, key, "changed_elsewhere", thread.environmentId);
      const e = submittedEntry;
      if (isFleet !== fleetMode() || (e && !current(e)) || (previous && previous.entry !== e))
        return fail(action, key, "unconfirmed", thread.environmentId);
      if (!e || !writable(e)) return fail(action, key, unavailable(e), thread.environmentId);
      let workstream: PersonalWorkstream | null = null;
      if (isFleet) {
        const reason = fleetOwnerFailure(e);
        if (reason) return fail(action, key, reason, thread.environmentId);
        if (target !== null) {
          const authority = fleetAuthority();
          const a = authority.entry;
          if (
            (authority.status !== "ready" && authority.status !== "read_only") ||
            !a?.confirmed ||
            a.failed ||
            !stillAuthority(a)
          )
            return fail(
              action,
              key,
              authority.status === "ready" ? "unconfirmed" : authority.status,
              authority.id,
            );
          if (a !== e && !authority.snapshot?.fleet.members.includes(thread.environmentId))
            return fail(action, key, "changed_elsewhere", thread.environmentId);
          if (pendingFleetCloses(authority).has(target))
            return fail(action, key, "changed_elsewhere", authority.id);
          const row = authority.snapshot?.workstreams.find((w) => w.id === target);
          // A compound create/assign can use its same-session create receipt while the full frame catches up.
          const createReply =
            a === e &&
            created &&
            [...a.fleetReplies.values()]
              .map(({ reply }) => reply)
              .filter(({ operation }) => operation.workstreamId === target)
              .sort((left, right) => right.sequence - left.sequence)[0];
          const justCreated =
            createReply && createReply.workstream === created ? createReply.workstream : undefined;
          const authoritative = row ?? justCreated;
          if (!authoritative || authoritative.deletedAt !== null)
            return fail(action, key, "workstream_deleted", authority.id);
          if (isClosedPersonalWorkstream(authoritative.lifecycle))
            return fail(action, key, "workstream_closed", authority.id);
          const local =
            a === e ? authoritative : e.snapshot?.workstreams.find((w) => w.id === target);
          if (
            !local ||
            local.deletedAt !== null ||
            isClosedPersonalWorkstream(local.lifecycle) ||
            (local.lifecycleRevision ?? 0) !== (authoritative.lifecycleRevision ?? 0)
          )
            return fail(action, key, "changed_elsewhere", thread.environmentId);
          workstream = local;
        }
      } else if (target !== null) {
        const w = winners(true).get(target);
        if (!w || w.deletedAt !== null)
          return fail(action, key, "workstream_deleted", thread.environmentId);
        if (isClosedPersonalWorkstream(w.lifecycle))
          return fail(action, key, "workstream_closed", thread.environmentId);
        workstream = definitionFor(e, w);
      }
      const expectedRevision = previous?.confirmedRevision ?? submittedRevision;
      try {
        const reply = await options.transport.assign(e.connection, {
          threadId: thread.id,
          workstream,
          expectedRevision,
        });
        if (
          !current(e) ||
          isFleet !== fleetMode() ||
          (isFleet &&
            (fleetOwnerFailure(e) ||
              reply.assignment.threadId !== thread.id ||
              reply.assignment.workstreamId !== target))
        )
          return fail(action, key, "unconfirmed", thread.environmentId);
        mergeReply(e, reply);
        confirmedRevision = reply.assignment.revision;
        return { ok: true, value: undefined };
      } catch (error) {
        // Expose the canonical conflict, but never retry against its revision.
        if (isFleet && (!current(e) || fleetOwnerFailure(e)))
          return fail(action, key, "unconfirmed", thread.environmentId);
        if (current(e) && error && typeof error === "object" && "currentAssignment" in error) {
          const a = error.currentAssignment as PersonalWorkstreamAssignment | undefined;
          if (a && e.snapshot && (!isFleet || a.threadId === thread.id)) {
            const old = e.snapshot.assignments.find((x) => x.threadId === a.threadId);
            if (!old || a.revision >= old.revision)
              e.snapshot = {
                ...e.snapshot,
                assignments: [
                  ...e.snapshot.assignments.filter((x) => x.threadId !== a.threadId),
                  a,
                ],
              };
          }
        }
        return fail(
          action,
          key,
          isFleet ? fleetErrorReason(error) : errorReason(error),
          thread.environmentId,
        );
      }
    };
    const pending = predecessor ? predecessor.then(run) : Promise.resolve().then(() => run());
    const lane = pending
      .then((result) => ({ result, entry: submittedEntry, confirmedRevision }))
      .finally(() => {
        overlays.delete(id);
        publish();
        repair();
      });
    lanes.set(key, lane);
    void lane.finally(() => {
      if (lanes.get(key) === lane) lanes.delete(key);
    });
    return lane.then((completed) => completed.result);
  };
  const readHistory = async (id: string): Promise<PersonalWorkstreamHistory> => {
    const authority = fleetMode() ? fleetAuthority() : undefined;
    const roster = authority?.snapshot?.fleet;
    const targets = [...entries.values()].filter(({ connection, snapshot }) => {
      if (!authority) return true;
      if (roster)
        return (
          connection.environmentId === authority.id ||
          roster.members.includes(connection.environmentId)
        );
      return authority.id !== undefined && snapshot?.fleet?.authorityEnvironmentId === authority.id;
    });
    const results = await Promise.all(
      targets.map(async (entry) => {
        if (!entry.connection.connected || !entry.connection.capable || !options.transport.members)
          return null;
        if (
          authority &&
          (!entry.confirmed ||
            entry.failed ||
            !entry.connection.fleetCapable ||
            entry.snapshot?.fleet?.authorityEnvironmentId !== authority.id)
        )
          return null;
        try {
          const members = await options.transport.members(entry.connection, id);
          return current(entry)
            ? members.map((member) => ({
                ...member,
                environmentId: entry.connection.environmentId,
                id: member.threadId,
              }))
            : null;
        } catch {
          return null;
        }
      }),
    );
    const a = authority?.entry;
    const confirmedRoster =
      roster &&
      a &&
      a.confirmed &&
      !a.failed &&
      a.connection.fleetCapable &&
      stillAuthority(a) &&
      a.snapshot?.fleet?.rosterRevision === roster.rosterRevision &&
      targets.length === roster.members.length + 1;
    return {
      members: results.flatMap((members) => members ?? []),
      complete: (!authority || !!confirmedRoster) && results.every(Boolean),
    };
  };
  const changeFleetOperation = async (
    action: "cancelOperation" | "resolveOperation",
    requestId: string,
    mutate:
      | ((
          connection: PersonalConnection,
          operationId: string,
        ) => Promise<PersonalWorkstreamOperationResult>)
      | undefined,
  ): Promise<PersonalResult<PersonalWorkstreamOperationResult>> => {
    const authority = fleetAuthority();
    const entry = authority.entry;
    const rejected = (reason: PersonalWorkstreamsFailureReason) =>
      fail<PersonalWorkstreamOperationResult>(action, requestId, reason, authority.id);
    if (authority.status !== "ready") return rejected(authority.status);
    if (!entry || !mutate) return rejected("update_required");
    try {
      const operationId = uuid();
      const reply = await mutate(entry.connection, operationId);
      if (!stillAuthority(entry) || reply.operation.requestId !== operationId)
        return rejected("unconfirmed");
      observeFleetReply(entry, reply, requestId);
      return { ok: true, value: reply };
    } catch (error) {
      return rejected(stillAuthority(entry) ? fleetErrorReason(error) : "unconfirmed");
    }
  };
  const manageFleet: NonNullable<PersonalWorkstreamsController["manageFleet"]> = async (
    id,
    input,
  ) => {
    const entry = entries.get(id);
    const rejected = (reason: PersonalWorkstreamsFailureReason) =>
      fail<PersonalWorkstreamFleet>("manageFleet", id, reason, id);
    const status = fleetSessionStatus(entry);
    if (status !== "ready") return rejected(status);
    const manage = options.transport.manageFleet;
    if (!entry || !manage) return rejected("update_required");
    const authority = fleetAuthority();
    const expectedAuthority = input.kind === "follow" ? input.authorityEnvironmentId : id;
    if (authority.id !== undefined && authority.id !== expectedAuthority)
      return rejected("changed_elsewhere");
    if (observedFleets.size > 1 || (input.kind === "follow" && id === expectedAuthority))
      return rejected("changed_elsewhere");
    if (
      input.kind === "follow" &&
      !entry.snapshot?.fleetProposals?.some(
        (proposal) =>
          proposal.authorityEnvironmentId === expectedAuthority &&
          proposal.sessionId === input.sessionId,
      )
    )
      return rejected("changed_elsewhere");
    if (input.kind === "designate" && observedFleets.size > 0 && authority.status !== "ready")
      return rejected(authority.status);
    if (input.kind === "enroll" || input.kind === "remove") {
      if (authority.status !== "ready") return rejected(authority.status);
      if (input.expectedRosterRevision !== authority.snapshot?.fleet.rosterRevision)
        return rejected("changed_elsewhere");
    }
    const establishing =
      (input.kind === "designate" || input.kind === "follow") && observedFleets.size === 0;
    if (establishing && designating !== undefined) return rejected("unconfirmed");
    if (establishing) {
      designating = expectedAuthority;
      publish();
    }
    try {
      if (!current(entry)) return rejected("unconfirmed");
      const result = await manage(entry.connection, input);
      if (
        !current(entry) ||
        result.authorityEnvironmentId !== expectedAuthority ||
        result.role !== (input.kind === "follow" ? "follower" : "authority")
      )
        return rejected("unconfirmed");
      if (
        (input.kind === "enroll" && !result.members.includes(input.environmentId)) ||
        (input.kind === "remove" && result.members.includes(input.environmentId)) ||
        (input.kind === "designate" &&
          input.peerEnvironmentIds?.some((peer) => !result.members.includes(peer)))
      )
        return rejected("unconfirmed");
      // The management reply has no snapshot sequence. Keep the write fence until the stream confirms it.
      if (!observedFleets.has(expectedAuthority)) observedFleets.set(expectedAuthority, result);
      publish();
      return { ok: true, value: result };
    } catch (error) {
      const reason = current(entry) ? fleetErrorReason(error) : "unconfirmed";
      const discoveredSkew =
        error &&
        typeof error === "object" &&
        "code" in error &&
        error.code === "authority_required";
      if (
        establishing &&
        designating === expectedAuthority &&
        reason !== "unconfirmed" &&
        !discoveredSkew
      ) {
        designating = undefined;
        publish();
        repair();
      }
      return rejected(reason);
    }
  };
  const controller: ReturnType<typeof createPersonalWorkstreamsController> = {
    state,
    readHistory,
    manageFleet,
    releaseFollower: async (id, authorityId) => {
      const entry = entries.get(id);
      const rejected = (reason: PersonalWorkstreamsFailureReason) =>
        fail<void>("manageFleet", id, reason, id);
      const status = fleetSessionStatus(entry);
      if (status !== "ready") return rejected(status);
      const release = options.transport.releaseFollower;
      if (!entry || !release) return rejected("update_required");
      if (
        entry.snapshot?.fleet?.role !== "follower" ||
        entry.snapshot.fleet.authorityEnvironmentId !== authorityId
      )
        return rejected("changed_elsewhere");
      try {
        await release(entry.connection, authorityId);
        return current(entry) ? { ok: true, value: undefined } : rejected("unconfirmed");
      } catch (error) {
        return rejected(current(entry) ? fleetErrorReason(error) : "unconfirmed");
      }
    },
    cancelOperation: (requestId) => {
      const cancel = options.transport.cancel;
      return changeFleetOperation(
        "cancelOperation",
        requestId,
        cancel &&
          ((connection, cancellationId) => cancel(connection, { requestId, cancellationId })),
      );
    },
    resolveOperation: (input) => {
      const resolve = options.transport.resolve;
      return changeFleetOperation(
        "resolveOperation",
        input.requestId,
        resolve && ((connection, resolutionId) => resolve(connection, { ...input, resolutionId })),
      );
    },
    canAssign: (thread) => {
      const e = entries.get(thread.environmentId);
      return fleetMode() ? !fleetOwnerFailure(e) : !!e && writable(e);
    },
    create: async ({ name, nameIconProjectKey }) => {
      try {
        const id = uuid();
        const orderKey =
          pinOrderKeyBetween(
            [...editableWinners().values()]
              .filter((w) => w.deletedAt === null)
              .sort((a, b) => compareString(a.orderKey, b.orderKey))
              .at(-1)?.orderKey ?? null,
            null,
          ) ?? "n";
        const input = {
          name: name.trim(),
          orderKey,
          ...(nameIconProjectKey === undefined ? {} : { nameIconProjectKey }),
        };
        return fleetMode()
          ? await requestFleet("create", id, { kind: "create", ...input }, null)
          : await put(
              "create",
              newPersonalWorkstream({ id, ...input, now: now(), origin: options.origin }),
            );
      } catch {
        return fail("create", name, "failed");
      }
    },
    rename: (id, name) => edit("rename", id, { name: name.trim() }),
    setColor: (id, color) => edit("recolor", id, { color }),
    setNameIcon: (id, nameIconProjectKey) => edit("nameIcon", id, { nameIconProjectKey }),
    setLifecycle: (id, lifecycle) => edit("lifecycle", id, { lifecycle }),
    setSnoozedUntil: (id, snoozedUntil) => {
      const action = snoozedUntil === null ? "unsnooze" : "snooze";
      const startedAt = now();
      if (snoozedUntil !== null && !(Date.parse(snoozedUntil) > Date.parse(startedAt)))
        return Promise.resolve(fail(action, id, "failed"));
      return edit(action, id, {
        snoozedAt: startedAt,
        snoozedUntil,
      });
    },
    reorder: (id, { beforeId, afterId }) => {
      const merged = editableWinners();
      if (fleetMode()) {
        const authority = fleetAuthority();
        if (authority.status !== "ready")
          return Promise.resolve(fail("reorder", id, authority.status, authority.id));
        if (
          [beforeId, afterId].some(
            (neighbor) => neighbor !== null && merged.get(neighbor)?.deletedAt !== null,
          )
        )
          return Promise.resolve(fail("reorder", id, "changed_elsewhere", authority.id));
      }
      const orderKey = pinOrderKeyBetween(
        beforeId ? (merged.get(beforeId)?.orderKey ?? null) : null,
        afterId ? (merged.get(afterId)?.orderKey ?? null) : null,
      );
      return orderKey && orderKey.length <= 64
        ? edit("reorder", id, { orderKey })
        : Promise.resolve(fail("reorder", id, "failed"));
    },
    remove: async (id) => {
      if (fleetMode()) {
        const authority = fleetAuthority();
        if (authority.status !== "ready") return fail("delete", id, authority.status, authority.id);
        const base = authority.snapshot?.workstreams.find((w) => w.id === id);
        if (!base || base.deletedAt !== null)
          return fail("delete", id, "workstream_deleted", authority.id);
        const result = await requestFleet("delete", id, { kind: "delete" }, base);
        return result.ok ? { ok: true, value: undefined } : result;
      }
      const base = editableWinners().get(id);
      if (!base || base.deletedAt !== null) return fail("delete", id, "workstream_deleted");
      try {
        const result = await put("delete", deletePersonalWorkstream(base, now(), options.origin));
        return result.ok ? { ok: true, value: undefined } : result;
      } catch {
        return fail("delete", id, "failed");
      }
    },
    assign,
    createAndAssign: async (thread, name) => {
      const isFleet = fleetMode();
      const owner = entries.get(thread.environmentId);
      if (isFleet) {
        const reason = fleetOwnerFailure(owner);
        if (reason) return fail("assign", personalThreadKey(thread), reason, thread.environmentId);
      }
      const created = await controller.create({ name });
      if (!created.ok) return created;
      if (isFleet !== fleetMode() || (isFleet && (!owner || !current(owner))))
        return fail("assign", personalThreadKey(thread), "unconfirmed", thread.environmentId);
      const assigned = await assign(thread, created.value.id, isFleet ? created.value : undefined);
      return assigned.ok ? created : assigned;
    },
    loadCache: (snapshot) => {
      const e = entries.get(snapshot.environmentId);
      if (!e) return;
      const discoversFleet = snapshot.fleet !== undefined && e.fleetSnapshot === undefined;
      if (discoversFleet) observeFleet(e, snapshot);
      const canDisplay = !e.confirmed && e.connection.capable && (!e.snapshot || !fleetMode());
      if (canDisplay) e.snapshot = snapshot;
      if (discoversFleet || canDisplay) publish();
    },
    setEnvironments: (connections) => {
      if (disposed) return;
      const ids = new Set(connections.map((c) => c.environmentId));
      for (const [id, e] of entries)
        if (!ids.has(id)) {
          e.stop?.();
          entries.delete(id);
        }
      for (const c of connections) {
        const old = entries.get(c.environmentId);
        if (
          old &&
          old.connection.generation === c.generation &&
          old.connection.connected === c.connected &&
          old.connection.capable === c.capable &&
          old.connection.fleetCapable === c.fleetCapable &&
          old.connection.writable === c.writable
        )
          continue;
        old?.stop?.();
        const e: Entry = {
          connection: c,
          confirmed: false,
          sequence: -1,
          observations: new Map(),
          fleetReplies: new Map(),
          failed: false,
          ...(old?.snapshot ? { snapshot: old.snapshot } : {}),
          ...(old?.fleetSnapshot ? { fleetSnapshot: old.fleetSnapshot } : {}),
        };
        entries.set(c.environmentId, e);
        for (const key of repairFailures)
          if (JSON.parse(key)[0] === c.environmentId) repairFailures.delete(key);
        if (c.capable && c.connected) {
          try {
            e.stop = options.transport.subscribe(
              c,
              (snapshot) => {
                if (
                  !current(e) ||
                  snapshot.environmentId !== c.environmentId ||
                  snapshot.sequence < e.sequence
                )
                  return;
                e.snapshot = snapshot;
                e.sequence = snapshot.sequence;
                e.confirmed = true;
                observeFleet(e, snapshot);
                for (const [key, { reply }] of e.fleetReplies)
                  if (reply.sequence <= snapshot.sequence) e.fleetReplies.delete(key);
                for (const [key, observation] of e.observations) {
                  if (observation.sequence <= snapshot.sequence) e.observations.delete(key);
                  else mergeReply(e, observation.reply);
                }
                publish();
                repair();
              },
              () => {
                if (current(e)) {
                  e.failed = true;
                  publish();
                }
              },
            );
          } catch {
            e.failed = true;
          }
        }
      }
      publish();
      repair();
    },
    dispose: () => {
      disposed = true;
      for (const e of entries.values()) e.stop?.();
      entries.clear();
      overlays.clear();
      observedFleets.clear();
      fleetCloses.clear();
    },
  };
  return controller;
}
