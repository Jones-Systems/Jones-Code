import type { EnvironmentId } from "@t3tools/contracts";
import {
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
} from "@t3tools/contracts/jones/workstreamsPersonal";
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
}
interface Entry {
  connection: PersonalConnection;
  snapshot?: PersonalWorkstreamsSnapshot;
  confirmed: boolean;
  sequence: number;
  observations: Map<string, { sequence: number; reply: PersonalPutResult | PersonalAssignResult }>;
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
    const merged = winners();
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
      if (overlay.definition) {
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
      canCreate: [...entries.values()].some(writable),
      canSetNameIcon: [...entries.values()].some(
        (e) => writable(e) && e.snapshot?.supportsNameIcons,
      ),
      canSnooze: [...entries.values()].some((e) => writable(e) && e.snapshot?.supportsSnooze),
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
    if (reply.workstream) {
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
    if (disposed) return;
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
          .then(() => options.transport.put(e.connection, candidate))
          .then((reply) => {
            if (!current(e)) return;
            mergeReply(e, reply);
            if (comparePersonalWorkstreams(reply.workstream, candidate) < 0)
              throw new Error("Invalid canonical reply");
          })
          .catch(() => {
            if (current(e)) {
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
  const put = async (
    action: PersonalWorkstreamsAction,
    w: PersonalWorkstream,
  ): Promise<PersonalResult<PersonalWorkstream>> => {
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
          const candidate = definitionFor(e, w);
          const reply = await options.transport.put(e.connection, candidate);
          if (!current(e)) return;
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
      if (!history.complete || entries.size !== targets.length || !targets.every(writable))
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
            if (!current(entry)) return { entry, reason: "unconfirmed" } as const;
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
              if (!current(entry)) return false;
              const local = definitionFor(entry, restore);
              const reply = await options.transport.put(
                entry.connection,
                local,
                definitionFor(entry, candidate),
              );
              if (!current(entry)) return false;
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
  const assign = (
    thread: PersonalThreadRef,
    target: string | null,
  ): Promise<PersonalResult<void>> => {
    const key = personalThreadKey(thread);
    const id = ++actionId;
    const predecessor = lanes.get(key);
    const submittedEntry = entries.get(thread.environmentId);
    const submittedRevision =
      submittedEntry?.snapshot?.assignments.find((a) => a.threadId === thread.id)?.revision ?? 0;
    let confirmedRevision: number | undefined;
    overlays.set(id, { thread, target });
    publish();
    const run = async (previous?: AssignmentLaneResult): Promise<PersonalResult<void>> => {
      const action = target === null ? "unassign" : "assign";
      // A queued intent depends on its predecessor's confirmed result, not on a
      // newer snapshot or conflict revision observed while that result was pending.
      if (previous && (!previous.result.ok || previous.confirmedRevision === undefined))
        return fail(action, key, "changed_elsewhere", thread.environmentId);
      const e = submittedEntry;
      if ((e && !current(e)) || (previous && previous.entry !== e))
        return fail(action, key, "unconfirmed", thread.environmentId);
      if (!e || !writable(e)) return fail(action, key, unavailable(e), thread.environmentId);
      const w = target === null ? null : winners(true).get(target);
      if (target !== null && (!w || w.deletedAt !== null))
        return fail(action, key, "workstream_deleted", thread.environmentId);
      if (w && isClosedPersonalWorkstream(w.lifecycle))
        return fail(action, key, "workstream_closed", thread.environmentId);
      const expectedRevision = previous?.confirmedRevision ?? submittedRevision;
      try {
        const reply = await options.transport.assign(e.connection, {
          threadId: thread.id,
          workstream: w === null || w === undefined ? null : definitionFor(e, w),
          expectedRevision,
        });
        if (!current(e)) return fail(action, key, "unconfirmed", thread.environmentId);
        mergeReply(e, reply);
        confirmedRevision = reply.assignment.revision;
        return { ok: true, value: undefined };
      } catch (error) {
        // Expose the canonical conflict, but never retry against its revision.
        if (current(e) && error && typeof error === "object" && "currentAssignment" in error) {
          const a = error.currentAssignment as PersonalWorkstreamAssignment | undefined;
          if (a && e.snapshot) {
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
        return fail(action, key, errorReason(error), thread.environmentId);
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
    const results = await Promise.all(
      [...entries.values()].map(async (entry) => {
        if (!entry.connection.connected || !entry.connection.capable || !options.transport.members)
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
    return {
      members: results.flatMap((members) => members ?? []),
      complete: results.every(Boolean),
    };
  };
  const controller: ReturnType<typeof createPersonalWorkstreamsController> = {
    state,
    readHistory,
    canAssign: (thread) => {
      const e = entries.get(thread.environmentId);
      return !!e && writable(e);
    },
    create: async ({ name, nameIconProjectKey }) => {
      try {
        return await put(
          "create",
          newPersonalWorkstream({
            id: uuid(),
            name: name.trim(),
            ...(nameIconProjectKey === undefined ? {} : { nameIconProjectKey }),
            orderKey:
              pinOrderKeyBetween(
                [...editableWinners().values()]
                  .filter((w) => w.deletedAt === null)
                  .sort((a, b) => compareString(a.orderKey, b.orderKey))
                  .at(-1)?.orderKey ?? null,
                null,
              ) ?? "n",
            now: now(),
            origin: options.origin,
          }),
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
      const orderKey = pinOrderKeyBetween(
        beforeId ? (merged.get(beforeId)?.orderKey ?? null) : null,
        afterId ? (merged.get(afterId)?.orderKey ?? null) : null,
      );
      return orderKey && orderKey.length <= 64
        ? edit("reorder", id, { orderKey })
        : Promise.resolve(fail("reorder", id, "failed"));
    },
    remove: async (id) => {
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
      const created = await controller.create({ name });
      if (!created.ok) return created;
      const assigned = await assign(thread, created.value.id);
      return assigned.ok ? created : assigned;
    },
    loadCache: (snapshot) => {
      const e = entries.get(snapshot.environmentId);
      if (!e || e.confirmed || !e.connection.capable) return;
      e.snapshot = snapshot;
      publish();
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
          old.connection.writable === c.writable
        )
          continue;
        old?.stop?.();
        const e: Entry = {
          connection: c,
          confirmed: false,
          sequence: -1,
          observations: new Map(),
          failed: false,
          ...(old?.snapshot ? { snapshot: old.snapshot } : {}),
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
    },
  };
  return controller;
}
