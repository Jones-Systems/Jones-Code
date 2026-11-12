import { describe, expect, it } from "vite-plus/test";
import { AtomRegistry } from "effect/reactivity";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import {
  comparePersonalWorkstreams,
  type PersonalWorkstream,
  type PersonalWorkstreamsSnapshot,
} from "@t3tools/contracts/jones/workstreamsPersonal";
import {
  createPersonalWorkstreamsController,
  groupPersonalThreads,
  personalThreadKey,
  type PersonalConnection,
  type PersonalWorkstreamsTransport,
  type PersonalPutResult,
  type PersonalAssignResult,
} from "./controller.ts";
const env = (s: string) => s as EnvironmentId;
const thread = { environmentId: env("a"), id: "thread" as ThreadId };
const definition = (patch: Partial<PersonalWorkstream> = {}): PersonalWorkstream => ({
  id: "group",
  name: "Group",
  lifecycle: "active",
  color: null,
  orderKey: "n",
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-01T00:00:00.000Z",
  deletedAt: null,
  rev: 1,
  origin: "writer",
  ...patch,
});
const connection = (id: string, patch: Partial<PersonalConnection> = {}): PersonalConnection => ({
  environmentId: env(id),
  generation: 1,
  connected: true,
  capable: true,
  writable: true,
  ...patch,
});
const snapshot = (
  id: string,
  workstreams: readonly PersonalWorkstream[] = [],
  sequence = 0,
): PersonalWorkstreamsSnapshot => ({
  environmentId: env(id),
  sequence,
  workstreams,
  assignments: [],
  importIssue: null,
});
const deferred = <T>() => {
  let resolve!: (t: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((a, b) => {
    resolve = a;
    reject = b;
  });
  return { promise, resolve, reject };
};
const flush = async () => {
  for (let i = 0; i < 30; i++) await Promise.resolve();
};
function harness(overrides: Partial<PersonalWorkstreamsTransport> = {}) {
  const registry = AtomRegistry.make();
  const frames: {
    c: PersonalConnection;
    emit: (s: PersonalWorkstreamsSnapshot) => void;
    error: (e: unknown) => void;
  }[] = [];
  const puts: { c: PersonalConnection; w: PersonalWorkstream }[] = [];
  const transport: PersonalWorkstreamsTransport = {
    subscribe: (c, emit, error) => {
      frames.push({ c, emit, error });
      return () => {};
    },
    put: async (c, w) => {
      puts.push({ c, w });
      return { outcome: "applied", workstream: w, sequence: 1 };
    },
    assign: async (_c, input) => ({
      outcome: "applied",
      assignment: {
        threadId: input.threadId,
        workstreamId: input.workstream?.id ?? null,
        revision: input.expectedRevision + 1,
        updatedAt: "2026-01-01T00:00:00.000Z",
      },
      workstream: input.workstream,
      sequence: 1,
    }),
    ...overrides,
  };
  const controller = createPersonalWorkstreamsController({
    registry,
    origin: "test",
    transport,
    randomUUID: () => "new-group",
    now: () => "2026-01-02T00:00:00.000Z",
  });
  return {
    controller,
    frames,
    puts,
    read: () => registry.get(controller.state),
    close: () => {
      controller.dispose();
      registry.dispose();
    },
  };
}
describe("personal Workstreams controller", () => {
  it("converges across deterministic random interleavings, including equal-tuple payload ties", async () => {
    const records = Array.from({ length: 12 }, (_, i) =>
      definition({ name: `Name ${i}`, rev: 1 + (i % 3), origin: `writer${i % 2}` }),
    );
    for (let seed = 1; seed <= 20; seed++) {
      let random = seed;
      const shuffled = [...records].sort(() => {
        random = (random * 1664525 + 1013904223) >>> 0;
        return (random % 3) - 1;
      });
      const h = harness();
      h.controller.setEnvironments(
        shuffled.map((_, i) => connection(String(i), { writable: false })),
      );
      shuffled.forEach((w, i) => h.frames[i]!.emit(snapshot(String(i), [w])));
      const winner = records.reduce((a, b) => (comparePersonalWorkstreams(a, b) > 0 ? a : b));
      expect(h.read().workstreams[0]?.name).toBe(winner.name);
      expect(h.puts).toHaveLength(0);
      h.close();
    }
  });
  it("terminal tombstones suppress read-only assignments and later repair unchanged", async () => {
    const h = harness();
    const tombstone = definition({ deletedAt: "2026-01-02T00:00:00.000Z" });
    h.controller.setEnvironments([
      connection("a", { writable: false }),
      connection("b", { writable: false }),
    ]);
    h.frames[0]!.emit({
      ...snapshot("a", [definition({ rev: 999 })]),
      assignments: [{ threadId: thread.id, workstreamId: "group", revision: 1, updatedAt: null }],
    });
    h.frames[1]!.emit(snapshot("b", [tombstone]));
    expect(h.read().workstreams).toEqual([]);
    expect(groupPersonalThreads(h.read(), [thread]).ungrouped).toEqual([thread]);
    expect(h.puts).toHaveLength(0);
    h.controller.setEnvironments([
      connection("a", { generation: 2 }),
      connection("b", { writable: false }),
    ]);
    h.frames[2]!.emit(snapshot("a", [definition({ rev: 999 })]));
    await flush();
    expect(h.puts[0]?.w).toEqual(tombstone);
    h.close();
  });
  it("drops stale generations, frames and caches; downgrade suppresses grouping", () => {
    const h = harness();
    h.controller.setEnvironments([connection("a", { writable: false })]);
    h.controller.loadCache(snapshot("a", [definition({ name: "Cache" })], 100));
    expect(h.read().environments[0]?.confirmed).toBe(false);
    h.frames[0]!.emit(snapshot("a", [definition({ name: "Live" })], 2));
    h.controller.loadCache(snapshot("a", [definition({ name: "Cache" })], 100));
    h.frames[0]!.emit(snapshot("a", [definition({ name: "Stale" })], 1));
    expect(h.read().workstreams[0]?.name).toBe("Live");
    h.controller.setEnvironments([connection("a", { generation: 2, writable: false })]);
    h.frames[1]!.emit(snapshot("a", [definition({ name: "Replacement" })], 0));
    h.frames[0]!.emit(snapshot("a", [definition({ name: "Late" })], 999));
    expect(h.read().workstreams[0]?.name).toBe("Replacement");
    h.controller.setEnvironments([connection("a", { generation: 3, capable: false })]);
    expect(h.read().workstreams).toEqual([]);
    expect(h.read().environments[0]?.status).toBe("update_required");
    h.close();
  });
  it("partial fanout succeeds with no compensating write; superseded alone fails", async () => {
    const calls: PersonalWorkstream[] = [];
    const h = harness({
      put: async (c, w) => {
        calls.push(w);
        if (c.environmentId === env("b")) throw new Error("offline");
        return { outcome: "already_satisfied", workstream: w, sequence: 1 };
      },
    });
    h.controller.setEnvironments([connection("a"), connection("b")]);
    h.frames[0]!.emit(snapshot("a"));
    h.frames[1]!.emit(snapshot("b"));
    expect((await h.controller.create({ name: "Created" })).ok).toBe(true);
    expect(calls.every((w) => w.name === "Created" && w.rev === 1 && w.deletedAt === null)).toBe(
      true,
    );
    h.close();
    const j = harness({
      put: async (_c, w) => ({
        outcome: "superseded",
        workstream: { ...w, rev: 3, name: "Winner" },
        sequence: 2,
      }),
    });
    j.controller.setEnvironments([connection("a")]);
    j.frames[0]!.emit(snapshot("a"));
    expect(await j.controller.create({ name: "Loser" })).toMatchObject({
      ok: false,
      failure: { reason: "changed_elsewhere" },
    });
    expect(j.read().workstreams[0]?.name).toBe("Winner");
    j.close();
  });
  it("serializes assignments using confirmed revisions and removes only its own overlay", async () => {
    const first = deferred<PersonalAssignResult>();
    const expected: number[] = [];
    const h = harness({
      assign: async (_c, input) => {
        expected.push(input.expectedRevision);
        if (expected.length === 1) return first.promise;
        return {
          outcome: "applied",
          assignment: { threadId: thread.id, workstreamId: null, revision: 2, updatedAt: null },
          workstream: null,
          sequence: 2,
        };
      },
    });
    h.controller.setEnvironments([connection("a")]);
    h.frames[0]!.emit(snapshot("a", [definition()]));
    const a = h.controller.assign(thread, "group");
    const b = h.controller.assign(thread, null);
    await flush();
    expect(expected).toEqual([0]);
    expect(h.read().assignmentsByThreadKey.get(personalThreadKey(thread))?.workstreamId).toBe(null);
    first.resolve({
      outcome: "applied",
      assignment: { threadId: thread.id, workstreamId: "group", revision: 1, updatedAt: null },
      workstream: definition(),
      sequence: 1,
    });
    await a;
    await b;
    expect(expected).toEqual([0, 1]);
    expect(h.read().pendingThreadKeys.size).toBe(0);
    expect(h.read().assignmentsByThreadKey.get(personalThreadKey(thread))?.revision).toBe(2);
    h.close();
  });
  it("lost reply retry retains original confirmed revision; conflict never auto-uprevs", async () => {
    const expected: number[] = [];
    let mode = "lost";
    const h = harness({
      assign: async (_c, input) => {
        expected.push(input.expectedRevision);
        if (mode === "lost") throw new Error("lost reply");
        if (mode === "conflict")
          throw {
            code: "precondition_failed",
            currentAssignment: {
              threadId: thread.id,
              workstreamId: null,
              revision: 3,
              updatedAt: null,
            },
          };
        return {
          outcome: "already_satisfied",
          assignment: { threadId: thread.id, workstreamId: "group", revision: 1, updatedAt: null },
          workstream: definition(),
          sequence: 1,
        };
      },
    });
    h.controller.setEnvironments([connection("a")]);
    h.frames[0]!.emit(snapshot("a", [definition()]));
    expect(await h.controller.assign(thread, "group")).toMatchObject({
      ok: false,
      failure: { reason: "unconfirmed" },
    });
    mode = "same";
    expect((await h.controller.assign(thread, "group")).ok).toBe(true);
    expect(expected).toEqual([0, 0]);
    mode = "conflict";
    expect(await h.controller.assign(thread, null)).toMatchObject({
      ok: false,
      failure: { reason: "changed_elsewhere" },
    });
    await flush();
    expect(expected).toEqual([0, 0, 1]);
    expect(h.read().assignmentsByThreadKey.get(personalThreadKey(thread))?.revision).toBe(3);
    h.close();
  });
  it("ignores mutation replies after reconnect and preserves replies ahead of an older full frame", async () => {
    const reply = deferred<PersonalAssignResult>();
    const h = harness({ assign: () => reply.promise });
    h.controller.setEnvironments([connection("a")]);
    h.frames[0]!.emit(snapshot("a", [definition()]));
    const action = h.controller.assign(thread, "group");
    await flush();
    h.controller.setEnvironments([connection("a", { generation: 2 })]);
    h.frames[1]!.emit(snapshot("a", [definition()]));
    reply.resolve({
      outcome: "applied",
      assignment: { threadId: thread.id, workstreamId: "group", revision: 1, updatedAt: null },
      workstream: definition(),
      sequence: 10,
    });
    expect(await action).toMatchObject({ ok: false });
    expect(h.read().assignmentsByThreadKey.size).toBe(0);
    h.close();
    const j = harness();
    j.controller.setEnvironments([connection("a")]);
    j.frames[0]!.emit(snapshot("a", [definition()]));
    await j.controller.assign(thread, "group");
    j.frames[0]!.emit(snapshot("a", [definition()], 0));
    expect(j.read().assignmentsByThreadKey.get(personalThreadKey(thread))?.revision).toBe(1);
    j.close();
  });
  it("bounds repairs to four, deduplicates frames and does not retry persistent failures per render", async () => {
    const requests: ReturnType<typeof deferred<PersonalPutResult>>[] = [];
    const h = harness({
      put: () => {
        const d = deferred<PersonalPutResult>();
        requests.push(d);
        return d.promise;
      },
    });
    h.controller.setEnvironments([connection("a"), connection("b", { writable: false })]);
    h.frames[0]!.emit(snapshot("a"));
    h.frames[1]!.emit(
      snapshot(
        "b",
        Array.from({ length: 8 }, (_, i) => definition({ id: `group${i}` })),
      ),
    );
    await flush();
    expect(requests).toHaveLength(4);
    h.frames[1]!.emit(
      snapshot(
        "b",
        Array.from({ length: 8 }, (_, i) => definition({ id: `group${i}` })),
      ),
    );
    await flush();
    expect(requests).toHaveLength(4);
    for (let i = 0; i < 4; i++) requests[i]!.reject(new Error("denied"));
    await flush();
    expect(requests).toHaveLength(8);
    for (let i = 4; i < 8; i++) requests[i]!.reject(new Error("denied"));
    await flush();
    expect(h.read().environments[0]?.status).toBe("sync_failed");
    for (let i = 0; i < 10; i++) h.read();
    await flush();
    expect(requests).toHaveLength(8);
    h.close();
  });
});

describe("bridged clients", () => {
  it("two clients repair equal-tuple divergent servers to one unchanged canonical winner", async () => {
    const a = definition({ name: "Alpha" });
    const b = definition({ name: "Zulu" });
    const stores = new Map([
      [env("a"), snapshot("a", [a])],
      [env("b"), snapshot("b", [b])],
    ]);
    const subscribers = new Map<
      EnvironmentId,
      Set<(snapshot: PersonalWorkstreamsSnapshot) => void>
    >();
    const writes: PersonalWorkstream[] = [];
    const transport: Partial<PersonalWorkstreamsTransport> = {
      subscribe: (c, emit) => {
        const listeners = subscribers.get(c.environmentId) ?? new Set();
        subscribers.set(c.environmentId, listeners);
        listeners.add(emit);
        emit(stores.get(c.environmentId)!);
        return () => {
          listeners.delete(emit);
        };
      },
      put: async (c, w) => {
        const old = stores.get(c.environmentId)!;
        const prior = old.workstreams[0]!;
        const cmp = comparePersonalWorkstreams(w, prior);
        writes.push(w);
        if (cmp > 0) {
          const next = { ...old, sequence: old.sequence + 1, workstreams: [w] };
          stores.set(c.environmentId, next);
          for (const emit of subscribers.get(c.environmentId) ?? []) emit(next);
        }
        return {
          outcome: cmp > 0 ? "applied" : cmp === 0 ? "already_satisfied" : "superseded",
          workstream: stores.get(c.environmentId)!.workstreams[0]!,
          sequence: stores.get(c.environmentId)!.sequence,
        };
      },
    };
    const h = harness(transport);
    const j = harness(transport);
    h.controller.setEnvironments([connection("a"), connection("b")]);
    j.controller.setEnvironments([connection("b"), connection("a")]);
    await flush();
    const winner = comparePersonalWorkstreams(a, b) > 0 ? a : b;
    expect(stores.get(env("a"))?.workstreams).toEqual([winner]);
    expect(stores.get(env("b"))?.workstreams).toEqual([winner]);
    expect(h.read().workstreams[0]?.name).toBe(winner.name);
    expect(j.read().workstreams[0]?.name).toBe(winner.name);
    expect(writes.length).toBeLessThanOrEqual(2);
    expect(writes.every((w) => w.rev === 1 && w.origin === "writer")).toBe(true);
    h.close();
    j.close();
  });
  it("cached definitions never initiate repair, and old servers never receive commands", async () => {
    const h = harness();
    h.controller.setEnvironments([connection("a"), connection("b", { capable: false })]);
    h.controller.loadCache(snapshot("a", [definition()]));
    h.controller.loadCache(snapshot("b", [definition()]));
    await flush();
    expect(h.puts).toHaveLength(0);
    expect(
      await h.controller.assign({ environmentId: env("b"), id: thread.id }, "group"),
    ).toMatchObject({ ok: false, failure: { reason: "update_required" } });
    h.frames[0]!.emit(snapshot("a"));
    await flush();
    expect(h.puts).toHaveLength(0);
    expect(h.read().workstreams).toEqual([]);
    h.close();
  });
});

describe("CORE-01 queued revision dependencies", () => {
  it.each([
    { lost: false, aba: false },
    { lost: false, aba: true },
    { lost: true, aba: false },
    { lost: true, aba: true },
  ])(
    "does not rebase queued follow-ups after a competing snapshot (lost=$lost, ABA=$aba)",
    async ({ lost, aba }) => {
      const first = deferred<PersonalAssignResult>();
      const a = definition();
      const b = definition({ id: "other", name: "Other" });
      let canonical = {
        threadId: thread.id,
        workstreamId: null as string | null,
        revision: 0,
        updatedAt: null,
      };
      const expected: number[] = [];
      const h = harness({
        assign: async (_c, input) => {
          expected.push(input.expectedRevision);
          if (expected.length === 1) return first.promise;
          const target = input.workstream?.id ?? null;
          if (target === canonical.workstreamId)
            return {
              outcome: "already_satisfied",
              assignment: canonical,
              workstream: input.workstream,
              sequence: canonical.revision,
            };
          if (input.expectedRevision !== canonical.revision)
            throw { code: "precondition_failed", currentAssignment: canonical };
          canonical = { ...canonical, workstreamId: target, revision: canonical.revision + 1 };
          return {
            outcome: "applied",
            assignment: canonical,
            workstream: input.workstream,
            sequence: canonical.revision,
          };
        },
      });
      try {
        h.controller.setEnvironments([connection("a")]);
        h.frames[0]!.emit(snapshot("a", [a, b]));
        const assignA = h.controller.assign(thread, a.id);
        const unassign = h.controller.assign(thread, null);
        const queuedAfterUnassign = h.controller.assign(thread, null);
        await flush();
        expect(expected).toEqual([0]);
        // A committed at revision 1. Another client then moved to B, and optionally
        // back to A (ABA), before A's reply reached this controller.
        canonical = { ...canonical, workstreamId: aba ? a.id : b.id, revision: aba ? 3 : 2 };
        const competing = { ...canonical };
        h.frames[0]!.emit({
          ...snapshot("a", [a, b], canonical.revision),
          assignments: [canonical],
        });
        if (lost) first.reject(new Error("committed reply lost"));
        else
          first.resolve({
            outcome: "applied",
            assignment: { ...canonical, workstreamId: a.id, revision: 1 },
            workstream: a,
            sequence: 1,
          });
        expect(await assignA).toMatchObject({ ok: !lost });
        expect(await unassign).toMatchObject({
          ok: false,
          failure: { reason: "changed_elsewhere" },
        });
        expect(await queuedAfterUnassign).toMatchObject({
          ok: false,
          failure: { reason: "changed_elsewhere" },
        });
        expect(expected).toEqual(lost ? [0] : [0, 1]);
        expect(canonical).toEqual(competing);
        expect(h.read().assignmentsByThreadKey.get(personalThreadKey(thread))).toEqual(competing);
        expect(h.read().pendingThreadKeys.size).toBe(0);
        // A genuinely new intent after confirmation/conflict can use the observed
        // revision; failing a stale queue does not permanently poison this thread.
        expect((await h.controller.assign(thread, null)).ok).toBe(true);
        expect(expected.at(-1)).toBe(competing.revision);
        expect(canonical.workstreamId).toBe(null);
      } finally {
        h.close();
      }
    },
  );
});
