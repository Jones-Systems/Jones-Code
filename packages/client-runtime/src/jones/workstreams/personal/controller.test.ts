import { describe, expect, it } from "vite-plus/test";
import { AtomRegistry } from "effect/reactivity";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import {
  comparePersonalWorkstreams,
  type PersonalWorkstream,
  type PersonalWorkstreamOperationResult,
  type PersonalWorkstreamsSnapshot,
  type RequestPersonalWorkstreamOperation,
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
function harness(
  overrides: Partial<PersonalWorkstreamsTransport> = {},
  randomUUID = () => "new-group",
) {
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
    randomUUID,
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

const laptop = env("laptop");
const fleetConnection = (id: string, patch: Partial<PersonalConnection> = {}) =>
  connection(id, { fleetCapable: true, ...patch });
const fleetFrame = (
  id: string,
  workstreams: readonly PersonalWorkstream[] = [],
  sequence = 0,
  operations: NonNullable<PersonalWorkstreamsSnapshot["fleet"]>["operations"] = [],
): PersonalWorkstreamsSnapshot => ({
  ...snapshot(id, workstreams, sequence),
  fleet: {
    authorityEnvironmentId: laptop,
    role: id === laptop ? "authority" : "follower",
    rosterRevision: 1,
    members: id === laptop ? [thread.environmentId] : [],
    operations,
  },
  supportsNameIcons: true,
  supportsSnooze: true,
});
const operation = (
  input: RequestPersonalWorkstreamOperation,
  patch: Partial<PersonalWorkstreamOperationResult["operation"]> = {},
): PersonalWorkstreamOperationResult["operation"] => ({
  requestId: input.requestId,
  workstreamId: input.workstreamId,
  kind: "put",
  status: "pending",
  rosterRevision: 1,
  requestedLifecycle: null,
  reason: null,
  peers: [{ environmentId: thread.environmentId, state: "pending", reason: null }],
  ...patch,
});

describe("laptop-authoritative personal Workstreams", () => {
  it("uses the authority snapshot even when a peer has a higher revision or extra rows", async () => {
    const requests: { c: PersonalConnection; input: RequestPersonalWorkstreamOperation }[] = [];
    const base = definition({ name: "Laptop row" });
    const renamed = definition({ name: "Changed on laptop", rev: 2, origin: "env:laptop" });
    const h = harness({
      request: async (c, input) => {
        requests.push({ c, input });
        return { workstream: renamed, operation: operation(input), sequence: 5 };
      },
    });
    try {
      h.controller.setEnvironments([fleetConnection("a"), fleetConnection(laptop)]);
      h.frames[0]!.emit(
        fleetFrame("a", [
          definition({ name: "Peer row", rev: 99 }),
          definition({ id: "peer-only" }),
        ]),
      );
      expect(h.read()).toMatchObject({ workstreams: [], canCreate: false });
      expect(await h.controller.rename(base.id, "Too soon")).toMatchObject({
        failure: { reason: "unconfirmed", environmentId: laptop },
      });
      h.frames[1]!.emit(fleetFrame(laptop, [base]));
      expect(h.read()).toMatchObject({
        workstreams: [base],
        canCreate: true,
        fleetAuthorityStatus: "ready",
      });
      expect(await h.controller.rename(base.id, renamed.name)).toEqual({
        ok: true,
        value: renamed,
      });
      expect(requests).toEqual([
        {
          c: h.frames[1]!.c,
          input: {
            requestId: "new-group",
            workstreamId: base.id,
            expected: base,
            action: { kind: "edit", patch: { name: renamed.name } },
          },
        },
      ]);
      expect(h.read().workstreams).toEqual([base]);
      h.frames[1]!.emit(fleetFrame(laptop, [renamed], 5));
      expect(h.read().workstreams).toEqual([renamed]);
      await flush();
      expect(h.puts).toEqual([]);
    } finally {
      h.close();
    }
  });

  it("keeps cached authority display-only across offline, downgrade and authority removal", async () => {
    const cached = definition({ name: "Cached laptop" });
    const h = harness({
      request: async () => {
        throw new Error("Unexpected fleet request");
      },
    });
    try {
      h.controller.setEnvironments([
        fleetConnection(laptop, { connected: false }),
        fleetConnection("a"),
      ]);
      h.controller.loadCache(fleetFrame(laptop, [cached], 99));
      h.frames[0]!.emit(snapshot("a", [definition({ name: "Legacy peer", rev: 100 })]));
      expect(h.read()).toMatchObject({ workstreams: [cached], fleetAuthorityStatus: "offline" });
      expect(await h.controller.create({ name: "Offline" })).toMatchObject({
        failure: { reason: "offline", environmentId: laptop },
      });
      h.controller.setEnvironments([connection(laptop, { generation: 2 }), fleetConnection("a")]);
      h.frames[1]!.emit(snapshot(laptop, [definition({ name: "Downgrade" })]));
      expect(h.read()).toMatchObject({
        workstreams: [cached],
        fleetAuthorityStatus: "update_required",
      });
      expect(await h.controller.remove(cached.id)).toMatchObject({
        failure: { reason: "update_required", environmentId: laptop },
      });
      h.controller.setEnvironments([fleetConnection("a")]);
      expect(h.read()).toMatchObject({ workstreams: [], fleetAuthorityStatus: "offline" });
      await flush();
      expect(h.puts).toEqual([]);
    } finally {
      h.close();
    }
  });

  it("reports read-only and conflicting authority without writing to a writable peer", async () => {
    const h = harness();
    try {
      h.controller.setEnvironments([
        fleetConnection(laptop, { writable: false }),
        fleetConnection("a"),
      ]);
      h.frames[0]!.emit(fleetFrame(laptop, [definition()]));
      h.frames[1]!.emit(fleetFrame("a", [definition({ rev: 2 })]));
      expect(await h.controller.rename("group", "Read-only laptop")).toMatchObject({
        failure: { reason: "read_only", environmentId: laptop },
      });
      const other = fleetFrame("a", [definition()]);
      h.frames[1]!.emit({
        ...other,
        sequence: 1,
        fleet: { ...other.fleet!, authorityEnvironmentId: thread.environmentId, role: "authority" },
      });
      expect(h.read()).toMatchObject({
        workstreams: [],
        fleetAuthorityStatus: "changed_elsewhere",
      });
      expect(await h.controller.create({ name: "Conflicting laptop identities" })).toMatchObject({
        failure: { reason: "changed_elsewhere" },
      });
      await flush();
      expect(h.puts).toEqual([]);
    } finally {
      h.close();
    }
  });

  it("moves an accepted close out of active groups and keeps its threads discoverable", async () => {
    const base = definition();
    const closed = definition({ lifecycle: "completed", lifecycleRevision: 1, rev: 2 });
    const receipt = deferred<PersonalWorkstreamOperationResult>();
    const requests: RequestPersonalWorkstreamOperation[] = [];
    const assignments: PersonalConnection[] = [];
    const h = harness({
      request: (_c, input) => {
        requests.push(input);
        return receipt.promise;
      },
      assign: (c) => {
        assignments.push(c);
        return Promise.reject(new Error("Unexpected assignment"));
      },
    });
    try {
      h.controller.setEnvironments([fleetConnection(laptop), fleetConnection("a")]);
      h.frames[0]!.emit(fleetFrame(laptop, [base]));
      h.frames[1]!.emit({
        ...fleetFrame("a", [base]),
        assignments: [{ threadId: thread.id, workstreamId: base.id, revision: 1, updatedAt: null }],
      });
      const closing = h.controller.setLifecycle(base.id, "completed");
      expect(h.read().pendingCloseWorkstreamIds?.has(base.id)).toBe(true);
      expect(groupPersonalThreads(h.read(), [thread]).groups[0]?.threads).toEqual([thread]);
      const close = operation(requests[0]!, { kind: "close", requestedLifecycle: "completed" });
      receipt.resolve({ operation: close, workstream: base, sequence: 1 });
      expect(await closing).toEqual({ ok: true, value: base });
      h.frames[1]!.emit({
        ...fleetFrame("a", [closed], 1),
        assignments: [{ threadId: thread.id, workstreamId: base.id, revision: 1, updatedAt: null }],
      });
      expect(h.read().workstreams[0]?.lifecycle).toBe("active");
      const grouping = groupPersonalThreads(h.read(), [thread]);
      expect(grouping.groups).toEqual([]);
      expect(grouping.ungrouped).toEqual([thread]);
      expect(h.read().assignmentsByThreadKey.get(personalThreadKey(thread))?.workstreamId).toBe(
        base.id,
      );
      expect(h.read().fleet?.operations).toEqual([close]);
      expect(await h.controller.rename(base.id, "Wait for close")).toMatchObject({
        failure: { reason: "changed_elsewhere", environmentId: laptop },
      });
      expect(await h.controller.assign(thread, base.id)).toMatchObject({
        failure: { reason: "changed_elsewhere" },
      });
      h.frames[0]!.emit(fleetFrame(laptop, [closed], 2));
      expect(h.read().pendingCloseWorkstreamIds?.has(base.id)).toBe(false);
      expect(groupPersonalThreads(h.read(), [thread]).ungrouped).toEqual([thread]);
      expect(await h.controller.assign(thread, base.id)).toMatchObject({
        failure: { reason: "workstream_closed", environmentId: laptop },
      });
      expect(requests).toHaveLength(1);
      expect(assignments).toEqual([]);
      expect(h.puts).toEqual([]);
    } finally {
      h.close();
    }
  });

  it("starts assignment from the exact owner's confirmed revision and ignores a replaced reply", async () => {
    const base = definition();
    const assignment = (revision: number) => ({
      threadId: thread.id,
      workstreamId: null,
      revision,
      updatedAt: null,
    });
    const reply = deferred<PersonalAssignResult>();
    const calls: { c: PersonalConnection; revision: number }[] = [];
    const h = harness({
      assign: (c, input) => {
        calls.push({ c, revision: input.expectedRevision });
        return reply.promise;
      },
    });
    try {
      const authority = fleetConnection(laptop);
      h.controller.setEnvironments([authority, fleetConnection("a")]);
      h.frames[0]!.emit(fleetFrame(laptop, [base]));
      h.controller.loadCache({ ...fleetFrame("a", [base], 99), assignments: [assignment(99)] });
      const cached = h.controller.assign(thread, base.id);
      h.frames[1]!.emit({ ...fleetFrame("a", [base]), assignments: [assignment(3)] });
      expect(await cached).toMatchObject({ failure: { reason: "unconfirmed" } });
      expect(calls).toEqual([]);
      const first = h.controller.assign(thread, base.id);
      const queued = h.controller.assign(thread, null);
      await flush();
      expect(calls).toEqual([{ c: h.frames[1]!.c, revision: 3 }]);
      h.controller.setEnvironments([authority, fleetConnection("a", { generation: 2 })]);
      h.frames[2]!.emit({ ...fleetFrame("a", [base]), assignments: [assignment(8)] });
      reply.resolve({
        outcome: "applied",
        assignment: { ...assignment(4), workstreamId: base.id },
        workstream: base,
        sequence: 10,
      });
      expect(await first).toMatchObject({ failure: { reason: "unconfirmed" } });
      expect(await queued).toMatchObject({ failure: { reason: "changed_elsewhere" } });
      expect(calls).toHaveLength(1);
      expect(h.read().assignmentsByThreadKey.get(personalThreadKey(thread))).toEqual(assignment(8));
      expect(h.read().pendingThreadKeys.size).toBe(0);
    } finally {
      h.close();
    }
  });

  it("rejects an authority reply from a replaced session and refreshes its preimage on the new session", async () => {
    const result = deferred<PersonalWorkstreamOperationResult>();
    const inputs: RequestPersonalWorkstreamOperation[] = [];
    const h = harness({
      request: async (_c, input) => {
        inputs.push(input);
        if (inputs.length === 1) return result.promise;
        throw { code: "precondition_failed" };
      },
    });
    try {
      const base = definition();
      h.controller.setEnvironments([fleetConnection(laptop)]);
      h.frames[0]!.emit(fleetFrame(laptop, [base], 9));
      const first = h.controller.rename(base.id, "Old request");
      h.controller.setEnvironments([fleetConnection(laptop, { generation: 2 })]);
      const newer = definition({ name: "Replacement baseline" });
      h.frames[1]!.emit(fleetFrame(laptop, [newer]));
      result.resolve({ operation: operation(inputs[0]!), workstream: base, sequence: 10 });
      expect(await first).toMatchObject({ failure: { reason: "unconfirmed" } });
      expect(await h.controller.rename(base.id, "New request")).toMatchObject({
        failure: { reason: "changed_elsewhere" },
      });
      expect(inputs.map((input) => input.expected)).toEqual([base, newer]);
      expect(h.read().workstreams).toEqual([newer]);
      expect(h.read().fleet?.operations).toEqual([]);
    } finally {
      h.close();
    }
  });

  it("routes cancellation, resolution and roster changes to their exact authority session", async () => {
    const base = definition();
    const request = {
      requestId: "close",
      workstreamId: base.id,
      expected: base,
      action: { kind: "delete" },
    } as const;
    const stale = operation(request, {
      kind: "close",
      status: "stale",
      requestedLifecycle: "completed",
    });
    const observed = {
      key: "a".repeat(64),
      name: "Peer Workstream",
      lifecycle: "active",
      deleted: false,
    } as const;
    const conflict = operation(
      { ...request, requestId: "conflict" },
      {
        peers: [
          {
            environmentId: thread.environmentId,
            state: "conflict",
            reason: "definition_changed",
            observed,
          },
        ],
      },
    );
    const calls: unknown[] = [];
    let id = 0;
    const h = harness(
      {
        cancel: async (connection, input) => {
          calls.push({ connection, input });
          return {
            operation: operation({ ...request, requestId: input.cancellationId }),
            workstream: base,
            sequence: 2,
          };
        },
        resolve: async (connection, input) => {
          calls.push({ connection, input });
          return {
            operation: operation({ ...request, requestId: input.resolutionId }),
            workstream: base,
            sequence: 4,
          };
        },
        manageFleet: async (connection, input) => {
          calls.push({ connection, input });
          return {
            authorityEnvironmentId: laptop,
            role: "authority",
            rosterRevision: 2,
            members: [],
            operations: [],
          };
        },
      },
      () => `operation-${++id}`,
    );
    try {
      h.controller.setEnvironments([fleetConnection("a"), fleetConnection(laptop)]);
      h.frames[0]!.emit(fleetFrame("a", [base]));
      h.frames[1]!.emit(fleetFrame(laptop, [base], 1, [stale]));
      expect(h.read().pendingCloseWorkstreamIds?.has(base.id)).toBe(true);
      expect(await h.controller.cancelOperation?.(stale.requestId)).toMatchObject({
        ok: true,
        value: { operation: { requestId: "operation-1" } },
      });
      expect(h.read().pendingCloseWorkstreamIds?.has(base.id)).toBe(false);
      h.frames[1]!.emit(fleetFrame(laptop, [base], 3, [conflict]));
      const choice = {
        requestId: conflict.requestId,
        environmentId: thread.environmentId,
        expectedObservationKey: observed.key,
        choice: "use_peer",
      } as const;
      expect(await h.controller.resolveOperation?.(choice)).toMatchObject({
        ok: true,
        value: { operation: { requestId: "operation-2" } },
      });
      expect(h.read().fleet?.operations.map(({ requestId }) => requestId)).toEqual(["operation-2"]);
      const removePeer = {
        kind: "remove",
        environmentId: thread.environmentId,
        expectedRosterRevision: 1,
      } as const;
      expect(await h.controller.manageFleet?.(thread.environmentId, removePeer)).toMatchObject({
        failure: { reason: "changed_elsewhere" },
      });
      expect(await h.controller.manageFleet?.(laptop, removePeer)).toMatchObject({
        ok: true,
        value: { rosterRevision: 2, members: [] },
      });
      expect(h.read().fleet?.rosterRevision).toBe(1);
      const connection = h.frames[1]!.c;
      expect(calls).toEqual([
        { connection, input: { requestId: stale.requestId, cancellationId: "operation-1" } },
        { connection, input: { ...choice, resolutionId: "operation-2" } },
        { connection, input: removePeer },
      ]);
    } finally {
      h.close();
    }
  });

  it("uses only a live exact follower approval offer", async () => {
    const proposal = {
      authorityEnvironmentId: laptop,
      sessionId: "offered-session",
      label: "Laptop",
    };
    const request = {
      kind: "follow",
      authorityEnvironmentId: laptop,
      sessionId: proposal.sessionId,
    } as const;
    const calls: PersonalConnection[] = [];
    const h = harness({
      manageFleet: async (connection) => {
        calls.push(connection);
        return fleetFrame("a").fleet!;
      },
    });
    try {
      h.controller.setEnvironments([fleetConnection("a")]);
      const offered = { ...snapshot("a"), fleetProposals: [proposal] };
      h.controller.loadCache(offered);
      expect(h.read().environments[0]?.fleetProposals).toBeUndefined();
      expect(await h.controller.manageFleet?.(thread.environmentId, request)).toMatchObject({
        failure: { reason: "unconfirmed" },
      });
      h.frames[0]!.emit(offered);
      expect(h.read().environments[0]?.fleetProposals).toEqual([proposal]);
      expect(
        await h.controller.manageFleet?.(thread.environmentId, { ...request, sessionId: "other" }),
      ).toMatchObject({
        failure: { reason: "changed_elsewhere" },
      });
      expect(await h.controller.manageFleet?.(thread.environmentId, request)).toMatchObject({
        ok: true,
        value: { role: "follower", authorityEnvironmentId: laptop },
      });
      expect(calls).toEqual([h.frames[0]!.c]);
      expect(h.read()).toMatchObject({ canCreate: false, fleetAuthorityStatus: "offline" });
    } finally {
      h.close();
    }
  });

  it("uses a same-session create receipt for laptop-owned create and assign before the frame arrives", async () => {
    const requests: RequestPersonalWorkstreamOperation[] = [];
    const assignedTo: string[] = [];
    let nextId = 0;
    const h = harness(
      {
        request: async (_c, input) => {
          requests.push(input);
          if (input.action.kind !== "create") throw new Error("Expected create request");
          return {
            operation: operation(input),
            sequence: 1,
            workstream: definition({
              id: input.workstreamId,
              name: input.action.name,
              orderKey: input.action.orderKey,
            }),
          };
        },
        assign: async (c, input) => {
          assignedTo.push(c.environmentId);
          return {
            outcome: "applied",
            workstream: input.workstream,
            sequence: 2,
            assignment: {
              threadId: input.threadId,
              workstreamId: input.workstream?.id ?? null,
              revision: input.expectedRevision + 1,
              updatedAt: null,
            },
          };
        },
      },
      () => `id-${++nextId}`,
    );
    try {
      h.controller.setEnvironments([fleetConnection(laptop)]);
      h.frames[0]!.emit(fleetFrame(laptop));
      const localThread = { ...thread, environmentId: laptop };
      expect(await h.controller.createAndAssign(localThread, "New stream")).toMatchObject({
        ok: true,
        value: { id: "id-1" },
      });
      expect(requests).toEqual([
        {
          workstreamId: "id-1",
          requestId: "id-2",
          expected: null,
          action: { kind: "create", name: "New stream", orderKey: expect.any(String) },
        },
      ]);
      expect(assignedTo).toEqual([laptop]);
      expect(h.read().workstreams).toEqual([]);
    } finally {
      h.close();
    }
  });

  it("retains legacy repair and writes when fleet-capable servers have no fleet blocks", async () => {
    const h = harness();
    try {
      h.controller.setEnvironments([fleetConnection("a"), fleetConnection("b")]);
      h.frames[0]!.emit(snapshot("a", [definition()]));
      h.frames[1]!.emit(snapshot("b"));
      await flush();
      expect(h.read().fleet).toBeUndefined();
      expect(h.puts.map(({ c }) => c.environmentId)).toEqual([env("b")]);
      expect(await h.controller.rename("group", "Legacy rename")).toMatchObject({ ok: true });
      expect(h.puts.slice(1).map(({ c }) => c.environmentId)).toEqual([env("a"), env("b")]);
    } finally {
      h.close();
    }
  });
});

describe("personal Workstreams controller", () => {
  it("confirms icon-aware creation before replicating a compatible definition to older servers", async () => {
    const h = harness();
    try {
      h.controller.setEnvironments([connection("a"), connection("b")]);
      h.frames[0]!.emit({ ...snapshot("a"), supportsSnooze: true });
      expect(h.read().canSetNameIcon).toBe(false);
      expect(
        await h.controller.create({ name: "Icons", nameIconProjectKey: "project:one" }),
      ).toMatchObject({
        ok: false,
        failure: { reason: "update_required" },
      });
      h.frames[1]!.emit({ ...snapshot("b"), supportsNameIcons: true });
      expect(h.read().canSetNameIcon).toBe(true);
      const result = await h.controller.create({
        name: "Icons",
        nameIconProjectKey: "project:one",
      });
      expect(result).toMatchObject({
        ok: true,
        value: { nameIcon: { projectKey: "project:one" } },
      });
      await flush();
      expect(h.puts.map(({ c }) => c.environmentId)).toEqual([env("b"), env("a")]);
      expect(h.puts[1]?.w).not.toHaveProperty("nameIcon");
      expect(h.read().workstreams[0]?.nameIcon?.projectKey).toBe("project:one");
      const snoozedUntil = "2026-02-01T00:00:00.000Z";
      expect(await h.controller.setSnoozedUntil?.("new-group", snoozedUntil)).toMatchObject({
        ok: true,
      });
      await flush();
      expect(h.puts[2]?.w).toMatchObject({ snoozedUntil });
      expect(h.puts[2]?.w).not.toHaveProperty("nameIcon");
      expect(h.puts[3]?.w).toMatchObject({ nameIcon: { projectKey: "project:one" } });
      expect(h.puts[3]?.w).not.toHaveProperty("snoozedUntil");
      expect(await h.controller.setNameIcon?.("new-group", null)).toMatchObject({ ok: true });
      await flush();
      expect(h.read().workstreams[0]?.nameIcon?.projectKey).toBeNull();
      expect(h.read().workstreams[0]?.snoozedUntil).toBe(snoozedUntil);
    } finally {
      h.close();
    }
  });

  it("keeps a woken member in history while showing it in Unassigned until explicit reopen", async () => {
    const h = harness({
      members: async () => [{ threadId: thread.id, title: "Finished work", settled: true }],
    });
    h.controller.setEnvironments([connection("a")]);
    h.frames[0]!.emit({
      ...snapshot("a", [definition()]),
      assignments: [{ threadId: thread.id, workstreamId: "group", revision: 1, updatedAt: null }],
    });
    expect(await h.controller.setLifecycle("group", "completed")).toMatchObject({ ok: true });
    expect(groupPersonalThreads(h.read(), [thread])).toMatchObject({
      groups: [],
      ungrouped: [thread],
    });
    expect(h.read().assignmentsByThreadKey.get(personalThreadKey(thread))?.workstreamId).toBe(
      "group",
    );
    expect(await h.controller.readHistory?.("group")).toMatchObject({
      complete: true,
      members: [{ ...thread, title: "Finished work" }],
    });
    expect(await h.controller.setLifecycle("group", "active")).toMatchObject({ ok: true });
    expect(groupPersonalThreads(h.read(), [thread]).groups[0]?.threads).toEqual([thread]);
    h.close();
  });

  it("checks every configured environment before writing a close", async () => {
    const h = harness({
      members: async (c) =>
        c.environmentId === "b"
          ? [{ threadId: thread.id, title: "Still open on B", settled: false }]
          : [],
    });
    h.controller.setEnvironments([connection("a"), connection("b")]);
    h.frames[0]!.emit(snapshot("a", [definition()]));
    h.frames[1]!.emit(snapshot("b", [definition()]));
    expect(await h.controller.setLifecycle("group", "completed")).toMatchObject({
      ok: false,
      failure: { reason: "unsettled_threads" },
    });
    expect(h.puts).toEqual([]);
    expect(h.read().workstreams[0]?.lifecycle).toBe("active");
    h.controller.setEnvironments([connection("a"), connection("b", { connected: false })]);
    expect(await h.controller.setLifecycle("group", "abandoned")).toMatchObject({
      ok: false,
      failure: { reason: "closure_unavailable" },
    });
    expect(h.puts).toEqual([]);
    h.close();
  });

  it("restores an accepted copy when another environment detects a racing wake", async () => {
    const firstWrite = deferred<void>();
    const wakeChecked = deferred<void>();
    const restorationTargets: string[] = [];
    const h = harness({
      members: async () => [],
      put: async (c, w, expected) => {
        if (w.lifecycle !== "active") {
          if (c.environmentId === "b") {
            await wakeChecked.promise;
            throw { code: "unsettled_threads" };
          }
          firstWrite.resolve();
          return { outcome: "applied", workstream: w, sequence: 1 };
        }
        if (expected) {
          expect(expected.lifecycle).toBe("abandoned");
          restorationTargets.push(c.environmentId);
        }
        return { outcome: "applied", workstream: w, sequence: 2 };
      },
    });
    h.controller.setEnvironments([connection("a"), connection("b")]);
    h.frames[0]!.emit(snapshot("a", [definition()]));
    h.frames[1]!.emit(snapshot("b", [definition()]));
    const result = h.controller.setLifecycle("group", "abandoned");
    await firstWrite.promise;
    expect(h.read().workstreams[0]?.lifecycle).toBe("active");
    wakeChecked.resolve();
    expect(await result).toMatchObject({ ok: false, failure: { reason: "unsettled_threads" } });
    expect(restorationTargets).toEqual(["a"]);
    expect(h.read().workstreams[0]?.lifecycle).toBe("active");
    h.close();
  });
  it("keeps source definitions separate from the merged Workstream winner", () => {
    const h = harness();
    h.controller.setEnvironments([
      connection("a", { writable: false }),
      connection("b", { writable: false }),
    ]);
    h.frames[0]!.emit(snapshot("a"));
    h.frames[1]!.emit(snapshot("b", [definition({ name: "Only on B" })]));
    expect(h.read().workstreams[0]?.name).toBe("Only on B");
    expect(h.read().environments[0]?.workstreams).toEqual([]);
    h.frames[0]!.emit(snapshot("a", [definition({ name: "Reported by A" })], 1));
    expect(h.read().environments.map((entry) => entry.workstreams?.[0]?.name)).toEqual([
      "Reported by A",
      "Only on B",
    ]);
    h.close();
  });
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

describe("automatically assigned threads", () => {
  const key = personalThreadKey(thread);
  const assigned = (revision: number, workstreamId: string | null = "group", sequence = 1) => ({
    ...snapshot("a", [definition(), definition({ id: "other", name: "Other" })], sequence),
    assignments: [{ threadId: thread.id, workstreamId, revision, updatedAt: null }],
  });
  const auto = [
    {
      threadId: thread.id,
      workstreamId: "group",
      revision: 1,
      decidedAt: "2026-01-01T00:00:00.000Z",
    },
  ];
  it("exposes the key while the assignment matches and drops it on a remote move", () => {
    const h = harness();
    h.controller.setEnvironments([connection("a")]);
    h.frames[0]!.emit(assigned(1));
    expect(h.read().autoAssignedThreadKeys?.has(key)).toBe(false);
    h.frames[0]!.emit({ ...assigned(1, "group", 2), autoAssignments: auto });
    expect(h.read().autoAssignedThreadKeys?.has(key)).toBe(true);
    // A stale entry beside a newer assignment revision is not shown.
    h.frames[0]!.emit({ ...assigned(2, "other", 3), autoAssignments: auto });
    expect(h.read().autoAssignedThreadKeys?.has(key)).toBe(false);
    h.close();
  });
  it("drops the key during and after a local move or remove", async () => {
    for (const target of ["other", null]) {
      const pending = deferred<PersonalAssignResult>();
      const h = harness({ assign: () => pending.promise });
      h.controller.setEnvironments([connection("a")]);
      h.frames[0]!.emit({ ...assigned(1), autoAssignments: auto });
      expect(h.read().autoAssignedThreadKeys?.has(key)).toBe(true);
      const moved = h.controller.assign(thread, target);
      await flush();
      expect(h.read().autoAssignedThreadKeys?.has(key)).toBe(false);
      pending.resolve({
        outcome: "applied",
        assignment: { threadId: thread.id, workstreamId: target, revision: 2, updatedAt: null },
        workstream: target === null ? null : definition({ id: "other", name: "Other" }),
        sequence: 2,
      });
      expect((await moved).ok).toBe(true);
      expect(h.read().autoAssignedThreadKeys?.has(key)).toBe(false);
      h.close();
    }
  });
});
