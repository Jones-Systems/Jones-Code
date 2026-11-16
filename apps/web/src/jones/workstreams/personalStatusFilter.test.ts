import {
  personalThreadKey,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type { PersonalWorkstreamAssignment } from "@t3tools/contracts/jones/workstreamsPersonal";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import {
  personalStatusFilterKey,
  readPersonalStatusFilter,
  selectPersonalStatusThreads,
  type PersonalStatusFilter,
} from "./personalStatusFilter";

describe("personal status filter storage", () => {
  let stored: Map<string, string>;

  beforeEach(() => {
    stored = new Map();
    vi.stubGlobal("window", {
      localStorage: { getItem: (key: string) => stored.get(key) ?? null },
    });
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("inherits missing preferences and isolates exact Workstream IDs", () => {
    expect(readPersonalStatusFilter("Alpha")).toBeNull();
    stored.set(personalStatusFilterKey("Alpha"), '"working"');
    stored.set(personalStatusFilterKey("Alpha:Beta"), '"input"');
    expect(readPersonalStatusFilter("Alpha")).toBe("working");
    expect(readPersonalStatusFilter("Alpha:Beta")).toBe("input");
    expect(readPersonalStatusFilter("Beta")).toBeNull();
  });

  it.each<PersonalStatusFilter>([
    "all",
    "working",
    "input",
    "done",
    "hide-working",
    "hide-input",
    "hide-done",
  ])("restores the %s selection", (filter) => {
    stored.set(personalStatusFilterKey("Alpha"), JSON.stringify(filter));
    expect(readPersonalStatusFilter("Alpha")).toBe(filter);
  });

  it.each(["", "not-json", "null", '"inherit"', "{}", "[]", "false", "42"])(
    "inherits for empty or malformed stored selection %s",
    (value) => {
      stored.set(personalStatusFilterKey("Alpha"), value);
      expect(readPersonalStatusFilter("Alpha")).toBeNull();
    },
  );

  it("inherits when browser storage cannot be read", () => {
    vi.stubGlobal("window", {
      get localStorage() {
        throw new Error("Storage unavailable");
      },
    });
    expect(readPersonalStatusFilter("Alpha")).toBeNull();
  });
});

function fixture() {
  const environmentId = EnvironmentId.make("personal-status-unit");
  const row = (id: string) => ({
    environmentId,
    id: ThreadId.make(id),
    pinnedAt: null as string | null,
  });
  const alphaWorking = { ...row("alpha-working"), pinnedAt: "2026-11-16T12:00:00.000Z" };
  const alphaDone = row("alpha-done");
  const betaInput = row("beta-input");
  const betaDone = row("beta-done");
  const unassigned = row("unassigned-done");
  const stale = row("deleted-group-done");
  const threads = [betaDone, alphaWorking, unassigned, betaInput, stale, alphaDone];
  const at = "2026-11-16T12:00:00.000Z";
  const assignments = new Map<string, PersonalWorkstreamAssignment>(
    (
      [
        [alphaWorking, "Alpha"],
        [alphaDone, "Alpha"],
        [betaInput, "Beta"],
        [betaDone, "Beta"],
        [stale, "deleted"],
      ] as const
    ).map(([thread, workstreamId]) => [
      personalThreadKey(thread),
      { threadId: thread.id, workstreamId, revision: 1, updatedAt: at },
    ]),
  );
  const state: PersonalWorkstreamsState = {
    workstreams: ["Alpha", "Beta"].map((id) => ({
      id,
      name: id,
      lifecycle: "active",
      color: null,
      orderKey: id,
      createdAt: at,
      updatedAt: at,
    })),
    assignmentsByThreadKey: assignments,
    environments: [],
    pendingThreadKeys: new Set(),
    canCreate: false,
  };
  const selections = {
    all: threads,
    working: [alphaWorking],
    input: [betaInput],
    done: [alphaDone, betaDone, unassigned, stale],
    "hide-working": [betaDone, unassigned, betaInput, stale, alphaDone],
  };
  const select = (
    filters: ReadonlyMap<string, PersonalStatusFilter | null>,
    scoped = threads,
    global = selections.done,
  ) => selectPersonalStatusThreads(scoped, state, global, filters, selections);
  return {
    state,
    assignments,
    threads,
    alphaWorking,
    alphaDone,
    betaInput,
    betaDone,
    unassigned,
    stale,
    select,
  };
}

describe("personal status membership", () => {
  it("combines global Done, Alpha Working and Beta Input in scoped order with original objects", () => {
    const f = fixture();
    const selected = f.select(
      new Map([
        ["Alpha", "working"],
        ["Beta", "input"],
      ]),
    );
    expect(selected).toEqual([f.alphaWorking, f.unassigned, f.betaInput, f.stale]);
    selected.forEach((thread) => expect(f.threads.includes(thread)).toBe(true));
    expect(selected[0]).toBe(f.alphaWorking);
  });

  it("inherits unset filters and ignores stale preferences for deleted groups", () => {
    const f = fixture();
    expect(
      f.select(
        new Map([
          ["Alpha", null],
          ["deleted", "working"],
        ]),
      ),
    ).toEqual([f.betaDone, f.unassigned, f.stale, f.alphaDone]);
  });

  it.each([
    ["all", ["alpha-working", "alpha-done"]],
    ["hide-working", ["alpha-done"]],
    ["hide-input", ["alpha-working", "alpha-done"]],
    ["hide-done", ["alpha-working"]],
  ] as const)("applies %s only to its assigned group", (filter, ids) => {
    const f = fixture();
    expect(
      f
        .select(new Map([["Alpha", filter]]))
        .filter((row) => row.id.startsWith("alpha"))
        .map((row) => row.id),
    ).toEqual(ids);
  });

  it("cannot restore candidates removed by project, environment or search scoping", () => {
    const f = fixture();
    expect(
      f.select(
        new Map([
          ["Alpha", "all"],
          ["Beta", "all"],
        ]),
        [f.alphaDone],
      ),
    ).toEqual([f.alphaDone]);
    expect(f.select(new Map([["Alpha", "all"]]), [])).toEqual([]);
  });

  it.each([
    ["hide-input", "beta-done"],
    ["hide-done", "beta-input"],
  ] as const)("inverts the selected %s classification within Beta", (filter, remaining) => {
    const f = fixture();
    const selected = f.select(new Map([["Beta", filter]]), f.threads, f.threads);
    expect(
      selected.filter((thread) => thread.id.startsWith("beta")).map((thread) => thread.id),
    ).toEqual([remaining]);
    expect(selected).toContain(f.alphaWorking);
    expect(selected).toContain(f.unassigned);
  });

  it("recomputes overrides after moves, unassignment and group deletion", () => {
    const f = fixture();
    const filters = new Map<string, PersonalStatusFilter>([
      ["Alpha", "working"],
      ["Beta", "input"],
    ]);
    const assignment = f.assignments.get(personalThreadKey(f.alphaWorking))!;
    f.assignments.set(personalThreadKey(f.alphaWorking), { ...assignment, workstreamId: "Beta" });
    expect(f.select(filters)).not.toContain(f.alphaWorking);
    f.assignments.set(personalThreadKey(f.alphaDone), {
      ...assignment,
      threadId: f.alphaDone.id,
      workstreamId: null,
    });
    expect(f.select(filters)).toContain(f.alphaDone);
    const withoutBeta = {
      ...f.state,
      workstreams: f.state.workstreams.filter((row) => row.id !== "Beta"),
    };
    expect(
      selectPersonalStatusThreads(f.threads, withoutBeta, [f.betaDone], filters, {
        all: f.threads,
        working: [],
        input: [f.betaInput],
        done: [f.betaDone],
        "hide-working": f.threads,
      }),
    ).toEqual([f.betaDone]);
  });

  it("keeps equal local thread IDs in other environments independent", () => {
    const f = fixture();
    const remote = { ...f.alphaWorking, environmentId: EnvironmentId.make("other") };
    expect(f.select(new Map([["Alpha", "working"]]), [remote, f.alphaWorking], [remote])).toEqual([
      remote,
      f.alphaWorking,
    ]);
  });
});
