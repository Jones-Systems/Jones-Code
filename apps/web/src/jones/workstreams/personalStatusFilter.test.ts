import {
  personalThreadKey,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type { PersonalWorkstreamAssignment } from "@t3tools/contracts/jones/workstreamsPersonal";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import {
  effectivePersonalStatusFilter,
  nextSharedPersonalStatusChoice,
  personalSharedStatusFilterKey,
  personalStatusFilterKey,
  readPersonalStatusChoice,
  readPersonalStatusFilter,
  readSharedPersonalStatusChoice,
  selectPersonalStatusThreads,
  setPersonalStatusFilter,
  setSharedPersonalStatusFilter,
  stampLocalPersonalStatusChoice,
  type PersonalStatusChoice,
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
    expect(readSharedPersonalStatusChoice()).toBeNull();
  });

  describe("revisioned choices", () => {
    it("reads the legacy bare literal as revision 0 and the struct as written", () => {
      stored.set(personalStatusFilterKey("Alpha"), '"done"');
      stored.set(personalStatusFilterKey("Beta"), '{"filter":"hide-input","rev":4}');
      stored.set(personalStatusFilterKey("Gamma"), '{"filter":null,"rev":7}');
      expect(readPersonalStatusChoice("Alpha")).toEqual({ filter: "done", rev: 0 });
      expect(readPersonalStatusChoice("Beta")).toEqual({ filter: "hide-input", rev: 4 });
      expect(readPersonalStatusChoice("Gamma")).toEqual({ filter: null, rev: 7 });
      expect(readPersonalStatusFilter("Beta")).toBe("hide-input");
      expect(readPersonalStatusFilter("Gamma")).toBeNull();
    });

    it.each([
      "null",
      "{}",
      '{"filter":"working"}',
      '{"rev":1}',
      '{"filter":"inherit","rev":1}',
      '{"filter":"working","rev":-1}',
      '{"filter":"working","rev":1.5}',
      '{"filter":"working","rev":"1"}',
      "[]",
      "not-json",
    ])("reads %s as absent for a group and for the shared choice", (value) => {
      stored.set(personalStatusFilterKey("Alpha"), value);
      stored.set(personalSharedStatusFilterKey, value);
      expect(readPersonalStatusChoice("Alpha")).toBeNull();
      expect(readSharedPersonalStatusChoice()).toBeNull();
    });

    it("never accepts the legacy bare literal as the shared choice", () => {
      stored.set(personalSharedStatusFilterKey, '"working"');
      expect(readSharedPersonalStatusChoice()).toBeNull();
    });
  });
});

describe("personal status choice resolution", () => {
  const choice = (filter: PersonalStatusFilter | null, rev: number): PersonalStatusChoice => ({
    filter,
    rev,
  });

  it("prefers a group's own choice while it is at least as recent as the shared choice", () => {
    expect(effectivePersonalStatusFilter(null, null)).toBeNull();
    expect(effectivePersonalStatusFilter(choice("done", 0), null)).toBe("done");
    expect(effectivePersonalStatusFilter(null, choice("working", 1))).toBe("working");
    expect(effectivePersonalStatusFilter(choice("done", 1), choice("working", 1))).toBe("done");
    expect(effectivePersonalStatusFilter(choice("done", 1), choice("working", 2))).toBe("working");
    expect(effectivePersonalStatusFilter(choice("done", 3), choice("working", 2))).toBe("done");
  });

  it("treats a group's own clear as a tombstone and a shared clear as following the global filter", () => {
    expect(effectivePersonalStatusFilter(choice(null, 2), choice("input", 2))).toBeNull();
    expect(effectivePersonalStatusFilter(choice("done", 2), choice(null, 3))).toBeNull();
    expect(effectivePersonalStatusFilter(null, choice(null, 3))).toBeNull();
  });

  it("allocates revisions: only shared writes tick, a group's write is never older than shared", () => {
    expect(nextSharedPersonalStatusChoice("working", null, [])).toEqual(choice("working", 1));
    expect(
      nextSharedPersonalStatusChoice("input", choice("working", 1), [choice("done", 1), null]),
    ).toEqual(choice("input", 2));
    // A lost shared key still outranks every present group.
    expect(
      nextSharedPersonalStatusChoice("done", null, [choice("hide-done", 5), choice(null, 2)]),
    ).toEqual(choice("done", 6));
    expect(stampLocalPersonalStatusChoice("done", null, null)).toEqual(choice("done", 0));
    expect(
      stampLocalPersonalStatusChoice("done", choice("working", 4), choice("input", 2)),
    ).toEqual(choice("done", 4));
    expect(stampLocalPersonalStatusChoice(null, choice("working", 1), choice("input", 6))).toEqual(
      choice(null, 6),
    );
  });
});

describe("personal status writes", () => {
  let stored: Map<string, string>;
  const ids = ["Alpha", "Beta"];
  const effective = (id: string) =>
    effectivePersonalStatusFilter(readPersonalStatusChoice(id), readSharedPersonalStatusChoice());
  const storedChoice = (key: string) => JSON.parse(stored.get(key) ?? "null") as unknown;

  beforeEach(() => {
    stored = new Map();
    vi.stubGlobal("window", {
      localStorage: {
        getItem: (key: string) => stored.get(key) ?? null,
        setItem: (key: string, value: string) => void stored.set(key, value),
        removeItem: (key: string) => void stored.delete(key),
      },
      dispatchEvent: () => true,
    });
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("follows the last change across the shared choice and each group", () => {
    expect([effective("Alpha"), effective("Beta")]).toEqual([null, null]);
    setSharedPersonalStatusFilter(ids, "working");
    expect([effective("Alpha"), effective("Beta")]).toEqual(["working", "working"]);
    setPersonalStatusFilter("Alpha", "done");
    expect([effective("Alpha"), effective("Beta")]).toEqual(["done", "working"]);
    setSharedPersonalStatusFilter(ids, "input");
    expect([effective("Alpha"), effective("Beta")]).toEqual(["input", "input"]);
    // Clicking the selected status clears only that group; it does not fall back to the shared choice.
    setPersonalStatusFilter("Alpha", null);
    expect([effective("Alpha"), effective("Beta")]).toEqual([null, "input"]);
    // Clearing the shared choice sends every group to the global filter; nothing older revives.
    setSharedPersonalStatusFilter(ids, null);
    expect([effective("Alpha"), effective("Beta")]).toEqual([null, null]);
    setPersonalStatusFilter("Alpha", "done");
    expect([effective("Alpha"), effective("Beta")]).toEqual(["done", null]);
  });

  it("stores structs, keeps cleared records and lets only shared writes tick the sequence", () => {
    setSharedPersonalStatusFilter(ids, "working");
    setPersonalStatusFilter("Alpha", "done");
    setPersonalStatusFilter("Alpha", null);
    expect(storedChoice(personalSharedStatusFilterKey)).toEqual({ filter: "working", rev: 1 });
    expect(storedChoice(personalStatusFilterKey("Alpha"))).toEqual({ filter: null, rev: 1 });
    expect(stored.has(personalStatusFilterKey("Beta"))).toBe(false);
    setSharedPersonalStatusFilter(ids, null);
    expect(storedChoice(personalSharedStatusFilterKey)).toEqual({ filter: null, rev: 2 });
  });

  it("hydrates a legacy group value, then yields to a later shared choice", () => {
    stored.set(personalStatusFilterKey("Alpha"), '"done"');
    expect(effective("Alpha")).toBe("done");
    setSharedPersonalStatusFilter(ids, "working");
    expect(effective("Alpha")).toBe("working");
    expect(storedChoice(personalSharedStatusFilterKey)).toEqual({ filter: "working", rev: 1 });
  });

  it("outranks every present group when the shared record was lost", () => {
    setSharedPersonalStatusFilter(ids, "working");
    setSharedPersonalStatusFilter(ids, "input");
    setPersonalStatusFilter("Alpha", "done");
    stored.delete(personalSharedStatusFilterKey);
    expect(effective("Alpha")).toBe("done");
    setSharedPersonalStatusFilter(ids, "hide-done");
    expect(storedChoice(personalSharedStatusFilterKey)).toEqual({ filter: "hide-done", rev: 3 });
    expect([effective("Alpha"), effective("Beta")]).toEqual(["hide-done", "hide-done"]);
  });

  it("gives a group created after a shared choice that choice", () => {
    setSharedPersonalStatusFilter(ids, "working");
    expect(effective("Gamma")).toBe("working");
    setPersonalStatusFilter("Gamma", "done");
    expect(effective("Gamma")).toBe("done");
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
