import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId, ThreadId } from "../baseSchemas.ts";
import {
  AssignPersonalWorkstreamInput,
  PersonalWorkstream,
  PersonalWorkstreamAssignment,
  PersonalWorkstreamsSnapshot,
  canonicalPersonalWorkstream,
  comparePersonalWorkstreams,
  deletePersonalWorkstream,
  editPersonalWorkstream,
  mergePersonalWorkstreams,
  newPersonalWorkstream,
} from "./workstreamsPersonal.ts";

const now = "2026-11-12T12:00:00.000Z";
const later = "2026-11-12T12:00:01.000Z";
const original = newPersonalWorkstream({
  id: "legacy:group-1",
  name: "Example",
  orderKey: "n",
  now,
  origin: "client:one",
});

describe("personal Workstream convergence", () => {
  it("converges equal-version divergent payloads in either arrival order", () => {
    const a = { ...original, name: "A" };
    const b = { ...original, name: "B" };
    expect(comparePersonalWorkstreams(a, b)).toBe(-1);
    expect(comparePersonalWorkstreams(b, a)).toBe(1);
    expect(mergePersonalWorkstreams([a, b])).toBe(b);
    expect(mergePersonalWorkstreams([b, a])).toBe(b);
    expect(comparePersonalWorkstreams(a, { ...a })).toBe(0);
  });

  it("uses UTF-16 code units for the payload tiebreak", () => {
    const astral = { ...original, name: "\u{10000}" };
    const bmp = { ...original, name: "\uE000" };
    expect(comparePersonalWorkstreams(astral, bmp)).toBe(-1);
    expect(mergePersonalWorkstreams([bmp, astral])).toBe(bmp);
  });

  it("compares revision numerically before timestamp, origin and payload", () => {
    const older = { ...original, rev: 2, updatedAt: later, origin: "z", name: "Z" };
    const newer = { ...original, rev: 10, origin: "a", name: "A" };
    expect(comparePersonalWorkstreams(older, newer)).toBe(-1);
    expect(comparePersonalWorkstreams(original, { ...original, updatedAt: later })).toBe(-1);
    expect(comparePersonalWorkstreams(original, { ...original, origin: "client:two" })).toBe(-1);
  });

  it("keeps a terminal tombstone above every live revision and orders tombstones", () => {
    const deleted = deletePersonalWorkstream(original, later, "client:one");
    const offlineEdit = { ...original, rev: Number.MAX_SAFE_INTEGER, updatedAt: later };
    expect(comparePersonalWorkstreams(deleted, offlineEdit)).toBe(1);
    expect(mergePersonalWorkstreams([deleted, offlineEdit])).toBe(deleted);
    expect(mergePersonalWorkstreams([offlineEdit, deleted])).toBe(deleted);
    expect(comparePersonalWorkstreams(deleted, { ...deleted, rev: deleted.rev + 1 })).toBe(-1);
  });

  it("canonicalizes fields independently of object property insertion order", () => {
    const reversed = Schema.decodeUnknownSync(PersonalWorkstream)(
      Object.fromEntries(Object.entries(original).reverse()),
    );
    expect(canonicalPersonalWorkstream(reversed)).toBe(canonicalPersonalWorkstream(original));
    for (const changed of [
      { ...original, id: "other" },
      { ...original, name: "Other" },
      { ...original, lifecycle: "paused" as const },
      { ...original, color: "#ABCDEF" },
      { ...original, orderKey: "o" },
      { ...original, createdAt: later },
      { ...original, updatedAt: later },
      { ...original, deletedAt: later },
      { ...original, rev: 2 },
      { ...original, origin: "other" },
    ]) {
      expect(comparePersonalWorkstreams(original, changed)).not.toBe(0);
    }
  });

  it("rejects empty merge input and copies of unrelated ids", () => {
    expect(() => mergePersonalWorkstreams([])).toThrow();
    expect(() => mergePersonalWorkstreams([original, { ...original, id: "other" }])).toThrow();
  });
});

describe("personal Workstream edits", () => {
  it("creates normalized live revision one with defaults and explicit metadata", () => {
    expect(original).toMatchObject({
      lifecycle: "active",
      color: null,
      rev: 1,
      createdAt: now,
      updatedAt: now,
      deletedAt: null,
    });
    const created = newPersonalWorkstream({
      id: "second",
      name: "  Planned work  ",
      orderKey: "o",
      now,
      origin: "env:example",
      lifecycle: "planned",
      color: "#ABCDEF",
    });
    expect(created).toMatchObject({ name: "Planned work", lifecycle: "planned", color: "#ABCDEF" });
    expect(() => newPersonalWorkstream({ ...created, name: "   ", now })).toThrow();
  });

  it("edits the observed merged winner without mutating it or its creation identity", () => {
    const base = mergePersonalWorkstreams([original, { ...original, rev: 7 }]);
    const edited = editPersonalWorkstream(
      base,
      { name: " Renamed ", lifecycle: "completed", color: "#ABCDEF", orderKey: "p" },
      later,
      "client:two",
    );
    expect(edited).toEqual({
      ...base,
      name: "Renamed",
      lifecycle: "completed",
      color: "#ABCDEF",
      orderKey: "p",
      rev: 8,
      updatedAt: later,
      origin: "client:two",
    });
    expect(base.name).toBe("Example");
    expect(base.rev).toBe(7);
    expect(
      editPersonalWorkstream(edited, { color: null, lifecycle: "active" }, later, "two"),
    ).toMatchObject({ color: null, lifecycle: "active", rev: 9, deletedAt: null });
  });

  it("deletes once and forbids editing or deleting a terminal record", () => {
    const deleted = deletePersonalWorkstream(original, later, "client:two");
    expect(deleted).toEqual({
      ...original,
      rev: 2,
      updatedAt: later,
      deletedAt: later,
      origin: "client:two",
    });
    expect(original.deletedAt).toBeNull();
    expect(() => editPersonalWorkstream(deleted, { name: "Resurrect" }, later, "two")).toThrow();
    expect(() => deletePersonalWorkstream(deleted, later, "two")).toThrow();
  });

  it("rejects invalid edits and revision overflow", () => {
    expect(() => editPersonalWorkstream(original, { name: " " }, later, "two")).toThrow();
    expect(() => editPersonalWorkstream(original, { orderKey: "a" }, later, "two")).toThrow();
    expect(() =>
      editPersonalWorkstream({ ...original, rev: Number.MAX_SAFE_INTEGER }, {}, later, "two"),
    ).toThrow();
  });
});

describe("personal Workstream wire validation", () => {
  it.each([
    { id: "" },
    { id: "bad/id" },
    { id: "a".repeat(129) },
    { name: "" },
    { name: " padded " },
    { name: "x".repeat(161) },
    { lifecycle: "deleted" },
    { color: "#abcdef" },
    { color: "red" },
    { orderKey: "" },
    { orderKey: "a" },
    { orderKey: "na" },
    { orderKey: "N" },
    { orderKey: "b".repeat(65) },
    { createdAt: "2026-11-12T12:00:00Z" },
    { updatedAt: "2026-11-12T07:00:00.000-05:00" },
    { deletedAt: "2026-11-12T12:00:00.000001Z" },
    { rev: 0 },
    { rev: 1.5 },
    { rev: Number.MAX_SAFE_INTEGER + 1 },
    { origin: "" },
    { origin: "client one" },
  ])("rejects malformed definition fields %j", (patch) => {
    expect(Schema.is(PersonalWorkstream)({ ...original, ...patch })).toBe(false);
  });

  it("accepts bounded legacy ids, fractional order keys and safe revisions", () => {
    expect(
      Schema.is(PersonalWorkstream)({
        ...original,
        id: "legacy:" + "x".repeat(121),
        orderKey: "a".repeat(63) + "b",
        rev: Number.MAX_SAFE_INTEGER,
      }),
    ).toBe(true);
  });

  it("round-trips implicit revision zero, retained null rows, and snapshots with tombstones", () => {
    const implicit = {
      threadId: ThreadId.make("thread-1"),
      workstreamId: null,
      revision: 0,
      updatedAt: null,
    };
    expect(Schema.decodeSync(PersonalWorkstreamAssignment)(implicit)).toEqual(implicit);
    expect(Schema.is(PersonalWorkstreamAssignment)({ ...implicit, revision: -1 })).toBe(false);
    const cleared = { ...implicit, revision: 2, updatedAt: later };
    const snapshot = {
      environmentId: EnvironmentId.make("environment-1"),
      sequence: 3,
      workstreams: [deletePersonalWorkstream(original, later, "two")],
      assignments: [cleared],
      importIssue: { importId: null, reason: "invalid_file" as const },
    };
    expect(Schema.decodeSync(PersonalWorkstreamsSnapshot)(snapshot)).toEqual(snapshot);
    expect(
      Schema.is(AssignPersonalWorkstreamInput)({
        threadId: implicit.threadId,
        workstream: original,
        expectedRevision: 0,
      }),
    ).toBe(true);
    expect(
      Schema.is(AssignPersonalWorkstreamInput)({
        threadId: implicit.threadId,
        workstream: null,
      }),
    ).toBe(false);
  });
});
