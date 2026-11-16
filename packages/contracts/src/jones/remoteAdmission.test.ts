import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  LinkedRequestContext,
  LinkedRequestParent,
  LinkedRequestProject,
  LinkedRequestThread,
  RemoteAdmissionAdvisory,
  isLinkedRequestSentAt,
} from "./remoteAdmission.ts";

const base = {
  schemaVersion: "t3.linked-request-context/v1" as const,
  sentAt: "2026-10-26T12:34:56.789Z",
};
const decode = Schema.decodeUnknownSync(LinkedRequestContext);
const decodeAdvisory = Schema.decodeUnknownSync(RemoteAdmissionAdvisory);
const advisoryJson = Schema.fromJsonString(RemoteAdmissionAdvisory);
const decodeAdvisoryJson = Schema.decodeUnknownSync(advisoryJson);
const encodeAdvisoryJson = Schema.encodeSync(advisoryJson);

describe("linked request advisory contract (K0-1)", () => {
  it("accepts exact ID/title limits and rejects the next code unit on every part", () => {
    for (const [schema, field] of [
      [LinkedRequestThread, "threadId"],
      [LinkedRequestParent, "threadId"],
      [LinkedRequestProject, "projectId"],
    ] as const) {
      const read = Schema.decodeUnknownSync(schema);
      expect(read({ [field]: "i".repeat(256), title: "t".repeat(120) })).toEqual({
        [field]: "i".repeat(256),
        title: "t".repeat(120),
      });
      expect(() => read({ [field]: "i".repeat(257) })).toThrow();
      expect(() => read({ [field]: "id", title: "t".repeat(121) })).toThrow();
      expect(() => read({ [field]: " \t " })).toThrow();
      expect(() => read({ [field]: "id", title: "   " })).toThrow();
    }
    const parent = Schema.decodeUnknownSync(LinkedRequestParent);
    expect(
      parent({ threadId: "parent", environmentId: "e".repeat(256) }).environmentId,
    ).toHaveLength(256);
    expect(() => parent({ threadId: "parent", environmentId: "e".repeat(257) })).toThrow();
  });

  it.each([
    "\u0000",
    "\u001f",
    "\u007f",
    "\u0085",
    "\u200b",
    "\u202e",
    "\u2066",
    "\u2028",
    "\u2029",
    "\ud800",
    "\udfff",
  ])("rejects unsafe ID and title characters (%j)", (character) => {
    for (const part of [
      { thread: { threadId: `a${character}b` } },
      { thread: { threadId: "thread", title: `a${character}b` } },
      { parent: { threadId: "parent", environmentId: `a${character}b` } },
      { parent: { threadId: "parent", title: `a${character}b` } },
      { project: { projectId: `a${character}b` } },
      { project: { projectId: "project", title: `a${character}b` } },
    ])
      expect(() => decode({ ...base, ...part })).toThrow();
  });

  it("retains joined emoji and ZWNJ in titles, but keeps v1's stricter ID rule", () => {
    const title = "👩‍💻 می‌روم";
    expect(decode({ ...base, thread: { threadId: "机器 / thread", title } }).thread?.title).toBe(
      title,
    );
    for (const character of ["\u200c", "\u200d"]) {
      expect(() => decode({ ...base, thread: { threadId: `a${character}b` } })).toThrow();
    }
  });

  it("strips unknown keys at every level and rejects other versions", () => {
    expect(
      decode({
        ...base,
        secret: "discard",
        thread: { threadId: "thread", model: "discard" },
        parent: { threadId: "parent", workspaceRoot: "discard" },
        project: { projectId: "project", faviconPath: "discard" },
      }),
    ).toEqual({
      ...base,
      thread: { threadId: "thread" },
      parent: { threadId: "parent" },
      project: { projectId: "project" },
    });
    expect(() => decode({ ...base, schemaVersion: "t3.linked-request-context/v2" })).toThrow();
    expect(() =>
      decodeAdvisory({
        schemaVersion: "t3.remote-admission-advisory/v2",
        requestContext: base,
      }),
    ).toThrow();
  });

  it.each([
    [
      { kind: "lucide", name: "folder-code", color: "blue" },
      { kind: "lucide", name: "folder-code", color: "blue" },
    ],
    [
      { kind: "emoji", emoji: "👩‍💻" },
      { kind: "emoji", emoji: "👩‍💻" },
    ],
    [
      { kind: "monogram", text: "JC", color: "green" },
      { kind: "monogram", text: "JC", color: "green" },
    ],
    [{ kind: "future-icon", data: "discard" }, null],
    [null, null],
    [
      { kind: "lucide", name: "folder-code", color: "blue", monogramText: "JC" },
      { kind: "monogram", text: "JC", color: "blue" },
    ],
    [
      { kind: "lucide", name: "folder-code", color: "blue", monogram: "JC" },
      { kind: "monogram", text: "JC", color: "blue" },
    ],
  ])("round-trips decoded icon %j through persisted JSON and schema encoding", (icon, expected) => {
    const advisory = decodeAdvisory({
      schemaVersion: "t3.remote-admission-advisory/v1",
      requestContext: { ...base, project: { projectId: "project", title: "Project", icon } },
    });
    expect(advisory.requestContext.project?.icon).toEqual(expected);
    const stored = JSON.stringify(advisory);
    expect(decodeAdvisoryJson(stored)).toEqual(advisory);
    const encoded = encodeAdvisoryJson(advisory);
    expect(decodeAdvisoryJson(encoded)).toEqual(advisory);
  });

  it("accepts only calendar-valid ISO-Z sender times with at most millisecond precision", () => {
    for (const value of [
      "2024-02-29T23:59:59Z",
      "2026-10-26T12:34:56.1Z",
      base.sentAt,
      "0000-01-01T00:00:00Z",
    ]) {
      expect(isLinkedRequestSentAt(value)).toBe(true);
    }
    for (const value of [
      undefined,
      null,
      0,
      "2026-02-30T00:00:00Z",
      "2025-02-29T00:00:00Z",
      "2026-04-31T00:00:00Z",
      "2026-01-01T24:00:00Z",
      "2026-01-01T00:00:60Z",
      "2026-13-01T00:00:00Z",
      "2026-10-26T12:34:56.1234Z",
      "2026-10-26T12:34:56+00:00",
      "2026-10-26",
    ]) {
      expect(isLinkedRequestSentAt(value)).toBe(false);
    }
  });
});
