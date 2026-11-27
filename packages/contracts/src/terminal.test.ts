import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  chunkTerminalWrite,
  DEFAULT_TERMINAL_ID,
  retainTerminalSnapshotHistory,
  skipTerminalSnapshotContinuation,
  TERMINAL_WRITE_MAX_LENGTH,
  TerminalError,
  TerminalOpenInput,
  TerminalWriteInput,
  TerminalProviderEnvironmentError,
  terminalSequenceStateAfter,
} from "./terminal.ts";
import { ProviderInstanceId } from "./providerInstance.ts";

const encodeTerminalError = Schema.encodeUnknownSync(TerminalError);
const decodeTerminalError = Schema.decodeUnknownSync(TerminalError);

describe("terminal snapshot replay after an omitted DCS", () => {
  const pendingDcs = "\u001bPz";
  const state = terminalSequenceStateAfter(0, pendingDcs);

  it.each([
    ["a next-line control", "\u0085ready", "\u001bEready"],
    ["a tab stop before carriage return and tab", "\u0088\r\tready", "\u001bH\r\tready"],
    ["a byte after a string terminator", "\u1700ready", "\ufffdready"],
  ])("preserves %s in a continuation", (_name, data, replay) => {
    expect(skipTerminalSnapshotContinuation(state, data)).toEqual({ state: 0, data: replay });
  });

  it.each([
    [8, "\u0085ready", "\u001bEready"],
    [8, "\u2140ready", "\u001bEready"],
    [10, "\u2140ready", "\u001bE\ufffdready"],
  ])("keeps the line break within a %i-byte history budget", (budget, rest, replay) => {
    const history = pendingDcs + "z".repeat(50) + rest;
    expect(retainTerminalSnapshotHistory(history, budget)).toEqual({ history: replay });
  });
});

function decodeSync<S extends Schema.Top>(schema: S, input: unknown): Schema.Schema.Type<S> {
  return Schema.decodeUnknownSync(schema as never)(input) as Schema.Schema.Type<S>;
}

describe("TerminalProviderEnvironmentError", () => {
  it("round-trips its required cause without exposing it in the message", () => {
    const cause = { operation: "read-secret", detail: "secret backend unavailable" };
    const error = new TerminalProviderEnvironmentError({
      providerInstanceId: ProviderInstanceId.make("codex_work"),
      cause,
    });
    const encoded = encodeTerminalError(error);
    const decoded = decodeTerminalError(encoded);

    expect(decoded).toMatchObject({
      _tag: "TerminalProviderEnvironmentError",
      providerInstanceId: "codex_work",
      cause,
    });
    expect(decoded.message).toBe(
      "Could not prepare the terminal environment for provider instance: codex_work",
    );
    expect(decoded.message).not.toContain("secret backend unavailable");
  });
});

describe("TerminalOpenInput", () => {
  it("drops invalid env keys while retaining valid overrides", () => {
    const parsed = decodeSync(TerminalOpenInput, {
      threadId: "thread-1",
      terminalId: DEFAULT_TERMINAL_ID,
      cwd: "/tmp/project",
      env: { "bad-key": "1", GOOD_KEY: "2" },
    });
    expect(parsed.env).toEqual({ GOOD_KEY: "2" });
  });
});

describe("optional project-script write observation", () => {
  const ordinary = { threadId: "thread", terminalId: "terminal", data: " build\r" };
  const observation = {
    invocationId: "4a792973-8d01-4eb5-a710-e34c74f57c6b",
    projectId: "project",
    scriptId: "build",
  };
  it("keeps legacy writes identical and decodes the bounded additive descriptor", () => {
    expect(decodeSync(TerminalWriteInput, ordinary)).toEqual(ordinary);
    expect(decodeSync(TerminalWriteInput, { ...ordinary, observation })).toEqual({
      ...ordinary,
      observation,
    });
    expect(
      decodeSync(TerminalWriteInput, {
        ...ordinary,
        observation: { ...observation, worktreePath: "/caller-cannot-bind-physical-target" },
      }),
    ).toEqual({ ...ordinary, observation });
  });
  it("rejects oversized or malformed descriptors without weakening existing write bounds", () => {
    for (const value of [
      { ...observation, invocationId: "not-a-uuid" },
      { ...observation, projectId: "x".repeat(257) },
      { ...observation, scriptId: "x".repeat(257) },
    ])
      expect(() => decodeSync(TerminalWriteInput, { ...ordinary, observation: value })).toThrow();
    expect(() =>
      decodeSync(TerminalWriteInput, { ...ordinary, data: "x".repeat(65537) }),
    ).toThrow();
  });
});

describe("chunkTerminalWrite", () => {
  it("splits oversized writes so every chunk fits the contract", () => {
    const chunks = chunkTerminalWrite("y".repeat(TERMINAL_WRITE_MAX_LENGTH * 2 + 5));
    expect(chunks.map((chunk) => chunk.length)).toEqual([
      TERMINAL_WRITE_MAX_LENGTH,
      TERMINAL_WRITE_MAX_LENGTH,
      5,
    ]);
    expect(chunks.join("")).toHaveLength(TERMINAL_WRITE_MAX_LENGTH * 2 + 5);
  });

  it("does not cut a surrogate pair in half at the boundary", () => {
    const data = `${"z".repeat(TERMINAL_WRITE_MAX_LENGTH - 1)}😀tail`;
    const chunks = chunkTerminalWrite(data);
    expect(chunks[0]).toHaveLength(TERMINAL_WRITE_MAX_LENGTH - 1);
    expect(chunks[1]).toBe("😀tail");
    expect(chunks.join("")).toBe(data);
  });
});
