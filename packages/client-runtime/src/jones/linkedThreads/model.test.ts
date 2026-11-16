import { describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type {
  LinkedThreadLink,
  LinkedThreadsListResult,
  ThreadEndpointRef,
} from "@t3tools/contracts/jones/linkedThreads";
import {
  classifyMirrorFailure,
  endpointKey,
  mirrorLinkedMutation,
  supportsLinkedThreads,
  unionLinkedThreads,
} from "./model.ts";

const endpoint = (environmentId: string, threadId: string): ThreadEndpointRef => ({
  environmentId: EnvironmentId.make(environmentId),
  threadId: ThreadId.make(threadId),
});
const origin = endpoint("origin", "same-id");
const destination = endpoint("destination", "same-id");
const link = (overrides: Partial<LinkedThreadLink> = {}): LinkedThreadLink => ({
  linkId: `tl1:${"a".repeat(64)}`,
  kind: "continuation",
  from: origin,
  to: destination,
  basis: "handoff",
  assertedBy: "user",
  reverse: "unknown",
  acceptance: "offered",
  handoff: null,
  remote: null,
  createdAt: "2026-10-24T10:00:00Z",
  updatedAt: "2026-10-24T10:00:00Z",
  acceptedAt: null,
  ...overrides,
});
const result = (
  environmentId: string,
  links: readonly LinkedThreadLink[],
  overrides: Partial<LinkedThreadsListResult> = {},
): LinkedThreadsListResult => ({
  environmentId: EnvironmentId.make(environmentId),
  links,
  siblings: [],
  truncated: false,
  ...overrides,
});

describe("linked thread union", () => {
  it("deduplicates copies, keeps accepted and recorded facts, and prefers the newest handoff", () => {
    const handoff = {
      schemaVersion: "t3.thread-handoff/v1",
      origin: { ...origin, title: "Origin", environmentLabel: "Laptop" },
      destination: null,
      returnRoute: { kind: "linked_environment", ...origin },
      goal: "Latest goal",
      scope: "Scope",
      status: "Ready",
      sourceRefs: [],
      offeredAt: "2026-10-24T11:00:00Z",
    } as const;
    const view = unionLinkedThreads(origin, [
      result("destination", [
        link({
          acceptance: "accepted",
          acceptedAt: "2026-10-24T12:30:00Z",
          updatedAt: "2026-10-24T12:30:00Z",
          reverse: "recorded",
          handoff: { ...handoff, offeredAt: "2026-10-24T09:00:00Z", goal: "Old goal" },
        }),
      ]),
      result(
        "origin",
        [
          link({
            handoff,
            updatedAt: "2026-10-24T11:00:00Z",
            remote: { title: "Receiver", environmentLabel: "VPS" },
          }),
        ],
        { truncated: true },
      ),
    ]);
    expect(view.links).toHaveLength(1);
    expect(view.links[0]).toMatchObject({ acceptance: "accepted", reverse: "recorded", handoff });
    expect(view.relations).toMatchObject([
      { endpoint: destination, label: "Continued in", group: "continuations" },
    ]);
    expect(view.snapshots.get(endpointKey(destination))?.title).toBe("Receiver");
    expect(view.hasSuccessor).toBe(true);
    expect(view.truncated).toBe(true);
    expect(endpointKey(origin)).not.toBe(endpointKey(destination));
  });
  it("does not use a remote snapshot for the wrong endpoint and keeps the newest snapshot across three copies", () => {
    const snapshot = (title: string) => ({ title, environmentLabel: "VPS" });
    const view = unionLinkedThreads(origin, [
      result("origin", [link({ remote: snapshot("Old") })]),
      result("origin", [link({ remote: snapshot("Newest"), updatedAt: "2026-10-24T12:00:00Z" })]),
      result("origin", [link({ remote: snapshot("Middle"), updatedAt: "2026-10-24T11:00:00Z" })]),
      result("destination", [
        link({ remote: { title: "Origin snapshot", environmentLabel: "Laptop" } }),
      ]),
    ]);
    expect(view.snapshots.get(endpointKey(destination))?.title).toBe("Newest");
    expect(view.snapshots.get(endpointKey(origin))?.title).toBe("Origin snapshot");
  });
  it("classifies incoming launch parents and shared-launcher siblings separately", () => {
    const sibling = endpoint("origin", "sibling");
    const parent = endpoint("origin", "launcher");
    const view = unionLinkedThreads(origin, [
      result("origin", [link({ kind: "launch", from: parent, to: origin })], {
        siblings: [
          {
            via: parent,
            link: link({
              linkId: `tl1:${"b".repeat(64)}`,
              kind: "launch",
              from: parent,
              to: sibling,
            }),
          },
        ],
      }),
    ]);
    expect(view.relations.map((row) => [row.group, row.label, row.endpoint])).toEqual([
      ["parents", "Launched by", parent],
      ["siblings", "Sibling", sibling],
    ]);
  });
  it("gates unknown, old and mismatched capabilities", () => {
    expect(supportsLinkedThreads(undefined)).toBe(false);
    expect(supportsLinkedThreads({})).toBe(false);
    expect(supportsLinkedThreads({ linkedThreads: { schemaVersion: "v2" } })).toBe(false);
    expect(
      supportsLinkedThreads({ linkedThreads: { schemaVersion: "t3.linked-threads/v1" } }),
    ).toBe(true);
  });
});

describe("linked thread mutations", () => {
  it("writes locally, mirrors, and idempotently updates reverse status", async () => {
    const execute = vi.fn(async () => "ok");
    const outcome = await mirrorLinkedMutation({
      local: origin.environmentId,
      remote: destination.environmentId,
      remoteCapable: true,
      execute,
    });
    expect(execute.mock.calls).toEqual([
      [origin.environmentId],
      [destination.environmentId, "recorded"],
      [origin.environmentId, "recorded"],
    ]);
    expect(outcome.steps.map((step) => step.state)).toEqual(["recorded", "recorded"]);
  });
  it("does not call an old or disconnected peer, but saves a local unavailable mirror", async () => {
    const execute = vi.fn(async () => "ok");
    const outcome = await mirrorLinkedMutation({
      local: origin.environmentId,
      remote: destination.environmentId,
      remoteCapable: false,
      execute,
    });
    expect(execute.mock.calls).toEqual([
      [origin.environmentId],
      [origin.environmentId, "unavailable"],
    ]);
    expect(outcome.steps[1]?.state).toBe("unavailable");
  });
  it("retains a recorded local result after ambiguous remote failure or reverse-status failure", async () => {
    const execute = vi.fn(async (id: EnvironmentId, reverse?: string) => {
      if (id === destination.environmentId) throw new Error("socket closed");
      if (reverse) throw new Error("reply lost");
      return "saved";
    });
    const outcome = await mirrorLinkedMutation({
      local: origin.environmentId,
      remote: destination.environmentId,
      remoteCapable: true,
      execute,
    });
    expect(outcome.result).toBe("saved");
    expect(outcome.steps).toEqual([
      {
        environmentId: origin.environmentId,
        state: "recorded",
        detail: "Reverse status could not be updated",
      },
      { environmentId: destination.environmentId, state: "unknown" },
    ]);
    expect(
      classifyMirrorFailure(new Error("Only the receiving thread can accept this handoff")),
    ).toBe("unavailable");
    expect(classifyMirrorFailure(new Error("unknown RPC method"))).toBe("unavailable");
  });
  it("never mirrors a failed first write and writes same-environment links once", async () => {
    const refused = vi.fn(async () => {
      throw new Error("forbidden");
    });
    await expect(
      mirrorLinkedMutation({
        local: origin.environmentId,
        remote: destination.environmentId,
        remoteCapable: true,
        execute: refused,
      }),
    ).rejects.toThrow("forbidden");
    expect(refused).toHaveBeenCalledTimes(1);
    const execute = vi.fn(async () => "saved");
    await mirrorLinkedMutation({
      local: origin.environmentId,
      remote: origin.environmentId,
      remoteCapable: true,
      execute,
    });
    expect(execute).toHaveBeenCalledTimes(1);
  });
});
