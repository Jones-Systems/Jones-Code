import { expect, it } from "vite-plus/test";
import {
  groupPersonalThreads,
  personalThreadKey,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { ThreadId } from "@t3tools/contracts";
import { makeThreadFixture } from "../../test-fixtures";
import { projectWorkstreamSnoozes } from "./personalSnooze";

it("parks a group while its individually woken members remain unassigned until the group wakes", () => {
  const before = "2026-11-17T12:00:00.000Z";
  const start = "2026-11-17T13:00:00.000Z";
  const now = "2026-11-17T14:00:00.000Z";
  const until = "2026-11-17T15:00:00.500Z";
  const hidden = makeThreadFixture({ id: ThreadId.make("hidden") });
  const woken = makeThreadFixture({ id: ThreadId.make("woken"), lastSnoozeWakeAt: now });
  const snoozed = makeThreadFixture({
    id: ThreadId.make("snoozed"),
    snoozedAt: before,
    snoozedUntil: "2026-11-17T16:00:00.000Z",
  });
  const workstream = {
    id: "group",
    name: "Group",
    lifecycle: "active" as const,
    color: null,
    orderKey: "n",
    createdAt: before,
    updatedAt: start,
    snoozedAt: start,
    snoozedUntil: until,
  };
  const state: PersonalWorkstreamsState = {
    workstreams: [workstream, { ...workstream, id: "other", snoozedAt: null, snoozedUntil: null }],
    environments: [],
    assignmentsByThreadKey: new Map(
      [hidden, woken, snoozed].map((thread) => [
        personalThreadKey(thread),
        { threadId: thread.id, workstreamId: workstream.id, revision: 1, updatedAt: before },
      ]),
    ),
    pendingThreadKeys: new Set(),
    canCreate: true,
  };
  const during = projectWorkstreamSnoozes(state, {
    utcMinute: "2026-11-17T14:00",
    wakeClockMs: Date.parse(now),
  });
  expect(during.snoozed).toEqual([workstream]);
  expect(during.hidesThread(hidden)).toBe(true);
  expect(during.hidesThread(woken)).toBe(false);
  expect(during.hidesThread(snoozed)).toBe(false);
  expect(groupPersonalThreads(during.visibleState, [woken]).ungrouped).toEqual([woken]);
  // Moving the woken thread updates its real assignment while the group sleeps.
  const movedState = {
    ...state,
    assignmentsByThreadKey: new Map(state.assignmentsByThreadKey).set(personalThreadKey(woken), {
      threadId: woken.id,
      workstreamId: "other",
      revision: 2,
      updatedAt: now,
    }),
  };
  const atWake = { utcMinute: "2026-11-17T15:00", wakeClockMs: Date.parse(until) };
  expect(
    projectWorkstreamSnoozes(state, { ...atWake, wakeClockMs: atWake.wakeClockMs - 1 }).snoozed,
  ).toEqual([workstream]);
  const awake = projectWorkstreamSnoozes(movedState, atWake);
  expect(awake.snoozed).toEqual([]);
  expect(awake.hidesThread(hidden)).toBe(false);
  expect(
    groupPersonalThreads(awake.visibleState, [hidden, woken]).groups.map((g) => g.threads),
  ).toEqual([[hidden], [woken]]);
  expect(
    groupPersonalThreads(projectWorkstreamSnoozes(state, atWake).visibleState, [woken]).groups[0]
      ?.threads,
  ).toEqual([woken]);
});
