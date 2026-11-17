import {
  personalThreadKey,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { expect, it } from "vite-plus/test";
import { makeThreadFixture } from "../../test-fixtures";
import {
  personalSidebarRoster,
  personalSidebarSummaryThreads,
  summarizePersonalThreads,
} from "./personalPresentation";

it("excludes snoozed input from work stream attention until wake without losing membership", () => {
  const thread = makeThreadFixture({
    hasPendingUserInput: true,
    pendingUserInputAt: "2026-11-16T12:00:00.000Z",
    snoozedAt: "2026-11-16T13:00:00.000Z",
    snoozedUntil: "2026-11-16T15:00:00.000Z",
  });
  expect(summarizePersonalThreads([thread, thread], Date.parse("2026-11-16T14:00:00Z"))).toEqual({
    total: 1,
    running: 0,
    attention: 0,
    failed: 0,
    done: 0,
  });
  expect(summarizePersonalThreads([thread], Date.parse("2026-11-16T15:00:00Z")).attention).toBe(1);
});

it("keeps all three assigned threads together even when pinned or on the working shelf", () => {
  const at = "2026-11-17T12:00:00Z";
  const active = makeThreadFixture({ title: "Active" });
  const pinned = { ...active, id: "pinned" as typeof active.id, pinnedAt: at };
  const working = { ...active, id: "working" as typeof active.id };
  const loosePin = { ...pinned, id: "unassigned-pin" as typeof active.id };
  const looseWorking = { ...working, id: "unassigned-working" as typeof active.id };
  const state: PersonalWorkstreamsState = {
    workstreams: [
      {
        id: "sync",
        name: "JC Sync",
        lifecycle: "active",
        color: null,
        orderKey: "n",
        createdAt: at,
        updatedAt: at,
      },
    ],
    environments: [],
    pendingThreadKeys: new Set(),
    canCreate: true,
    assignmentsByThreadKey: new Map(
      [active, pinned, working].map((thread) => [
        personalThreadKey(thread),
        { threadId: thread.id, workstreamId: "sync", revision: 1, updatedAt: at },
      ]),
    ),
  };
  const result = personalSidebarRoster(state, {
    activeThreads: [active],
    pinnedThreads: [pinned, loosePin],
    workingThreads: [working, looseWorking],
  });
  expect(result.activeThreads).toEqual([active, pinned, working]);
  expect(result.pinnedThreads).toEqual([loosePin]);
  expect(result.workingThreads).toEqual([looseWorking]);
  expect(pinned.pinnedAt).toBe(at);
  const settled = {
    ...active,
    id: "settled" as typeof active.id,
    settledOverride: "settled" as const,
  };
  const snoozed = {
    ...active,
    id: "snoozed" as typeof active.id,
    snoozedAt: at,
    snoozedUntil: "2026-11-18T12:00:00Z",
  };
  expect(personalSidebarSummaryThreads([active, pinned, working, settled, snoozed], at)).toEqual([
    active,
    pinned,
    working,
  ]);
  expect(personalSidebarSummaryThreads([snoozed], "2026-11-18T12:00:00Z")).toEqual([snoozed]);
});
