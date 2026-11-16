import { expect, it } from "vite-plus/test";
import { makeThreadFixture } from "../../test-fixtures";
import { summarizePersonalThreads } from "./personalPresentation";

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
