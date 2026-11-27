import { RunId } from "@t3tools/contracts";
import { expect, it } from "vite-plus/test";
import {
  createThreadFeedCache,
  deriveThreadFeedPresentation,
  LIVE_ACTIVITY_ROW_ID,
  type ThreadFeedEntry,
} from "../lib/threadActivity";

it("updates the same mobile activity slot from connection to startup to confirmed thinking", () => {
  const startedAt = "2026-11-14T13:00:00.000Z";
  const run = {
    runId: RunId.make("activity-run"),
    startedAt: null,
    completedAt: null,
    providerActivityStarted: false,
  };
  const feed: ReadonlyArray<ThreadFeedEntry> = [];
  const cache = createThreadFeedCache();
  const present = (latestRun: Parameters<typeof deriveThreadFeedPresentation>[1]) =>
    deriveThreadFeedPresentation(
      feed,
      latestRun,
      new Set(),
      new Set(),
      startedAt,
      false,
      cache,
    ).find((row) => row.id === LIVE_ACTIVITY_ROW_ID);

  const connecting = present(null);
  const starting = present({ ...run, status: "starting" });
  const awaitingProvider = present({ ...run, status: "running", startedAt });
  const thinking = present({ ...run, status: "running", startedAt, providerActivityStarted: true });
  expect(connecting).toMatchObject({ id: "live-activity-row", label: "Connecting" });
  expect(starting).toMatchObject({ id: connecting?.id, label: "Starting" });
  expect(awaitingProvider).toMatchObject({ id: connecting?.id, label: "Starting" });
  expect(thinking).toMatchObject({ id: connecting?.id });
  expect(thinking?.type === "thinking" ? (thinking.label ?? "Thinking") : null).toBe("Thinking");
  expect(present({ ...run, status: "running", startedAt, providerActivityStarted: true })).toBe(
    thinking,
  );
  expect(present({ ...run, status: "completed", completedAt: startedAt })).toMatchObject({
    label: "Connecting",
  });
});
