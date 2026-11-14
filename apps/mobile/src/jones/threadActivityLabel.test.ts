import { RunId } from "@t3tools/contracts";
import { expect, it } from "vite-plus/test";
import { deriveThreadFeedPresentation } from "../lib/threadActivity";

it("updates the same mobile activity slot from connection to startup to confirmed thinking", () => {
  const startedAt = "2026-11-14T13:00:00.000Z";
  const run = { runId: RunId.make("activity-run"), startedAt: null, completedAt: null };
  const present = (latestRun: Parameters<typeof deriveThreadFeedPresentation>[1]) =>
    deriveThreadFeedPresentation([], latestRun, new Set(), new Set(), startedAt)[0];

  const connecting = present(null);
  const starting = present({ ...run, status: "starting" });
  const thinking = present({ ...run, status: "running", startedAt });
  expect(connecting).toMatchObject({ id: "live-activity-row", label: "Connecting" });
  expect(starting).toMatchObject({ id: connecting?.id, label: "Starting" });
  expect(thinking).toMatchObject({ id: connecting?.id });
  expect(thinking?.type === "thinking" ? (thinking.label ?? "Thinking") : null).toBe("Thinking");
  expect(present({ ...run, status: "running", startedAt })).toBe(thinking);
  expect(present({ ...run, status: "completed", completedAt: startedAt })).toMatchObject({
    label: "Connecting",
  });
});
