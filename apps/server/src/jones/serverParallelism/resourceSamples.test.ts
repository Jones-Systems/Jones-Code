import { expect, it } from "vite-plus/test";
import { processResourceSample, resourcePredicates } from "./resourceSamples.ts";
const row = (pid: number, rssBytes = 64 * 1024 * 1024) => ({ pid, rssBytes, cpuMs: 1 });
it("retains missing PID evidence after a prior complete overlap sample", () => {
  const complete = processResourceSample([1, 2, 3], [row(1), row(2), row(3)]);
  const failed = processResourceSample([1, 2, 3], [row(1), row(2)]);
  expect(failed.missingPids).toEqual([3]);
  expect(failed.status).toBe("partial");
  expect(resourcePredicates([complete, failed], 1, 2)).toEqual({ resourcesComplete: false, childRssWithinBudget: "unknown" });
});
it("does not turn empty, non-finite or unreadable RSS samples into a pass", () => {
  expect(processResourceSample([], []).status).toBe("unavailable");
  expect(processResourceSample([2], [row(2, NaN)]).missingPids).toEqual([2]);
  expect(resourcePredicates([], 1, 2).childRssWithinBudget).toBe("unknown");
  expect(resourcePredicates([processResourceSample([1, 2], [])], 1, 2).resourcesComplete).toBe(false);
});
it("rejects a complete over-budget sample even when other samples are missing", () => {
  const over = processResourceSample([1, 2, 3], [row(1), row(2, 513 * 1024 * 1024), row(3)]);
  expect(resourcePredicates([over], 1, 2)).toEqual({ resourcesComplete: true, childRssWithinBudget: false });
  expect(resourcePredicates([over, processResourceSample([1, 2, 3], [])], 1, 2).childRssWithinBudget).toBe(false);
});
