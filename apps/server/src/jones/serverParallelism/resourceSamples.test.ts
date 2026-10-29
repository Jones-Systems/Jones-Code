import { expect, it } from "vite-plus/test";
import { processResourceSample, resourcePredicates } from "./resourceSamples.ts";
const row = (pid: number, rssBytes = 64 * 1024 * 1024) => ({ pid, rssBytes, cpuMs: 1 });
it("retains missing PID evidence after a prior complete overlap sample", () => {
  const complete = processResourceSample([1, 2, 3], [row(1), row(2), row(3)]);
  const failed = processResourceSample([1, 2, 3], [row(1), row(2)]);
  expect(failed.missingPids).toEqual([3]);
  expect(failed.status).toBe("partial");
  expect(resourcePredicates([complete, failed], 1, 2)).toEqual({
    resourcesComplete: false,
    childRssWithinBudget: "unknown",
  });
});
it("does not turn empty, non-finite or unreadable RSS samples into a pass", () => {
  expect(processResourceSample([], []).status).toBe("unavailable");
  expect(processResourceSample([2], [row(2, NaN)]).missingPids).toEqual([2]);
  expect(resourcePredicates([], 1, 2).childRssWithinBudget).toBe("unknown");
  expect(resourcePredicates([processResourceSample([1, 2], [])], 1, 2).resourcesComplete).toBe(
    false,
  );
});
it("rejects a complete over-budget sample even when other samples are missing", () => {
  const over = processResourceSample([1, 2, 3], [row(1), row(2, 513 * 1024 * 1024), row(3)]);
  expect(resourcePredicates([over], 1, 2)).toEqual({
    resourcesComplete: true,
    childRssWithinBudget: false,
  });
  expect(
    resourcePredicates([over, processResourceSample([1, 2, 3], [])], 1, 2).childRssWithinBudget,
  ).toBe(false);
});

it("parses only PID-attributed macOS sample thread headers, never stack names", async () => {
  const { parseMacNativeThreads } = await import("./resourceSamples.ts");
  const raw = `Sampling process 123 for 1 second
Process: node [123]
Call graph:
    100 Thread_0x1: fff-bg-0
    + 100 work (in native)
    100 Thread_0x2: fff-search-0
    100 Thread_0x3 DispatchQueue_1: com.apple.main-thread (serial)
    + 100 fff-bg-99 (in native)
`;
  const result = parseMacNativeThreads(123, raw);
  expect(result.status).toBe("complete");
  expect(result.threads).toEqual([
    { tid: "0x1", name: "fff-bg-0" },
    { tid: "0x2", name: "fff-search-0" },
    { tid: "0x3", name: "DispatchQueue_1: com.apple.main-thread (serial)" },
  ]);
  expect(result.raw).toBe(raw);
  expect(parseMacNativeThreads(124, raw).status).toBe("unavailable");
  expect(parseMacNativeThreads(123, raw.replace("Thread_0x2", "Thread_0x1")).status).toBe(
    "unavailable",
  );
  expect(parseMacNativeThreads(123, "Process: node [123]\n + 1 fff-bg-0").status).toBe(
    "unavailable",
  );
  expect(parseMacNativeThreads(123, raw.replace("Process:", "Unattributed:")).status).toBe(
    "unavailable",
  );
});
