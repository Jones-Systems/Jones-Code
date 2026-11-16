// @effect-diagnostics nodeBuiltinImport:off globalDate:off -- Controlled proc fixtures, paired clocks and one explicit captured-own-thread Linux readback.
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeOS from "node:os";
import * as NodeProcess from "node:process";
import {
  classifyCpuEnvelope,
  diagnosticTrace,
  type DiagnosticEvent,
} from "./diagnosticObservation.ts";
import {
  parseLinuxThreadScheduling,
  sampleProcesses,
  sampleCpuStats,
  processResourceSample,
  resourcePredicates,
} from "./resourceSamples.ts";
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

const proc = vi.hoisted(() => ({ readFile: vi.fn(), readdir: vi.fn() }));
vi.mock("node:fs/promises", async (original) => ({
  ...(await original<typeof import("node:fs/promises")>()),
  ...proc,
}));
vi.mock("node:child_process", async (original) => {
  const execFile = vi.fn();
  Object.defineProperty(execFile, Symbol.for("nodejs.util.promisify.custom"), {
    value: async () => ({ stdout: "100\n", stderr: "" }),
  });
  return { ...(await original<typeof import("node:child_process")>()), execFile };
});
const stat = (start = "10", cpu = 2) => {
  const fields = Array.from({ length: 22 }, () => "0");
  fields[11] = String(cpu);
  fields[12] = "1";
  fields[19] = start;
  return `2 (synthetic process) ${fields.join(" ")}`;
};
describe("optional Linux resource diagnostics", () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ["Date", "performance"] });
    vi.setSystemTime(0);
    vi.spyOn(HostProcess.Platform, "defaultValue").mockReturnValue("linux");
    proc.readFile.mockReset().mockImplementation(async (path: string) => {
      if (path.endsWith("/stat")) return stat();
      if (path.endsWith("/status")) return "VmRSS: 64 kB\n";
      return "inotify wd:1\n";
    });
    proc.readdir
      .mockReset()
      .mockImplementation(async (path: string) => (path.endsWith("/task") ? ["2", "3"] : ["4"]));
  });
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("preserves full results/order even when a diagnostic sink throws and an fd closes", async () => {
    proc.readFile.mockImplementation(async (path: string) => {
      if (path.includes("/fdinfo/"))
        throw Object.assign(new Error("closed fd"), { code: "ENOENT" });
      return path.endsWith("/stat") ? stat() : "VmRSS: 64 kB\n";
    });
    const expected = await sampleProcesses([2, 3]);
    const events: DiagnosticEvent[] = [];
    const actual = await sampleProcesses([2, 3], {
      readId: 1,
      phase: "churn-9",
      sink: (event) => {
        events.push(event);
        throw new Error("sink");
      },
    });
    expect(actual).toEqual(expected);
    expect(actual.processes.map((row) => row.pid)).toEqual([2, 3]);
    expect(actual.processes.every((row) => row.inotifyWatches === undefined)).toBe(true);
    expect(events).toContainEqual(
      expect.objectContaining({ stage: "fdinfo", edge: "error", errno: "ENOENT" }),
    );
    expect(events).toContainEqual(
      expect.objectContaining({
        stage: "stat",
        edge: "end",
        startTimeTicks: "10",
        clockTicks: 100,
      }),
    );
    expect(vi.getMockedSystemTime()?.getTime()).toBe(0);
  });

  it("retains the original retirement-crossing failure and later completion of already admitted reads", async () => {
    let completeStat!: (value: string) => void;
    const delayed = new Promise<string>((resolve) => {
      completeStat = resolve;
    });
    proc.readFile.mockImplementation((path: string) =>
      path.endsWith("/stat")
        ? delayed
        : Promise.reject(Object.assign(new Error("retired"), { code: "ESRCH" })),
    );
    const trace = diagnosticTrace(100),
      pending: Promise<unknown>[] = [];
    const sample = await sampleProcesses([2], {
      readId: 8,
      phase: "shutdown",
      sink: trace.append,
      trackPending: (work) => {
        pending.push(work);
      },
    });
    expect(sample.errors).toEqual(["pid:2:unavailable"]);
    expect(sample.missingPids).toEqual([2]);
    expect(
      trace.rows.some(
        (row) => row.kind === "resource" && row.stage === "stat" && row.edge === "end",
      ),
    ).toBe(false);
    vi.advanceTimersByTime(5);
    completeStat(stat());
    await delayed;
    await Promise.allSettled(pending);
    expect(trace.rows).toContainEqual(
      expect.objectContaining({ stage: "stat", edge: "end", readId: 8, atMs: 5 }),
    );
    expect(sample.errors).toEqual(["pid:2:unavailable"]);
    expect(sample.processes).toEqual([]);
    expect(vi.getMockedSystemTime()?.getTime()).toBe(5);
  });

  it("keeps a stable unavailable PID and permission errno rather than retrying or zero filling", async () => {
    proc.readFile.mockRejectedValue(Object.assign(new Error("denied"), { code: "EACCES" }));
    const trace = diagnosticTrace(100);
    const sample = await sampleProcesses([2], { readId: 3, phase: "steady-0", sink: trace.append });
    expect(sample).toMatchObject({
      processes: [],
      missingPids: [2],
      errors: ["pid:2:unavailable"],
      status: "unavailable",
    });
    expect(
      proc.readFile.mock.calls.filter(([path]) => String(path).endsWith("/stat")),
    ).toHaveLength(1);
    expect(trace.rows).toContainEqual(
      expect.objectContaining({ stage: "stat", edge: "error", errno: "EACCES" }),
    );
  });

  it("keeps CPU-only receipts separate, unavailable resource fields and PID start-time changes", async () => {
    const first = await sampleCpuStats([2]);
    proc.readFile.mockResolvedValue(stat("11", 3));
    const next = await sampleCpuStats([2]);
    expect(first).toMatchObject({
      eligible: false,
      unavailable: ["rssBytes", "threads", "inotifyDescriptors", "inotifyWatches"],
    });
    expect(first.processes[0]).not.toHaveProperty("rssBytes");
    expect(proc.readdir).not.toHaveBeenCalled();
    const point = { ...first.processes[0]!, generation: 1, nonce: "n", atMs: 0, finishedAtMs: 1 };
    expect(
      classifyCpuEnvelope(point, { ...point, ...next.processes[0]!, atMs: 2, finishedAtMs: 3 }, []),
    ).toEqual({ relation: "identity-mismatch", requestId: null });
  });

  it("bounds fdinfo concurrency while retaining every completed and failed descriptor", async () => {
    const gates = Array.from({ length: 3 }, () => Promise.withResolvers<void>());
    const entered = Array.from({ length: 3 }, () => Promise.withResolvers<void>());
    const widths = [8, 8, 1];
    const started = [0, 0, 0];
    let active = 0;
    let peak = 0;
    proc.readdir.mockImplementation(async (path: string) =>
      path.endsWith("/task") ? ["2", "3"] : Array.from({ length: 17 }, (_, fd) => String(fd)),
    );
    proc.readFile.mockImplementation(async (path: string) => {
      if (!path.includes("/fdinfo/")) return path.endsWith("/stat") ? stat() : "VmRSS: 64 kB\n";
      const fd = Number(path.slice(path.lastIndexOf("/") + 1));
      const batch = Math.floor(fd / 8);
      active++;
      peak = Math.max(peak, active);
      if (++started[batch]! === widths[batch]) entered[batch]!.resolve();
      try {
        await gates[batch]!.promise;
        if (fd === 5) throw Object.assign(new Error("denied"), { code: "EACCES" });
        return "inotify wd:1\n";
      } finally {
        active--;
      }
    });
    const trace = diagnosticTrace(100);
    const sample = sampleProcesses([2], { readId: 50, phase: "steady-0", sink: trace.append });
    try {
      for (let batch = 0; batch < 3; batch++) {
        await entered[batch]!.promise;
        expect(active).toBe(widths[batch]);
        gates[batch]!.resolve();
      }
      const result = await sample;
      expect(peak).toBe(8);
      expect(result.processes[0]?.fdinfo).toEqual({ attempted: 17, completed: 16, errors: 1 });
      expect(result.processes[0]?.inotifyWatches).toBeUndefined();
      expect(trace.rows).toContainEqual(
        expect.objectContaining({ stage: "fdinfo", edge: "error", errno: "EACCES" }),
      );
      expect(
        proc.readFile.mock.calls.filter(([path]) => String(path).includes("/fdinfo/")),
      ).toHaveLength(17);
    } finally {
      for (const gate of gates) gate.resolve();
      await sample;
    }
  });

  it("tracks actual late OS reads after aggregate failure without erasing the failure", async () => {
    const late = Promise.withResolvers<string>();
    const pending: Promise<unknown>[] = [];
    proc.readFile.mockImplementation((path: string) =>
      path.endsWith("/stat") ? late.promise : Promise.reject(new Error("captured PID disappeared")),
    );
    const result = await sampleProcesses([2], undefined, (read) => {
      pending.push(read);
    });
    expect(result.status).toBe("unavailable");
    expect(result.missingPids).toEqual([2]);
    expect(pending).toHaveLength(4);
    let drained = false;
    const settled = Promise.allSettled(pending).then(() => {
      drained = true;
    });
    await Promise.resolve();
    expect(drained).toBe(false);
    late.resolve(stat());
    await settled;
    expect(drained).toBe(true);
    expect(result.processes).toEqual([]);
    expect(result.errors).toEqual(["pid:2:unavailable"]);
  });

  it("refuses a PID identity change across the complete resource read", async () => {
    let reads = 0;
    proc.readFile.mockImplementation(async (path: string) =>
      path.endsWith("/stat")
        ? stat(++reads === 1 ? "10" : "11")
        : path.endsWith("/status")
          ? "VmRSS: 64 kB\n"
          : "inotify wd:1\n",
    );
    const sample = await sampleProcesses([2]);
    expect(reads).toBe(2);
    expect(sample.status).toBe("unavailable");
    expect(sample.missingPids).toEqual([2]);
    expect(sample.errors).toEqual(["pid:2:unavailable"]);
  });
});

const threadStat = (tid = "100", nice = 10, start = "123") => {
  const fields = Array<string>(22).fill("0");
  fields[0] = "S";
  fields[16] = String(nice);
  fields[19] = start;
  return `${tid} (captured worker) ${fields.join(" ")}\n`;
};
const threadStatus = "Cpus_allowed_list:\t0-1\n";
it("retains exact captured thread birth, actual nice and complete affinity", () => {
  expect(
    parseLinuxThreadScheduling("100", threadStat(), threadStatus, threadStat(), threadStatus),
  ).toEqual({ startTimeTicks: "123", nice: 10, cpusAllowed: [0, 1] });
});
it.each([
  ["wrong thread", threadStat("101"), threadStatus, threadStat(), threadStatus],
  ["reused TID", threadStat(), threadStatus, threadStat("100", 10, "124"), threadStatus],
  ["invalid nice", threadStat("100", 20), threadStatus, threadStat("100", 20), threadStatus],
  ["changed nice", threadStat(), threadStatus, threadStat("100", 11), threadStatus],
  ["missing affinity", threadStat(), "", threadStat(), threadStatus],
  [
    "duplicate affinity field",
    threadStat(),
    threadStatus + threadStatus,
    threadStat(),
    threadStatus,
  ],
  [
    "overlapping affinity",
    threadStat(),
    "Cpus_allowed_list: 0-1,1-2\n",
    threadStat(),
    threadStatus,
  ],
  ["descending range", threadStat(), "Cpus_allowed_list: 2-0\n", threadStat(), threadStatus],
  ["unbounded range", threadStat(), "Cpus_allowed_list: 0-4096\n", threadStat(), threadStatus],
  ["changed affinity", threadStat(), threadStatus, threadStat(), "Cpus_allowed_list: 0,2\n"],
] as const)(
  "keeps %s readback unavailable instead of inventing scheduling proof",
  (_label, before, status, after, statusAfter) => {
    expect(() => parseLinuxThreadScheduling("100", before, status, after, statusAfter)).toThrow();
  },
);
it("checks captured own Linux main-thread nice against the independent OS API", async () => {
  if (HostProcess.Platform.defaultValue() !== "linux") {
    expect(() => parseLinuxThreadScheduling("100", "", "", "", "")).toThrow();
    return;
  }
  const fs = await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises");
  const tid = String(NodeProcess.pid),
    base = `/proc/${tid}/task/${tid}`;
  const before = await fs.readFile(`${base}/stat`, "utf8"),
    status = await fs.readFile(`${base}/status`, "utf8");
  const after = await fs.readFile(`${base}/stat`, "utf8"),
    statusAfter = await fs.readFile(`${base}/status`, "utf8");
  const actual = parseLinuxThreadScheduling(tid, before, status, after, statusAfter);
  expect(actual.nice).toBe(NodeOS.getPriority(NodeProcess.pid));
  expect(actual.cpusAllowed.length).toBeGreaterThan(0);
});
