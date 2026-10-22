import * as NodeOS from "node:os";
import { describe, expect, it } from "vite-plus/test";
import {
  assertResourceAdmission,
  cpuSample,
  memoryAdmitted,
  swapRate,
  vmSample,
} from "./ios-admission.ts";

describe("macOS resource admission", () => {
  const base = () => ({
    sourceSha: "a".repeat(40),
    hostname: NodeOS.hostname(),
    serialSlot: "owner-slot",
    measuredAt: 100,
    cpuUsed: 30,
    availableMemoryBytes: 1024 ** 3,
    memoryPressure: "normal",
    swapBytesPerSecond: 0,
    freeDiskBytes: 30 * 1024 ** 3,
  });
  const env = {
    GITHUB_ACTIONS: "true",
    RUNNER_ENVIRONMENT: "github-hosted",
    RUNNER_OS: "macOS",
    ImageOS: "macos26-arm64",
    GITHUB_RUN_ID: "123",
    GITHUB_RUN_ATTEMPT: "1",
  };
  const hosted = () => ({
    ...base(),
    hostClass: "github-hosted-ephemeral",
    runId: "123",
    runAttempt: "1",
    serialSlot: "gh-123-1",
    freeDiskBytes: 1024,
  });
  it("keeps laptop disk protection and binds the hosted exception to the actual job", () => {
    expect(() => assertResourceAdmission(base(), base().sourceSha, 101, {})).not.toThrow();
    expect(() =>
      assertResourceAdmission(
        { ...base(), freeDiskBytes: 29 * 1024 ** 3 },
        base().sourceSha,
        101,
        {},
      ),
    ).toThrow("30 GiB");
    expect(() => assertResourceAdmission(hosted(), base().sourceSha, 101, env)).not.toThrow();
    for (const changed of [
      { RUNNER_ENVIRONMENT: "self-hosted" },
      { GITHUB_RUN_ID: "124" },
      { ImageOS: "macos15" },
    ])
      expect(() =>
        assertResourceAdmission(hosted(), base().sourceSha, 101, { ...env, ...changed }),
      ).toThrow();
    expect(() =>
      assertResourceAdmission({ ...hosted(), serialSlot: "other" }, base().sourceSha, 101, env),
    ).toThrow("slot");
    expect(() =>
      assertResourceAdmission({ ...base(), cpuUsed: 85 }, base().sourceSha, 101, {}),
    ).toThrow("CPU");
    expect(() => assertResourceAdmission(base(), base().sourceSha, 60_100, {})).toThrow("fresh");
  });
  it("holds critical pressure only above 10 MiB/s and rejects unknown samples", () => {
    expect(memoryAdmitted("warning", 100 * 1024 ** 2)).toBe(true);
    expect(memoryAdmitted("normal", 100 * 1024 ** 2)).toBe(true);
    expect(memoryAdmitted("critical", 10 * 1024 ** 2)).toBe(true);
    expect(memoryAdmitted("critical", 10 * 1024 ** 2 + 1)).toBe(false);
    expect(() => memoryAdmitted("unknown", 0)).toThrow("unavailable");
    expect(() => memoryAdmitted("normal", Number.NaN)).toThrow("unavailable");
  });
  it("uses the second CPU sample and actual elapsed swap interval", () => {
    expect(
      cpuSample(
        "CPU usage: 80.0% user, 10.0% sys, 10.0% idle\nCPU usage: 20.0% user, 5.0% sys, 75.0% idle",
      ),
    ).toBe(25);
    expect(() => cpuSample("CPU usage: 20% user, 5% sys, 75% idle")).toThrow("Two");
    const sample = vmSample(
      "Mach Virtual Memory Statistics: (page size of 16384 bytes)\nPages free: 10.\nPages inactive: 20.\nPages speculative: 3.\nSwapins: 2.\nSwapouts: 4.\n",
    );
    expect(sample.availableMemoryBytes).toBe(33 * 16384);
    expect(swapRate(sample, { ...sample, swap: 10 }, 2)).toBe(2 * 16384);
    expect(() => swapRate(sample, { ...sample, swap: 1 }, 2)).toThrow("invalid");
    expect(() => swapRate(sample, sample, 0)).toThrow("invalid");
  });
});
