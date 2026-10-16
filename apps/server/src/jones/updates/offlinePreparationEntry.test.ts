import { describe, expect, it, vi } from "vite-plus/test";
import {
  offlinePreparationEntryOutput,
  offlinePreparationMain,
} from "./offlinePreparationEntry.ts";

describe("preparation executable entry", () => {
  it("does not print or change the exit status when imported", async () => {
    vi.resetModules();
    const previousExitCode = process.exitCode;
    const write = vi.spyOn(process.stdout, "write");
    try {
      await import("./offlinePreparationEntry.ts");
      expect(write).not.toHaveBeenCalled();
      expect(process.exitCode).toBe(previousExitCode);
    } finally {
      write.mockRestore();
    }
  });
  it("reports the closed production gate in one machine-readable line", () => {
    const output = offlinePreparationEntryOutput();
    expect(output.split("\n")).toHaveLength(2);
    expect(JSON.parse(output)).toEqual({
      status: "held",
      reason: "maintenance-owner-binding-unqualified",
      databaseOpened: false,
      productionInvocation: false,
      activation: false,
    });
  });
});

describe("closed preparation invocation", () => {
  it("keeps argument-free execution held", async () => {
    const argv = process.argv,
      exitCode = process.exitCode;
    const write = vi.spyOn(process.stdout, "write").mockReturnValue(true);
    try {
      process.argv = [argv[0]!, "jones-offline-preparation"];
      await offlinePreparationMain();
      expect(write).toHaveBeenCalledExactlyOnceWith(offlinePreparationEntryOutput());
      expect(process.exitCode).toBe(exitCode);
    } finally {
      process.argv = argv;
      process.exitCode = exitCode;
      write.mockRestore();
    }
  });
  it("rejects a supplied database path with usage status 64", async () => {
    const argv = process.argv,
      exitCode = process.exitCode;
    const write = vi.spyOn(process.stdout, "write").mockReturnValue(true);
    try {
      process.argv = [argv[0]!, "jones-offline-preparation", "--database", "/unowned/state.sqlite"];
      await offlinePreparationMain();
      expect(process.exitCode).toBe(64);
      expect(write).toHaveBeenCalledExactlyOnceWith(offlinePreparationEntryOutput());
    } finally {
      process.argv = argv;
      process.exitCode = exitCode;
      write.mockRestore();
    }
  });
});

it("emits only fixed failure diagnostics and retains exit 70", async () => {
  const worker = await import("./offlinePreparationWorker.ts");
  const run = vi
    .spyOn(worker, "runInheritedPreparationWorker")
    .mockRejectedValueOnce(new Error("Bearer private-sql"));
  const write = vi.spyOn(process.stderr, "write").mockReturnValue(true);
  const argv = process.argv,
    exitCode = process.exitCode;
  try {
    process.argv = [
      argv[0]!,
      "jones-offline-preparation",
      "--jones-preparation-worker-v1",
      "--lock-fd",
      "3",
      "--owner-directory-fd",
      "4",
    ];
    await offlinePreparationMain();
    // Source execution has no held build define, so recognized arguments reach the worker.
    expect(run).toHaveBeenCalledOnce();
    expect(process.exitCode).toBe(70);
    expect(write).toHaveBeenCalledExactlyOnceWith(
      "Closed preparation worker refused.\nJONES_PREPARATION_FAILURE phase=entry kind=unexpected\n",
    );
  } finally {
    process.argv = argv;
    process.exitCode = exitCode;
    run.mockRestore();
    write.mockRestore();
  }
});
