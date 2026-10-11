import { describe, expect, it, vi } from "vite-plus/test";
import { offlinePreparationEntryOutput } from "./offlinePreparationEntry.ts";

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
