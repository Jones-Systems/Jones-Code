import { describe, expect, it } from "@effect/vitest";
import { resolveServerParallelismProfile } from "./config.ts";

describe("server parallelism profiles", () => {
  it("defaults to two readers and compute workers without machine heuristics", () => {
    expect(resolveServerParallelismProfile(undefined)).toEqual({
      profile: "laptop",
      selectionSource: "default",
      readerCount: 2,
      computeWorkerCount: 2,
    });
  });

  it("requires an explicit VPS selection for three of each", () => {
    expect(resolveServerParallelismProfile("vps")).toEqual({
      profile: "vps",
      selectionSource: "environment",
      readerCount: 3,
      computeWorkerCount: 3,
    });
    expect(resolveServerParallelismProfile("laptop").selectionSource).toBe("environment");
    expect(Object.isFrozen(resolveServerParallelismProfile("vps"))).toBe(true);
  });

  it("rejects explicit empty, misspelled and numeric profiles", () => {
    for (const value of ["", " ", "VPS", "vps ", "server", "2", "3"]) {
      expect(() => resolveServerParallelismProfile(value)).toThrow(
        "JONES_SERVER_PARALLELISM_PROFILE must be laptop or vps.",
      );
    }
  });
});
