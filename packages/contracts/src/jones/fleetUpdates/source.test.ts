import { describe, expect, it } from "vite-plus/test";
import { readJonesBuildSource } from "./source.ts";

const jonesSource = {
  repository: "Jones-Systems/Jones-Code",
  sha: "a".repeat(40),
  tree: "b".repeat(40),
};

describe("embedded Jones build source", () => {
  it("returns the complete source stamp, not a version-derived guess", () => {
    expect(readJonesBuildSource({ version: "preview", jonesSource })).toEqual(jonesSource);
  });
  it.each([
    null,
    {},
    { version: "0.0.45-preview.20261010.123.1" },
    { jonesSource: { ...jonesSource, repository: "elsewhere/code" } },
    { jonesSource: { ...jonesSource, tree: "invalid" } },
  ])("leaves absent or invalid provenance unknown", (metadata) => {
    expect(readJonesBuildSource(metadata)).toBeUndefined();
  });
});
