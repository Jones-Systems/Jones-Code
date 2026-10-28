import { describe, expect, it } from "vite-plus/test";
import { readJonesBuildSource, readJonesBuildStartedAt } from "./source.ts";

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

describe("embedded Jones build time", () => {
  it.each(["2026-10-20T21:13:43Z", "2028-02-29T01:02:03.123Z"])(
    "reads %s without changing it",
    (jonesBuildStartedAt) => {
      expect(readJonesBuildStartedAt({ jonesBuildStartedAt, jonesSource })).toBe(
        jonesBuildStartedAt,
      );
    },
  );
  it.each([
    null,
    {},
    { jonesBuildStartedAt: 0 },
    { jonesBuildStartedAt: "2026-02-30T12:00:00Z" },
    { jonesBuildStartedAt: "2026-10-20T21:13:43+00:00" },
    { jonesBuildStartedAt: "2026-10-20T21:13:43.1234Z" },
  ])("rejects absent and invalid metadata", (metadata) =>
    expect(readJonesBuildStartedAt(metadata)).toBeUndefined(),
  );
});
