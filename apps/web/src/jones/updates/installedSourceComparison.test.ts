import { describe, expect, it } from "vite-plus/test";
import { compareInstalledSource } from "./installedSourceComparison";

const repository = "Jones-Systems/Jones-Code";
const hostSha = "a".repeat(40);
const laptopSha = "b".repeat(40);
const host = { repository, sha: hostSha };
const laptop = { repository, sha: laptopSha };

describe("installed source comparison", () => {
  it("matches equal full commits and keeps distinct commits unordered without ancestry", () => {
    expect(compareInstalledSource({ host, laptop: host })).toBe("matches");
    expect(compareInstalledSource({ host, laptop })).toBe("different");
  });

  it("orders the host against the laptop only from ancestry for exactly these commits", () => {
    const ancestry = (relation: "ahead" | "behind" | "identical" | "diverged") => ({
      base: hostSha,
      head: laptopSha,
      relation,
    });
    expect(compareInstalledSource({ host, laptop, ancestry: ancestry("ahead") })).toBe("older");
    expect(compareInstalledSource({ host, laptop, ancestry: ancestry("behind") })).toBe("newer");
    expect(compareInstalledSource({ host, laptop, ancestry: ancestry("diverged") })).toBe(
      "diverged",
    );
    expect(compareInstalledSource({ host, laptop, ancestry: ancestry("identical") })).toBe(
      "different",
    );
    expect(
      compareInstalledSource({
        host,
        laptop,
        ancestry: { base: laptopSha, head: hostSha, relation: "ahead" },
      }),
    ).toBe("different");
  });

  it("does not compare a missing, abbreviated, or other-repository source", () => {
    expect(compareInstalledSource({ laptop })).toBe("unavailable");
    expect(compareInstalledSource({ host })).toBe("unavailable");
    expect(compareInstalledSource({ host: { repository, sha: "aaaaaaa" }, laptop })).toBe(
      "unavailable",
    );
    expect(
      compareInstalledSource({ host: { repository: "someone/fork", sha: hostSha }, laptop }),
    ).toBe("unavailable");
  });
});
