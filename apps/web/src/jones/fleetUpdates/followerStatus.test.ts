import { describe, expect, it } from "vite-plus/test";
import { describeFollower } from "./followerStatus";

const leaderSource = "b".repeat(40);
const time = (iso: string) => iso.slice(11, 16);

describe("following host status line", () => {
  it("names the laptop's build and why an install waits", () => {
    expect(
      describeFollower({ protocol: 1, leaderSource: null, state: "waiting-for-leader" }, time),
    ).toBe("Waiting for this laptop's installed build");
    expect(
      describeFollower(
        {
          protocol: 1,
          leaderSource,
          state: "waiting-for-quiet",
          reason: "Waiting for terminal commands (1) to finish.",
          notBefore: "2026-10-20T10:30:00.000Z",
        },
        time,
      ),
    ).toBe(
      "Ready to install bbbbbbb when the host is quiet · Waiting for terminal commands (1) to finish. · not before 10:30",
    );
    expect(describeFollower({ protocol: 1, leaderSource, state: "current" }, time)).toBe(
      "On this laptop's build bbbbbbb",
    );
    expect(
      describeFollower(
        { protocol: 1, leaderSource, state: "prepare-blocked", reason: "Not qualified." },
        time,
      ),
    ).toBe("Couldn't prepare bbbbbbb · Not qualified.");
  });
});
