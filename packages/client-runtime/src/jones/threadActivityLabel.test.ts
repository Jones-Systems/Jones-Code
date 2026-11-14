import { describe, expect, it } from "vite-plus/test";
import { threadActivityLabel } from "./threadActivityLabel.ts";

describe("threadActivityLabel", () => {
  it("waits for server acknowledgement on a first send or after a settled run", () => {
    expect(threadActivityLabel(null)).toBe("Connecting");
    for (const status of [
      "completed",
      "failed",
      "cancelled",
      "interrupted",
      "rolled_back",
    ] as const) {
      expect(threadActivityLabel(status)).toBe("Connecting");
    }
  });

  it("separates accepted startup from a confirmed active run", () => {
    expect(threadActivityLabel("preparing")).toBe("Starting");
    expect(threadActivityLabel("queued")).toBe("Starting");
    expect(threadActivityLabel("starting")).toBe("Starting");
    expect(threadActivityLabel("running")).toBe("Thinking");
    expect(threadActivityLabel("waiting")).toBe("Thinking");
  });

  it("recognizes confirmed runless activity without letting it hide startup", () => {
    expect(threadActivityLabel(null, true)).toBe("Thinking");
    expect(threadActivityLabel("completed", true)).toBe("Thinking");
    expect(threadActivityLabel("starting", true)).toBe("Starting");
  });
});
