import { describe, expect, it } from "vite-plus/test";

import { encodeThreadDeepLink } from "./navigation";

describe("settlement thread deep links", () => {
  it("targets the entry's environment as well as its thread", () => {
    expect(encodeThreadDeepLink({ environmentId: "remote-a", threadId: "thread-1" })).toBe(
      "/threads/remote-a/thread-1",
    );
    expect(encodeThreadDeepLink({ environmentId: "remote-b", threadId: "thread-1" })).toBe(
      "/threads/remote-b/thread-1",
    );
  });

  it("keeps path delimiters, query fragments, percent signs and Unicode inside each ID", () => {
    const input = { environmentId: "remote/a?b#c", threadId: "日本語 /%" };
    const path = encodeThreadDeepLink(input);
    const segments = path.split("/");
    expect(segments).toHaveLength(4);
    expect(segments[1]).toBe("threads");
    expect(decodeURIComponent(segments[2]!)).toBe(input.environmentId);
    expect(decodeURIComponent(segments[3]!)).toBe(input.threadId);
    expect(path).not.toContain("?");
    expect(path).not.toContain("#");
  });
});
