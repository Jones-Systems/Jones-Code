import { describe, expect, it } from "vite-plus/test";
import { mobilePrimaryMembershipBlock } from "./actions";
import { data } from "./actions.fixtures";

describe("mobile primary assignment capability gate", () => {
  const input = {
    snapshot: { data, placements: null, stale: false },
    threadKey: "never-registered",
    conflictingThreadKeys: new Set<string>(),
  };
  it("allows the capable server to register and verify a never-registered thread", () => {
    expect(mobilePrimaryMembershipBlock({ ...input, capable: true })).toBeNull();
  });
  it("keeps the verified enrollment requirement on legacy servers", () => {
    expect(mobilePrimaryMembershipBlock({ ...input, capable: false })).toContain(
      "verified enrollment",
    );
  });
  it("preserves stale and permission fences on both paths", () => {
    for (const capable of [true, false]) {
      expect(
        mobilePrimaryMembershipBlock({
          ...input,
          capable,
          snapshot: { ...input.snapshot, stale: true },
        }),
      ).toContain("read-only");
      expect(
        mobilePrimaryMembershipBlock({
          ...input,
          capable,
          snapshot: {
            ...input.snapshot,
            data: { ...data, binding: { ...data.binding, permissions: ["workstreams:read"] } },
          },
        }),
      ).toContain("read-only");
    }
  });
});
