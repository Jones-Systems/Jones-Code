import { describe, expect, it } from "vite-plus/test";

import {
  WORKSTREAMS_T3_AVAILABILITY_HEADER,
  formatWorkstreamsT3Availability,
  parseWorkstreamsT3Availability,
} from "./workstreamsRegistrationAvailability.ts";

describe("Workstreams registration availability header", () => {
  it("uses the additive diagnostic header and exact ready grammar", () => {
    expect(WORKSTREAMS_T3_AVAILABILITY_HEADER).toBe("x-jones-workstreams-t3-availability");
    expect(parseWorkstreamsT3Availability("ready")).toEqual({ state: "ready" });
    expect(formatWorkstreamsT3Availability({ state: "ready" })).toBe("ready");
  });

  it.each([
    "descriptor_absent",
    "build_unavailable",
    "contract_mismatch",
    "source_mismatch",
    "authority_mismatch",
    "generation_mismatch",
    "authority_fenced",
    "authority_unavailable",
  ] as const)("preserves the closed %s reason", (reason) => {
    const value = { state: "unavailable" as const, reason };
    expect(parseWorkstreamsT3Availability(`unavailable;reason=${reason}`)).toEqual(value);
    expect(formatWorkstreamsT3Availability(value)).toBe(`unavailable;reason=${reason}`);
  });

  it.each([
    null,
    undefined,
    "",
    " ready",
    "ready ",
    "READY",
    "ready;reason=descriptor_absent",
    "ready, ready",
    "unavailable",
    "unavailable;reason=unknown",
    "unavailable;reason=",
    "unavailable; reason=descriptor_absent",
    "unavailable;reason=descriptor_absent;source=private",
    "unavailable;reason=descriptor_absent, ready",
    "unavailable;reason=descriptor_absent\n",
  ])("treats absent or malformed %s as unknown", (value) => {
    expect(parseWorkstreamsT3Availability(value)).toBeNull();
  });
});
