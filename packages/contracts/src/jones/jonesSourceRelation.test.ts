import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { JonesSourceRelation, JonesSourceRelationQuery } from "./jonesSourceRelation.ts";

const BASE = "a".repeat(40),
  HEAD = "b".repeat(40);

describe("Jones source relation contract", () => {
  it("accepts only full lowercase commits for both sides of the pair", () => {
    const decode = Schema.decodeUnknownSync(JonesSourceRelationQuery);
    expect(decode({ base: BASE, head: HEAD })).toEqual({ base: BASE, head: HEAD });
    for (const query of [
      { base: BASE.slice(0, 7), head: HEAD },
      { base: BASE, head: HEAD.toUpperCase() },
      { base: BASE, head: `${HEAD.slice(1)}g` },
      { base: BASE },
    ])
      expect(() => decode(query)).toThrow();
  });

  it("keeps the fixed repository and closed relation vocabulary", () => {
    const decode = Schema.decodeUnknownSync(JonesSourceRelation);
    const response = {
      repository: "Jones-Systems/Jones-Code",
      base: BASE,
      head: HEAD,
      relation: "behind",
      observedAt: "2026-10-20T00:00:00.000Z",
    };
    expect(decode(response)).toEqual(response);
    expect(decode({ ...response, relation: "unknown" }).relation).toBe("unknown");
    expect(() => decode({ ...response, repository: "someone/else" })).toThrow();
    expect(() => decode({ ...response, relation: "older" })).toThrow();
  });
});
