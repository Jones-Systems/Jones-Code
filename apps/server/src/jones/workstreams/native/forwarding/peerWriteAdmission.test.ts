import { expect, it } from "@effect/vitest";
import * as Pins from "../runtime/peerSessionPins.ts";
import type { WorkstreamsTransportCeiling } from "@t3tools/contracts/jones/workstreamsPeerAuthority";
// Genuine NativePeerSessions/Guard/substitution/SQL tests live in homeTools.test.ts.
// These cases isolate the existing unchanged transport ceiling from private write proofs.
it.each(["read-only", "plan", "missing-operate", "missing-write"] as const)(
  "current transport ceiling refuses %s",
  (fault) => {
    const ceiling: WorkstreamsTransportCeiling = {
      access: "full-access",
      runtime_mode: "full-access",
      interaction_mode: "default",
      scopes: ["orchestration:read", "orchestration:operate"],
      permissions: ["workstreams:read", "workstreams:write"],
    };
    if (fault === "read-only")
      Object.assign(ceiling, {
        access: "read-only",
        runtime_mode: "approval-required",
        permissions: ["workstreams:read"],
      });
    if (fault === "plan") Object.assign(ceiling, { interaction_mode: "plan" });
    if (fault === "missing-operate") Object.assign(ceiling, { scopes: ["orchestration:read"] });
    if (fault === "missing-write") Object.assign(ceiling, { permissions: ["workstreams:read"] });
    expect(Pins.ceilingAllows(ceiling, "workstreams:read")).toBe(true);
    expect(Pins.ceilingAllows(ceiling, "workstreams:write")).toBe(false);
  },
);
