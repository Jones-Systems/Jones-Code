import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type { FleetPublicationReceipt } from "@t3tools/client-runtime/jones/fleet-updates";
import {
  describePublication,
  describePublicationReceipt,
  formatFleetTime,
  selectPublicationReceipt,
} from "./publicationStatus";

const source = {
  repository: "Jones-Systems/Jones-Code",
  sha: "a".repeat(40),
  tree: "b".repeat(40),
};
const receipt: FleetPublicationReceipt = {
  enrollment: {
    environmentId: EnvironmentId.make("vps"),
    enrollmentId: "11111111-1111-4111-8111-111111111111",
    enabled: true,
    continueRunningThreads: false,
  },
  outcome: "observed",
  observedAt: "2026-11-12T11:00:00.000Z",
  follower: {
    protocol: 1,
    leaderSource: "c".repeat(40),
    leaderPublishedAt: "2026-10-20T11:00:00.000Z",
    state: "current",
  },
};

describe("fleet publication copy", () => {
  it.each([
    ["plan-managed", "update plan"],
    ["unproved", "native proof missing"],
    ["blocked", "native verification blocked"],
    ["unavailable", "native proof unavailable"],
  ] as const)("explains %s without promoting installed-source evidence", (code, reason) => {
    expect(describePublication({ kind: "withheld", code, message: "Native detail." }, source)).toBe(
      `Automatic publication withheld (${reason}). Laptop last observed installed build: aaaaaaa.`,
    );
  });

  it("distinguishes native authorization from the host's older receipt", () => {
    expect(
      describePublication({
        kind: "ordinary-verified",
        source: { ...source, repository: "Jones-Systems/Jones-Code" },
      }),
    ).toBe("Laptop publication authorized for aaaaaaa.");
    expect(describePublicationReceipt("VPS", receipt)).toBe(
      "VPS: last reported laptop build ccccccc at Oct 20, 2026, 7:00 AM EDT · host status observed.",
    );
  });

  it("keeps missing/offline acknowledgement and missing timestamps explicit", () => {
    expect(describePublicationReceipt("VPS", undefined)).toBe(
      "VPS: host report unknown; not reached yet.",
    );
    expect(describePublicationReceipt("VPS", { ...receipt, outcome: "unknown" })).toContain(
      "latest acknowledgement unknown; host not reached or response not confirmed",
    );
    expect(
      describePublicationReceipt("VPS", {
        ...receipt,
        follower: { protocol: 1, leaderSource: source.sha, state: "current" },
      }),
    ).toContain("aaaaaaa at time unknown");
    expect(
      describePublicationReceipt("VPS", {
        ...receipt,
        follower: { protocol: 1, leaderSource: null, state: "waiting-for-leader" },
      }),
    ).toContain("last reported laptop build unknown");
    expect(describePublicationReceipt("VPS", { ...receipt, outcome: "directive-held" })).toContain(
      "update plan holds automatic following",
    );
  });

  it("uses Eastern time with the correct seasonal abbreviation", () => {
    expect(formatFleetTime("2026-10-20T11:00:00.000Z")).toBe("Oct 20, 2026, 7:00 AM EDT");
    expect(formatFleetTime("2026-11-12T11:00:00.000Z")).toBe("Nov 12, 2026, 6:00 AM EST");
    expect(formatFleetTime("invalid")).toBe("time unknown");
  });

  it("rejects late reports for retired enrollments and gives a newer failed read precedence", () => {
    const enrollment = receipt.enrollment;
    const local = { status: null, observedAt: undefined, unavailable: false };
    expect(
      selectPublicationReceipt({ ...enrollment, enabled: false }, [receipt], local),
    ).toBeUndefined();
    expect(
      selectPublicationReceipt({ ...enrollment, enrollmentId: "new-enrollment" }, [receipt], local),
    ).toBeUndefined();
    expect(
      selectPublicationReceipt({ ...enrollment, continueRunningThreads: true }, [receipt], local),
    ).toBeUndefined();
    expect(selectPublicationReceipt(enrollment, [receipt], local)).toBe(receipt);
    expect(
      selectPublicationReceipt(enrollment, [receipt], {
        status: null,
        observedAt: "2026-11-12T12:00:00.000Z",
        unavailable: true,
      }),
    ).toMatchObject({ outcome: "unknown", enrollment });
    expect(
      selectPublicationReceipt(enrollment, [receipt], {
        status: null,
        observedAt: "2026-11-12T10:00:00.000Z",
        unavailable: true,
      }),
    ).toBe(receipt);
  });
});
