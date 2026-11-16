import { EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  accountingUnavailableMessage,
  formatAccountingCount,
  formatAccountingMetric,
  selectAccountingEnvironment,
  type AccountingEnvironment,
} from "./tokenAccounting";

function environment(
  id: string,
  overrides: Partial<AccountingEnvironment> = {},
): AccountingEnvironment {
  return {
    environmentId: EnvironmentId.make(id),
    label: id,
    primary: false,
    connected: true,
    supported: true,
    ...overrides,
  };
}

describe("saved accounting environment selection", () => {
  it("prefers a capable connected primary and honors an explicit eligible selection", () => {
    const primary = environment("primary", { primary: true });
    const remote = environment("remote");
    expect(selectAccountingEnvironment([remote, primary], null)).toBe(primary);
    expect(selectAccountingEnvironment([remote, primary], remote.environmentId)).toBe(remote);
  });

  it("uses the sole eligible environment and requires a choice when several have no primary", () => {
    const first = environment("first");
    const second = environment("second");
    expect(selectAccountingEnvironment([first], null)).toBe(first);
    expect(selectAccountingEnvironment([first, second], null)).toBeNull();
    expect(selectAccountingEnvironment([first, second], second.environmentId)).toBe(second);
  });

  it("excludes disconnected and unsupported environments even when selected or primary", () => {
    const disconnected = environment("disconnected", { primary: true, connected: false });
    const old = environment("old", { supported: false });
    const eligible = environment("eligible");
    expect(selectAccountingEnvironment([disconnected, old], disconnected.environmentId)).toBeNull();
    expect(selectAccountingEnvironment([disconnected, old, eligible], old.environmentId)).toBe(
      eligible,
    );
  });
});

describe("saved accounting missingness", () => {
  it("distinguishes known zero, partial known zero and an entirely unknown metric", () => {
    expect(
      formatAccountingMetric({ total: 0, known_sum: 0, known_requests: 1, missing_requests: 0 }),
    ).toBe("0");
    expect(
      formatAccountingMetric({ total: null, known_sum: 0, known_requests: 1, missing_requests: 1 }),
    ).toBe("Known: 0 · total unknown");
    expect(
      formatAccountingMetric({
        total: null,
        known_sum: null,
        known_requests: 0,
        missing_requests: 2,
      }),
    ).toBe("Unknown");
    expect(
      formatAccountingMetric({
        total: null,
        known_sum: 42,
        known_requests: 1,
        missing_requests: 1,
      }),
    ).toBe("Known: 42 · total unknown");
  });

  it("formats large counts without claiming additional precision", () => {
    expect(formatAccountingCount(5_850_000)).toBe("5.85M");
    expect(formatAccountingCount(30_500_000_000)).toBe("30.5B");
    expect(formatAccountingCount(400_000_000)).toBe("400M");
  });

  it("uses a fixed message for a typed identity failure", () => {
    expect(
      accountingUnavailableMessage({ status: "invalid", reason: "report_identity_mismatch" }),
    ).toBe("The saved report did not pass its identity check.");
  });
});
