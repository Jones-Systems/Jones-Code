import { page, userEvent } from "vite-plus/test/browser";
import { describe, expect, it } from "vite-plus/test";
import type { JonesRecoveryPreflight } from "@t3tools/contracts/jones/jonesOperations";
import { EnvironmentId } from "@t3tools/contracts";
import { act, render } from "../../../test/render";
import { chooseRecoveryPolicy, RecoveryReview } from "./recoveryReview";
import "../../index.css";

function preflight(overrides: Partial<JonesRecoveryPreflight> = {}): JonesRecoveryPreflight {
  return {
    protocol: 1,
    preflightId: "11111111-1111-4111-8111-111111111111",
    measuredAt: "2026-10-27T04:00:00Z",
    expiresAt: "2026-10-27T04:05:00Z",
    binding: {
      environmentId: EnvironmentId.make("review-host"),
      stagedHandle: "stage",
      currentVersion: "1.0.0",
      expectedInstalledSource: "a".repeat(40),
    },
    firstInstall: false,
    stateBytes: 1000,
    volumes: [
      { role: "state", availableBytes: 2000, copyRequiredBytes: 3000, cloneRequiredBytes: 1200 },
    ],
    clone: { status: "proved" },
    policies: {
      cloneOrCopy: { admissible: false, reason: "Not enough copy space." },
      cloneRequired: { admissible: true },
    },
    ...overrides,
  };
}
describe("negotiated recovery review", () => {
  it("offers Clone or Stop only with target clone proof and displays measured bytes", async () => {
    await using _screen = await render(<RecoveryReview />, { router: false });
    let result!: ReturnType<typeof chooseRecoveryPolicy>;
    await act(() => {
      result = chooseRecoveryPolicy(preflight());
    });
    expect(document.body.textContent).toContain("2,000 bytes available");
    await page.getByRole("button", { name: "Clone", exact: true }).click();
    expect(await result!).toBe("clone-required");
  });
  it("shows the refusal reason with Stop alone when clone is unproved", async () => {
    await using _screen = await render(<RecoveryReview />, { router: false });
    let result!: ReturnType<typeof chooseRecoveryPolicy>;
    await act(() => {
      result = chooseRecoveryPolicy(
        preflight({
          clone: { status: "unknown" },
          policies: {
            cloneOrCopy: { admissible: false, reason: "Target volume measurement unavailable." },
            cloneRequired: { admissible: false },
          },
        }),
      );
    });
    expect(document.body.textContent).toContain("Target volume measurement unavailable.");
    expect(
      [...document.querySelectorAll("button")].some((node) => node.textContent === "Clone"),
    ).toBe(false);
    await page.getByRole("button", { name: "Stop", exact: true }).click();
    expect(await result!).toBeNull();
  });
  it("describes first install without claiming a backup", async () => {
    await using _screen = await render(<RecoveryReview />, { router: false });
    let result!: ReturnType<typeof chooseRecoveryPolicy>;
    await act(() => {
      result = chooseRecoveryPolicy(
        preflight({
          firstInstall: true,
          stateBytes: 0,
          policies: { cloneOrCopy: { admissible: true }, cloneRequired: { admissible: false } },
          clone: { status: "unsupported" },
        }),
      );
    });
    expect(document.body.textContent).toContain("There is no existing data to back up.");
    await page.getByRole("button", { name: "Continue", exact: true }).click();
    expect(await result!).toBe("clone-or-copy");
  });
  it("settles an abandoned review as Stop and accepts a fresh review after remount", async () => {
    await using screen = await render(<RecoveryReview />, { router: false });
    let abandoned!: ReturnType<typeof chooseRecoveryPolicy>;
    await act(() => {
      abandoned = chooseRecoveryPolicy(preflight());
    });
    await screen.unmount();
    expect(await abandoned).toBeNull();

    await using _fresh = await render(<RecoveryReview />, { router: false });
    let current!: ReturnType<typeof chooseRecoveryPolicy>;
    await act(() => {
      current = chooseRecoveryPolicy(
        preflight({
          clone: { status: "unsupported" },
          policies: {
            cloneOrCopy: { admissible: false, reason: "Fresh clone proof unavailable." },
            cloneRequired: { admissible: false },
          },
        }),
      );
    });
    await expect.element(page.getByText("Fresh clone proof unavailable.")).toBeVisible();
    expect(
      [...document.querySelectorAll("button")].some((node) => node.textContent === "Clone"),
    ).toBe(false);
    await page.getByRole("button", { name: "Stop", exact: true }).click();
    expect(await current).toBeNull();
  });
  it("keeps the current review bound when another request arrives and Escape stops it", async () => {
    await using _screen = await render(<RecoveryReview />, { router: false });
    let current!: ReturnType<typeof chooseRecoveryPolicy>;
    await act(() => {
      current = chooseRecoveryPolicy(preflight());
    });
    expect(await chooseRecoveryPolicy(preflight({ firstInstall: true }))).toBeNull();
    await expect
      .element(page.getByText("Not enough space for a full backup copy", { exact: true }))
      .toBeVisible();
    await userEvent.keyboard("{Escape}");
    expect(await current).toBeNull();
  });
});
