import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import { createWebReviewAuthority } from "./webReviewAuthority";

describe("control-owned review authority", () => {
  it("captures selection and permission even when the environment stays the same", async () => {
    const environmentId = EnvironmentId.make("same-host");
    const original = createWebReviewAuthority(environmentId, true, "source-a");
    const retargeted = createWebReviewAuthority(environmentId, true, "source-b");
    const denied = createWebReviewAuthority(environmentId, false, "source-b");
    expect(original.binding).toEqual({ environmentId, allowed: true, selection: "source-a" });
    expect(retargeted.binding.selection).toBe("source-b");
    denied.activate();
    expect(denied.isCurrent()).toBe(false);
    let sent = false;
    expect(
      await denied.wait(async () => {
        sent = true;
        return true;
      }, false),
    ).toBe(false);
    expect(sent).toBe(false);
  });
  it("revokes pending confirmation and recovery, without cancelling another control", async () => {
    const a = createWebReviewAuthority(EnvironmentId.make("a"));
    const b = createWebReviewAuthority(EnvironmentId.make("b"));
    a.activate();
    b.activate();
    let confirm!: (value: boolean) => void, recover!: (value: string | null) => void;
    const confirmation = a.wait(
      () =>
        new Promise<boolean>((resolve) => {
          confirm = resolve;
        }),
      false,
    );
    const recovery = a.wait(
      () =>
        new Promise<string | null>((resolve) => {
          recover = resolve;
        }),
      null,
    );
    a.revoke();
    expect(await confirmation).toBe(false);
    expect(await recovery).toBeNull();
    confirm(true);
    recover("clone-required");
    expect(a.isCurrent()).toBe(false);
    expect(await b.wait(async () => true, false)).toBe(true);
  });
});
