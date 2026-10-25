import { describe, expect, it, vi } from "vite-plus/test";
import { actionableAdmissions, admissionBindingKey, createAdmissionStore } from "./store";
import { admissionFixture, admissionSnapshot } from "./testFixtures";

describe("owner request state", () => {
  it("retains the snapshot limit warning while a decision removes a row and refresh is unavailable", async () => {
    const pending = Array.from({ length: 200 }, (_, index) =>
      admissionFixture({ id: `bounded-${index}` }),
    );
    const request = vi.fn().mockResolvedValue(admissionSnapshot(pending));
    const store = createAdmissionStore(request);
    await store.refresh();
    const held = pending[0]!;
    request
      .mockResolvedValueOnce({ ...held, status: "denied" })
      .mockRejectedValueOnce(new Error("offline"));
    await store.decide({ id: held.id, bindingDigest: held.bindingDigest, decision: "deny" });
    expect(store.getSnapshot().snapshot?.pending).toHaveLength(199);
    expect(store.getSnapshot().atLimit).toBe(true);
  });
  it("retains a successful decision across failed refreshes and stale held snapshots, then follows dispatch", async () => {
    const held = admissionFixture();
    const request = vi.fn().mockResolvedValue(admissionSnapshot());
    const store = createAdmissionStore(request);
    await store.refresh();
    request
      .mockResolvedValueOnce({ ...held, status: "approved" })
      .mockRejectedValueOnce(new Error("offline"));
    await store.decide({ id: held.id, bindingDigest: held.bindingDigest, decision: "approve" });
    expect(store.getSnapshot().loadFailed).toBe(true);
    expect(store.getSnapshot().snapshot?.pending[0]?.status).toBe("approved");
    expect(actionableAdmissions(store.getSnapshot())).toHaveLength(0);
    await store.refresh();
    expect(store.getSnapshot().snapshot?.pending[0]?.status).toBe("approved");
    request.mockResolvedValue(admissionSnapshot([{ ...held, status: "dispatching" }]));
    await store.refresh();
    expect(store.getSnapshot().snapshot?.pending[0]?.status).toBe("dispatching");
    await store.decide({ id: held.id, bindingDigest: held.bindingDigest, decision: "approve" });
    expect(request.mock.calls.filter(([input]) => input.action === "decide")).toHaveLength(1);
  });

  it("does not revive denied controls and never automatically retries an uncertain decision", async () => {
    const held = admissionFixture();
    const request = vi.fn().mockResolvedValue(admissionSnapshot());
    const store = createAdmissionStore(request);
    await store.refresh();
    request.mockRejectedValueOnce(new Error("private server detail"));
    await store.decide({ id: held.id, bindingDigest: held.bindingDigest, decision: "deny" });
    expect(store.getSnapshot().unconfirmed.has(admissionBindingKey(held))).toBe(true);
    expect(store.getSnapshot().error).not.toContain("private server detail");
    expect(actionableAdmissions(store.getSnapshot())).toHaveLength(0);
    await store.decide({ id: held.id, bindingDigest: held.bindingDigest, decision: "deny" });
    expect(request.mock.calls.filter(([input]) => input.action === "decide")).toHaveLength(1);
    const other = createAdmissionStore(vi.fn().mockResolvedValue(admissionSnapshot()));
    await other.refresh();
    expect(actionableAdmissions(other.getSnapshot())).toHaveLength(1);
  });

  it("removes a confirmed denial immediately even when server refresh fails", async () => {
    const held = admissionFixture();
    const request = vi.fn().mockResolvedValue(admissionSnapshot());
    const store = createAdmissionStore(request);
    await store.refresh();
    request
      .mockResolvedValueOnce({ ...held, status: "denied" })
      .mockRejectedValueOnce(new Error("offline"));
    await store.decide({ id: held.id, bindingDigest: held.bindingDigest, decision: "deny" });
    expect(store.getSnapshot().snapshot?.pending).toEqual([]);
    await store.refresh();
    expect(store.getSnapshot().snapshot?.pending).toEqual([]);
  });

  it("rejects stale bindings and policy approvals, permits a bound denial and counts only held requests", async () => {
    const held = admissionFixture();
    const request = vi.fn().mockResolvedValue({
      ...admissionSnapshot([
        held,
        admissionFixture({ id: "evaluating", status: "evaluating" }),
        admissionFixture({ id: "dispatching", status: "dispatching" }),
      ]),
      policy: { enabled: true, revision: "new-policy", modelSelection: null },
    });
    const store = createAdmissionStore(request);
    await store.refresh();
    expect(actionableAdmissions(store.getSnapshot())).toHaveLength(1);
    await store.decide({ id: held.id, bindingDigest: "c".repeat(64), decision: "deny" });
    await store.decide({ id: held.id, bindingDigest: held.bindingDigest, decision: "approve" });
    expect(request).toHaveBeenCalledTimes(1);
    request.mockResolvedValueOnce({ ...held, status: "denied" });
    await store.decide({ id: held.id, bindingDigest: held.bindingDigest, decision: "deny" });
    expect(request).toHaveBeenCalledWith({
      action: "decide",
      input: { id: held.id, bindingDigest: held.bindingDigest, decision: "deny" },
    });
  });

  it("suppresses overlapping reads and ignores a snapshot that started before a decision", async () => {
    const held = admissionFixture();
    const request = vi.fn().mockResolvedValue(admissionSnapshot());
    const store = createAdmissionStore(request);
    await store.refresh();
    let resolve!: (value: ReturnType<typeof admissionSnapshot>) => void;
    request.mockImplementationOnce(
      () =>
        new Promise((done) => {
          resolve = done;
        }),
    );
    const stale = store.refresh();
    expect(store.refresh()).toBe(stale);
    request.mockResolvedValueOnce({ ...held, status: "approved" });
    const decision = store.decide({
      id: held.id,
      bindingDigest: held.bindingDigest,
      decision: "approve",
    });
    resolve(admissionSnapshot());
    await Promise.all([stale, decision]);
    expect(store.getSnapshot().snapshot?.pending[0]?.status).toBe("approved");
  });
});
