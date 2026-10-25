import { describe, expect, it, vi } from "vite-plus/test";
import {
  actionableAdmissions,
  admissionAttention,
  admissionBindingKey,
  createAdmissionStore,
} from "./store";
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

it.each([
  [403, "unauthorized"],
  [404, "unsupported"],
  [503, "transient"],
] as const)(
  "retains HTTP %s classification, blocks stale policy and permits explicit recovery",
  async (status, kind) => {
    const snapshot = admissionSnapshot();
    const request = vi.fn().mockResolvedValue(snapshot);
    const store = createAdmissionStore(request);
    await store.refresh();
    request.mockRejectedValueOnce({
      _tag: "RemoteEnvironmentAuthUndeclaredStatusError",
      status,
      message: "synthetic private response",
    });
    await store.refresh();
    expect(store.getSnapshot()).toMatchObject({
      loadFailed: true,
      loadFailure: kind,
      loading: false,
    });
    expect(store.getSnapshot().snapshot).toEqual(snapshot);
    await store.savePolicy({
      enabled: false,
      expectedRevision: snapshot.policy.revision,
      modelSelection: null,
    });
    expect(request).toHaveBeenCalledTimes(2);
    await store.refresh();
    expect(store.getSnapshot()).toMatchObject({
      loadFailed: false,
      loadFailure: null,
      loading: false,
    });
    request.mockResolvedValue({ ...snapshot, policy: { ...snapshot.policy, enabled: false } });
    await store.savePolicy({
      enabled: false,
      expectedRevision: snapshot.policy.revision,
      modelSelection: null,
    });
    expect(store.getSnapshot().snapshot?.policy.enabled).toBe(false);
  },
);

it("blocks policy changes while a current snapshot is loading", async () => {
  const snapshot = admissionSnapshot();
  const request = vi.fn().mockResolvedValue(snapshot);
  const store = createAdmissionStore(request);
  await store.refresh();
  let resolve!: (value: typeof snapshot) => void;
  request.mockImplementationOnce(
    () =>
      new Promise((done) => {
        resolve = done;
      }),
  );
  const reading = store.refresh();
  expect(store.getSnapshot().loading).toBe(true);
  await store.savePolicy({
    enabled: false,
    expectedRevision: snapshot.policy.revision,
    modelSelection: null,
  });
  expect(request).toHaveBeenCalledTimes(2);
  resolve(snapshot);
  await reading;
  expect(store.getSnapshot().loading).toBe(false);
});

it("backs off transient reads from ten seconds to five minutes, permits Refresh, and resets after success", async () => {
  let now = 0;
  const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
  try {
    const request = vi.fn().mockRejectedValue({ _tag: "RemoteEnvironmentAuthFetchError" });
    const store = createAdmissionStore(request);
    await store.refreshWhenDue();
    for (const delay of [10_000, 20_000, 40_000, 80_000, 160_000, 300_000, 300_000]) {
      const before = request.mock.calls.length;
      now += delay - 1;
      await store.refreshWhenDue();
      expect(request).toHaveBeenCalledTimes(before);
      now++;
      await store.refreshWhenDue();
      expect(request).toHaveBeenCalledTimes(before + 1);
    }
    const before = request.mock.calls.length;
    request.mockResolvedValue(admissionSnapshot());
    await store.refresh();
    expect(request).toHaveBeenCalledTimes(before + 1);
    expect(store.getSnapshot().loadFailure).toBeNull();
    request.mockRejectedValue({ _tag: "RemoteEnvironmentAuthFetchError" });
    await store.refreshWhenDue();
    now += 10_000;
    await store.refreshWhenDue();
    expect(request).toHaveBeenCalledTimes(before + 3);
  } finally {
    clock.mockRestore();
  }
});

it.each([403, 404])(
  "excludes terminal HTTP %s from attention and automatic retries until Refresh recovers",
  async (status) => {
    const request = vi.fn().mockResolvedValue(admissionSnapshot());
    const store = createAdmissionStore(request);
    await store.refresh();
    expect(admissionAttention(store.getSnapshot()).held).toBe(1);
    request.mockRejectedValue({ _tag: "RemoteEnvironmentAuthUndeclaredStatusError", status });
    await store.refresh();
    expect(admissionAttention(store.getSnapshot())).toEqual({
      held: 0,
      unknown: false,
      capped: false,
    });
    expect(actionableAdmissions(store.getSnapshot())).toEqual([]);
    await store.refreshWhenDue();
    expect(request).toHaveBeenCalledTimes(2);
    request.mockResolvedValue(admissionSnapshot());
    await store.refresh();
    expect(admissionAttention(store.getSnapshot())).toEqual({
      held: 1,
      unknown: false,
      capped: false,
    });
    request.mockRejectedValue({ _tag: "RemoteEnvironmentAuthFetchError" });
    await store.refresh();
    expect(admissionAttention(store.getSnapshot()).unknown).toBe(true);
  },
);
