import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import { createDesktopFleetStore } from "./store.ts";

const campaignId = "11111111-1111-4111-8111-111111111111";
const enrollmentId = "22222222-2222-4222-8222-222222222222";
const source = "a".repeat(40);
const stagedHandle = "c".repeat(64);
const transactionId = "d".repeat(64);
const enrollment = {
  enrollmentId,
  environmentId: EnvironmentId.make("fleet-test-host"),
  enabled: true,
  continueRunningThreads: false,
};
async function fixture(
  run: (home: string, store: ReturnType<typeof createDesktopFleetStore>) => Promise<void>,
) {
  const home = await mkdtemp(join(tmpdir(), "jones-fleet-store-test-"));
  try {
    const store = createDesktopFleetStore({ home, profile: undefined });
    await store.request({ action: "enroll", enrollment });
    await store.request({
      action: "prepare",
      input: { campaignId, targetSource: source, desktopStagedHandle: stagedHandle },
    });
    await run(home, store);
  } finally {
    await rm(home, { recursive: true, force: true });
  }
}

describe("native fleet commit gate", () => {
  it("requires the exact native transaction and retains its proof across a process restart", async () =>
    fixture(async (home, store) => {
      const before = await store.request({ action: "read" });
      const operationId = before.campaigns[0]!.members[0]!.operationId;
      const dispatch = {
        action: "updateMember" as const,
        input: {
          campaignId,
          operationId,
          expectedPhase: "waiting" as const,
          phase: "dispatching" as const,
        },
      };
      await expect(store.request(dispatch)).rejects.toThrow("has not committed");
      await store.bindInstall({
        campaignId,
        stagedHandle: stagedHandle,
        targetSource: source,
        transactionId,
        fromGeneration: "previous",
      });
      await store.recordOutcome({
        transactionId: "unrelated",
        status: "committed",
        activeGeneration: "unrelated",
        activeSource: source,
      });
      await expect(store.request(dispatch)).rejects.toThrow("has not committed");
      await store.recordOutcome({
        transactionId,
        status: "committed",
        activeGeneration: transactionId,
        activeSource: source,
      });
      const restarted = createDesktopFleetStore({ home, profile: undefined });
      const after = await restarted.request(dispatch);
      expect(after.campaigns[0]?.committedGeneration).toBe(transactionId);
      expect(after.campaigns[0]?.members[0]?.phase).toBe("dispatching");
    }));

  it("does not activate after rollback or a source mismatch", async () => {
    for (const mismatch of [false, true])
      await fixture(async (_home, store) => {
        await store.bindInstall({
          campaignId,
          stagedHandle: stagedHandle,
          targetSource: source,
          transactionId: stagedHandle,
          fromGeneration: "previous",
        });
        await store.recordOutcome({
          transactionId: stagedHandle,
          status: mismatch ? "committed" : "rolled-back",
          activeGeneration: mismatch ? stagedHandle : "previous",
          activeSource: "b".repeat(40),
        });
        const state = await store.request({ action: "read" });
        expect(state.campaigns[0]?.phase).toBe(mismatch ? "blocked" : "rolled-back");
        await expect(
          store.request({
            action: "updateMember",
            input: {
              campaignId,
              operationId: state.campaigns[0]!.members[0]!.operationId,
              expectedPhase: "waiting",
              phase: "dispatching",
            },
          }),
        ).rejects.toThrow("has not committed");
      });
  });

  it("preserves in-flight members across a newer campaign and prevents source rebinding", async () =>
    fixture(async (_home, store) => {
      const state = await store.request({ action: "read" });
      const operationId = state.campaigns[0]!.members[0]!.operationId;
      await store.request({
        action: "updateMember",
        input: {
          campaignId,
          operationId,
          expectedPhase: "waiting",
          phase: "staging",
          expectedInstalledSource: "b".repeat(40),
        },
      });
      await expect(
        store.request({
          action: "updateMember",
          input: {
            campaignId,
            operationId,
            expectedPhase: "staging",
            phase: "staged",
            expectedInstalledSource: "c".repeat(40),
          },
        }),
      ).rejects.toThrow("source binding changed");
      await store.bindInstall({
        campaignId,
        stagedHandle: stagedHandle,
        targetSource: source,
        transactionId: stagedHandle,
        fromGeneration: "previous",
      });
      await store.recordOutcome({
        transactionId: stagedHandle,
        status: "committed",
        activeGeneration: stagedHandle,
        activeSource: source,
      });
      await store.request({
        action: "updateMember",
        input: { campaignId, operationId, expectedPhase: "staging", phase: "pending" },
      });
      const next = await store.request({
        action: "prepare",
        input: {
          campaignId: "33333333-3333-4333-8333-333333333333",
          targetSource: "d".repeat(40),
          desktopStagedHandle: "next-handle",
        },
      });
      expect(next.campaigns[0]?.members[0]?.phase).toBe("pending");
      await expect(
        store.request({
          action: "updateMember",
          input: { campaignId, operationId, expectedPhase: "pending", phase: "offline" },
        }),
      ).rejects.toThrow("must be reconciled");
    }));
});

it("holds an old unaccepted stage for exact retirement instead of forgetting its occupied slot", async () =>
  fixture(async (_home, store) => {
    const state = await store.request({ action: "read" });
    const operationId = state.campaigns[0]!.members[0]!.operationId;
    await store.request({
      action: "updateMember",
      input: {
        campaignId,
        operationId,
        expectedPhase: "waiting",
        phase: "staged",
        expectedInstalledSource: "b".repeat(40),
      },
    });
    const nextId = "33333333-3333-4333-8333-333333333333";
    const next = await store.request({
      action: "prepare",
      input: { campaignId: nextId, targetSource: "c".repeat(40), desktopStagedHandle: "new-stage" },
    });
    expect(next.campaigns[0]?.members[0]?.phase).toBe("retiring");
    const nextOperation = next.campaigns[1]!.members[0]!.operationId;
    await expect(
      store.request({
        action: "updateMember",
        input: {
          campaignId: nextId,
          operationId: nextOperation,
          expectedPhase: "waiting",
          phase: "staging",
        },
      }),
    ).rejects.toThrow("requires reconciliation");
    await store.request({
      action: "updateMember",
      input: { campaignId, operationId, expectedPhase: "retiring", phase: "superseded" },
    });
    expect(
      (
        await store.request({
          action: "updateMember",
          input: {
            campaignId: nextId,
            operationId: nextOperation,
            expectedPhase: "waiting",
            phase: "staging",
          },
        })
      ).campaigns[1]?.members[0]?.phase,
    ).toBe("staging");
  }));

it("binds one native attempt to one campaign and retains it on repeated calls after reload", async () =>
  fixture(async (home, store) => {
    const binding = {
      campaignId,
      stagedHandle,
      transactionId,
      targetSource: source,
      fromGeneration: "previous",
    };
    const before = await store.request({ action: "read" });
    await store.bindInstall(binding);
    const restarted = createDesktopFleetStore({ home, profile: undefined });
    await restarted.bindInstall(binding);
    expect((await restarted.request({ action: "read" })).campaigns[0]?.members).toEqual(
      before.campaigns[0]?.members,
    );
    for (const altered of [
      { ...binding, transactionId: "e".repeat(64) },
      { ...binding, fromGeneration: "another-generation" },
      { ...binding, stagedHandle: "f".repeat(64) },
      { ...binding, targetSource: "b".repeat(40) },
      { ...binding, transactionId: "not-a-native-attempt" },
    ])
      await expect(restarted.bindInstall(altered)).rejects.toThrow();
    expect((await restarted.request({ action: "read" })).campaigns[0]?.installation).toEqual({
      transactionId,
      fromGeneration: "previous",
    });
  }));

it("uses a fresh campaign and host operation after rollback without reusing the old native attempt", async () =>
  fixture(async (home, store) => {
    const first = {
      campaignId,
      stagedHandle,
      transactionId,
      targetSource: source,
      fromGeneration: "previous",
    };
    await store.bindInstall(first);
    await store.recordOutcome({
      transactionId,
      status: "rolled-back",
      activeGeneration: "previous",
      activeSource: "b".repeat(40),
    });
    const firstState = await store.request({ action: "read" });
    const oldOperationId = firstState.campaigns[0]!.members[0]!.operationId;
    await expect(store.bindInstall({ ...first, transactionId: "e".repeat(64) })).rejects.toThrow();
    const nextId = "33333333-3333-4333-8333-333333333333";
    const prepared = await store.request({
      action: "prepare",
      input: { campaignId: nextId, targetSource: source, desktopStagedHandle: stagedHandle },
    });
    expect(prepared.campaigns[0]?.phase).toBe("rolled-back");
    const nextOperationId = prepared.campaigns[1]!.members[0]!.operationId;
    expect(nextOperationId).not.toBe(oldOperationId);
    await expect(store.bindInstall({ ...first, campaignId: nextId })).rejects.toThrow(
      "already belongs",
    );
    const nextAttempt = "e".repeat(64);
    await store.bindInstall({ ...first, campaignId: nextId, transactionId: nextAttempt });
    const restarted = createDesktopFleetStore({ home, profile: undefined });
    await restarted.bindInstall({ ...first, campaignId: nextId, transactionId: nextAttempt });
    await restarted.recordOutcome({
      transactionId,
      status: "committed",
      activeGeneration: transactionId,
      activeSource: source,
    });
    expect(
      (await restarted.request({ action: "read" })).campaigns.map((campaign) => campaign.phase),
    ).toEqual(["rolled-back", "installing"]);
    await restarted.recordOutcome({
      transactionId: nextAttempt,
      status: "committed",
      activeGeneration: nextAttempt,
      activeSource: source,
    });
    const final = await restarted.request({ action: "read" });
    expect(final.campaigns.map((campaign) => campaign.phase)).toEqual(["rolled-back", "committed"]);
    expect(final.campaigns[1]?.members[0]?.operationId).toBe(nextOperationId);
    expect(final.campaigns[1]?.committedGeneration).toBe(nextAttempt);
  }));
