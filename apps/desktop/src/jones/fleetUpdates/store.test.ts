import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import { createDesktopFleetStore } from "./store.ts";

const campaignId = "11111111-1111-4111-8111-111111111111";
const enrollmentId = "22222222-2222-4222-8222-222222222222";
const source = "a".repeat(40);
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
      input: { campaignId, targetSource: source, desktopStagedHandle: "selected-handle" },
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
        stagedHandle: "selected-handle",
        targetSource: source,
        transactionId: "selected-handle",
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
        transactionId: "selected-handle",
        status: "committed",
        activeGeneration: "selected-handle",
        activeSource: source,
      });
      const restarted = createDesktopFleetStore({ home, profile: undefined });
      const after = await restarted.request(dispatch);
      expect(after.campaigns[0]?.committedGeneration).toBe("selected-handle");
      expect(after.campaigns[0]?.members[0]?.phase).toBe("dispatching");
    }));

  it("does not activate after rollback or a source mismatch", async () => {
    for (const mismatch of [false, true])
      await fixture(async (_home, store) => {
        await store.bindInstall({
          campaignId,
          stagedHandle: "selected-handle",
          targetSource: source,
          transactionId: "selected-handle",
          fromGeneration: "previous",
        });
        await store.recordOutcome({
          transactionId: "selected-handle",
          status: mismatch ? "committed" : "rolled-back",
          activeGeneration: mismatch ? "selected-handle" : "previous",
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
        stagedHandle: "selected-handle",
        targetSource: source,
        transactionId: "selected-handle",
        fromGeneration: "previous",
      });
      await store.recordOutcome({
        transactionId: "selected-handle",
        status: "committed",
        activeGeneration: "selected-handle",
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
