// @effect-diagnostics nodeBuiltinImport:off -- Synthetic native journal fixtures exercise filesystem custody and retain exact-root cleanup.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type { FleetHostOperation, FleetHostStatus } from "@t3tools/contracts/jones/fleet-updates";
import {
  advanceFleetCampaigns,
  type FleetCampaignDriver,
} from "@t3tools/client-runtime/jones/fleet-updates";
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
const secondEnrollment = {
  ...enrollment,
  environmentId: EnvironmentId.make("fleet-second-host"),
  enrollmentId: "44444444-4444-4444-8444-444444444444",
};
const twoHostCampaignId = "55555555-5555-4555-8555-555555555555";

async function createTwoHostCampaign(
  home: string,
  store: ReturnType<typeof createDesktopFleetStore>,
) {
  await store.request({ action: "enroll", enrollment: secondEnrollment });
  await store.request({
    action: "prepare",
    input: {
      campaignId: twoHostCampaignId,
      targetSource: source,
      desktopStagedHandle: stagedHandle,
    },
  });
  await store.bindInstall({
    campaignId: twoHostCampaignId,
    stagedHandle,
    targetSource: source,
    transactionId,
    fromGeneration: "previous",
  });
  await store.recordOutcome({
    transactionId,
    status: "committed",
    activeGeneration: transactionId,
    activeSource: source,
  });
  const operations = new Map<string, FleetHostOperation>();
  const active = new Set<EnvironmentId>();
  const activations: EnvironmentId[] = [];
  const offline = new Set<EnvironmentId>();
  const lostReplies = new Set<EnvironmentId>();
  const refusals = new Set<EnvironmentId>();
  let maximumActive = 0;
  const host = vi.fn<FleetCampaignDriver["host"]>(async (environmentId, request) => {
    if (offline.has(environmentId)) throw new Error("Synthetic host offline.");
    if (request.action === "enroll") throw new Error("Enrollment belongs to fixture setup.");
    const selectedEnrollment =
      environmentId === enrollment.environmentId ? enrollment : secondEnrollment;
    const operationId =
      request.action === "status" ? request.operationId : request.input.operationId;
    if (operationId === undefined) throw new Error("Expected an exact operation.");
    if (request.action === "stage")
      operations.set(operationId, {
        input: request.input,
        phase: "staged",
        currentVersion: "old",
        stagedHandle: "host-stage",
        continueRunningThreads: false,
      });
    if (request.action === "activate") {
      const operation = operations.get(operationId)!;
      activations.push(environmentId);
      if (refusals.has(environmentId)) {
        operations.set(operationId, {
          ...operation,
          phase: "install-blocked",
          reason: "Preacceptance capacity refusal.",
        });
      } else {
        active.add(environmentId);
        maximumActive = Math.max(maximumActive, active.size);
        operations.set(operationId, { ...operation, phase: "pending" });
      }
      if (lostReplies.has(environmentId)) throw new Error("Synthetic activation response lost.");
    }
    return {
      environmentId,
      enrollment: selectedEnrollment,
      operationProtocol: 1,
      operation: operations.get(operationId) ?? null,
      update: {
        source: "jones-actions",
        channel: "jones-main",
        phase: "available",
        currentVersion: "old",
        environmentId,
        installedSource: "b".repeat(40),
        capability: { check: true, download: true, install: true },
      },
    } satisfies FleetHostStatus;
  });
  const driver = (nativeStore = store): FleetCampaignDriver => ({
    desktop: nativeStore.request,
    host,
    cancelled: () => false,
  });
  const restart = () => createDesktopFleetStore({ home, profile: undefined });
  const members = async (nativeStore = store) =>
    (await nativeStore.request({ action: "read" })).campaigns.find(
      (campaign) => campaign.campaignId === twoHostCampaignId,
    )!.members;
  const finish = (environmentId: EnvironmentId) => {
    const operation = [...operations.values()].find(
      (entry) => entry.input.environmentId === environmentId,
    )!;
    operations.set(operation.input.operationId, { ...operation, phase: "committed" });
    active.delete(environmentId);
  };
  return {
    driver,
    restart,
    members,
    operations,
    active,
    activations,
    offline,
    lostReplies,
    refusals,
    finish,
    maximumActive: () => maximumActive,
  };
}
async function fixture(
  run: (home: string, store: ReturnType<typeof createDesktopFleetStore>) => Promise<void>,
) {
  const home = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-fleet-store-test-"));
  try {
    const store = createDesktopFleetStore({ home, profile: undefined });
    await store.request({ action: "enroll", enrollment });
    await store.request({
      action: "prepare",
      input: { campaignId, targetSource: source, desktopStagedHandle: stagedHandle },
    });
    await run(home, store);
  } finally {
    await NodeFSP.rm(home, { recursive: true, force: true });
    await expect(NodeFSP.lstat(home)).rejects.toMatchObject({ code: "ENOENT" });
  }
}

describe("native fleet commit gate", () => {
  it("serializes native activations across two controllers and retains the occupied slot after restart", async () =>
    fixture(async (home, store) => {
      const f = await createTwoHostCampaign(home, store);
      await Promise.all([advanceFleetCampaigns(f.driver()), advanceFleetCampaigns(f.driver())]);
      expect(f.activations).toHaveLength(1);
      expect((await f.members()).map((member) => member.phase).sort()).toEqual([
        "pending",
        "staged",
      ]);
      const restarted = f.restart();
      await advanceFleetCampaigns(f.driver(restarted));
      expect(f.activations).toHaveLength(1);
      const first = f.activations[0]!;
      f.finish(first);
      // A parallel status read can precede terminal persistence; the next pass
      // must observe that persistence before granting the waiting host.
      await advanceFleetCampaigns(f.driver(restarted));
      await advanceFleetCampaigns(f.driver(restarted));
      expect(f.activations).toHaveLength(2);
      expect(f.activations[1]).not.toBe(first);
      expect(f.maximumActive()).toBe(1);
    }));

  it("keeps a lost reply and offline dispatched host occupying activation while another host stages", async () =>
    fixture(async (home, store) => {
      const f = await createTwoHostCampaign(home, store);
      f.lostReplies.add(enrollment.environmentId);
      f.lostReplies.add(secondEnrollment.environmentId);
      await advanceFleetCampaigns(f.driver());
      expect(f.activations).toHaveLength(1);
      const first = f.activations[0]!;
      f.offline.add(first);
      const restarted = f.restart();
      await advanceFleetCampaigns(f.driver(restarted));
      expect(
        (await f.members(restarted)).find((member) => member.enrollment.environmentId === first)
          ?.phase,
      ).toBe("dispatching");
      expect(
        (await f.members(restarted)).find((member) => member.enrollment.environmentId !== first)
          ?.phase,
      ).toBe("staged");
      expect(f.activations).toHaveLength(1);
      f.offline.delete(first);
      const operation = [...f.operations.values()].find(
        (entry) => entry.input.environmentId === first,
      )!;
      f.operations.delete(operation.input.operationId);
      await advanceFleetCampaigns(f.driver(restarted));
      expect(
        (await f.members(restarted)).find((member) => member.enrollment.environmentId === first)
          ?.phase,
      ).toBe("reconciling");
      expect(f.activations).toHaveLength(1);
      f.operations.set(operation.input.operationId, operation);
      f.finish(first);
      await advanceFleetCampaigns(f.driver(restarted));
      await advanceFleetCampaigns(f.driver(restarted));
      expect(f.activations).toHaveLength(2);
      expect(f.maximumActive()).toBe(1);
    }));

  it("lets a different ready host proceed after an exact preacceptance refusal", async () =>
    fixture(async (home, store) => {
      const f = await createTwoHostCampaign(home, store);
      f.refusals.add(enrollment.environmentId);
      f.refusals.add(secondEnrollment.environmentId);
      await advanceFleetCampaigns(f.driver());
      expect(f.activations).toHaveLength(1);
      const refused = f.activations[0]!;
      const other =
        refused === enrollment.environmentId
          ? secondEnrollment.environmentId
          : enrollment.environmentId;
      f.refusals.delete(other);
      await advanceFleetCampaigns(f.driver(f.restart()));
      expect(f.active.has(other)).toBe(true);
      expect(f.activations.filter((entry) => entry === refused)).toHaveLength(1);
      expect(f.maximumActive()).toBe(1);
    }));

  it("arbitrates separate native stores and carries activation occupancy into a newer campaign", async () =>
    fixture(async (home, store) => {
      const f = await createTwoHostCampaign(home, store);
      const members = await f.members();
      for (const member of members)
        await store.request({
          action: "updateMember",
          input: {
            campaignId: twoHostCampaignId,
            operationId: member.operationId,
            expectedPhase: "waiting",
            phase: "staged",
            expectedInstalledSource: "b".repeat(40),
          },
        });
      const stores = [store, f.restart()];
      const claims = await Promise.allSettled(
        members.map((member, index) =>
          stores[index]!.request({
            action: "updateMember",
            input: {
              campaignId: twoHostCampaignId,
              operationId: member.operationId,
              expectedPhase: "staged",
              phase: "dispatching",
            },
          }),
        ),
      );
      expect(claims.some((claim) => claim.status === "fulfilled")).toBe(true);
      const after = await f.members();
      expect(after.filter((member) => member.phase === "dispatching")).toHaveLength(1);
      const owner = after.find((member) => member.phase === "dispatching")!;
      const waiting = after.find((member) => member.phase === "staged")!;
      await expect(
        store.request({
          action: "updateMember",
          input: {
            campaignId: twoHostCampaignId,
            operationId: owner.operationId,
            expectedPhase: "dispatching",
            phase: "dispatching",
          },
        }),
      ).rejects.toThrow("grant is already outstanding");
      await store.request({
        action: "updateMember",
        input: {
          campaignId: twoHostCampaignId,
          operationId: waiting.operationId,
          expectedPhase: "staged",
          phase: "superseded",
        },
      });
      const nextCampaignId = "66666666-6666-4666-8666-666666666666";
      await store.request({
        action: "prepare",
        input: {
          campaignId: nextCampaignId,
          targetSource: "f".repeat(40),
          desktopStagedHandle: "e".repeat(64),
        },
      });
      await store.bindInstall({
        campaignId: nextCampaignId,
        stagedHandle: "e".repeat(64),
        targetSource: "f".repeat(40),
        transactionId: "f".repeat(64),
        fromGeneration: transactionId,
      });
      await store.recordOutcome({
        transactionId: "f".repeat(64),
        status: "committed",
        activeGeneration: "f".repeat(64),
        activeSource: "f".repeat(40),
      });
      const restarted = f.restart();
      const state = await restarted.request({ action: "read" });
      const next = state.campaigns
        .find((campaign) => campaign.campaignId === nextCampaignId)!
        .members.find(
          (member) => member.enrollment.environmentId === waiting.enrollment.environmentId,
        )!;
      await restarted.request({
        action: "updateMember",
        input: {
          campaignId: nextCampaignId,
          operationId: next.operationId,
          expectedPhase: "waiting",
          phase: "staged",
        },
      });
      const claim = {
        action: "updateMember" as const,
        input: {
          campaignId: nextCampaignId,
          operationId: next.operationId,
          expectedPhase: "staged" as const,
          phase: "dispatching" as const,
        },
      };
      const refused = await restarted.request(claim);
      expect(
        refused.campaigns
          .find((campaign) => campaign.campaignId === nextCampaignId)!
          .members.find((member) => member.operationId === next.operationId)?.phase,
      ).toBe("staged");
      await restarted.request({
        action: "updateMember",
        input: {
          campaignId: twoHostCampaignId,
          operationId: owner.operationId,
          expectedPhase: "dispatching",
          phase: "committed",
        },
      });
      const granted = await restarted.request(claim);
      expect(
        granted.campaigns
          .find((campaign) => campaign.campaignId === nextCampaignId)!
          .members.find((member) => member.operationId === next.operationId)?.phase,
      ).toBe("dispatching");
    }));

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
      await expect(
        store.request({
          action: "updateMember",
          input: { campaignId, operationId, expectedPhase: "pending", phase: "install-blocked" },
        }),
      ).rejects.toThrow("without resubmission");
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
