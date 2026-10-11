import { describe, expect, it, vi } from "vitest";
import { EnvironmentId } from "@t3tools/contracts";
import type { FleetDesktopState, FleetHostStatus, FleetHostOperation } from "@t3tools/contracts/jones/fleet-updates";
import { advanceFleetCampaigns, type FleetCampaignDriver } from "./campaignController.ts";

const source = "a".repeat(40);
const installed = "b".repeat(40);
const environmentId = EnvironmentId.make("fleet-server");
const enrollment = { environmentId, enrollmentId: "11111111-1111-4111-8111-111111111111", enabled: true, continueRunningThreads: false };
const operationId = "22222222-2222-4222-8222-222222222222";
const campaignId = "33333333-3333-4333-8333-333333333333";
function fixture(committed = false) {
  let state: FleetDesktopState = {
    schema: 1, enrollments: [enrollment], campaigns: [{
      campaignId, targetSource: source, desktopStagedHandle: "app-handle",
      phase: committed ? "committed" : "prepared",
      ...(committed ? { committedGeneration: "app-handle" } : {}),
      members: [{ enrollment, operationId, phase: "waiting" }],
    }],
  };
  let operation: FleetHostOperation | null = null;
  let lostActivation = false;
  const status = (): FleetHostStatus => ({
    operationProtocol: 1, environmentId, enrollment, operation,
    update: { source: "jones-actions", channel: "jones-main", phase: "available", capability: { check: true, download: true, install: true }, installedSource: installed, environmentId, currentVersion: "preview-old" },
  });
  const host = vi.fn<FleetCampaignDriver["host"]>(async (_id, request) => {
    if (request.action === "stage") operation = { input: request.input, phase: "staged", continueRunningThreads: false, currentVersion: "preview-old", stagedHandle: "server-handle" };
    if (request.action === "activate") {
      operation = { ...operation!, phase: "pending" };
      if (lostActivation) throw new Error("response lost");
    }
    return status();
  });
  const desktop = vi.fn<FleetCampaignDriver["desktop"]>(async (request) => {
    if (request.action === "updateMember") {
      const campaign = state.campaigns.find((entry) => entry.campaignId === request.input.campaignId)!;
      const old = campaign.members.find((member) => member.operationId === request.input.operationId)!;
      if (old.phase !== request.input.expectedPhase) throw new Error("CAS conflict");
      state = { ...state, campaigns: state.campaigns.map((entry) => entry !== campaign ? entry : {
        ...entry, members: entry.members.map((member) => member !== old ? member : {
          ...member, phase: request.input.phase,
          ...(request.input.expectedInstalledSource === undefined ? {} : { expectedInstalledSource: request.input.expectedInstalledSource }),
        }),
      }) };
    }
    return state;
  });
  const driver = { desktop, host, cancelled: () => false };
  return { driver, state: () => state, operation: () => operation,
    loseActivation: () => { lostActivation = true; },
    setOperation: (next: FleetHostOperation) => { operation = next; },
    setState: (next: FleetDesktopState) => { state = next; },
  };
}

describe("fleet campaign driver", () => {
  it("stages the frozen SHA and never activates from a prepared campaign", async () => {
    const f = fixture();
    await advanceFleetCampaigns(f.driver);
    expect(f.operation()?.input.targetSource).toBe(source);
    expect(f.state().campaigns[0]?.members[0]?.phase).toBe("staged");
    expect(f.driver.host.mock.calls.filter(([, input]) => input.action === "activate")).toHaveLength(0);
  });

  it("reconciles a lost acceptance without duplicating native activation", async () => {
    const f = fixture(true);
    f.loseActivation();
    await advanceFleetCampaigns(f.driver);
    expect(f.state().campaigns[0]?.members[0]?.phase).toBe("dispatching");
    await advanceFleetCampaigns(f.driver);
    expect(f.state().campaigns[0]?.members[0]?.phase).toBe("pending");
    expect(f.driver.host.mock.calls.filter(([, input]) => input.action === "activate")).toHaveLength(1);
  });

  it("continues a healthy host when another enrolled host is offline", async () => {
    const f = fixture(true);
    const offlineEnrollment = { ...enrollment, environmentId: EnvironmentId.make("offline") };
    const state = f.state();
    f.setState({ ...state, enrollments: [...state.enrollments, offlineEnrollment], campaigns: [{
      ...state.campaigns[0]!, members: [
        { enrollment: offlineEnrollment, operationId: "44444444-4444-4444-8444-444444444444", phase: "waiting" },
        ...state.campaigns[0]!.members,
      ],
    }] });
    const original = f.driver.host.getMockImplementation()!;
    f.driver.host.mockImplementation((id, request) => id === offlineEnrollment.environmentId ? Promise.reject(new Error("offline")) : original(id, request));
    await advanceFleetCampaigns(f.driver);
    expect(f.state().campaigns[0]?.members.map((member) => member.phase)).toEqual(["offline", "pending"]);
  });

  it("does not stage or activate after laptop rollback or revoked enrollment", async () => {
    for (const revoked of [false, true]) {
      const f = fixture();
      const state = f.state();
      f.setState({ ...state, enrollments: revoked ? [{ ...enrollment, enabled: false }] : state.enrollments,
        campaigns: state.campaigns.map((campaign) => ({ ...campaign, phase: revoked ? "prepared" : "rolled-back" })),
      });
      await advanceFleetCampaigns(f.driver);
      expect(f.driver.host).not.toHaveBeenCalled();
    }
  });

  it("rejects a response for a changed immutable operation binding", async () => {
    const f = fixture(true);
    f.setOperation({ input: { operationId, enrollmentId: enrollment.enrollmentId, environmentId, expectedInstalledSource: installed, targetSource: "c".repeat(40) }, phase: "staged", currentVersion: "old", stagedHandle: "handle", continueRunningThreads: false });
    await advanceFleetCampaigns(f.driver);
    expect(f.state().campaigns[0]?.members[0]?.phase).toBe("blocked");
    expect(f.driver.host.mock.calls).toHaveLength(1);
  });
});
