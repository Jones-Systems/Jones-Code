import { describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type {
  FleetDesktopState,
  FleetHostStatus,
  FleetHostOperation,
} from "@t3tools/contracts/jones/fleet-updates";
import { advanceFleetCampaigns, type FleetCampaignDriver } from "./campaignController.ts";

const source = "a".repeat(40);
const installed = "b".repeat(40);
const environmentId = EnvironmentId.make("fleet-server");
const enrollment = {
  environmentId,
  enrollmentId: "11111111-1111-4111-8111-111111111111",
  enabled: true,
  continueRunningThreads: false,
};
const operationId = "22222222-2222-4222-8222-222222222222";
const campaignId = "33333333-3333-4333-8333-333333333333";
function fixture(committed = false) {
  let state: FleetDesktopState = {
    schema: 1,
    enrollments: [enrollment],
    campaigns: [
      {
        campaignId,
        targetSource: source,
        desktopStagedHandle: "app-handle",
        phase: committed ? "committed" : "prepared",
        ...(committed ? { committedGeneration: "app-handle" } : {}),
        members: [{ enrollment, operationId, phase: "waiting" }],
      },
    ],
  };
  let operation: FleetHostOperation | null = null;
  let lostActivation = false;
  const status = (): FleetHostStatus => ({
    operationProtocol: 1,
    environmentId,
    enrollment,
    operation,
    update: {
      source: "jones-actions",
      channel: "jones-main",
      phase: "available",
      capability: { check: true, download: true, install: true },
      installedSource: installed,
      environmentId,
      currentVersion: "preview-old",
    },
  });
  const host = vi.fn<FleetCampaignDriver["host"]>(async (_id, request) => {
    if (request.action === "stage")
      operation = {
        input: request.input,
        phase: "staged",
        continueRunningThreads: false,
        currentVersion: "preview-old",
        stagedHandle: "server-handle",
      };
    if (request.action === "activate") {
      operation = { ...operation!, phase: "pending" };
      if (lostActivation) throw new Error("response lost");
    }
    return status();
  });
  const desktop = vi.fn<FleetCampaignDriver["desktop"]>(async (request) => {
    if (request.action === "updateMember") {
      const campaign = state.campaigns.find(
        (entry) => entry.campaignId === request.input.campaignId,
      )!;
      const old = campaign.members.find(
        (member) => member.operationId === request.input.operationId,
      )!;
      if (old.phase !== request.input.expectedPhase) throw new Error("CAS conflict");
      state = {
        ...state,
        campaigns: state.campaigns.map((entry) =>
          entry !== campaign
            ? entry
            : {
                ...entry,
                members: entry.members.map((member) =>
                  member !== old
                    ? member
                    : {
                        ...member,
                        phase: request.input.phase,
                        ...(request.input.expectedInstalledSource === undefined
                          ? {}
                          : { expectedInstalledSource: request.input.expectedInstalledSource }),
                      },
                ),
              },
        ),
      };
    }
    return state;
  });
  const driver = { desktop, host, cancelled: () => false };
  return {
    driver,
    state: () => state,
    operation: () => operation,
    loseActivation: () => {
      lostActivation = true;
    },
    setOperation: (next: FleetHostOperation) => {
      operation = next;
    },
    setState: (next: FleetDesktopState) => {
      state = next;
    },
  };
}

describe("fleet campaign driver", () => {
  it("retains the staged host when the durable store refuses its activation grant", async () => {
    const f = fixture(true);
    const desktop = f.driver.desktop.getMockImplementation()!;
    f.driver.desktop.mockImplementation((request) =>
      request.action === "updateMember" && request.input.phase === "dispatching"
        ? Promise.resolve(f.state())
        : desktop(request),
    );
    await advanceFleetCampaigns(f.driver);
    expect(f.state().campaigns[0]?.members[0]?.phase).toBe("staged");
    expect(f.operation()?.phase).toBe("staged");
    expect(
      f.driver.host.mock.calls.filter(([, request]) => request.action === "activate"),
    ).toHaveLength(0);
  });

  it("does not release an in-transit activation grant using an older preacceptance status", async () => {
    const f = fixture(true);
    const state = f.state();
    f.setState({
      ...state,
      campaigns: state.campaigns.map((campaign) => ({
        ...campaign,
        members: campaign.members.map((member) => ({
          ...member,
          phase: "dispatching",
          expectedInstalledSource: installed,
        })),
      })),
    });
    const operation: FleetHostOperation = {
      input: {
        operationId,
        enrollmentId: enrollment.enrollmentId,
        environmentId,
        targetSource: source,
        expectedInstalledSource: installed,
      },
      phase: "install-blocked",
      currentVersion: "old",
      stagedHandle: "old-stage",
      continueRunningThreads: false,
    };
    f.setOperation(operation);
    await advanceFleetCampaigns(f.driver);
    expect(f.state().campaigns[0]?.members[0]?.phase).toBe("dispatching");
    expect(f.driver.host.mock.calls.map(([, request]) => request.action)).toEqual(["status"]);
    f.setOperation({ ...operation, phase: "committed" });
    await advanceFleetCampaigns(f.driver);
    expect(f.state().campaigns[0]?.members[0]?.phase).toBe("committed");
  });

  it("stages the frozen SHA and never activates from a prepared campaign", async () => {
    const f = fixture();
    await advanceFleetCampaigns(f.driver);
    expect(f.operation()?.input.targetSource).toBe(source);
    expect(f.state().campaigns[0]?.members[0]?.phase).toBe("staged");
    expect(
      f.driver.host.mock.calls.filter(([, input]) => input.action === "activate"),
    ).toHaveLength(0);
  });

  it("reconciles a lost acceptance without duplicating native activation", async () => {
    const f = fixture(true);
    f.loseActivation();
    await advanceFleetCampaigns(f.driver);
    expect(f.state().campaigns[0]?.members[0]?.phase).toBe("dispatching");
    await advanceFleetCampaigns(f.driver);
    expect(f.state().campaigns[0]?.members[0]?.phase).toBe("pending");
    expect(
      f.driver.host.mock.calls.filter(([, input]) => input.action === "activate"),
    ).toHaveLength(1);
  });

  it("continues a healthy host when another enrolled host is offline", async () => {
    const f = fixture(true);
    const offlineEnrollment = { ...enrollment, environmentId: EnvironmentId.make("offline") };
    const state = f.state();
    f.setState({
      ...state,
      enrollments: [...state.enrollments, offlineEnrollment],
      campaigns: [
        {
          ...state.campaigns[0]!,
          members: [
            {
              enrollment: offlineEnrollment,
              operationId: "44444444-4444-4444-8444-444444444444",
              phase: "waiting",
            },
            ...state.campaigns[0]!.members,
          ],
        },
      ],
    });
    const original = f.driver.host.getMockImplementation()!;
    f.driver.host.mockImplementation((id, request) =>
      id === offlineEnrollment.environmentId
        ? Promise.reject(new Error("offline"))
        : original(id, request),
    );
    await advanceFleetCampaigns(f.driver);
    expect(f.state().campaigns[0]?.members.map((member) => member.phase)).toEqual([
      "offline",
      "pending",
    ]);
  });

  it("does not stage or activate after laptop rollback or revoked enrollment", async () => {
    for (const revoked of [false, true]) {
      const f = fixture();
      const state = f.state();
      f.setState({
        ...state,
        enrollments: revoked ? [{ ...enrollment, enabled: false }] : state.enrollments,
        campaigns: state.campaigns.map((campaign) => ({
          ...campaign,
          phase: revoked ? "prepared" : "rolled-back",
        })),
      });
      await advanceFleetCampaigns(f.driver);
      expect(f.driver.host).not.toHaveBeenCalled();
    }
  });

  it("rejects a response for a changed immutable operation binding", async () => {
    const f = fixture(true);
    f.setOperation({
      input: {
        operationId,
        enrollmentId: enrollment.enrollmentId,
        environmentId,
        expectedInstalledSource: installed,
        targetSource: "c".repeat(40),
      },
      phase: "staged",
      currentVersion: "old",
      stagedHandle: "handle",
      continueRunningThreads: false,
    });
    await advanceFleetCampaigns(f.driver);
    expect(f.state().campaigns[0]?.members[0]?.phase).toBe("blocked");
    expect(f.driver.host.mock.calls).toHaveLength(1);
  });
});

it("retires an occupied old stage before driving a newer campaign, including after laptop rollback", async () => {
  const f = fixture();
  const old = f.state().campaigns[0]!;
  const nextId = "55555555-5555-4555-8555-555555555555";
  f.setState({
    ...f.state(),
    campaigns: [
      {
        ...old,
        phase: "rolled-back",
        members: [{ ...old.members[0]!, phase: "retiring", expectedInstalledSource: installed }],
      },
      {
        ...old,
        campaignId: "66666666-6666-4666-8666-666666666666",
        targetSource: "c".repeat(40),
        members: [{ ...old.members[0]!, operationId: nextId, phase: "waiting" }],
      },
    ],
  });
  f.setOperation({
    input: {
      operationId,
      enrollmentId: enrollment.enrollmentId,
      environmentId,
      expectedInstalledSource: installed,
      targetSource: source,
    },
    phase: "staged",
    currentVersion: "old",
    stagedHandle: "old-stage",
    continueRunningThreads: false,
  });
  const original = f.driver.host.getMockImplementation()!;
  f.driver.host.mockImplementation(async (id, request) => {
    if (request.action === "retire") f.setOperation({ ...f.operation()!, phase: "superseded" });
    return original(id, request);
  });
  await advanceFleetCampaigns(f.driver);
  expect(f.state().campaigns[0]?.members[0]?.phase).toBe("superseded");
  expect(f.driver.host.mock.calls.map(([, request]) => request.action)).toEqual([
    "status",
    "retire",
  ]);
  // The next pass observes no operation for the fresh UUID.
  f.driver.host.mockImplementation(async (id, request) => {
    const result = await original(id, request);
    return request.action === "status" && request.operationId === nextId
      ? { ...result, operation: null }
      : result;
  });
  await advanceFleetCampaigns(f.driver);
  expect(f.state().campaigns[1]?.members[0]?.phase).toBe("staged");
  expect(f.operation()?.input.targetSource).toBe("c".repeat(40));
});

it("keeps uncertain native acceptance occupying its host while observing it", async () => {
  const f = fixture(true);
  const old = f.state().campaigns[0]!;
  f.setState({
    ...f.state(),
    campaigns: [
      {
        ...old,
        members: [{ ...old.members[0]!, phase: "reconciling", expectedInstalledSource: installed }],
      },
      {
        ...old,
        campaignId: "66666666-6666-4666-8666-666666666666",
        targetSource: "c".repeat(40),
        members: [
          {
            ...old.members[0]!,
            operationId: "55555555-5555-4555-8555-555555555555",
            phase: "waiting",
          },
        ],
      },
    ],
  });
  f.setOperation({
    input: {
      operationId,
      enrollmentId: enrollment.enrollmentId,
      environmentId,
      expectedInstalledSource: installed,
      targetSource: source,
    },
    phase: "reconciling",
    currentVersion: "old",
    stagedHandle: "old-stage",
    continueRunningThreads: false,
  });
  await advanceFleetCampaigns(f.driver);
  expect(f.driver.host.mock.calls.map(([, request]) => request.action)).toEqual(["status"]);
  expect(f.state().campaigns[1]?.members[0]?.phase).toBe("waiting");
});
