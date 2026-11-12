import { describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type {
  FleetDesktopState,
  FleetHostStatus,
  FleetHostOperation,
} from "@t3tools/contracts/jones/fleet-updates";
import { advanceFleetCampaigns, type FleetCampaignDriver, type FleetPublicationReport } from "./campaignController.ts";

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
      installedSource: {
        repository: "Jones-Systems/Jones-Code",
        sha: installed,
        tree: "c".repeat(40),
      },
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
                members: entry.members.map((member) => {
                  if (member !== old) return member;
                  const { reason: _reason, ...previous } = member;
                  return {
                    ...previous,
                    phase: request.input.phase,
                    ...(request.input.reason === undefined ? {} : { reason: request.input.reason }),
                    ...(request.input.expectedInstalledSource === undefined
                      ? {}
                      : { expectedInstalledSource: request.input.expectedInstalledSource }),
                  };
                }),
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
    expect(f.driver.host.mock.calls.map(([, request]) => request.action)).toEqual(["status", "status"]);
    f.setOperation({ ...operation, phase: "committed" });
    await advanceFleetCampaigns(f.driver);
    expect(f.state().campaigns[0]?.members[0]?.phase).toBe("committed");
  });

  it("does not stage or activate before the laptop commit and generation are proved", async () => {
    const f = fixture();
    await advanceFleetCampaigns(f.driver);
    expect(f.operation()).toBeNull();
    expect(f.state().campaigns[0]?.members[0]?.phase).toBe("waiting");
    expect(f.state().campaigns[0]?.members[0]?.reason).toBe(
      "Waits for the laptop update to commit.",
    );
    expect(
      f.driver.host.mock.calls.filter(([, input]) => input.action === "activate"),
    ).toHaveLength(0);
    expect(f.driver.host.mock.calls.filter(([, input]) => input.action === "stage")).toHaveLength(
      0,
    );
    const state = f.state();
    f.setState({
      ...state,
      campaigns: state.campaigns.map((c) => ({ ...c, committedGeneration: "native-generation" })),
    });
    await advanceFleetCampaigns(f.driver);
    expect(f.operation()).toBeNull();
    expect(f.state().campaigns[0]?.members[0]?.reason).toBe(
      "Waits for the laptop update to commit.",
    );
    f.setState({
      ...state,
      campaigns: state.campaigns.map((c) => {
        const { committedGeneration: _generation, ...unproved } = c;
        return { ...unproved, phase: "committed" };
      }),
    });
    await advanceFleetCampaigns(f.driver);
    expect(f.operation()).toBeNull();
    expect(f.state().campaigns[0]?.members[0]?.reason).toBe(
      "Waits for the laptop update to commit.",
    );
    expect(f.driver.host.mock.calls.map(([, input]) => input.action)).toEqual(
      Array.from({ length: 6 }, () => "status"),
    );
    f.setState({
      ...f.state(),
      campaigns: f.state().campaigns.map((c) => ({
        ...c,
        phase: "committed",
        committedGeneration: "native-generation",
      })),
    });
    await advanceFleetCampaigns(f.driver);
    expect(f.operation()?.input.targetSource).toBe(source);
    expect(f.driver.host.mock.calls.map(([, input]) => input.action)).toEqual([
      ...Array.from({ length: 8 }, () => "status"),
      "stage",
      "activate",
    ]);
    expect(f.state().campaigns[0]?.members[0]?.reason).toBeUndefined();
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
      expect(f.driver.host.mock.calls.map(([, request]) => request.action)).toEqual(
        revoked ? [] : ["status"],
      );
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
    expect(f.driver.host.mock.calls).toHaveLength(2);
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
        phase: "committed",
        committedGeneration: "new-native-generation",
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
  expect(f.state().campaigns[1]?.members[0]?.phase).toBe("pending");
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
  expect(f.driver.host.mock.calls.map(([, request]) => request.action)).toEqual(["status", "status"]);
  expect(f.state().campaigns[1]?.members[0]?.phase).toBe("waiting");
});

describe("hosts that follow the laptop themselves", () => {
  const laptopSource = "d".repeat(40);
  const laptop = {
    repository: "Jones-Systems/Jones-Code",
    sha: laptopSource,
    tree: "e".repeat(40),
  };
  const followerHost = (f: ReturnType<typeof fixture>, leaderSource: string | null) => {
    const legacy = f.driver.host.getMockImplementation()!;
    const leaders: string[] = [];
    f.driver.host.mockImplementation(async (id, request) => {
      if (request.action === "leader") {
        // The host checks this provenance against the repository before following it.
        expect(request.input).toMatchObject({
          repository: laptop.repository,
          committedTree: laptop.tree,
        });
        leaders.push(request.input.committedSource);
        leaderSource = request.input.committedSource;
      }
      const status = await legacy(id, request);
      return { ...status, follower: { protocol: 1, leaderSource, state: "preparing" } };
    });
    return leaders;
  };

  it("publishes the laptop's committed source and stops driving campaigns for them", async () => {
    const f = fixture(true);
    const leaders = followerHost(f, null);
    await advanceFleetCampaigns({ ...f.driver, committedSource: async () => laptop });
    expect(leaders).toEqual([laptopSource]);
    expect(f.state().campaigns[0]!.members[0]).toMatchObject({ phase: "superseded" });
    expect(f.driver.host.mock.calls.map(([, request]) => request.action)).not.toContain("stage");
    expect(f.driver.host.mock.calls.map(([, request]) => request.action)).not.toContain("activate");
    await advanceFleetCampaigns({ ...f.driver, committedSource: async () => laptop });
    expect(leaders).toEqual([laptopSource]);
  });

  it("keeps settling an activation the campaign already dispatched", async () => {
    const f = fixture(true);
    followerHost(f, laptopSource);
    f.setState({
      ...f.state(),
      campaigns: f.state().campaigns.map((campaign) => ({
        ...campaign,
        members: campaign.members.map((member) => ({ ...member, phase: "reconciling" as const })),
      })),
    });
    f.setOperation({
      input: {
        operationId,
        enrollmentId: enrollment.enrollmentId,
        environmentId,
        targetSource: source,
        expectedInstalledSource: installed,
      },
      phase: "committed",
      continueRunningThreads: false,
      currentVersion: "preview-old",
      stagedHandle: "server-handle",
    });
    await advanceFleetCampaigns({ ...f.driver, committedSource: async () => laptop });
    expect(f.state().campaigns[0]!.members[0]!.phase).toBe("committed");
  });

  it("keeps campaigns when the laptop cannot prove its committed source or the host is older", async () => {
    const unproven = fixture(true);
    const published = followerHost(unproven, null);
    await advanceFleetCampaigns({ ...unproven.driver, committedSource: async () => undefined });
    for (const malformed of [
      { ...laptop, repository: "someone/else" },
      { ...laptop, tree: "not-a-tree" },
    ])
      await advanceFleetCampaigns({ ...unproven.driver, committedSource: async () => malformed });
    expect(published).toEqual([]);
    expect(unproven.operation()?.phase).toBe("pending");
    const older = fixture(true);
    await advanceFleetCampaigns({ ...older.driver, committedSource: async () => laptop });
    expect(older.operation()?.phase).toBe("pending");
    expect(older.driver.host.mock.calls.map(([, request]) => request.action)).not.toContain(
      "leader",
    );
  });
});

describe("publication acknowledgement boundaries", () => {
  const laptop = {
    repository: "Jones-Systems/Jones-Code",
    sha: "d".repeat(40),
    tree: "e".repeat(40),
  };
  const directive = {
    directiveId: "44444444-4444-4444-8444-444444444444",
    targetSource: source,
    targetTree: laptop.tree,
    mode: "manual-review" as const,
    issuedAt: "2026-11-12T11:00:00.000Z",
  };
  function followingFixture() {
    const f = fixture(true);
    const legacy = f.driver.host.getMockImplementation()!;
    let follower: NonNullable<FleetHostStatus["follower"]> = {
      protocol: 1,
      leaderSource: installed,
      leaderPublishedAt: "2026-11-11T11:00:00.000Z",
      state: "current",
    };
    const respond: FleetCampaignDriver["host"] = async (id, request) => {
      if (request.action === "leader") follower = {
        ...follower,
        leaderSource: request.input.committedSource,
        leaderPublishedAt: "2026-11-12T11:00:00.000Z",
      };
      return { ...await legacy(id, request), follower };
    };
    f.driver.host.mockImplementation(respond);
    return { ...f, respond, setFollower: (next: typeof follower) => { follower = next; } };
  }
  const actions = (f: ReturnType<typeof fixture>) =>
    f.driver.host.mock.calls.map(([, request]) => request.action);

  it.each(["environment", "enrollment", "disabled", "protocol", "source"] as const)(
    "holds an invalid %s leader response until authoritative readback",
    async (invalid) => {
      const f = followingFixture();
      f.driver.host.mockImplementation(async (id, request) => {
        const status = await f.respond(id, request);
        if (request.action !== "leader") return status;
        switch (invalid) {
          case "environment": return { ...status, environmentId: EnvironmentId.make("other") };
          case "enrollment": return { ...status, enrollment: { ...enrollment, enrollmentId: directive.directiveId } };
          case "disabled": return { ...status, enrollment: { ...enrollment, enabled: false } };
          case "protocol": return { ...status, follower: undefined } as unknown as FleetHostStatus;
          case "source": return { ...status, follower: { ...status.follower!, leaderSource: installed } };
        }
      });
      const report = vi.fn<(value: FleetPublicationReport) => void>();
      const driver = { ...f.driver, committedSource: async () => laptop, onPublication: report };
      await advanceFleetCampaigns(driver);
      expect(f.state().campaigns[0]!.members[0]!.phase).toBe("waiting");
      expect(actions(f)).toEqual(["status", "leader", "status"]);
      expect(report.mock.calls[0]?.[0].hosts[0]?.outcome).toBe("unknown");
      await advanceFleetCampaigns(driver);
      expect(f.state().campaigns[0]!.members[0]!.phase).toBe("superseded");
      expect(actions(f).filter((action) => action === "leader")).toHaveLength(1);
    },
  );

  it.each(["environment", "enrollment", "disabled", "protocol"] as const)(
    "does not publish or supersede from an invalid prior %s status", async (invalid) => {
      const f = followingFixture();
      f.driver.host.mockImplementation(async (id, request) => {
        const status = await f.respond(id, request);
        if (invalid === "environment") return { ...status, environmentId: EnvironmentId.make("other") };
        if (invalid === "enrollment") return { ...status, enrollment: { ...enrollment, enrollmentId: directive.directiveId } };
        if (invalid === "disabled") return { ...status, enrollment: { ...enrollment, enabled: false } };
        return { ...status, follower: { ...status.follower, protocol: 2 } } as unknown as FleetHostStatus;
      });
      await advanceFleetCampaigns({ ...f.driver, committedSource: async () => laptop });
      expect(actions(f).every((action) => action === "status")).toBe(true);
      expect(f.state().campaigns[0]!.members[0]!.phase).not.toBe("superseded");
    },
  );

  it("holds a lost acknowledgement, then observes the committed receipt without replaying publication", async () => {
    const f = followingFixture();
    f.driver.host.mockImplementation(async (id, request) => {
      const status = await f.respond(id, request);
      if (request.action === "leader") throw new Error("response lost after commit");
      return status;
    });
    const driver = { ...f.driver, committedSource: async () => laptop };
    await advanceFleetCampaigns(driver);
    expect(f.state().campaigns[0]!.members[0]!.phase).toBe("waiting");
    expect(actions(f)).toEqual(["status", "leader", "status"]);
    await advanceFleetCampaigns(driver);
    expect(f.state().campaigns[0]!.members[0]!.phase).toBe("superseded");
    expect(actions(f).filter((action) => action === "leader")).toHaveLength(1);
  });

  it("holds campaigns under a directive while accepted operations still reconcile", async () => {
    const f = followingFixture();
    f.setFollower({ protocol: 1, leaderSource: laptop.sha, state: "waiting-for-leader", planDirective: directive });
    const report = vi.fn<(value: FleetPublicationReport) => void>();
    const driver = { ...f.driver, committedSource: async () => laptop, onPublication: report };
    await advanceFleetCampaigns(driver);
    expect(f.state().campaigns[0]!.members[0]!.phase).toBe("waiting");
    expect(report.mock.calls[0]?.[0].hosts[0]?.outcome).toBe("directive-held");
    f.setState({ ...f.state(), campaigns: f.state().campaigns.map((campaign) => ({
      ...campaign, members: campaign.members.map((member) => ({ ...member, phase: "reconciling" as const })),
    })) });
    f.setOperation({
      input: { operationId, enrollmentId: enrollment.enrollmentId, environmentId,
        targetSource: source, expectedInstalledSource: installed },
      phase: "committed", continueRunningThreads: false, currentVersion: "old",
    });
    await advanceFleetCampaigns(driver);
    expect(f.state().campaigns[0]!.members[0]!.phase).toBe("committed");
    expect(actions(f).every((action) => action === "status")).toBe(true);
  });

  it.each(["source-read", "status", "leader"] as const)("rechecks cancellation after %s", async (at) => {
    const f = followingFixture();
    let cancelled = false;
    const report = vi.fn();
    f.driver.host.mockImplementation(async (id, request) => {
      const status = await f.respond(id, request);
      if (request.action === at) cancelled = true;
      return status;
    });
    await advanceFleetCampaigns({ ...f.driver, cancelled: () => cancelled, onPublication: report,
      committedSource: async () => {
        if (at === "source-read") cancelled = true;
        return laptop;
      },
    });
    expect(f.state().campaigns[0]!.members[0]!.phase).toBe("waiting");
    expect(actions(f)).toEqual(at === "source-read" ? [] : at === "status" ? ["status"] : ["status", "leader"]);
    expect(report).not.toHaveBeenCalled();
  });

  it("discards an acknowledgement for an enrollment replaced while publication was in flight", async () => {
    const f = followingFixture();
    const report = vi.fn<(value: FleetPublicationReport) => void>();
    f.driver.host.mockImplementation(async (id, request) => {
      const status = await f.respond(id, request);
      if (request.action === "leader") f.setState({ ...f.state(), enrollments: [{ ...enrollment, enrollmentId: directive.directiveId }] });
      return status;
    });
    await advanceFleetCampaigns({ ...f.driver, committedSource: async () => laptop, onPublication: report });
    expect(f.state().campaigns[0]!.members[0]!.phase).toBe("waiting");
    expect(report.mock.calls[0]?.[0].hosts).toEqual([]);
    expect(actions(f)).toEqual(["status", "leader"]);
  });

  it("does not send a leader request after local enrollment is revoked during status observation", async () => {
    const f = followingFixture();
    f.driver.host.mockImplementation(async (id, request) => {
      const status = await f.respond(id, request);
      f.setState({ ...f.state(), enrollments: [{ ...enrollment, enabled: false }] });
      return status;
    });
    await advanceFleetCampaigns({ ...f.driver, committedSource: async () => laptop });
    expect(actions(f)).toEqual(["status"]);
    expect(f.state().campaigns[0]!.members[0]!.phase).toBe("waiting");
  });

  it("does not dispatch after cancellation while the durable member update was awaited", async () => {
    const f = fixture(true);
    const desktop = f.driver.desktop.getMockImplementation()!;
    let cancelled = false;
    f.driver.desktop.mockImplementation(async (request) => {
      const state = await desktop(request);
      if (request.action === "updateMember" && request.input.phase === "staging") cancelled = true;
      return state;
    });
    await advanceFleetCampaigns({ ...f.driver, cancelled: () => cancelled });
    expect(actions(f)).toEqual(["status", "status"]);
    expect(f.operation()).toBeNull();
  });

  it("does not publish an old source when native proof changes during the status read", async () => {
    const f = followingFixture();
    let latest = laptop;
    f.driver.host.mockImplementation(async (id, request) => {
      const status = await f.respond(id, request);
      latest = { ...laptop, sha: "f".repeat(40) };
      return status;
    });
    const report = vi.fn();
    await advanceFleetCampaigns({ ...f.driver, committedSource: async () => latest, onPublication: report });
    expect(actions(f)).toEqual(["status", "status"]);
    expect(f.state().campaigns[0]!.members[0]!.phase).toBe("waiting");
    expect(report).toHaveBeenCalledWith({ publication: { kind: "committed", source: latest }, hosts: [] });
  });

  it("always observes enabled hosts when native publication is withheld, including an offline host", async () => {
    const f = followingFixture();
    const offline = { ...enrollment, environmentId: EnvironmentId.make("offline-host") };
    f.setState({ ...f.state(), enrollments: [enrollment, offline], campaigns: [] });
    f.driver.host.mockImplementation((id, request) => id === offline.environmentId
      ? Promise.reject(new Error("offline")) : f.respond(id, request));
    const report = vi.fn<(value: FleetPublicationReport) => void>();
    const publication = { kind: "withheld", code: "plan-managed", message: "Plan owns source." } as const;
    await advanceFleetCampaigns({ ...f.driver, publication: async () => publication, onPublication: report });
    expect(actions(f)).toEqual(["status", "status"]);
    expect(report.mock.calls[0]?.[0]).toMatchObject({ publication, hosts: [
      { enrollment, outcome: "observed", follower: { leaderSource: installed, leaderPublishedAt: "2026-11-11T11:00:00.000Z" } },
      { enrollment: offline, outcome: "unknown" },
    ] });
  });

  it.each(["throw", "reject"])("does not let a diagnostic %s change a confirmed handoff", async (failure) => {
    const f = followingFixture();
    await advanceFleetCampaigns({ ...f.driver, committedSource: async () => laptop,
      onPublication: () => {
        if (failure === "reject") return Promise.reject(new Error("diagnostic failed"));
        throw new Error("diagnostic failed");
      },
    });
    expect(f.state().campaigns[0]!.members[0]!.phase).toBe("superseded");
    expect(actions(f)).toEqual(["status", "leader"]);
  });

  it("does not let an older concurrent pass replace a newer source report", async () => {
    const f = followingFixture();
    let release!: (status: FleetHostStatus) => void;
    let captured!: () => void;
    const waiting = new Promise<void>((resolve) => { captured = resolve; });
    const held = new Promise<FleetHostStatus>((resolve) => { release = resolve; });
    f.driver.host.mockImplementationOnce(async () => { captured(); return held; });
    const oldReport = vi.fn();
    const newReport = vi.fn<(value: FleetPublicationReport) => void>();
    const first = advanceFleetCampaigns({ ...f.driver, committedSource: async () => laptop, onPublication: oldReport });
    await waiting;
    const newer = { ...laptop, sha: "f".repeat(40) };
    await advanceFleetCampaigns({ ...f.driver, committedSource: async () => newer, onPublication: newReport });
    release(await f.respond(environmentId, { action: "status" }));
    await first;
    expect(oldReport).not.toHaveBeenCalled();
    expect(newReport.mock.calls[0]?.[0].publication).toMatchObject({ source: { sha: newer.sha } });
    expect(actions(f).filter((action) => action === "leader")).toHaveLength(1);
  });
});
