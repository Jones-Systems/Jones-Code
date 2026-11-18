import type {
  PersonalWorkstreamsState,
  PersonalWorkstreamView,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { EnvironmentId, ProjectId } from "@t3tools/contracts";
import { beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { buildSidebarProjectSnapshots } from "../../sidebarProjectGrouping";
import { useStartWorkstreamThread } from "./useStartWorkstreamThread";

const fixtures = vi.hoisted(() => ({
  primaryId: "env-primary",
  projectEnvId: "env-project",
  openScratchProject: vi.fn(),
  handleNewThread: vi.fn(),
}));

vi.mock("@tanstack/react-router", () => ({
  useRouter: () => ({ state: { location: { href: "/" } } }),
}));
vi.mock("../../state/environments", () => ({
  usePrimaryEnvironmentId: () => fixtures.primaryId,
}));
vi.mock("../../state/session", () => ({
  readEnvironmentScope: () => true,
  useEnvironmentsWithScope: () => new Set([fixtures.primaryId, fixtures.projectEnvId]),
}));
vi.mock("../../hooks/useScratchProject", () => ({
  useScratchProject: () => ({
    scratchWorkspaceRootFor: (id: string) => (id === fixtures.primaryId ? "/test/scratch" : null),
    openScratchProject: fixtures.openScratchProject,
  }),
}));

const primaryId = EnvironmentId.make(fixtures.primaryId);
const projectEnvId = EnvironmentId.make(fixtures.projectEnvId);
const project = {
  id: ProjectId.make("fixture-project"),
  environmentId: projectEnvId,
  archivedAt: null,
  title: "Fixture Project",
  workspaceRoot: "/test/project",
  defaultModelSelection: null,
  scripts: [],
  createdAt: "2026-11-12T12:00:00.000Z",
  updatedAt: "2026-11-12T12:00:00.000Z",
};
const [projectGroup] = buildSidebarProjectSnapshots({
  projects: [project],
  settings: { sidebarProjectGroupingMode: "repository", sidebarProjectGroupingOverrides: {} },
  primaryEnvironmentId: primaryId,
  resolveEnvironmentLabel: (id) => id,
});
if (!projectGroup) throw new Error("Expected a project group for the test project.");
const scratch = { id: ProjectId.make("fixture-scratch"), environmentId: primaryId };
const workstream: PersonalWorkstreamView = {
  id: "fixture-workstream",
  name: "Fixture",
  lifecycle: "active",
  color: null,
  orderKey: "a",
  createdAt: "2026-11-12T12:00:00.000Z",
  updatedAt: "2026-11-12T12:00:00.000Z",
};
const state: PersonalWorkstreamsState = {
  workstreams: [workstream],
  environments: [primaryId, projectEnvId].map((environmentId) => ({
    environmentId,
    status: "ready",
    confirmed: true,
    workstreamCount: 1,
  })),
  assignmentsByThreadKey: new Map(),
  pendingThreadKeys: new Set(),
  canCreate: true,
};
const linkedWorkstream: PersonalWorkstreamView = {
  ...workstream,
  nameIcon: { projectKey: projectGroup.projectKey, updatedAt: workstream.updatedAt },
};

describe("Workstream new-thread targets", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    fixtures.openScratchProject.mockResolvedValue(scratch);
    fixtures.handleNewThread.mockResolvedValue(null);
  });

  it("uses the linked project's environment even when the current thread is elsewhere", () => {
    const getAction = useStartWorkstreamThread(
      [projectGroup],
      state,
      primaryId,
      fixtures.handleNewThread,
    );
    const action = getAction(linkedWorkstream);
    if (!("run" in action)) throw new Error(action.disabledReason);
    action.run();

    expect(fixtures.openScratchProject).not.toHaveBeenCalled();
    expect(fixtures.handleNewThread).toHaveBeenCalledExactlyOnceWith(
      { environmentId: projectEnvId, projectId: project.id },
      { workstreamId: workstream.id },
    );
  });

  it("opens a No project draft with the Workstream selected", async () => {
    const getAction = useStartWorkstreamThread(
      [projectGroup],
      state,
      projectEnvId,
      fixtures.handleNewThread,
    );
    const action = getAction(workstream);
    if (!("run" in action)) throw new Error(action.disabledReason);
    action.run();

    expect(fixtures.openScratchProject).toHaveBeenCalledExactlyOnceWith(primaryId);
    await vi.waitFor(() =>
      expect(fixtures.handleNewThread).toHaveBeenCalledExactlyOnceWith(
        { environmentId: primaryId, projectId: scratch.id },
        { workstreamId: workstream.id },
      ),
    );
  });

  it("keeps the selected project when its environment is read only", () => {
    const readOnlyState: PersonalWorkstreamsState = {
      ...state,
      environments: state.environments.map((environment) =>
        environment.environmentId === projectEnvId
          ? { ...environment, status: "read_only" }
          : environment,
      ),
    };
    const getAction = useStartWorkstreamThread(
      [projectGroup],
      readOnlyState,
      primaryId,
      fixtures.handleNewThread,
    );
    expect(getAction(linkedWorkstream)).toEqual({ disabledReason: expect.any(String) });
  });
});
