import { EnvironmentId, ProjectId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import type { EnvironmentProject } from "../state/models.ts";
import { buildProjectGroups } from "../state/projectGrouping.ts";

const repositoryIdentity = {
  canonicalKey: "github.com/jones-systems/codex-v3",
  locator: {
    source: "git-remote" as const,
    remoteName: "origin",
    remoteUrl: "https://github.com/Jones-Systems/Codex-V3.git",
  },
  name: "Codex-V3",
  displayName: "Jones-Systems/Codex-V3",
};

function project(machine: string, title: string): EnvironmentProject {
  return {
    environmentId: EnvironmentId.make(machine),
    id: ProjectId.make(machine),
    title,
    workspaceRoot: `/work/${machine}/Codex-V3`,
    repositoryIdentity,
    defaultModelSelection: null,
    scripts: [],
    createdAt: "2026-10-25T00:00:00.000Z",
    updatedAt: "2026-10-25T00:00:00.000Z",
  };
}

describe("Jones grouped project names", () => {
  it.each(["repository", "repository_path"] as const)(
    "honors the saved repository name in %s grouping after shell reload",
    (mode) => {
      const projects = [project("laptop", "Codex-V3"), project("mini", "Codex-V3")];
      const input = {
        projects,
        settings: { sidebarProjectGroupingMode: mode, sidebarProjectGroupingOverrides: {} },
      };

      for (const reloadedProjects of [projects, structuredClone(projects)]) {
        const groups = buildProjectGroups({ ...input, projects: reloadedProjects });
        expect(groups).toHaveLength(1);
        expect(groups[0]?.label).toBe("Codex-V3");
        expect(groups[0]?.key).toBe(repositoryIdentity.canonicalKey);
        expect(groups[0]?.memberProjectRefs).toEqual([
          { environmentId: "laptop", projectId: "laptop" },
          { environmentId: "mini", projectId: "mini" },
        ]);
      }
    },
  );

  it("follows a rename back to the repository name without changing group identity", () => {
    const projects = [project("laptop", "Work"), project("mini", "Work")];
    const settings = {
      sidebarProjectGroupingMode: "repository" as const,
      sidebarProjectGroupingOverrides: {},
    };
    const before = buildProjectGroups({ projects, settings })[0]!;
    const renamed = buildProjectGroups({
      projects: projects.map((member) => ({ ...member, title: "Codex-V3" })),
      settings,
    })[0]!;
    expect(before.label).toBe("Work");
    expect(renamed.label).toBe("Codex-V3");
    expect(renamed.key).toBe(before.key);
    expect(renamed.memberProjectRefs).toEqual(before.memberProjectRefs);
  });

  it("keeps the repository fallback while member titles disagree", () => {
    const groups = buildProjectGroups({
      projects: [project("laptop", "Work"), project("mini", "Codex-V3")],
      settings: { sidebarProjectGroupingMode: "repository", sidebarProjectGroupingOverrides: {} },
    });
    expect(groups[0]?.label).toBe("Jones-Systems/Codex-V3");
  });
});
