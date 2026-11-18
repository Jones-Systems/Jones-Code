import { EnvironmentId, OrchestrationProjectShell, ProjectId } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { scopeProject, type EnvironmentProject } from "../state/models.ts";
import { buildProjectGroups } from "../state/projectGrouping.ts";

const decodeProjectShell = Schema.decodeUnknownSync(OrchestrationProjectShell);

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

  it.each(["repository", "repository_path"] as const)(
    "distinguishes modern automatic, explicit custom and mixed legacy titles in %s grouping",
    (mode) => {
      const legacy = [project("laptop", "Codex-V3"), project("mini", "Codex-V3")];
      const label = (projects: ReadonlyArray<EnvironmentProject>) =>
        buildProjectGroups({
          projects,
          settings: { sidebarProjectGroupingMode: mode, sidebarProjectGroupingOverrides: {} },
        })[0]?.label;
      const automatic = legacy.map((member) => ({ ...member, hasCustomTitle: false }));
      const mixed = [legacy[0]!, automatic[1]!];
      expect(label(legacy)).toBe("Codex-V3");
      expect(label(automatic)).toBe("Jones-Systems/Codex-V3");
      expect(label(mixed)).toBe("Jones-Systems/Codex-V3");
      expect(label([...mixed].reverse())).toBe("Jones-Systems/Codex-V3");
      for (const members of [automatic, mixed]) {
        const custom = [{ ...members[0]!, hasCustomTitle: true }, members[1]!];
        expect(label(custom)).toBe("Codex-V3");
        expect(label([...custom].reverse())).toBe("Codex-V3");
      }
      expect(label([{ ...legacy[0]!, hasCustomTitle: true }, project("mini", "Other")])).toBe(
        "Jones-Systems/Codex-V3",
      );
    },
  );

  it("preserves absent title provenance through wire decoding and environment scoping", () => {
    const legacy = project("laptop", "Codex-V3");
    for (const marker of [undefined, false, true]) {
      const wire = { ...legacy, ...(marker === undefined ? {} : { hasCustomTitle: marker }) };
      const decoded = decodeProjectShell(JSON.parse(JSON.stringify(wire)));
      const scoped = scopeProject(legacy.environmentId, decoded);
      expect(decoded.hasCustomTitle).toBe(marker);
      expect(scoped.hasCustomTitle).toBe(marker);
      if (marker === undefined) {
        expect(Object.hasOwn(decoded, "hasCustomTitle")).toBe(false);
        expect(Object.hasOwn(scoped, "hasCustomTitle")).toBe(false);
      }
      const groups = buildProjectGroups({
        projects: [scoped, { ...scoped, environmentId: EnvironmentId.make("mini") }],
        settings: { sidebarProjectGroupingMode: "repository", sidebarProjectGroupingOverrides: {} },
      });
      expect(groups[0]?.label).toBe(marker === false ? "Jones-Systems/Codex-V3" : "Codex-V3");
    }
  });
});
