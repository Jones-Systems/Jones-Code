import { EnvironmentId, ProjectId, type SidebarProjectGroupingMode } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import {
  buildSidebarProjectSnapshots,
  buildSidebarProjectPickerEntries,
} from "../../sidebarProjectGrouping";
import type { Project } from "../../types";
import { legacyProjectCwdPreferenceKey } from "../../uiStateStore";
import { groupPreferenceIds, mergeVisibleOrder, orderProjectGroups } from "./projectOrder";

function project(id: string, environment = "local", overrides: Partial<Project> = {}): Project {
  return {
    id: ProjectId.make(id),
    environmentId: EnvironmentId.make(environment),
    title: id,
    workspaceRoot: `/${environment}/${id}`,
    repositoryIdentity: {
      canonicalKey: `github.com/example/${id}`,
      locator: {
        source: "git-remote",
        remoteName: "origin",
        remoteUrl: `https://github.com/example/${id}.git`,
      },
      provider: "github",
      owner: "example",
      name: id,
      displayName: id,
      rootPath: `/${environment}/${id}`,
    },
    defaultModelSelection: null,
    scripts: [],
    createdAt: "2026-01-01T00:00:00Z",
    updatedAt: "2026-01-01T00:00:00Z",
    ...overrides,
  };
}
function groups(
  projects: Project[],
  mode: SidebarProjectGroupingMode = "repository",
  overrides: Record<string, SidebarProjectGroupingMode> = {},
) {
  return buildSidebarProjectSnapshots({
    projects,
    groupScratchProjects: true,
    settings: { sidebarProjectGroupingMode: mode, sidebarProjectGroupingOverrides: overrides },
    primaryEnvironmentId: EnvironmentId.make("local"),
    resolveEnvironmentLabel: (id) => id,
  });
}
const ordered = (
  items: ReturnType<typeof groups>,
  projectOrder: string[],
  sortOrder: "manual" | "updated_at" | "created_at" = "manual",
) =>
  orderProjectGroups({
    groups: items,
    threads: [],
    projectOrder,
    sortOrder,
    delegatedUnderParent: new Set(),
  });
const names = (items: ReturnType<typeof groups>) => items.map((item) => item.title);

describe("logical project presentation order", () => {
  it.each(["repository", "repository_path", "separate"] as const)(
    "keeps logical order after grouping changes to %s",
    (mode) => {
      const items = groups([project("A"), project("B"), project("B", "remote")], mode);
      expect(names(ordered(items, ["github.com/example/B", "github.com/example/A"]))).toEqual(
        mode === "separate" ? ["B", "B", "A"] : ["B", "A"],
      );
    },
  );
  it("honors per-project overrides and old per-environment or legacy cwd keys", () => {
    const projects = [project("A"), project("B", "remote")];
    const items = groups(projects, "repository", { "remote:/remote/B": "separate" });
    for (const key of [
      "remote:/remote/B",
      legacyProjectCwdPreferenceKey("/remote/B"),
      "github.com/example/B",
    ]) {
      expect(names(ordered(items, [key]))).toEqual(["B", "A"]);
    }
  });
  it("keeps repository-path orders when switching back to repository grouping", () => {
    const child = project("B", "remote", { workspaceRoot: "/remote/B/subdir" });
    const pathGroups = groups([project("A"), child], "repository_path");
    const key = pathGroups.find((group) => group.title === "B")!.projectKey;
    expect(key).toBe("github.com/example/B::subdir");
    expect(names(ordered(groups([project("A"), child]), [key]))).toEqual(["B", "A"]);
  });
  it("uses activity order for empty manual order and ignores saved order outside manual", () => {
    const items = groups([
      project("A"),
      project("B", "local", { updatedAt: "2026-02-01T00:00:00Z" }),
    ]);
    expect(names(ordered(items, []))).toEqual(["B", "A"]);
    expect(names(ordered(items, ["github.com/example/A"], "updated_at"))).toEqual(["B", "A"]);
    expect(names(ordered(items, ["github.com/example/A"]))).toEqual(["A", "B"]);
  });
  it("honors explicit root and subdirectory positions over shared repository aliases", () => {
    const root = project("A");
    const child = project("A-child", "local", {
      workspaceRoot: "/local/A/subdir",
      repositoryIdentity: root.repositoryIdentity,
    });
    const items = groups([root, child, project("B")], "repository_path");
    expect(
      ordered(items, [
        "github.com/example/A",
        "github.com/example/B",
        "github.com/example/A::subdir",
      ]).map((group) => group.projectKey),
    ).toEqual(["github.com/example/A", "github.com/example/B", "github.com/example/A::subdir"]);
  });

  it("places new projects last without changing a group's primary target", () => {
    const items = groups([project("A", "remote"), project("A"), project("B"), project("C")]);
    const result = ordered(items, ["github.com/example/B", "github.com/example/A"]);
    expect(names(result)).toEqual(["B", "A", "C"]);
    expect(result[1]!.environmentId).toBe("local");
  });
  it("keeps scratch placement and current-context promotion in picker entries", () => {
    const items = groups([
      project("A"),
      project("scratch", "local", { isScratch: true }),
      project("B"),
    ]);
    const baseline = ordered(items, []);
    const result = ordered(items, ["github.com/example/B", "github.com/example/A"]);
    expect(result.findIndex((item) => item.isScratch)).toBe(
      baseline.findIndex((item) => item.isScratch),
    );
    const entries = buildSidebarProjectPickerEntries({
      groups: result,
      preferredProjectRef: {
        environmentId: EnvironmentId.make("local"),
        projectId: ProjectId.make("A"),
      },
    });
    expect(entries[0]!.group.title).toBe("A");
  });
  it("retains missing and loading projects' slots through reorder and reconnect", () => {
    const all = groups([project("A"), project("B", "remote"), project("C")]);
    const visible = all.filter((item) => item.title !== "B");
    const key = (name: string) => `github.com/example/${name}`;
    const stored = mergeVisibleOrder([key("A"), key("B"), key("C")], [key("C"), key("A")], (id) =>
      groupPreferenceIds(visible.find((group) => group.projectKey === id)!),
    );
    expect(stored).toEqual([key("C"), key("B"), key("A")]);
    expect(names(ordered(all, stored))).toEqual(["C", "B", "A"]);
  });
  it("replaces duplicate checkout aliases with logical keys and preserves unknown keys", () => {
    const items = groups([project("A"), project("A", "remote"), project("B")]);
    const keys = ["github.com/example/B", "github.com/example/A"];
    expect(
      mergeVisibleOrder(
        ["local:/local/A", "remote:/remote/A", "offline", "local:/local/B"],
        keys,
        (key) => groupPreferenceIds(items.find((item) => item.projectKey === key)!),
      ),
    ).toEqual([keys[0], "offline", keys[1]]);
    expect(mergeVisibleOrder(["offline"], keys)).toEqual(["offline", ...keys]);
  });
});
