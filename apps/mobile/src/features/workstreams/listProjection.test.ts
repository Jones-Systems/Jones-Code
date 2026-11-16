import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { describe, expect, it } from "vite-plus/test";

import { makeThreadShellFixture } from "../../test-fixtures";
import { buildThreadListV2Items, buildThreadListV2ListItems } from "../threads/threadListV2";
import { projectMobileWorkstreamList } from "./listProjection";

const NOW = "2026-06-02T00:00:00.000Z";
const environmentId = EnvironmentId.make("environment-1");
const thread = (id: string) =>
  makeThreadShellFixture({ environmentId, id: ThreadId.make(id), title: id });
const key = (item: EnvironmentThreadShell) => JSON.stringify([item.environmentId, item.id]);
const rows = (threads: readonly EnvironmentThreadShell[]) =>
  buildThreadListV2ListItems({
    ...buildThreadListV2Items({ threads, environmentId: null, searchQuery: "", now: NOW }),
    pendingTasks: [],
  });

describe("mobile Workstream Unassigned labeling", () => {
  const grouped = thread("grouped");
  const confirmed = thread("confirmed");
  const unknown = thread("unknown");
  const projection = {
    enabled: true,
    groups: [
      { key: "delivery", name: "Delivery", color: "#123456", threadKeys: new Set([key(grouped)]) },
    ],
    collapsedKeys: new Set<string>(),
    secondaryLabelsByKey: new Map<string, readonly string[]>(),
  };
  const headers = (items: ReturnType<typeof projectMobileWorkstreamList>) =>
    items.flatMap((item) => (item.type === "v2-workstream" ? [[item.name, item.count]] : []));

  it("labels only index-confirmed rows Unassigned and the rest Assignment unknown", () => {
    expect(
      headers(
        projectMobileWorkstreamList(rows([grouped, confirmed, unknown]), {
          ...projection,
          confirmedUnassignedKeys: new Set([key(confirmed)]),
        }),
      ),
    ).toEqual([
      ["Delivery", 1],
      ["Unassigned", 1],
      ["Assignment unknown", 1],
    ]);
  });

  it("never shows confirmed Unassigned without an index, and keeps grouped rows visible", () => {
    expect(
      headers(
        projectMobileWorkstreamList(rows([grouped, confirmed, unknown]), {
          ...projection,
          confirmedUnassignedKeys: new Set(),
        }),
      ),
    ).toEqual([
      ["Delivery", 1],
      ["Assignment unknown", 2],
    ]);
  });

  it("keeps the single Unassigned group for callers without assignment evidence", () => {
    expect(headers(projectMobileWorkstreamList(rows([grouped, unknown]), projection))).toEqual([
      ["Delivery", 1],
      ["Unassigned", 1],
    ]);
  });
});
