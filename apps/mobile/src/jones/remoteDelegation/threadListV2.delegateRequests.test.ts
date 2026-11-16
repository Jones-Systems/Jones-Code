import { presentThreadShell } from "@t3tools/client-runtime/state/shell";
import { EnvironmentId, RuntimeRequestId, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import {
  buildThreadListV2Items,
  buildThreadListV2ListItems,
} from "../../features/threads/threadListV2";
import { makeRawThreadShell } from "../../test-fixtures";

const now = "2026-10-27T12:00:00.000Z";
const destination = EnvironmentId.make("destination");

function delegate(kind: "command" | "user_input", settledOverride: "settled" | null) {
  return presentThreadShell(
    destination,
    makeRawThreadShell({
      id: ThreadId.make("revealed-delegate"),
      delegatedFrom: {
        environmentId: EnvironmentId.make("origin"),
        threadId: ThreadId.make("parent"),
        title: "Parent",
      },
      status: "waiting",
      activityRunStatus: "waiting",
      pendingRuntimeRequest: {
        id: RuntimeRequestId.make("request"),
        kind,
        createdAt: DateTime.makeUnsafe(now),
      },
      settledOverride,
      settledAt: settledOverride === null ? null : DateTime.makeUnsafe(now),
    }),
  );
}

describe("mobile list placement of revealed delegates", () => {
  it.each(["command", "user_input"] as const)(
    "lists an actionable %s in the active block",
    (kind) => {
      const thread = delegate(kind, null);
      const layout = buildThreadListV2Items({
        threads: [thread],
        environmentId: null,
        searchQuery: "",
        now,
        workingShelfEnabled: true,
      });
      expect(layout.items.map((item) => [item.thread.id, item.variant, item.snoozed])).toEqual([
        [thread.id, "card", false],
      ]);
      expect(layout.workingCount).toBe(0);
      expect(layout.settledCount).toBe(0);
      expect(
        buildThreadListV2ListItems({ ...layout, pendingTasks: [] }).map((item) => item.type),
      ).toEqual(["v2-thread"]);
    },
  );

  it("keeps an asking settled delegate in the settled shelf", () => {
    const thread = delegate("user_input", "settled");
    const layout = buildThreadListV2Items({
      threads: [thread],
      environmentId: null,
      searchQuery: "",
      now,
      workingShelfEnabled: true,
    });
    expect(layout.items.map((item) => [item.thread.id, item.variant])).toEqual([
      [thread.id, "slim"],
    ]);
    expect(layout.settledCount).toBe(1);
    expect(layout.settledShelfHeaderIndex).toBe(0);
    expect(
      buildThreadListV2ListItems({ ...layout, pendingTasks: [] }).map((item) => item.type),
    ).toEqual(["v2-settled-shelf", "v2-thread"]);
  });
});
