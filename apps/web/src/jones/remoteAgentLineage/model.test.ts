import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId, ThreadId, type OrchestrationV2Subagent } from "@t3tools/contracts";
import type { ThreadRelationshipWalkRow } from "@t3tools/client-runtime/state/thread-relationships";
import {
  groupLineageRows,
  resolveMachineIdentity,
  resolveParentRef,
  type ParentSource,
} from "./model";

const environmentId = EnvironmentId.make("local");
const threadId = ThreadId.make("parent");
const remote = (status: string, stoppedThere: boolean): OrchestrationV2Subagent =>
  ({
    id: `remote-${status}`,
    childThreadId: null,
    status,
    remoteChild: {
      environmentId: "other",
      threadId: "equal-id",
      label: "Test Server",
      stoppedThere,
    },
  }) as OrchestrationV2Subagent;
const local = (status: string): ThreadRelationshipWalkRow =>
  ({
    threadId: "child",
    edge: { kind: "subagent", sourceThreadId: threadId, targetThreadId: "child", status },
  }) as ThreadRelationshipWalkRow;

describe("machine identity", () => {
  it.each([
    [" VPS ", "vps", "VPS"],
    ["TEST_server", "test", "TSV"],
    ["mac-mini", "mini", "MINI"],
    ["Laptop", "laptop", "LAP"],
  ])("resolves exact normalized %s", (label, identity, shortLabel) => {
    expect(resolveMachineIdentity(label)).toMatchObject({ identity, shortLabel });
  });
  it("uses only laptop and Mini kinds after exact aliases", () => {
    expect(resolveMachineIdentity("Personal", "laptop").identity).toBe("laptop");
    expect(resolveMachineIdentity("Personal", "mac-mini").identity).toBe("mini");
    expect(resolveMachineIdentity("VPS", "laptop").identity).toBe("vps");
    for (const label of [
      "VPS backup",
      "https://vps.example",
      "private-environment-id",
      "constructor",
      "My project",
    ]) {
      expect(resolveMachineIdentity(label, "server")).toMatchObject({
        identity: "unknown",
        shortLabel: label,
      });
    }
    for (const kind of ["server", "cloud", "linux"] as const)
      expect(resolveMachineIdentity("Other", kind).identity).toBe("unknown");
  });
});

describe("scoped parent", () => {
  const thread = {
    lineage: { relationshipToParent: null },
    delegatedFrom: { environmentId: "other", threadId: "equal-id", title: "Delegated title" },
    linkOrigin: { label: "VPS" },
  } as ParentSource;
  it("retains the remote scope and title even without a cached shell", () => {
    expect(resolveParentRef(environmentId, thread)).toMatchObject({
      remote: true,
      ref: { environmentId: "other", threadId: "equal-id" },
      fallbackTitle: "Delegated title",
    });
  });
  it("gives the local parent precedence even with the same bare ID", () => {
    expect(
      resolveParentRef(environmentId, {
        ...thread,
        lineage: {
          ...thread.lineage,
          relationshipToParent: "subagent",
          parentThreadId: ThreadId.make("equal-id"),
        },
      }),
    ).toMatchObject({ remote: false, ref: { environmentId: "local", threadId: "equal-id" } });
    expect(resolveParentRef(environmentId, null)).toBeNull();
  });
});

describe("combined lineage", () => {
  it.each(["completed", "failed", "error", "cancelled", "interrupted", "idle"])(
    "places local and remote %s into the same history, regardless of stop acknowledgement",
    (status) => {
      for (const stoppedThere of [true, false]) {
        const grouped = groupLineageRows([local(status)], [remote(status, stoppedThere)], threadId);
        expect(grouped.previous).toHaveLength(2);
        expect(grouped.active).toHaveLength(0);
        expect(grouped.runningCount).toBe(0);
      }
    },
  );
  it.each(["pending", "running", "waiting"])(
    "keeps %s current even with stoppedThere",
    (status) => {
      const grouped = groupLineageRows([local(status)], [remote(status, true)], threadId);
      expect(grouped.previous).toHaveLength(0);
      expect(grouped.active).toHaveLength(2);
      expect(grouped.runningCount).toBe(status === "running" ? 2 : 0);
    },
  );
  it("counts remote childless tasks once and moves resumed history current", () => {
    const historical = remote("completed", true);
    expect(groupLineageRows([], [historical], threadId).previous).toHaveLength(1);
    const resumed = { ...historical, status: "running" } as OrchestrationV2Subagent;
    const unlinked = {
      ...resumed,
      id: "unlinked",
      remoteChild: undefined,
    } as unknown as OrchestrationV2Subagent;
    const grouped = groupLineageRows([local("running")], [resumed, unlinked], threadId);
    expect(grouped.active).toHaveLength(2);
    expect(grouped.previous).toHaveLength(0);
    expect(grouped.runningCount).toBe(3);
  });
});
