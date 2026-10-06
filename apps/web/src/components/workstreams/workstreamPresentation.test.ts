import { describe, expect, it } from "vite-plus/test";
import {
  getReceiptSummary,
  getWorkstreamMemberActions,
  pageWindow,
  planWorkstreamDrop,
} from "./workstreamPresentation";

describe("workstream member actions", () => {
  it("keeps removal separate from native settlement and provides every reverse path", () => {
    expect(getWorkstreamMemberActions({ association: "primary", isRemoved: false })).toEqual([
      { id: "move", label: "Move to another workstream" },
      { id: "link", label: "Link to another workstream" },
      { id: "remove", label: "Remove from workstream", destructive: true },
    ]);
    expect(getWorkstreamMemberActions({ association: "primary", isRemoved: true })).toEqual([
      { id: "reattach", label: "Reattach to workstream" },
    ]);
    expect(getWorkstreamMemberActions({ association: "secondary", isRemoved: false })).toEqual([
      { id: "remove", label: "Unlink from workstream", destructive: true },
    ]);
  });
});

describe("workstream movement planning", () => {
  it("does not invent same-workstream member ordering", () => {
    expect(
      planWorkstreamDrop({
        memberRef: "thread:1",
        sourceWorkstreamId: "ws:1",
        targetWorkstreamId: "ws:1",
      }),
    ).toBeNull();
  });

  it("plans cross-workstream drops as moves and rejects invalid positions", () => {
    expect(
      planWorkstreamDrop({
        memberRef: "thread:1",
        sourceWorkstreamId: "ws:1",
        targetWorkstreamId: "ws:2",
      }),
    ).toEqual({
      type: "move",
      memberRef: "thread:1",
      fromWorkstreamId: "ws:1",
      toWorkstreamId: "ws:2",
    });
  });
});

describe("receipt and paging presentation", () => {
  it("reports a partial result when coordination and native settlement differ", () => {
    const result = getReceiptSummary({
      coordination: { id: "c1", operation: "Remove", state: "succeeded" },
      nativeSettlement: { id: "s1", operation: "Settle", state: "failed" },
    });
    expect(result.isPartial).toBe(true);
    expect(result.coordination.id).toBe("c1");
    expect(result.nativeSettlement.id).toBe("s1");
  });

  it("clamps large-list page requests to a bounded window", () => {
    expect(pageWindow({ items: [1, 2, 3, 4, 5], page: 8, pageSize: 2 })).toEqual({
      items: [5],
      page: 2,
      pageCount: 3,
    });
  });
});
