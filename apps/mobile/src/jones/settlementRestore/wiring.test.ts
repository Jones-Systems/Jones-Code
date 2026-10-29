import * as NodeFS from "node:fs";
import * as NodeURL from "node:url";
import { describe, expect, it } from "vite-plus/test";

const source = (path: string) =>
  NodeFS.readFileSync(
    NodeURL.fileURLToPath(new URL(`../../features/${path}`, import.meta.url)),
    "utf8",
  );

describe("persistent Undo settle production entry points", () => {
  it("passes the hook action from HomeRouteScreen through HomeScreen to the shared row", () => {
    const route = source("home/HomeRouteScreen.tsx");
    expect(route).toMatch(/undoSettleThread,[\s\S]*?\} = useThreadListActions\(/);
    expect(route).toContain("onUndoSettleThread={undoSettleThread}");
    expect(source("home/HomeScreen.tsx")).toContain(
      "onUndoSettleThread={props.onUndoSettleThread}",
    );
  });
  it("passes the same action from ThreadNavigationSidebar to the shared row", () => {
    const sidebar = source("threads/ThreadNavigationSidebar.tsx");
    expect(sidebar).toMatch(/undoSettleThread,[\s\S]*?\} = useThreadListActions\(/);
    expect(sidebar).toContain("onUndoSettleThread={undoSettleThread}");
    const row = source("threads/thread-list-v2-items.tsx");
    expect(row).toContain("...settlementRestoreMenuItems({");
    expect(row).toMatch(/nativeEvent.event === "undo-settle"\) void onUndoSettleThread\(thread\)/);
    expect(row).toContain('nativeEvent.event === "unsettle") handleUnsettle()');
  });
  it("adds the same adapter to detail status actions and preserves ordinary Un-settle", () => {
    const detail = source("threads/ThreadDetailScreen.tsx");
    expect(detail).toContain(
      'import { undoSettleThread } from "../../jones/settlementRestore/runtime"',
    );
    expect(detail).toContain("const restoreMenuItems = settlementRestoreMenuItems({");
    expect(detail).toMatch(
      /await undoSettleThread\(\{\s*environmentId: props.environmentId,\s*id: props.selectedThread.id,?\s*\}\)/,
    );
    expect(detail).toContain('text: "Undo settle", onPress: () => void runUndoSettle()');
    expect(detail).toContain('text: "Un-settle", onPress: () => void runStatusAction("unsettle")');
  });
});
