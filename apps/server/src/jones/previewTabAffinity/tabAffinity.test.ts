import { expect, it } from "vite-plus/test";
import { PreviewTabId } from "@t3tools/contracts";
import { retireClosedTab, selectTabAffinity, type RuntimeTabPin } from "./tabAffinity.ts";

const a = PreviewTabId.make("a");
const b = PreviewTabId.make("b");
const identity = {
  schemaVersion: 1,
  runtimeKind: "electron",
  runtimeInstanceId: "local",
  appVersion: "test",
  buildCommit: null,
} as const;

it("target expiry releases only that pin, including when another hold already expired", () => {
  const runtimeTabs = new Map<PreviewTabId, RuntimeTabPin>([
    [a, { runtimeIdentity: identity, holdUntil: 30_000 }],
    [b, { runtimeIdentity: identity, holdUntil: 20_000 }],
  ]);
  const result = selectTabAffinity({
    runtimeTabs,
    tabId: a,
    runtimeIdentity: null,
    retired: new Set(),
    now: 30_000,
  });
  expect(result.decision).toBe("dispatch");
  expect(result.runtimeTabs.has(a)).toBe(false);
  expect(result.runtimeTabs.get(b)).toEqual(runtimeTabs.get(b));
  expect(runtimeTabs.has(a)).toBe(true);
});

it("matching reattachment clears its own hold without touching another pin", () => {
  const runtimeTabs = new Map<PreviewTabId, RuntimeTabPin>([
    [a, { runtimeIdentity: identity, holdUntil: 30_000 }],
    [b, { runtimeIdentity: identity, holdUntil: 40_000 }],
  ]);
  const result = selectTabAffinity({
    runtimeTabs,
    tabId: a,
    runtimeIdentity: identity,
    retired: new Set(),
    now: 10_000,
  });
  expect(result.runtimeTabs.get(a)).toEqual({ runtimeIdentity: identity });
  expect(result.runtimeTabs.get(b)).toEqual(runtimeTabs.get(b));
});

it("logical retirement never re-pins stale runtime evidence", () => {
  const result = selectTabAffinity({
    runtimeTabs: new Map([[a, { runtimeIdentity: identity }]]),
    tabId: a,
    runtimeIdentity: identity,
    retired: new Set([a]),
    now: 0,
  });
  expect(result.decision).toBe("dispatch");
  expect(result.runtimeTabs.has(a)).toBe(false);
});

it("close guards client and connection before changing any routing evidence", () => {
  const assignment = {
    clientId: "client",
    connectionId: "new",
    tabId: a,
    tabSequence: 50,
    runtimeTabs: new Map([[a, { runtimeIdentity: identity }]]),
  };
  for (const close of [
    { clientId: "other", connectionId: "new", tabId: a },
    { clientId: "client", connectionId: "old", tabId: a },
  ])
    expect(retireClosedTab(assignment, close, 100)).toBe(assignment);
});

it("close keeps another default and never lowers an existing watermark", () => {
  const assignment = {
    clientId: "client",
    connectionId: "connection",
    tabId: b,
    tabSequence: 100,
    runtimeTabs: new Map([
      [a, { runtimeIdentity: identity }],
      [b, { runtimeIdentity: identity }],
    ]),
  };
  const retired = retireClosedTab(assignment, { ...assignment, tabId: a }, 90);
  expect(retired.tabId).toBe(b);
  expect(retired.tabSequence).toBe(100);
  expect([...retired.runtimeTabs!.keys()]).toEqual([b]);
  expect(assignment.runtimeTabs.has(a)).toBe(true);
});
