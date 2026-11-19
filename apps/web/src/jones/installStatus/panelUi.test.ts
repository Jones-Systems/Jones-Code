import { EnvironmentId } from "@t3tools/contracts";
import type { JonesInstallPlanState } from "@t3tools/contracts/jones/installPlan";
import { startPlan } from "@t3tools/client-runtime/jones/install-plan";
import { describe, expect, it } from "vite-plus/test";
import { createInstallStatusStore } from "./panelUi";

const plan = startPlan({
  planId: "11111111-1111-4111-8111-111111111111",
  origin: "update-button",
  target: {
    repository: "Jones-Systems/Jones-Code",
    sourceSha: "a".repeat(40),
    sourceTree: "b".repeat(40),
    version: "1.2.0",
  },
  from: { version: "1.1.0" },
  laptopEnvironmentId: EnvironmentId.make("laptop"),
  select: [],
  now: "2026-10-27T04:00:00Z",
});
const state = (
  current: JonesInstallPlanState["current"],
  last: JonesInstallPlanState["last"] = null,
): JonesInstallPlanState => ({
  schema: 1,
  current,
  last,
  laptop: { observedAt: "2026-10-27T04:00:00Z", activationBlocked: false },
  view: { complete: false },
});
describe("renderer read ordering", () => {
  it("keeps newer membership when an earlier native read replies after an Edit event", () => {
    const store = createInstallStatusStore();
    const selected = {
      environmentId: EnvironmentId.make("vps"),
      label: "VPS",
      route: "operations" as const,
      membership: "selected" as const,
      addedAt: "2026-10-27T04:01:00Z",
      operations: [],
    };
    store.update({ native: state({ ...plan, revision: 2, devices: [selected] }) });
    store.update({ native: state({ ...plan, revision: 1 }) });
    expect(store.get().native?.current?.devices.map((d) => d.label)).toEqual(["VPS"]);
    expect(store.get().native?.current?.revision).toBe(2);
  });
  it("does not resurrect a dismissed plan from a delayed read or reopen it as editable", () => {
    const store = createInstallStatusStore();
    store.update({ native: state(null, { ...plan, dismissedAt: "2026-10-27T04:05:00Z" }) });
    store.update({ native: state(plan) });
    store.open(true);
    expect(store.get().native?.current).toBeNull();
    expect(store.get().showLast).toBe(true);
  });
  it("remembers collapse independently for each plan", () => {
    const store = createInstallStatusStore();
    store.update({ native: state(plan) });
    store.open();
    store.collapse(true);
    store.update({
      native: state({
        ...plan,
        planId: "22222222-2222-4222-8222-222222222222",
        createdAt: "2026-10-27T04:06:00Z",
      }),
    });
    store.open();
    expect(store.get().collapsed).toBe(false);
  });

  it("orders empty, current, and archive-only updates by the desk document revision", () => {
    const store = createInstallStatusStore();
    const closed = { ...plan, dismissedAt: "2026-10-27T04:05:00Z", revision: 4 };
    const history = {
      planId: plan.planId,
      revision: closed.revision,
      targetVersion: plan.target.version,
      closedAt: closed.dismissedAt,
      pendingLaptop: true,
      pendingDevices: [],
    };
    store.update({ native: { ...state(plan), documentRevision: 4 } });
    store.update({ native: { ...state(null), documentRevision: 3 } });
    expect(store.get().native?.current?.planId).toBe(plan.planId);
    store.update({
      native: { ...state(null, closed), documentRevision: 5, recoveryHistory: [history] },
    });
    store.open(true, plan.planId);
    store.update({ native: { ...state(null, closed), documentRevision: 5 }, historyPlan: closed });
    const later = { ...closed, revision: 5 };
    store.update({
      native: {
        ...state(null, closed),
        documentRevision: 6,
        recoveryHistory: [{ ...history, revision: 5 }],
      },
      historyPlan: later,
    });
    store.update({
      native: { ...state(null, closed), documentRevision: 5, recoveryHistory: [history] },
      historyPlan: closed,
    });
    expect(store.get().native?.documentRevision).toBe(6);
    expect(store.get().historyPlan?.revision).toBe(5);
    store.open(true, "different-plan");
    expect(store.get().historyPlan).toBeNull();
    store.bindDesk("previous-desk");
    store.bindDesk("replacement-desk");
    store.update({ native: { ...state(null), documentRevision: 1 } });
    expect(store.get().native?.documentRevision).toBe(1);
    expect(store.get().historyPlanId).toBeNull();
  });
});
