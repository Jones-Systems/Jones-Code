import { describe, expect, it, vi } from "vite-plus/test";
const manager = vi.hoisted(() => ({
  add: vi.fn((_options: unknown) => "toast-id"),
  update: vi.fn(),
  close: vi.fn(),
}));
vi.mock("../../components/ui/toast", () => ({ toastManager: manager }));
vi.mock("../../state/workstreamsPersonal", () => ({ usePersonalWorkstreams: vi.fn() }));
vi.mock("../../state/environments", () => ({ useEnvironments: vi.fn() }));
import {
  showPersonalFailure,
  reconcilePersonalConditions,
  personalFailureMessage,
} from "./personalNotifications";
describe("personal Workstreams notifications", () => {
  it("deduplicates a persistent actionable error, respects dismissal, and clears resolved conditions", () => {
    const failure = {
      action: "assign" as const,
      reason: "offline" as const,
      environmentId: "synthetic-env" as never,
      dedupeKey: "assign:thread:offline",
    };
    showPersonalFailure(failure);
    showPersonalFailure(failure);
    expect(manager.add).toHaveBeenCalledOnce();
    expect(manager.update).toHaveBeenCalledOnce();
    const options = manager.add.mock.calls[0]?.[0] as unknown as {
      timeout: number;
      data: { explicitDismissOnly: boolean; onClose: () => void };
      actionProps: unknown;
    };
    expect(options.timeout).toBe(0);
    expect(options.data.explicitDismissOnly).toBe(true);
    expect(options.actionProps).toBeDefined();
    options.data.onClose();
    showPersonalFailure(failure);
    expect(manager.add).toHaveBeenCalledOnce();
    reconcilePersonalConditions({
      environments: [],
      workstreams: [],
      assignmentsByThreadKey: new Map(),
      pendingThreadKeys: new Set(),
      canCreate: false,
    });
    showPersonalFailure(failure);
    expect(manager.add).toHaveBeenCalledTimes(2);
    reconcilePersonalConditions({
      environments: [],
      workstreams: [],
      assignmentsByThreadKey: new Map(),
      pendingThreadKeys: new Set(),
      canCreate: false,
    });
    expect(manager.close).toHaveBeenCalledWith("toast-id");
  });
  it("maps conflict and uncertain writes to product text", () => {
    expect(
      personalFailureMessage(
        { action: "assign", reason: "changed_elsewhere", environmentId: null, dedupeKey: "x" },
        "Mini",
      ),
    ).toBe("This thread was moved somewhere else. Try again.");
    expect(
      personalFailureMessage(
        { action: "recolor", reason: "unconfirmed", environmentId: null, dedupeKey: "y" },
        "Mini",
      ),
    ).toBe("Couldn't confirm that change. Check the Workstream and try again.");
  });
});
