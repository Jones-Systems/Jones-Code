import { describe, expect, it } from "vite-plus/test";
import { canDispatchQueuedRun } from "./queuedRunAdmission";

const idle = { importedHistory: false, busy: false, canStart: true, canSteer: false };
describe("selected queued run admission", () => {
  it.each([undefined, false])(
    "refuses a start without advertised support: %s",
    (queuedRunStart) => {
      expect(canDispatchQueuedRun({ ...idle, queuedRunStart })).toBe(false);
      expect(canDispatchQueuedRun({ ...idle, canSteer: true, queuedRunStart })).toBe(false);
    },
  );
  it("starts only when supported, idle, and outside imported history", () => {
    expect(canDispatchQueuedRun({ ...idle, queuedRunStart: true })).toBe(true);
    expect(canDispatchQueuedRun({ ...idle, queuedRunStart: true, busy: true })).toBe(false);
    expect(canDispatchQueuedRun({ ...idle, queuedRunStart: true, importedHistory: true })).toBe(
      false,
    );
  });
  it("preserves active-run steer while still respecting batch and history locks", () => {
    const steer = { ...idle, canStart: false, canSteer: true, queuedRunStart: undefined };
    expect(canDispatchQueuedRun(steer)).toBe(true);
    expect(canDispatchQueuedRun({ ...steer, busy: true })).toBe(false);
    expect(canDispatchQueuedRun({ ...steer, importedHistory: true })).toBe(false);
  });
});
