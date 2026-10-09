import { expect, it, vi } from "vite-plus/test";
import type { CDPSession, Page } from "playwright-core";
import * as ServerBrowserPage from "../../preview/ServerBrowserPage.ts";
import { CompanionHostUnavailable } from "./CompanionHostRegistry.ts";
import { companionOutcome } from "./outcome.ts";

const evidence = {
  controlled: true,
  dispatched: true,
  attachmentGeneration: 7,
  currentAttachmentGeneration: 7,
};
it("marks attachment loss after dispatch unknown even if the old action returned", () => {
  expect(companionOutcome({ ...evidence, currentAttachmentGeneration: 8 })).toBe("unknown");
  expect(companionOutcome({ ...evidence, currentAttachmentGeneration: null })).toBe("unknown");
  expect(companionOutcome(evidence)).toBeUndefined();
});
it("does not turn read failures, validation failures, or unchanged ordinary errors into unknown effects", () => {
  expect(
    companionOutcome(
      { ...evidence, controlled: false, currentAttachmentGeneration: null },
      new Error("Target closed"),
    ),
  ).toBeUndefined();
  expect(
    companionOutcome(
      { ...evidence, currentAttachmentGeneration: null },
      new Error("invalid"),
      "PreviewAutomationInvalidSelectorError",
    ),
  ).toBe("not_started");
  expect(companionOutcome(evidence, new Error("Invalid argument"))).toBeUndefined();
  expect(companionOutcome(undefined, new Error("Target closed"))).toBeUndefined();
});
it("rejects a stale locator before the actual dispatch hook", async () => {
  const onDispatch = vi.fn();
  const page = {
    locator: vi.fn(() => {
      throw new Error("native locator must not run");
    }),
  } as unknown as Page;
  let cause: unknown;
  try {
    await ServerBrowserPage.click(page, { locator: "aria-ref=expired" }, undefined, onDispatch);
  } catch (error) {
    cause = error;
  }
  expect(onDispatch).not.toHaveBeenCalled();
  expect(ServerBrowserPage.toOperationError(cause).tag).toBe(
    "PreviewAutomationInvalidSelectorError",
  );
  expect(
    companionOutcome(
      { ...evidence, dispatched: false },
      cause,
      ServerBrowserPage.toOperationError(cause).tag,
    ),
  ).toBe("not_started");
});
it("rejects a noneditable type target before dispatch", async () => {
  const onDispatch = vi.fn();
  const page = { locator: () => ({ evaluate: async () => false }) } as unknown as Page;
  let cause: unknown;
  try {
    await ServerBrowserPage.type(page, { locator: "button", text: "no" }, onDispatch);
  } catch (error) {
    cause = error;
  }
  expect(onDispatch).not.toHaveBeenCalled();
  expect(ServerBrowserPage.toOperationError(cause).tag).toBe(
    "PreviewAutomationTargetNotEditableError",
  );
  expect(companionOutcome({ ...evidence, dispatched: false }, cause)).toBe("not_started");
});
it("reports target loss after a real Runtime.evaluate dispatch boundary as unknown", async () => {
  const events: string[] = [];
  const cdp = {
    send: async () => {
      events.push("send");
      throw new Error("Target page, context or browser has been closed");
    },
  } as unknown as CDPSession;
  let dispatched = false;
  let cause: unknown;
  try {
    await ServerBrowserPage.evaluate(cdp, { expression: "click()" }, () => {
      events.push("dispatch");
      dispatched = true;
    });
  } catch (error) {
    cause = error;
  }
  expect(events).toEqual(["dispatch", "send"]);
  expect(companionOutcome({ ...evidence, dispatched }, cause)).toBe("unknown");
});
it("does not dispatch a keyboard mutation when the attachment check fails", async () => {
  const press = vi.fn(async () => {});
  const page = { keyboard: { press } } as unknown as Page;
  const unavailable = new CompanionHostUnavailable({
    hostId: "mini",
    label: "Mini",
    state: "offline",
  });
  await expect(
    ServerBrowserPage.press(page, { key: "Enter" }, () => {
      throw unavailable;
    }),
  ).rejects.toBe(unavailable);
  expect(press).not.toHaveBeenCalled();
  expect(companionOutcome({ ...evidence, dispatched: false }, unavailable)).toBe("not_started");
});
