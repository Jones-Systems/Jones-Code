import { describe, expect, it, vi } from "vite-plus/test";

import { notifyCompanionPopupBlocked, onCompanionPopupNotice } from "./CompanionPopup.ts";

const settle = () => new Promise<void>((resolve) => queueMicrotask(resolve));

describe("companion popup notices", () => {
  it.each([
    [
      "https://user:password@login.example.test/private/path?token=secret#private",
      "https://login.example.test",
    ],
    ["http://localhost:4317/private?code=secret", "http://localhost:4317"],
    ["https://[::1]:8443/private?code=secret", "https://[::1]:8443"],
  ])("emits only the safe origin", async (url, origin) => {
    const listener = vi.fn();
    const unsubscribe = onCompanionPopupNotice(listener);
    try {
      notifyCompanionPopupBlocked(url);
      expect(listener).not.toHaveBeenCalled();
      await settle();
      expect(listener).toHaveBeenCalledExactlyOnceWith({ kind: "popup_blocked", origin });
    } finally {
      unsubscribe();
    }
  });

  it.each([
    "file:///private/secret",
    "javascript:alert('secret')",
    "data:text/plain,secret",
    "chrome://settings/secret",
    "about:blank",
    "blob:https://example.test/private",
    "https://",
    "/relative/private?secret",
    "",
    `https://example.test/${"x".repeat(16_384)}`,
  ])("redacts unsafe or malformed destinations", async (url) => {
    const listener = vi.fn();
    const unsubscribe = onCompanionPopupNotice(listener);
    try {
      notifyCompanionPopupBlocked(url);
      await settle();
      expect(listener).toHaveBeenCalledExactlyOnceWith({ kind: "popup_blocked", origin: null });
    } finally {
      unsubscribe();
    }
  });

  it("isolates failing consumers and prevents one consumer changing another's notice", async () => {
    const throwing = onCompanionPopupNotice(() => {
      throw new Error("consumer unavailable");
    });
    const mutating = onCompanionPopupNotice((notice) => {
      Object.assign(notice, { origin: "https://wrong.example.test" });
    });
    const listener = vi.fn();
    const unsubscribe = onCompanionPopupNotice(listener);
    try {
      expect(() => notifyCompanionPopupBlocked("https://example.test/private")).not.toThrow();
      await settle();
      expect(listener).toHaveBeenCalledExactlyOnceWith({
        kind: "popup_blocked",
        origin: "https://example.test",
      });
    } finally {
      throwing();
      mutating();
      unsubscribe();
    }
  });

  it("stops pending delivery on unsubscribe and does not replay old notices to new listeners", async () => {
    const oldListener = vi.fn();
    const oldUnsubscribe = onCompanionPopupNotice(oldListener);
    notifyCompanionPopupBlocked("https://example.test/private");
    oldUnsubscribe();
    const listener = vi.fn();
    const unsubscribe = onCompanionPopupNotice(listener);
    try {
      await settle();
      expect(oldListener).not.toHaveBeenCalled();
      expect(listener).not.toHaveBeenCalled();
      notifyCompanionPopupBlocked("https://next.example.test/private");
      await settle();
      expect(listener).toHaveBeenCalledExactlyOnceWith({
        kind: "popup_blocked",
        origin: "https://next.example.test",
      });
    } finally {
      unsubscribe();
    }
  });

  it("does not deliver an old subscription's pending notice after the same listener resubscribes", async () => {
    const listener = vi.fn();
    const unsubscribeFirst = onCompanionPopupNotice(listener);
    notifyCompanionPopupBlocked("https://example.test/private");
    unsubscribeFirst();
    const unsubscribeSecond = onCompanionPopupNotice(listener);
    try {
      await settle();
      expect(listener).not.toHaveBeenCalled();
    } finally {
      unsubscribeSecond();
    }
  });
});
