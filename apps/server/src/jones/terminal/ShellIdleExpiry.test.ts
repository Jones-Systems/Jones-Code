import { describe, expect, it } from "vite-plus/test";
import { ShellIdleExpiry, SHELL_IDLE_EXPIRY_MS } from "./ShellIdleExpiry.ts";

describe("command terminal idle expiry", () => {
  it("requires a full thirty minutes of observed idle time", () => {
    const expiry = new ShellIdleExpiry();
    const session = {},
      process = {};
    expect(expiry.observe(session, process, 1, true, 0)).toBe(false);
    expect(expiry.observe(session, process, 1, true, SHELL_IDLE_EXPIRY_MS - 1)).toBe(false);
    expect(expiry.observe(session, process, 1, true, SHELL_IDLE_EXPIRY_MS)).toBe(true);
  });

  it("starts a fresh interval after activity, busy or uncertain inspection", () => {
    for (const interruption of ["activity", "busy", "unknown"] as const) {
      const expiry = new ShellIdleExpiry();
      const session = {},
        process = {};
      expiry.observe(session, process, 1, true, 0);
      const mark = interruption === "activity" ? 2 : 1;
      expiry.observe(session, process, mark, interruption === "activity", SHELL_IDLE_EXPIRY_MS - 1);
      expect(expiry.observe(session, process, mark, true, SHELL_IDLE_EXPIRY_MS)).toBe(false);
      expect(expiry.observe(session, process, mark, true, 2 * SHELL_IDLE_EXPIRY_MS - 1)).toBe(
        interruption === "activity",
      );
    }
  });

  it("does not transfer idle time across session/process replacement or clock rollback", () => {
    const expiry = new ShellIdleExpiry();
    const session = {},
      process = {};
    expiry.observe(session, process, 1, true, 1_000);
    expect(expiry.observe({}, process, 1, true, SHELL_IDLE_EXPIRY_MS + 1_000)).toBe(false);
    expect(expiry.observe(session, {}, 1, true, SHELL_IDLE_EXPIRY_MS + 1_000)).toBe(false);
    expect(expiry.observe(session, process, 1, true, 0)).toBe(false);
    expect(expiry.observe(session, process, 1, true, SHELL_IDLE_EXPIRY_MS - 1)).toBe(false);
  });

  it("keeps independent terminals and forgets uncertain global samples", () => {
    const expiry = new ShellIdleExpiry();
    const first = {},
      second = {},
      process = {};
    expiry.observe(first, process, 1, true, 0);
    expiry.observe(second, process, 1, true, 100);
    expect(expiry.observe(first, process, 1, true, SHELL_IDLE_EXPIRY_MS)).toBe(true);
    expect(expiry.observe(second, process, 1, true, SHELL_IDLE_EXPIRY_MS)).toBe(false);
    expiry.reset();
    expect(expiry.observe(first, process, 1, true, 2 * SHELL_IDLE_EXPIRY_MS - 1)).toBe(false);
    expect(expiry.observe(first, null, 1, true, 3 * SHELL_IDLE_EXPIRY_MS)).toBe(false);
  });
});
