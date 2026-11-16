import { expect, it, vi } from "vite-plus/test";

// Supplementary import check only; emitted scan and runtime evidence remain separate.
vi.mock("electron", () => {
  throw new Error("Forbidden core dependency: electron");
});
vi.mock("electron-updater", () => {
  throw new Error("Forbidden core dependency: updater");
});
vi.mock("@clerk/electron", () => {
  throw new Error("Forbidden core dependency: Clerk");
});
vi.mock("@clerk/electron/preload", () => {
  throw new Error("Forbidden core dependency: Clerk preload");
});
it("imports the core without Electron, updater, Clerk or application startup", async () => {
  const core = await import("./pickerVerificationCore.ts");
  expect(typeof core.createPickerVerificationCore).toBe("function");
});
