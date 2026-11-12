import { expect, it, vi } from "vite-plus/test";
import {
  assertCompanionCapability,
  cancelCompanionChooser,
  cancelCompanionDownload,
  CompanionOperationUnsupported,
} from "./companionCapabilities.ts";

const origin = {
  hostId: "mini",
  label: "Mini",
  capabilities: {
    cdp: true,
    clipboardText: true,
    uploads: false,
    downloads: false,
    recording: false,
  },
} as const;
it.each(["recording", "upload", "clearProfile"] as const)(
  "rejects %s for a companion while preserving server/local support",
  (operation) => {
    expect(() => assertCompanionCapability(origin, operation)).toThrow(
      CompanionOperationUnsupported,
    );
    expect(() => assertCompanionCapability(origin, operation)).toThrow(
      `unsupported on preview browser host Mini`,
    );
    expect(() => assertCompanionCapability(null, operation)).not.toThrow();
    expect(() => assertCompanionCapability("local", operation)).not.toThrow();
  },
);
it("cancels a remote download without accessing server download files", async () => {
  const download = {
    cancel: vi.fn(async () => {}),
    saveAs: vi.fn(async () => {
      throw new Error("filesystem access forbidden");
    }),
  };
  expect(await cancelCompanionDownload(origin, download)).toBe(true);
  expect(download.cancel).toHaveBeenCalledOnce();
  expect(download.saveAs).not.toHaveBeenCalled();
  download.cancel.mockClear();
  expect(await cancelCompanionDownload("local", download)).toBe(false);
  expect(download.cancel).not.toHaveBeenCalled();
});
it.each([false, true])(
  "cancels unsupported file selection and closes its viewer notice even after guest loss (%s)",
  async (lost) => {
    const events: string[] = [];
    const chooser = {
      setFiles: vi.fn(async (files: []) => {
        expect(files).toEqual([]);
        events.push("cancel");
        if (lost) throw new Error("Target closed");
      }),
    };
    expect(await cancelCompanionChooser(origin, chooser, () => events.push("closed"))).toBe(true);
    expect(events).toEqual(["cancel", "closed"]);
  },
);
