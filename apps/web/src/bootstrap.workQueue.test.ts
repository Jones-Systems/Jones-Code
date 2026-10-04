import { afterEach, describe, expect, it, vi } from "vite-plus/test";

afterEach(() => {
  vi.doUnmock("./main");
  vi.doUnmock("./workQueuePreview");
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
});

describe("standalone work queue startup", () => {
  it.each([
    ["/work-queue", true, false, true],
    ["/work-queue?sample=1", true, false, true],
    ["/work-queue", false, false, false],
    ["/work-queue", true, true, false],
    ["/", true, false, false],
    ["/welcome", true, false, false],
    ["/connect", true, false, false],
    ["/work-queue/", true, false, false],
    ["/work-queue/other", true, false, false],
  ])(
    "selects the entry for %s, hosted=%s, Electron=%s",
    async (path, hosted, electron, preview) => {
      vi.resetModules();
      vi.stubEnv("VITE_HOSTED_APP_URL", "https://preview.example.com");
      vi.stubEnv("VITE_HOSTED_APP_CHANNEL", "");
      vi.stubEnv("VITE_HTTP_URL", "");
      vi.stubEnv("VITE_WS_URL", "");
      vi.stubGlobal("window", {
        location: new URL(
          path,
          hosted ? "https://preview.example.com" : "https://local.example.com",
        ),
        ...(electron ? { desktopBridge: {} } : {}),
      });
      const loadMain = vi.fn(() => ({ startup: Promise.resolve() }));
      const loadPreview = vi.fn(() => ({}));
      vi.doMock("./main", loadMain);
      vi.doMock("./workQueuePreview", loadPreview);

      await import("./bootstrap");
      await vi.dynamicImportSettled();

      expect(loadPreview).toHaveBeenCalledTimes(preview ? 1 : 0);
      expect(loadMain).toHaveBeenCalledTimes(preview ? 0 : 1);
    },
  );
});
