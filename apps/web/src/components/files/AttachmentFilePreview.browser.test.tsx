import { EnvironmentId } from "@t3tools/contracts";
import { act, useEffect } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

const { refresh, pdfLoads } = vi.hoisted(() => ({
  refresh: vi.fn<() => Promise<string | null>>(),
  pdfLoads: vi.fn(),
}));

vi.mock(import("~/assets/assetUrls"), async (original) => ({
  ...(await original()),
  useAssetUrlRefresh: () => refresh,
}));
// The real source view highlights through a worker pool; the text it receives is what matters here.
vi.mock(import("./ReadOnlySourcePreview"), () => ({
  default: ({ text }: { text: string }) => <pre>{text}</pre>,
}));

vi.mock(import("./PdfPreview"), () => ({
  default: ({
    src,
    onRetry,
  }: {
    src: string;
    onRetry?: (() => void | Promise<void>) | undefined;
  }) => {
    useEffect(() => {
      pdfLoads(src);
    }, [src]);
    return (
      <div role="region" aria-label="PDF preview">
        <a href={src}>Open PDF</a>
        <button onClick={onRetry}>Retry PDF</button>
      </div>
    );
  },
}));

// Resolve lazy-viewer mocks before tests begin, so teardown cannot retire an in-flight mock route.
import "./PdfPreview";
import "./ReadOnlySourcePreview";
import { AttachmentFilePreview } from "./AttachmentFilePreview";

describe("attachment HTML and PDF preview recovery", () => {
  const originalUrl = "data:text/html,original";
  const renewedUrl = "data:text/html,renewed";
  let now = 0;
  let root: Root;
  let container: HTMLDivElement;

  beforeEach(() => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    now = 0;
    pdfLoads.mockClear();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    refresh.mockReset().mockResolvedValueOnce(originalUrl).mockResolvedValue(renewedUrl);
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("<p>Captured HTML</p>")),
    );
    container = document.createElement("div");
    document.body.append(container);
    root = createRoot(container);
  });

  afterEach(async () => {
    await act(async () => root.unmount());
    container.remove();
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  const openRemote = () =>
    act(async () =>
      root.render(
        <AttachmentFilePreview
          name="document.html"
          mimeType="text/html"
          sizeBytes={100}
          asset={{ environmentId: EnvironmentId.make("test-environment"), attachmentId: "html" }}
        />,
      ),
    );

  const openRemotePdf = async () => {
    await act(async () =>
      root.render(
        <AttachmentFilePreview
          name="document.pdf"
          mimeType="application/pdf"
          sizeBytes={100}
          asset={{ environmentId: EnvironmentId.make("test-environment"), attachmentId: "pdf" }}
        />,
      ),
    );
    await vi.waitFor(() => expect(pdfLoads).toHaveBeenCalledExactlyOnceWith(originalUrl));
  };
  const retryPdf = () =>
    act(async () => {
      [...container.querySelectorAll("button")]
        .find((button) => button.textContent === "Retry PDF")!
        .click();
    });

  const toggleMode = async (label: string) => {
    const button = container.querySelector<HTMLButtonElement>(`button[aria-label="${label}"]`);
    expect(button, label).not.toBeNull();
    await act(async () => button!.click());
  };
  const alert = () => container.querySelector('[role="alert"]');

  it("keeps a renewed URL for subsequent rendered and source views", async () => {
    await openRemote();
    expect(container.querySelector("iframe")?.getAttribute("src")).toBe(originalUrl);
    now = 61 * 60_000;
    await toggleMode("Show HTML source");
    expect(fetch).toHaveBeenCalledExactlyOnceWith(renewedUrl, expect.any(Object));
    await vi.waitFor(() =>
      expect(container.querySelector("pre")?.textContent).toBe("<p>Captured HTML</p>"),
    );

    await toggleMode("Show rendered page");
    expect(container.querySelector("iframe")?.getAttribute("src")).toBe(renewedUrl);
    expect(container.querySelector("iframe")?.getAttribute("sandbox")).toBe(
      "allow-scripts allow-forms allow-popups allow-downloads allow-modals",
    );
    await toggleMode("Show HTML source");
    expect(refresh).toHaveBeenCalledTimes(2);
    expect(vi.mocked(fetch).mock.calls.map(([url]) => url)).toEqual([renewedUrl, renewedUrl]);
  });

  it("does not fetch or mark an expired URL fresh when reauthorization is unavailable", async () => {
    await openRemote();
    now = 61 * 60_000;
    refresh.mockResolvedValue(null);
    await toggleMode("Show HTML source");
    expect(fetch).not.toHaveBeenCalled();
    expect(alert()?.textContent).toContain("Reconnect to the environment and try again.");

    await toggleMode("Show rendered page");
    await toggleMode("Show HTML source");
    expect(refresh).toHaveBeenCalledTimes(3);
    expect(fetch).not.toHaveBeenCalled();
  });

  it("reauthorizes a remote PDF before retrying its viewer", async () => {
    await openRemotePdf();
    expect(container.querySelector("a")?.getAttribute("href")).toBe(originalUrl);
    now = 61 * 60_000;
    await retryPdf();
    expect(refresh).toHaveBeenCalledTimes(2);
    expect(container.querySelector("a")?.getAttribute("href")).toBe(renewedUrl);
    expect(pdfLoads.mock.calls.map(([src]) => src)).toEqual([originalUrl, renewedUrl]);
    expect(fetch).not.toHaveBeenCalled();
  });

  it("does not remount a PDF with an expired URL when reauthorization is unavailable", async () => {
    await openRemotePdf();
    refresh.mockResolvedValue(null);
    await retryPdf();
    expect(refresh).toHaveBeenCalledTimes(2);
    expect(container.querySelector('[aria-label="PDF preview"]')).toBeNull();
    expect(pdfLoads.mock.calls.map(([src]) => src)).toEqual([originalUrl]);
    expect(alert()?.querySelector("p")?.textContent).toBe(
      "Reconnect to the environment and try again.",
    );
    const retry = alert()?.querySelector("button");
    expect(retry?.textContent?.trim()).toBe("Try again");
    expect(retry?.disabled).toBe(false);
  });

  it("can return to rendered HTML after local source decoding fails", async () => {
    const file = new Blob([new Uint8Array([0x3c, 0x70, 0x3e, 0xe9])], { type: "text/html" });
    await act(async () =>
      root.render(
        <AttachmentFilePreview
          name="document.html"
          mimeType="text/html"
          sizeBytes={file.size}
          file={file}
        />,
      ),
    );
    await toggleMode("Show HTML source");
    await vi.waitFor(() => expect(alert()?.textContent).toContain("not UTF-8"));

    await toggleMode("Show rendered page");
    expect(alert()).toBeNull();
    expect(container.querySelector("iframe")?.title).toBe("document.html");
  });
});
