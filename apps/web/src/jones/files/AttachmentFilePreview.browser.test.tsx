import { EnvironmentId } from "@t3tools/contracts";
import { useEffect } from "react";
import { act, render, stubGlobal, type Screen } from "../../../test/render";
import { describe, expect, it, vi } from "vite-plus/test";

const { refresh, pdfLoads } = vi.hoisted(() => ({
  refresh: vi.fn<() => Promise<string | null>>(),
  pdfLoads: vi.fn(),
}));

vi.mock(import("~/assets/assetUrls"), async (original) => ({
  ...(await original()),
  useAssetUrlRefresh: () => refresh,
}));
// The real source view highlights through a worker pool; the text it receives is what matters here.
vi.mock(import("~/components/files/ReadOnlySourcePreview"), () => ({
  default: ({ text }: { text: string }) => <pre>{text}</pre>,
}));

vi.mock(import("~/components/files/PdfPreview"), () => ({
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
import "~/components/files/PdfPreview";
import "~/components/files/ReadOnlySourcePreview";
import { AttachmentFilePreview } from "~/components/files/AttachmentFilePreview";

describe("attachment HTML and PDF preview recovery", () => {
  const originalUrl = "data:text/html,original";
  const renewedUrl = "data:text/html,renewed";
  let now = 0;
  let screen: Screen;
  let container: HTMLElement;

  async function setup() {
    const resources = new AsyncDisposableStack();
    try {
      now = 0;
      pdfLoads.mockClear();
      resources.use(vi.spyOn(Date, "now").mockImplementation(() => now));
      refresh.mockReset().mockResolvedValueOnce(originalUrl).mockResolvedValue(renewedUrl);
      resources.use(stubGlobal("fetch", vi.fn(async () => new Response("<p>Captured HTML</p>"))));
      screen = resources.use(await render(null));
      container = screen.container;
      return resources;
    } catch (error) {
      await resources.disposeAsync();
      throw error;
    }
  }

  const openRemote = () =>
    act(async () =>
      screen.rerender(
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
      screen.rerender(
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
    await using _fixture = await setup();
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

  it("reauthorizes a remote PDF before retrying its viewer", async () => {
    await using _fixture = await setup();
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
    await using _fixture = await setup();
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
});
