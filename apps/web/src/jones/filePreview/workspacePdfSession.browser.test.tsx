import { expect, it, vi } from "vite-plus/test";
import { act, render } from "../../../test/render";
import { BrowserDocumentFrame } from "../../components/files/BrowserDocumentFrame";

it("preserves the no-retry native PDF route and pins its grant until the revision changes", async () => {
  using _native = vi.spyOn(navigator, "pdfViewerEnabled", "get").mockReturnValue(true);
  const urls = ["original", "renewed", "changed"].map((text) =>
    URL.createObjectURL(new Blob(["%PDF-1.4 " + text], { type: "application/pdf" })),
  );
  using _urls = { [Symbol.dispose]: () => urls.forEach(URL.revokeObjectURL) };
  const view = (src: string, revision: string) => (
    <BrowserDocumentFrame
      pdf
      title="Native PDF"
      src={src}
      documentId="native-resource"
      documentKey={revision}
    />
  );
  await using screen = await render(view(urls[0]!, "one"));
  const iframe = screen.container.querySelector("iframe");
  expect(iframe).not.toBeNull();
  expect(iframe!.src).toBe(`${urls[0]}#navpanes=0&view=FitH`);
  await screen.rerender(view(urls[1]!, "one"));
  expect(screen.container.querySelector("iframe")).toBe(iframe);
  expect(iframe!.src).toBe(`${urls[0]}#navpanes=0&view=FitH`);
  await screen.rerender(view(urls[2]!, "two"));
  expect(screen.container.querySelector("iframe")).not.toBe(iframe);
  expect(screen.container.querySelector("iframe")!.src).toBe(`${urls[2]}#navpanes=0&view=FitH`);
});

const { loads } = vi.hoisted(() => ({ loads: [] as string[] }));
vi.mock(import("../../components/files/PdfPreview"), async () => {
  const { useEffect } = await import("react");
  return {
    default: (props: Parameters<typeof import("../../components/files/PdfPreview").default>[0]) => {
      const position = typeof props.initialPosition === "function"
        ? props.initialPosition()
        : props.initialPosition;
      useEffect(() => {
        loads.push(props.src);
      }, [props.src]);
      return (
        <button onClick={() => props.onPositionChange?.({ page: 2, offset: 90, left: 40, zoomPercent: 150 })}>
          {JSON.stringify(position)}
        </button>
      );
    },
  };
});

import { createWorkspacePdfSession } from "./workspacePdfSession";

it("bridges resource position across grant renewal and revision remounts without using the native plugin", async () => {
  using _native = vi.spyOn(navigator, "pdfViewerEnabled", "get").mockReturnValue(true);
  loads.length = 0;
  const session = createWorkspacePdfSession("environment/thread/resource");
  const view = (src: string, revision: string) => {
    session.selectRevision(revision);
    const load = session.load(src);
    return (
      <BrowserDocumentFrame
        pdf
        title="Owned PDF"
        src={load.src}
        documentId={session.resourceId}
        documentKey={`${revision}:${load.generation}`}
        pdfSession={session}
        onRetry={() => {}}
      />
    );
  };
  await using screen = await render(view("first-grant", "one"));
  await vi.waitFor(() => expect(loads).toEqual(["first-grant"]));
  expect(screen.container.querySelector("iframe")).toBeNull();
  await act(() => screen.container.querySelector("button")!.click());
  await screen.rerender(view("renewed-grant", "one"));
  expect(loads).toEqual(["first-grant"]);
  await screen.rerender(<div>Authorization unavailable</div>);
  await screen.rerender(view("renewed-grant", "one"));
  await vi.waitFor(() => expect(loads).toEqual(["first-grant", "first-grant"]));
  expect(screen.container.textContent).toContain('"offset":90');
  await screen.rerender(view("changed-grant", "two"));
  await vi.waitFor(() => expect(loads).toEqual(["first-grant", "first-grant", "changed-grant"]));
  expect(screen.container.textContent).toContain('"page":2');
  expect(screen.container.textContent).toContain('"zoomPercent":150');
});
