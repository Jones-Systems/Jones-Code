import { expect, it, vi } from "vite-plus/test";
import { render } from "../../../test/render";
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
