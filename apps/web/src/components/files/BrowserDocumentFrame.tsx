import { lazy, Suspense } from "react";

const PdfPreview = lazy(() => import("./PdfPreview"));

export const isPdfPreviewFile = (path: string): boolean =>
  /\.pdf$/i.test(path.split(/[?#]/, 1)[0] ?? "");

/** HTML uses an opaque sandbox origin so it cannot reach the app's session or storage. */
export function BrowserDocumentFrame(props: {
  readonly src: string;
  readonly title: string;
  readonly pdf: boolean;
  readonly onRetry?: () => void | Promise<void>;
}) {
  return props.pdf ? (
    <Suspense
      fallback={
        <div role="status" className="flex min-h-0 flex-1 items-center justify-center">
          Loading PDF…
        </div>
      }
    >
      <PdfPreview key={props.src} src={props.src} title={props.title} onRetry={props.onRetry} />
    </Suspense>
  ) : (
    <iframe
      key={props.src}
      src={props.src}
      title={props.title}
      className="min-h-0 flex-1 border-0 bg-white"
      sandbox="allow-scripts allow-forms allow-popups allow-modals"
    />
  );
}
