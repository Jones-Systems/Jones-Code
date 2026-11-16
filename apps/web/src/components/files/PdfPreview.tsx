import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { useStableCallback } from "~/hooks/useStableCallback";
import {
  clampWorkspacePdfPosition,
  initialWorkspacePdfPosition,
  type WorkspacePdfPosition,
} from "../../jones/filePreview/workspacePdfSession";
import { sharedPdfWorker } from "./pdfDocument";
import { getDocument, GlobalWorkerOptions, type PDFDocumentProxy } from "pdfjs-dist";
import {
  EventBus,
  LinkTarget,
  PDFFindController,
  PDFLinkService,
  PDFViewer,
} from "pdfjs-dist/web/pdf_viewer.mjs";
import workerUrl from "pdfjs-dist/build/pdf.worker.min.mjs?url";
import "pdfjs-dist/web/pdf_viewer.css";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { observeResize } from "~/lib/observeResize";

const assets = import.meta.glob<string>(
  "../../../node_modules/pdfjs-dist/{cmaps,standard_fonts,wasm}/*.{bcmap,pfb,ttf,wasm}",
  { eager: true, query: "?url", import: "default" },
);
const assetDirectories: Record<string, string> = {
  cMapUrl: "cmaps",
  standardFontDataUrl: "standard_fonts",
  wasmUrl: "wasm",
};
const interactive = (target: EventTarget | null) =>
  target instanceof Element &&
  !!target.closest("a,button,input,textarea,select,[contenteditable=true]");

function localBinaryDataFactory(signal: AbortSignal) {
  return class LocalBinaryDataFactory {
    async fetch({ kind, filename }: { kind: string; filename: string }) {
      const directory = assetDirectories[kind];
      const url = assets[`../../../node_modules/pdfjs-dist/${directory}/${filename}`];
      if (!url) throw new Error("PDF resource is unavailable");
      const response = await fetch(url, { signal: signal });
      if (!response.ok) throw new Error("PDF resource could not be loaded");
      return new Uint8Array(await response.arrayBuffer());
    }
  };
}

export default function PdfPreview({
  src,
  title,
  onRetry,
  initialPosition,
  onPositionChange,
}: {
  readonly src: string;
  readonly title: string;
  readonly initialPosition?: WorkspacePdfPosition | (() => WorkspacePdfPosition) | undefined;
  readonly onPositionChange?: ((position: WorkspacePdfPosition) => void) | undefined;
  readonly onRetry?: (() => void | Promise<void>) | undefined;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const pagesRef = useRef<HTMLDivElement>(null);
  const eventBusRef = useRef<EventBus | null>(null);
  const searchRef = useRef<HTMLInputElement>(null);
  const [searchOpen, setSearchOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [matches, setMatches] = useState("");
  const viewerRef = useRef<PDFViewer | null>(null);
  const zoomRef = useRef(100);
  const fitScaleRef = useRef(1);
  const readyRef = useRef(false);
  const restoringRef = useRef(false);
  const positionPropsRef = useRef({ initialPosition, onPositionChange });
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Refresh the position bridge without restarting an owned loading task when its observer changes.
  useLayoutEffect(() => {
    positionPropsRef.current = { initialPosition, onPositionChange };
  }, [initialPosition, onPositionChange]);
  const publishPosition = useStableCallback(() => {
    const container = containerRef.current;
    if (!container || !readyRef.current || restoringRef.current) return;
    const pages = Array.from(
      pagesRef.current?.querySelectorAll<HTMLElement>("[data-page-number]") ?? [],
    );
    if (!pages.length) return;
    const top = container.getBoundingClientRect().top + container.clientTop;
    const index = pages.findIndex(
      (page) => page.getBoundingClientRect().top + page.clientTop + page.clientHeight > top + 1,
    );
    const page = index < 0 ? pages.length - 1 : index;
    positionPropsRef.current.onPositionChange?.(
      clampWorkspacePdfPosition(
        {
          page,
          offset: top - (pages[page]!.getBoundingClientRect().top + pages[page]!.clientTop),
          left: container.scrollLeft,
          zoomPercent: zoomRef.current,
        },
        pages.map((node) => node.clientHeight),
        container.scrollWidth - container.clientWidth,
      ),
    );
  });

  const pointerRef = useRef<{ x: number; y: number; moved: boolean } | null>(null);
  const [percent, setPercent] = useState(100);
  const [input, setInput] = useState("100");
  const [status, setStatus] = useState<"loading" | "ready" | "error">("loading");
  const [attempt, setAttempt] = useState(0);
  const [retrying, setRetrying] = useState(false);
  const retryGeneration = useRef(0);
  const [documentIdentity, setDocumentIdentity] = useState({ src, attempt });
  if (documentIdentity.src !== src || documentIdentity.attempt !== attempt) {
    setDocumentIdentity({ src, attempt });
    setPercent(100);
    setInput("100");
    setStatus("loading");
    setRetrying(false);
    setSearchOpen(false);
    setQuery("");
    setMatches("");
  }

  const zoomTo = useStableCallback((value: number, origin?: [number, number]) => {
    const viewer = viewerRef.current;
    if (!viewer || !readyRef.current) return;
    const next = Math.min(800, Math.max(60, Math.round(value)));
    if (next === 100 && !origin) {
      viewer.currentScale = fitScaleRef.current;
      if (containerRef.current) containerRef.current.scrollLeft = 0;
    } else
      viewer.updateScale({
        scaleFactor: (fitScaleRef.current * next) / 100 / viewer.currentScale,
        origin,
        drawingDelay: 100,
      });
    const boundedScale = Math.min(
      fitScaleRef.current * 8,
      Math.max(fitScaleRef.current * 0.6, viewer.currentScale),
    );
    if (boundedScale !== viewer.currentScale) viewer.currentScale = boundedScale;
    zoomRef.current = next;
    setPercent(next);
    setInput(String(next));
    publishPosition();
  });

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Own the imperative PDF viewer, loading task, resize and DOM listeners; dispose all resources on document replacement or unmount.
  useEffect(() => {
    const container = containerRef.current;
    const pages = pagesRef.current;
    if (!container || !pages) return;
    let disposed = false;
    let painted = false;
    restoringRef.current = true;
    const abort = new AbortController();
    const LocalBinaryDataFactory = localBinaryDataFactory(abort.signal);
    GlobalWorkerOptions.workerSrc = workerUrl;
    const eventBus = new EventBus();
    const linkService = new PDFLinkService({
      eventBus,
      externalLinkTarget: LinkTarget.BLANK,
      externalLinkRel: "noopener noreferrer",
      ignoreDestinationZoom: true,
    });
    const findController = new PDFFindController({ eventBus, linkService });
    // PDF.js 6 supports lifetime cancellation here; its published options type omits it.
    const viewerOptions = {
      container,
      viewer: pages,
      eventBus,
      linkService,
      findController,
      abortSignal: abort.signal,
    };
    const viewer = new PDFViewer(viewerOptions);
    eventBusRef.current = eventBus;
    viewerRef.current = viewer;
    linkService.setViewer(viewer);
    readyRef.current = false;
    zoomRef.current = 100;

    const fail = () => {
      if (!disposed) {
        readyRef.current = false;
        setStatus("error");
      }
    };
    const fit = () => {
      viewer.currentScaleValue = "page-width";
      fitScaleRef.current = viewer.currentScale;
      zoomRef.current = 100;
      setPercent(100);
      setInput("100");
      publishPosition();
    };
    const initialize = () => {
      if (disposed) return;
      fit();
      readyRef.current = true;
      const saved = positionPropsRef.current.initialPosition;
      const position =
        typeof saved === "function" ? saved() : (saved ?? initialWorkspacePdfPosition);
      zoomTo(position.zoomPercent);
      // pagesPromise is the published PDFViewer completion for actual page geometry.
      void Promise.resolve(viewer.pagesPromise)
        .then(() => {
          if (disposed) return;
          const pageNodes = Array.from(pages.querySelectorAll<HTMLElement>("[data-page-number]"));
          if (pageNodes.length) {
            // Permit the final short page to keep its within-page viewport anchor.
            const clamped = clampWorkspacePdfPosition(
              position,
              pageNodes.map((page) => page.clientHeight),
              container.scrollWidth - container.clientWidth,
            );
            const page = pageNodes[clamped.page]!;
            const targetTop =
              clamped.page === 0 && clamped.offset === 0
                ? 0
                : container.scrollTop +
                  page.getBoundingClientRect().top +
                  page.clientTop -
                  container.getBoundingClientRect().top -
                  container.clientTop +
                  clamped.offset;
            pages.style.minHeight = `${Math.max(pages.scrollHeight, targetTop + container.clientHeight)}px`;
            container.scrollTop = targetTop;
            container.scrollLeft = clamped.left;
          }
          restoringRef.current = false;
          publishPosition();
          if (painted) setStatus("ready");
        })
        .catch(fail);
    };
    const rendered = ({ error }: { error?: unknown }) => {
      if (error) fail();
      else if (!disposed && readyRef.current) {
        painted = true;
        if (!restoringRef.current) setStatus("ready");
      }
    };
    const matchCount = ({ matchesCount }: { matchesCount: { current: number; total: number } }) => {
      if (!disposed) setMatches(`${matchesCount.current} of ${matchesCount.total} matches`);
    };
    eventBus.on("updatefindmatchescount", matchCount);
    eventBus.on("pagesinit", initialize);
    eventBus.on("pagerendered", rendered);
    const task = getDocument({
      worker: sharedPdfWorker(),
      url: documentIdentity.src,
      BinaryDataFactory: LocalBinaryDataFactory,
      useWorkerFetch: false,
    });
    task.onPassword = fail;
    void task.promise
      .then((document) => {
        if (disposed) return;
        linkService.setDocument(document);
        viewer.setDocument(document);
      })
      .catch(fail);
    const sizing = container.parentElement ?? container;
    let previousWidth = sizing.clientWidth;
    const stopResize = observeResize(sizing, () => {
      if (!readyRef.current || sizing.clientWidth === previousWidth) return;
      previousWidth = sizing.clientWidth;
      fit();
    });
    const wheel = (event: WheelEvent) => {
      if (!readyRef.current || interactive(event.target) || event.deltaY === 0) return;
      event.preventDefault();
      if (event.shiftKey) {
        container.scrollTop += event.deltaY;
        return;
      }
      zoomTo(zoomRef.current * Math.exp(-event.deltaY * 0.002), [event.clientX, event.clientY]);
    };
    container.addEventListener("wheel", wheel, { passive: false });
    container.addEventListener("scroll", publishPosition, { passive: true });
    return () => {
      publishPosition();
      disposed = true;
      pages.style.minHeight = "";
      retryGeneration.current += 1;
      readyRef.current = false;
      viewerRef.current = null;
      eventBusRef.current = null;
      abort.abort();
      stopResize();
      container.removeEventListener("wheel", wheel);
      container.removeEventListener("scroll", publishPosition);
      eventBus.off("updatefindmatchescount", matchCount);
      eventBus.off("pagesinit", initialize);
      eventBus.off("pagerendered", rendered);
      // The runtime accepts null to release a document; generated declarations omit it.
      const clearDocument = viewer.setDocument as (document: PDFDocumentProxy | null) => void;
      clearDocument.call(viewer, null);
      viewer.cleanup();
      // The runtime accepts null to release links; the generated declaration omits it.
      const clearLinks = linkService.setDocument as (document: PDFDocumentProxy | null) => void;
      clearLinks.call(linkService, null);
      void task.destroy().catch(() => {});
    };
  }, [documentIdentity, zoomTo, publishPosition]);

  const retry = async () => {
    if (!onRetry) {
      setAttempt((value) => value + 1);
      return;
    }
    const generation = retryGeneration.current;
    setRetrying(true);
    try {
      // The owner advances its load generation only after current authorization succeeds. Never reopen an expired grant here.
      await onRetry();
    } catch {
      // Keep the failure and external-open fallback available when reauthorization fails.
    }
    if (generation === retryGeneration.current) setRetrying(false);
  };

  const find = (value: string, previous = false, again = false) => {
    eventBusRef.current?.dispatch("find", {
      source: null,
      type: again ? "again" : "",
      query: value,
      caseSensitive: false,
      entireWord: false,
      highlightAll: true,
      findPrevious: previous,
      matchDiacritics: false,
    });
  };
  const closeSearch = () => {
    setSearchOpen(false);
    eventBusRef.current?.dispatch("findbarclose", {});
    containerRef.current?.focus();
  };
  const commitInput = () => {
    const value = Number(input);
    if (input.trim() !== "" && Number.isFinite(value) && value >= 60 && value <= 800) zoomTo(value);
    else setInput(String(zoomRef.current));
  };
  const cycle = (origin?: [number, number]) =>
    zoomTo(zoomRef.current >= 150 ? 100 : Math.min(150, zoomRef.current + 10), origin);

  return (
    <div
      className="flex min-h-0 flex-1 flex-col"
      onKeyDownCapture={(event) => {
        if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "f") {
          event.preventDefault();
          event.stopPropagation();
          setSearchOpen(true);
          requestAnimationFrame(() => searchRef.current?.focus());
        }
      }}
    >
      <div
        role="toolbar"
        aria-label="PDF zoom controls"
        className="flex shrink-0 items-center justify-center gap-1 border-b px-2 py-1"
      >
        <Button
          variant="ghost"
          size="icon-xs"
          aria-label="Zoom out"
          disabled={status !== "ready" || percent <= 60}
          onClick={() => zoomTo(zoomRef.current - 10)}
        >
          −
        </Button>
        <div className="w-16">
          <Input
            size="compact"
            font="mono"
            aria-label="Zoom percentage"
            inputMode="numeric"
            disabled={status !== "ready"}
            value={input}
            onChange={(event) => setInput(event.target.value)}
            onBlur={commitInput}
            onKeyDown={(event) => {
              event.stopPropagation();
              if (event.key === "Enter") {
                event.preventDefault();
                commitInput();
              }
              if (event.key === "Escape") setInput(String(zoomRef.current));
            }}
          />
        </div>
        <span aria-hidden="true">%</span>
        <Button
          variant="ghost"
          size="icon-xs"
          aria-label="Zoom in"
          disabled={status !== "ready" || percent >= 800}
          onClick={() => zoomTo(zoomRef.current + 10)}
        >
          +
        </Button>
        <Button
          variant="ghost"
          size="compact"
          aria-label="Reset zoom to 100%"
          disabled={status !== "ready"}
          onClick={() => zoomTo(100)}
        >
          Reset
        </Button>
      </div>
      {searchOpen && (
        <div role="search" className="flex shrink-0 items-center gap-1 border-b px-2 py-1">
          <div className="min-w-0 flex-1">
            <Input
              ref={searchRef}
              size="compact"
              aria-label="Find in PDF"
              value={query}
              onChange={(event) => {
                setQuery(event.target.value);
                find(event.target.value);
              }}
              onKeyDown={(event) => {
                event.stopPropagation();
                if (event.key === "Enter") {
                  event.preventDefault();
                  find(query, event.shiftKey, true);
                }
                if (event.key === "Escape") closeSearch();
              }}
            />
          </div>
          <Button
            variant="ghost"
            size="compact"
            aria-label="Previous match"
            onClick={() => find(query, true, true)}
          >
            ↑
          </Button>
          <Button
            variant="ghost"
            size="compact"
            aria-label="Next match"
            onClick={() => find(query, false, true)}
          >
            ↓
          </Button>
          <Button variant="ghost" size="compact" aria-label="Close search" onClick={closeSearch}>
            ×
          </Button>
          <span className="sr-only" aria-live="polite">
            {matches}
          </span>
        </div>
      )}
      <span className="sr-only" aria-live="polite">
        {percent}% zoom
      </span>
      <div className="relative min-h-0 flex-1 bg-muted/30">
        <div
          ref={containerRef}
          role="region"
          aria-label={`${title} PDF preview`}
          tabIndex={0}
          className="absolute inset-0 overflow-auto"
          style={{
            cursor: status === "ready" ? (percent >= 150 ? "zoom-out" : "zoom-in") : undefined,
          }}
          onPointerDown={(event) => {
            pointerRef.current = { x: event.clientX, y: event.clientY, moved: false };
          }}
          onPointerMove={(event) => {
            const pointer = pointerRef.current;
            if (pointer && Math.hypot(event.clientX - pointer.x, event.clientY - pointer.y) > 4)
              pointer.moved = true;
          }}
          onPointerCancel={() => {
            pointerRef.current = null;
          }}
          onClick={(event) => {
            const pointer = pointerRef.current;
            pointerRef.current = null;
            if (interactive(event.target) || pointer?.moved || window.getSelection()?.toString())
              return;
            cycle([event.clientX, event.clientY]);
          }}
          onKeyDown={(event) => {
            if (interactive(event.target)) return;
            if (["+", "=", "-", "0", "Enter", " "].includes(event.key)) {
              event.preventDefault();
              event.stopPropagation();
              if (event.key === "0") zoomTo(100);
              else if (event.key === "-") zoomTo(zoomRef.current - 10);
              else if (event.key === "+" || event.key === "=") zoomTo(zoomRef.current + 10);
              else cycle();
            }
          }}
        >
          {/* oxlint-disable-next-line shadcn/no-unknown-classes -- This class is defined by the imported PDF.js viewer stylesheet. */}
          <div ref={pagesRef} className="pdfViewer" />
        </div>
        {status === "loading" && (
          <div
            className="pointer-events-none absolute inset-0 flex items-center justify-center"
            role="status"
          >
            Loading PDF…
          </div>
        )}
        {status === "error" && (
          <div
            className="absolute inset-0 flex flex-col items-center justify-center gap-2 bg-background"
            role="alert"
          >
            <p>Unable to preview this PDF.</p>
            <Button variant="outline" size="sm" disabled={retrying} onClick={retry}>
              Retry
            </Button>
            <a href={src} target="_blank" rel="noopener noreferrer">
              Open PDF
            </a>
          </div>
        )}
      </div>
      <p className="shrink-0 px-2 py-1 text-center text-xs text-muted-foreground">
        Scroll to zoom · Shift-scroll or use scrollbars to scroll pages
      </p>
    </div>
  );
}
