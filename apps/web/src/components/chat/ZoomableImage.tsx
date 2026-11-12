import { imageFit, type ImageView } from "~/jones/filePreview/imageView";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { observeResize } from "~/lib/observeResize";
import {
  useImperativeHandle,
  useRef,
  useState,
  useSyncExternalStore,
  type Ref,
  type ReactNode,
} from "react";
import { flushSync } from "react-dom";

import {
  onWindowControlsOverlayGeometryChange,
  windowControlsOverlayHeight,
} from "../../lib/windowControlsOverlay";

import { AttachmentImage } from "../media/AttachmentImage";

const MIN_ZOOM = 0.6;
const MAX_ZOOM = 8;

// Subtract the caption strip, matching dialog CSS's --native-titlebar-height.
function readViewerSize() {
  return { width: window.innerWidth, height: window.innerHeight - windowControlsOverlayHeight() };
}

let viewerSize: ReturnType<typeof readViewerSize> | null = null;

// Cached so an unchanged size keeps its identity: a new one resets the zoom.
function getViewerSize() {
  const next = readViewerSize();
  if (next.width !== viewerSize?.width || next.height !== viewerSize.height) viewerSize = next;
  return viewerSize;
}

function subscribeViewerSize(onChange: () => void) {
  window.addEventListener("resize", onChange);
  // Fullscreen caption-strip changes may not emit resize.
  const stopOverlayListener = onWindowControlsOverlayGeometryChange(onChange);
  return () => {
    window.removeEventListener("resize", onChange);
    stopOverlayListener();
  };
}

export interface ZoomableImageHandle {
  pan: (key: string) => boolean;
  zoomTo: (value: number) => void;
  actualSize: () => void;
}

export function ZoomableImage({
  src,
  name,
  mimeType,
  onError,
  ref,
  layout = "dialog",
  controls = true,
  renderLayout,
}: {
  src: string;
  name: string;
  mimeType?: string | undefined;
  onError: () => void;
  ref?: Ref<ZoomableImageHandle>;
  layout?: "dialog" | "panel";
  controls?: boolean;
  renderLayout?: ((content: ReactNode, view: ImageView) => ReactNode) | undefined;
}) {
  const viewportRef = useRef<HTMLDivElement>(null);
  const [naturalSize, setNaturalSize] = useState({ width: 0, height: 0 });
  const viewerSize = useSyncExternalStore(subscribeViewerSize, getViewerSize);
  const [panelSize, setPanelSize] = useState({ width: 0, height: 0 });
  const [zoom, setZoom] = useState(1);
  const [percentInput, setPercentInput] = useState("100");
  const [zoomSize, setZoomSize] = useState({ viewerSize, panelSize, layout });
  if (
    zoomSize.viewerSize !== viewerSize ||
    zoomSize.panelSize !== panelSize ||
    zoomSize.layout !== layout
  ) {
    setZoomSize({ viewerSize, panelSize, layout });
    setZoom(1);
    setPercentInput("100");
  }
  const dragRef = useRef<{
    pointerId: number;
    x: number;
    y: number;
    left: number;
    top: number;
  } | null>(null);
  const suppressClickRef = useRef(false);
  const [dragging, setDragging] = useState(false);
  const maxHeight = Math.max(1, Math.min(viewerSize.height * 0.86, viewerSize.height - 160));
  const fit = imageFit({ naturalSize, viewerSize, panelSize, layout, mimeType, maxHeight });
  const width = naturalSize.width * fit * zoom;
  const height = naturalSize.height * fit * zoom;

  const changeZoom = (next: number, point?: { x: number; y: number }) => {
    const viewport = viewportRef.current;
    const previous = zoom;
    const clamped = Math.min(Math.max(MAX_ZOOM, 1 / (fit || 1)), Math.max(MIN_ZOOM, next));
    if (!viewport || previous === clamped) return;
    const bounds = viewport.getBoundingClientRect();
    const x = point ? point.x - bounds.left : viewport.clientWidth / 2;
    const y = point ? point.y - bounds.top : viewport.clientHeight / 2;
    const anchorX = (viewport.scrollLeft + x) / previous;
    const anchorY = (viewport.scrollTop + y) / previous;
    const clientX = bounds.left + x;
    const clientY = bounds.top + y;
    // Lay out the new image size before restoring the pointer's scroll anchor.
    flushSync(() => {
      setZoom(clamped);
      setPercentInput(String(Math.round(clamped * 100)));
    });
    const resized = viewport.getBoundingClientRect();
    viewport.scrollLeft = anchorX * clamped - (clientX - resized.left);
    viewport.scrollTop = anchorY * clamped - (clientY - resized.top);
  };

  const controller: ZoomableImageHandle = {
    zoomTo: (value) => changeZoom(value),
    actualSize: () => changeZoom(1 / (fit || 1)),
    pan(key) {
      const viewport = viewportRef.current;
      if (!viewport || zoom <= 1) return false;
      // A vertical scrollbar alone must not swallow gallery navigation.
      if (
        (key === "ArrowLeft" || key === "ArrowRight") &&
        viewport.scrollWidth <= viewport.offsetWidth
      )
        return false;
      switch (key) {
        case "ArrowLeft":
          viewport.scrollLeft -= 40;
          break;
        case "ArrowRight":
          viewport.scrollLeft += 40;
          break;
        case "ArrowUp":
          viewport.scrollTop -= 40;
          break;
        case "ArrowDown":
          viewport.scrollTop += 40;
          break;
        default:
          return false;
      }
      return true;
    },
  };
  useImperativeHandle(ref, () => controller);

  const attachSizing = (sizing: HTMLDivElement | null) => {
    if (!sizing || layout !== "panel") return;
    return observeResize(sizing, ([entry]) => {
      if (!entry) return;
      const { width, height } = entry.contentRect;
      setPanelSize((current) =>
        current.width === width && current.height === height ? current : { width, height },
      );
    });
  };

  const stepZoom = (direction: number) =>
    changeZoom((Math.round(zoom * 100) + direction * 10) / 100);
  const cycleZoom = (point?: { x: number; y: number }) =>
    changeZoom(zoom >= 1.5 ? 1 : (Math.round(zoom * 100) + 10) / 100, point);
  const commitPercent = () => {
    const value = Number(percentInput.trim());
    if (
      percentInput.trim() &&
      Number.isFinite(value) &&
      value >= MIN_ZOOM * 100 &&
      value <= MAX_ZOOM * 100
    ) {
      changeZoom(value / 100);
      setPercentInput(String(Math.round(value)));
    } else {
      setPercentInput(String(Math.round(zoom * 100)));
    }
  };

  const attachViewport = (viewport: HTMLDivElement | null) => {
    viewportRef.current = viewport;
    if (!viewport) return;
    // Trackpad pinch arrives as a ctrlKey wheel; plain wheels pan through native overflow scrolling.
    // React's onWheel is passive and cannot stop the browser's own page zoom.
    const wheel = (event: WheelEvent) => {
      if (!event.ctrlKey || event.deltaY === 0) return;
      event.preventDefault();
      const delta =
        event.deltaY *
        (event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? viewport.clientHeight : 1);
      changeZoom(zoom * Math.exp(-delta * 0.01), {
        x: event.clientX,
        y: event.clientY,
      });
    };
    viewport.addEventListener("wheel", wheel, { passive: false });
    return () => {
      viewport.removeEventListener("wheel", wheel);
      viewportRef.current = null;
    };
  };

  const content = (
    <div
      className={
        layout === "panel"
          ? "flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden"
          : "min-w-0 max-w-[var(--media-width)]"
      }
    >
      {layout === "panel" && controls && (
        <div
          role="toolbar"
          aria-label="Image zoom"
          className="mb-2 flex shrink-0 justify-end gap-1 text-sm"
          onKeyDown={(event) => event.stopPropagation()}
        >
          <Button
            type="button"
            aria-label="Zoom out"
            variant="outline"
            size="compact"
            onClick={() => stepZoom(-1)}
          >
            −
          </Button>
          <label className="flex items-center gap-1">
            <Input
              size="compact"
              font="mono"
              nativeInput
              aria-label="Zoom percentage"
              inputMode="decimal"
              className="w-16"
              value={percentInput}
              onChange={(event) => setPercentInput(event.target.value)}
              onBlur={commitPercent}
              onKeyDown={(event) => {
                if (event.key === "Enter") {
                  event.preventDefault();
                  commitPercent();
                }
              }}
            />
            <span aria-hidden="true">%</span>
          </label>
          <Button
            type="button"
            aria-label="Zoom in"
            variant="outline"
            size="compact"
            onClick={() => stepZoom(1)}
          >
            +
          </Button>
          <Button
            type="button"
            aria-label="Reset zoom to 100%"
            variant="outline"
            size="compact"
            onClick={() => changeZoom(1)}
          >
            100%
          </Button>
        </div>
      )}
      <div
        ref={attachSizing}
        className={layout === "panel" ? "relative min-h-0 min-w-0 flex-1" : undefined}
      >
        <div
          ref={attachViewport}
          role="region"
          aria-label={`${name}, zoomable image`}
          aria-description="Click to zoom in or return to fit. Pinch or Ctrl+scroll to zoom, scroll or drag to pan. Use Enter to cycle zoom, plus or minus to zoom, and 0 to fit."
          tabIndex={0}
          className={`${layout === "panel" ? "absolute inset-0 min-h-0 min-w-0" : "max-w-[var(--media-width)]"} overflow-auto overscroll-contain rounded-lg bg-background shadow-2xl ring-1 ring-border/70 outline-none focus-visible:outline-2 focus-visible:outline-solid focus-visible:-outline-offset-2 focus-visible:outline-ring`}
          style={{
            width: layout === "panel" ? undefined : width || undefined,
            height: layout === "panel" ? undefined : height || undefined,
            maxHeight: layout === "panel" ? undefined : maxHeight,
            cursor: dragging ? "grabbing" : zoom >= 1.5 ? "zoom-out" : "zoom-in",
          }}
          onClick={(event) => {
            // Pointer capture also produces a click after dragging; leave the image zoomed.
            if (suppressClickRef.current) return;
            cycleZoom({ x: event.clientX, y: event.clientY });
          }}
          onKeyDown={(event) => {
            if (event.ctrlKey || event.metaKey || event.altKey) return;
            if (event.key === "Enter" || event.key === " ") {
              event.preventDefault();
              if (!event.repeat) cycleZoom();
            } else if (event.key === "+" || event.key === "=") {
              event.preventDefault();
              stepZoom(1);
            } else if (event.key === "-") {
              event.preventDefault();
              stepZoom(-1);
            } else if (event.key === "0") {
              event.preventDefault();
              changeZoom(1);
            }
          }}
          onPointerDown={(event) => {
            if (dragRef.current) return;
            suppressClickRef.current = false;
            if (event.pointerType !== "mouse" || event.button !== 0 || zoom <= 1) return;
            const viewport = event.currentTarget;
            const bounds = viewport.getBoundingClientRect();
            if (
              event.clientX - bounds.left >= viewport.clientWidth ||
              event.clientY - bounds.top >= viewport.clientHeight
            )
              return;
            dragRef.current = {
              pointerId: event.pointerId,
              x: event.clientX,
              y: event.clientY,
              left: viewport.scrollLeft,
              top: viewport.scrollTop,
            };
            viewport.setPointerCapture(event.pointerId);
          }}
          onPointerMove={(event) => {
            const drag = dragRef.current;
            if (!drag || drag.pointerId !== event.pointerId) return;
            if (Math.hypot(event.clientX - drag.x, event.clientY - drag.y) > 4) {
              suppressClickRef.current = true;
              setDragging(true);
            }
            event.currentTarget.scrollLeft = drag.left - (event.clientX - drag.x);
            event.currentTarget.scrollTop = drag.top - (event.clientY - drag.y);
          }}
          onPointerUp={(event) => {
            if (dragRef.current?.pointerId !== event.pointerId) return;
            if (event.currentTarget.hasPointerCapture(event.pointerId)) {
              event.currentTarget.releasePointerCapture(event.pointerId);
            }
            dragRef.current = null;
            setDragging(false);
          }}
          onLostPointerCapture={(event) => {
            if (dragRef.current?.pointerId !== event.pointerId) return;
            dragRef.current = null;
            setDragging(false);
          }}
        >
          <div style={layout === "panel" ? { minHeight: "100%", display: "flex" } : undefined}>
            <AttachmentImage
              name={name}
              mimeType={mimeType}
              src={src}
              alt={name}
              draggable={false}
              className="block max-w-none select-none"
              style={
                naturalSize.width
                  ? {
                      width,
                      height,
                      flexShrink: 0,
                      margin: layout === "panel" ? "auto" : undefined,
                    }
                  : { maxWidth: "var(--media-width)", maxHeight }
              }
              onLoad={(event) => {
                setNaturalSize({
                  width: event.currentTarget.naturalWidth,
                  height: event.currentTarget.naturalHeight,
                });
              }}
              onError={onError}
            />
          </div>
        </div>
      </div>
      <span className="sr-only" aria-live="polite">
        {Math.round(zoom * 100)}% zoom
      </span>
    </div>
  );
  return renderLayout
    ? renderLayout(content, { width: naturalSize.width, height: naturalSize.height, zoom, fit })
    : content;
}
