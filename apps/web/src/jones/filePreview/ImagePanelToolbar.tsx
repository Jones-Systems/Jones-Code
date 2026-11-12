import { useState, type RefObject } from "react";
import { MaximizeIcon, MinusIcon, PlusIcon, ScanIcon } from "lucide-react";
import type { ZoomableImageHandle } from "~/components/chat/ZoomableImage";
import { Input } from "~/components/ui/input";
import { FileSurfaceAction } from "~/components/files/fileSurfaceChrome";
import type { ImageView } from "./imageView";

export function ImagePanelToolbar({
  view,
  viewer,
  onExpand,
}: {
  readonly view: ImageView;
  readonly viewer: RefObject<ZoomableImageHandle | null>;
  readonly onExpand: () => void;
}) {
  const [percent, setPercent] = useState("100");
  const [displayedView, setDisplayedView] = useState(view);
  if (
    displayedView.width !== view.width ||
    displayedView.height !== view.height ||
    displayedView.zoom !== view.zoom ||
    displayedView.fit !== view.fit
  ) {
    setDisplayedView(view);
    setPercent(String(Math.round(view.zoom * 100)));
  }
  const commitPercent = () => {
    const value = Number.parseFloat(percent);
    if (Number.isFinite(value)) viewer.current?.zoomTo(value / 100);
    else setPercent(String(Math.round(view.zoom * 100)));
  };
  return (
    <div className="flex min-h-10 shrink-0 flex-wrap items-center justify-between gap-2 border-b border-border/60 px-3 py-1 text-xs text-muted-foreground">
      <span className="tabular-nums">
        {view.width
          ? `${view.width.toLocaleString()} × ${view.height.toLocaleString()}`
          : "Loading image…"}
      </span>
      <div className="flex items-center gap-1">
        <FileSurfaceAction
          label="Zoom out"
          disabled={!view.width || view.zoom <= 0.6}
          onPress={() => viewer.current?.zoomTo(view.zoom - 0.1)}
        >
          <MinusIcon className="size-3.5" />
        </FileSurfaceAction>
        <label className="flex items-center gap-1">
          <Input
            size="compact"
            font="mono"
            nativeInput
            aria-label="Zoom percentage"
            inputMode="decimal"
            className="w-16"
            value={percent}
            onChange={(event) => setPercent(event.target.value)}
            onBlur={commitPercent}
            onKeyDown={(event) => {
              if (event.key === "Enter") {
                event.preventDefault();
                commitPercent();
              }
            }}
          />
          <span aria-hidden>%</span>
        </label>
        <FileSurfaceAction
          label="Zoom in"
          disabled={!view.width || view.zoom >= 8}
          onPress={() => viewer.current?.zoomTo(view.zoom + 0.1)}
        >
          <PlusIcon className="size-3.5" />
        </FileSurfaceAction>
        <FileSurfaceAction
          label="Fit image"
          pressed={view.zoom === 1}
          onPress={() => viewer.current?.zoomTo(1)}
        >
          <ScanIcon className="size-3.5" />
        </FileSurfaceAction>
        <FileSurfaceAction
          label="Actual image size"
          pressed={view.zoom * view.fit === 1}
          onPress={() => viewer.current?.actualSize()}
        >
          <MaximizeIcon className="size-3.5" />
        </FileSurfaceAction>
        <FileSurfaceAction label="Expand image" onPress={onExpand}>
          <MaximizeIcon className="size-3.5" />
        </FileSurfaceAction>
      </div>
    </div>
  );
}
