import { useRef, useState } from "react";
import { MaximizeIcon, MinusIcon, PlusIcon, ScanIcon } from "lucide-react";
import { ExpandedImageDialog } from "~/components/chat/ExpandedImageDialog";
import { ZoomableImage, type ZoomableImageHandle } from "../chat/ZoomableImage";
import { Input } from "../ui/input";
import { FileSurfaceAction, FileSurfaceFailure } from "./fileSurfaceChrome";

export default function PreviewImageContent(props: {
  readonly src: string;
  readonly name: string;
  readonly mimeType?: string;
  readonly onError?: () => void;
}) {
  const [failed, setFailed] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const [natural, setNatural] = useState({ width: 0, height: 0 });
  const viewer = useRef<ZoomableImageHandle>(null);
  const [view, setView] = useState({ width: 0, height: 0, zoom: 1, fit: 1 });
  const [percent, setPercent] = useState("100");
  const scale = view.fit * view.zoom;
  const commitPercent = () => {
    const value = Number(percent.trim());
    if (percent.trim() && Number.isFinite(value) && value >= 60 && value <= 800)
      viewer.current?.zoomTo(value / 100);
    else setPercent(String(Math.round(view.zoom * 100)));
  };
  if (failed)
    return <FileSurfaceFailure message="Unable to load image." onRetry={() => setFailed(false)} />;
  return (
    <>
      <div className="flex min-h-10 shrink-0 flex-wrap items-center justify-between gap-2 border-b border-border/60 px-3 py-1 text-xs text-muted-foreground">
        <span className="tabular-nums">
          {natural.width
            ? `${natural.width.toLocaleString()} × ${natural.height.toLocaleString()}`
            : "Loading image…"}
        </span>
        <div className="flex items-center gap-1">
          <FileSurfaceAction
            label="Zoom out"
            disabled={!natural.width || view.zoom <= 0.6}
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
            disabled={!natural.width || view.zoom >= 8}
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
            pressed={scale === 1}
            onPress={() => viewer.current?.actualSize()}
          >
            <MaximizeIcon className="size-3.5" />
          </FileSurfaceAction>
          <FileSurfaceAction label="Expand image" onPress={() => setExpanded(true)}>
            <MaximizeIcon className="size-3.5" />
          </FileSurfaceAction>
        </div>
      </div>
      <div className="flex min-h-0 min-w-0 flex-1 overflow-hidden p-4">
        <ZoomableImage
          ref={viewer}
          layout="panel"
          controls={false}
          src={props.src}
          name={props.name}
          mimeType={props.mimeType}
          onError={() => {
            setFailed(true);
            props.onError?.();
          }}
          onViewChange={(next) => {
            setView(next);
            setNatural({ width: next.width, height: next.height });
            setPercent(String(Math.round(next.zoom * 100)));
          }}
        />
      </div>
      {expanded ? (
        <ExpandedImageDialog
          preview={{
            images: [
              {
                src: props.src,
                name: props.name,
                ...(props.mimeType ? { mimeType: props.mimeType } : {}),
              },
            ],
            index: 0,
          }}
          onClose={() => setExpanded(false)}
        />
      ) : null}
    </>
  );
}
