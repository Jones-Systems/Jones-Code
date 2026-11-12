import { useRef, useState } from "react";
import { ExpandedImageDialog } from "~/components/chat/ExpandedImageDialog";
import { ZoomableImage, type ZoomableImageHandle } from "../chat/ZoomableImage";
import { FileSurfaceFailure } from "./fileSurfaceChrome";
import { ImagePanelToolbar } from "~/jones/filePreview/ImagePanelToolbar";

export default function PreviewImageContent(props: {
  readonly src: string;
  readonly name: string;
  readonly mimeType?: string;
  readonly onError?: () => void;
}) {
  const viewer = useRef<ZoomableImageHandle>(null);
  const [failed, setFailed] = useState(false);
  const [expanded, setExpanded] = useState(false);
  if (failed)
    return <FileSurfaceFailure message="Unable to load image." onRetry={() => setFailed(false)} />;
  return (
    <>
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
        renderLayout={(content, view) => (
          <>
            <ImagePanelToolbar view={view} viewer={viewer} onExpand={() => setExpanded(true)} />
            <div className="flex min-h-0 min-w-0 flex-1 overflow-hidden p-4">{content}</div>
          </>
        )}
      />
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
