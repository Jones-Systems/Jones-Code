import { hostPreviewMimeTypeFromExtension } from "@t3tools/shared/filePreview";

import { AttachmentFilePreview } from "~/components/files/AttachmentFilePreview";
import { filePreviewReadErrorMessage } from "~/components/files/filePreviewMode";
import {
  FILE_SURFACE_SUBHEADER_CLASS,
  FileSurfaceFailure,
  FileSurfaceLoading,
  FileSurfaceNotice,
} from "~/components/files/fileSurfaceChrome";
import { useProjectFileQuery } from "~/components/files/projectFilesQueryState";
import ReadOnlySourcePreview from "~/components/files/ReadOnlySourcePreview";
import { useFilesystemReadAccess } from "~/state/filesystem";
import { useEnvironmentPresentation } from "~/state/presentation";

import { remoteFileName, type RemoteFileReference } from "./remoteFileLink";

function RemoteTextPreview({
  environmentId,
  absolutePath,
  cwd,
  name,
  computer,
}: RemoteFileReference & { cwd: string; name: string; computer: string }) {
  const file = useProjectFileQuery(environmentId, cwd, absolutePath);
  return (
    <div className="flex min-h-0 flex-1 flex-col overflow-hidden bg-background">
      <div className={FILE_SURFACE_SUBHEADER_CLASS} data-surface-subheader>
        <span className="min-w-0 truncate text-xs">
          {computer} / {name}
        </span>
      </div>
      {file.error !== null ? (
        <FileSurfaceFailure
          message={file.readError ? filePreviewReadErrorMessage(file.readError) : file.error}
          onRetry={file.refresh}
        />
      ) : file.data === null ? (
        <FileSurfaceLoading />
      ) : (
        <>
          {file.data.truncated ? (
            <FileSurfaceNotice>Showing the first 1 MB.</FileSurfaceNotice>
          ) : null}
          <ReadOnlySourcePreview name={name} text={file.data.contents} />
        </>
      )}
    </div>
  );
}

export default function RemoteFilePreview({ environmentId, absolutePath }: RemoteFileReference) {
  const { presentation } = useEnvironmentPresentation(environmentId);
  const { canReadFiles, isPending, error } = useFilesystemReadAccess(environmentId);
  if (isPending) return <FileSurfaceLoading />;
  if (!canReadFiles) {
    return (
      <FileSurfaceFailure message={error ?? "This computer does not grant file read access."} />
    );
  }
  const cwd = /^[a-z]:[/\\]/i.test(absolutePath) ? absolutePath.slice(0, 3) : "/";
  const name = remoteFileName(absolutePath);
  const computer = presentation?.entry.target.label ?? environmentId;
  const mimeType = hostPreviewMimeTypeFromExtension(name.slice(name.lastIndexOf(".")));
  return mimeType === null ? (
    <RemoteTextPreview {...{ environmentId, absolutePath, cwd, name, computer }} />
  ) : (
    <AttachmentFilePreview
      name={name}
      mimeType={mimeType}
      sizeBytes={0}
      asset={{ environmentId, resource: { _tag: "draft-workspace-file", cwd, path: absolutePath } }}
      origin={computer}
    />
  );
}
