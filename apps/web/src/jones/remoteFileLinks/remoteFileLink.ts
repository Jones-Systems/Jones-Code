import { EnvironmentId } from "@t3tools/contracts";
import { isNetworkPath } from "@t3tools/shared/path";

export interface RemoteFileReference {
  readonly environmentId: EnvironmentId;
  readonly absolutePath: string;
}

export function parseRemoteFileHref(href: string): RemoteFileReference | null {
  const match = /^t3-file:\/\/v1\/([^/?#]+)\/([^/?#]+)$/.exec(href);
  if (!match) return null;
  try {
    const environmentId = decodeURIComponent(match[1]!);
    const absolutePath = decodeURIComponent(match[2]!);
    if (
      !environmentId.trim() ||
      environmentId !== environmentId.trim() ||
      /[\p{Cc}]/u.test(environmentId + absolutePath) ||
      isNetworkPath(absolutePath) ||
      !/^(?:\/|[a-z]:[/\\]).*[^/\\]$/i.test(absolutePath)
    )
      return null;
    return { environmentId: EnvironmentId.make(environmentId), absolutePath };
  } catch {
    return null;
  }
}

export function remoteFileName(absolutePath: string): string {
  return absolutePath.slice(
    Math.max(absolutePath.lastIndexOf("/"), absolutePath.lastIndexOf("\\")) + 1,
  );
}
