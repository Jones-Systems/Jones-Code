const previewBuild = /-preview\.\d{8}\.\d+(?:\.\d+)?$/;

export function isJonesPreviewBuildPair(clientVersion: string, serverVersion: string): boolean {
  return previewBuild.test(clientVersion) && previewBuild.test(serverVersion);
}
