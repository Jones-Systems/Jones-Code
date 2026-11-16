const SOURCE = /^[a-f0-9]{40}$/;
const PREVIEW = /^\d+\.\d+\.\d+-preview\.\d{8}\.\d+(?:\.\d+)?$/;

export const isPreviewRuntime = (version: string) => PREVIEW.test(version);

/** Endpoint availability does not select the update channel; the installed build does. */
export function isJonesRuntime(input: {
  version: string;
  buildMetadata: unknown;
  qualifiedRuntimeReceipt: boolean;
}): boolean {
  if (!isPreviewRuntime(input.version)) return false;
  if (input.qualifiedRuntimeReceipt) return true;
  if (typeof input.buildMetadata !== "object" || input.buildMetadata === null) return false;
  if (!("jonesSource" in input.buildMetadata)) return false;
  const source = input.buildMetadata.jonesSource;
  return (
    typeof source === "object" &&
    source !== null &&
    "repository" in source &&
    source.repository === "Jones-Systems/Jones-Code" &&
    "sha" in source &&
    typeof source.sha === "string" &&
    SOURCE.test(source.sha) &&
    "tree" in source &&
    typeof source.tree === "string" &&
    SOURCE.test(source.tree)
  );
}
