import type { ServerProvider } from "@t3tools/contracts";

export function resolveProviderUpdateVerification(input: {
  readonly provider: ServerProvider | undefined;
  readonly advisoryRefreshed: boolean;
  readonly targetVersion?: string | undefined;
}): { readonly status: "unchanged" | "succeeded"; readonly message: string } {
  const { provider, advisoryRefreshed, targetVersion } = input;
  const version = provider?.version?.trim().replace(/^v/, "");
  const readable = provider?.installed === true && Boolean(version);
  const verified =
    readable &&
    (targetVersion !== undefined
      ? version === targetVersion.trim().replace(/^v/, "")
      : advisoryRefreshed && provider?.versionAdvisory?.status === "current");
  if (verified) {
    return { status: "succeeded", message: "Provider updated." };
  }
  const outdated =
    readable &&
    targetVersion === undefined &&
    advisoryRefreshed &&
    provider?.versionAdvisory?.status === "behind_latest";
  return {
    status: "unchanged",
    message: outdated
      ? "Update command completed, but T3 Code still detects an outdated provider version."
      : "Update command completed, but T3 Code could not verify the provider version.",
  };
}
