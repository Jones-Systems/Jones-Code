import type { JonesUpdateState } from "@t3tools/contracts";

/** Receipt read support is independent of the advertised operation admission capability. */
export const JONES_OPERATION_RECEIPTS_HEADER = "x-jones-operation-receipts";

/** Advertise only operations accepted by the qualified native staging/install guards. */
export function qualifiedServerCapability(input: {
  readonly supported: boolean;
  readonly qualifiedRuntime: boolean;
  readonly launcherManaged: boolean;
  readonly qualifiedStaging: boolean;
  readonly qualifiedUpdates: boolean;
  readonly qualifiedRetirement?: boolean;
  readonly hasCurrentVersion: boolean;
  readonly hasStage: boolean;
  readonly hasInstall: boolean;
  readonly hasEnvironment: boolean;
}): JonesUpdateState["capability"] {
  const check = input.supported && input.qualifiedRuntime;
  const launcherReady = input.launcherManaged && input.hasCurrentVersion;
  const download = check && launcherReady && input.qualifiedStaging && input.hasStage;
  const install =
    check && launcherReady && input.qualifiedUpdates && input.hasInstall && input.hasEnvironment;
  return {
    check,
    download,
    install,
    // Exact-source staging reuses the download guards; older clients read absence as unsupported.
    ...(download ? { exactSource: true } : {}),
    ...(download && input.qualifiedRetirement === true && input.hasEnvironment
      ? { replaceStaged: true }
      : {}),
    ...(download && install
      ? {}
      : {
          reason: !input.supported
            ? ("unsupported-platform" as const)
            : !input.qualifiedRuntime
              ? ("source-unqualified" as const)
              : ("bootstrap-required" as const),
        }),
  };
}
