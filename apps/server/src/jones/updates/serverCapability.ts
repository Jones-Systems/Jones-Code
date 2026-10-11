import type { JonesUpdateState } from "@t3tools/contracts";

/** Advertise only operations accepted by the qualified native staging/install guards. */
export function qualifiedServerCapability(input: {
  readonly supported: boolean;
  readonly qualifiedRuntime: boolean;
  readonly launcherManaged: boolean;
  readonly qualifiedStaging: boolean;
  readonly qualifiedUpdates: boolean;
  readonly hasCurrentVersion: boolean;
  readonly hasStage: boolean;
  readonly hasInstall: boolean;
  readonly hasEnvironment: boolean;
  readonly restoreFailed: boolean;
}): JonesUpdateState["capability"] {
  const check = input.supported && input.qualifiedRuntime;
  const launcherReady = input.launcherManaged && input.hasCurrentVersion;
  const download = check && launcherReady && input.qualifiedStaging && input.hasStage;
  const install =
    check &&
    launcherReady &&
    input.qualifiedUpdates &&
    input.hasInstall &&
    input.hasEnvironment &&
    !input.restoreFailed;
  return {
    check,
    download,
    install,
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
