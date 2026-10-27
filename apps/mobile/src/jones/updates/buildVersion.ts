import type { ExecutionEnvironmentDescriptor } from "@t3tools/contracts";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import { jonesBuildLabel, type JonesBuildRef } from "@t3tools/shared/jones/buildTime";

export function mobileJonesBuildRef(
  version: string,
  environment: ExecutionEnvironmentDescriptor | undefined,
  state: JonesUpdateState | null,
): JonesBuildRef {
  if (version === environment?.serverVersion)
    return {
      version,
      startedAt: environment.jonesBuildStartedAt,
      sourceSha: environment.jonesSource?.sha,
    };
  if (version === state?.currentVersion)
    return {
      version,
      startedAt: state.currentBuildStartedAt,
      sourceSha: state.installedSource?.sha,
    };
  if (version === state?.provenance?.version)
    return {
      version,
      startedAt: state.provenance.buildStartedAt,
      sourceSha: state.provenance.sourceSha,
    };
  return { version };
}

export function mobileJonesBuildDescription(ref: JonesBuildRef): string {
  const label = jonesBuildLabel(ref);
  return label.text === ref.version ? ref.version : `${label.text} (${ref.version})`;
}
