import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import { jonesBuildLabel, jonesBuildLabels, type JonesBuildRef } from "./buildVersion";

export interface ClientServerBuilds {
  readonly clientVersion: string;
  readonly serverVersion: string;
  readonly clientBuildStartedAt?: string | undefined;
  readonly serverBuildStartedAt?: string | undefined;
  readonly clientSourceSha?: string | undefined;
  readonly serverSourceSha?: string | undefined;
}

export function clientServerBuildLabels(builds: ClientServerBuilds) {
  const [clientLabel, serverLabel] = jonesBuildLabels(
    [
      {
        version: builds.clientVersion,
        startedAt: builds.clientBuildStartedAt,
        sourceSha: builds.clientSourceSha,
      },
      {
        version: builds.serverVersion,
        startedAt: builds.serverBuildStartedAt,
        sourceSha: builds.serverSourceSha,
      },
    ],
    "day",
  );
  return { clientLabel: clientLabel!, serverLabel: serverLabel! };
}

export function outcomeBuildRefs(state: JonesUpdateState): JonesBuildRef[] {
  if (!state.outcome) return [];
  return [
    {
      version: state.outcome.fromVersion,
      ...(state.outcome.fromVersion === state.currentVersion
        ? { startedAt: state.currentBuildStartedAt, sourceSha: state.installedSource?.sha }
        : {}),
    },
    {
      version: state.outcome.targetVersion,
      ...(state.outcome.targetVersion === state.provenance?.version
        ? { startedAt: state.provenance.buildStartedAt, sourceSha: state.provenance.sourceSha }
        : {}),
    },
  ];
}

export function clientServerBuildDetail(builds: ClientServerBuilds): string {
  const { clientLabel, serverLabel } = clientServerBuildLabels(builds);
  return `Server ${serverLabel.text}: ${serverLabel.detail}; ${builds.serverVersion}; source ${builds.serverSourceSha ?? "not recorded"}\nClient ${clientLabel.text}: ${clientLabel.detail}; ${builds.clientVersion}; source ${builds.clientSourceSha ?? "not recorded"}`;
}

export function outcomeBuildDetail(state: JonesUpdateState): string {
  return outcomeBuildRefs(state)
    .map(
      (ref) =>
        `${jonesBuildLabel(ref).detail}; ${ref.version}; source ${ref.sourceSha ?? "not recorded"}`,
    )
    .join(" → ");
}
