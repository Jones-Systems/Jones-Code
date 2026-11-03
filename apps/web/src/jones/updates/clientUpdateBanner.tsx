import { ClientServerBuildLabels } from "./ClientServerBuildLabels";
import type { EnvironmentId, ServerConfig } from "@t3tools/contracts";
import { InfoIcon } from "lucide-react";
import { useState, type ReactNode } from "react";
import type { ComposerBannerStackItem } from "../../components/chat/ComposerBannerStack";
import {
  dismissVersionMismatch,
  isVersionMismatchDismissed,
  resolveServerConfigClientUpdateNotice,
} from "../../versionSkew";
import type { ExactSourceTarget } from "./ExactSourceUpdateAction";
import { ProductionExactSourceUpdateAction } from "./ProductionExactSourceUpdateAction";

interface NoticeEnvironment {
  readonly environmentId: EnvironmentId;
  readonly label: string;
  readonly serverConfig: Pick<ServerConfig, "environment"> | null;
}

export interface ClientUpdateBannerInput {
  readonly activeEnvironment: NoticeEnvironment | null | undefined;
  readonly automaticEnvironment: boolean;
  readonly eligibleEnvironments: readonly NoticeEnvironment[];
  /** Renders the qualified action for an older preview server; fixtures inject a synthetic host. */
  readonly renderExactSourceAction?: (target: ExactSourceTarget) => ReactNode;
}

const renderProductionAction = (target: ExactSourceTarget) => (
  <ProductionExactSourceUpdateAction {...target} />
);

export function useClientUpdateBanners({
  activeEnvironment,
  automaticEnvironment,
  eligibleEnvironments,
  renderExactSourceAction = renderProductionAction,
}: ClientUpdateBannerInput): ComposerBannerStackItem[] {
  const [dismissed, setDismissed] = useState<ReadonlySet<string>>(() => new Set());
  const environments = automaticEnvironment
    ? eligibleEnvironments
    : activeEnvironment
      ? [activeEnvironment]
      : [];
  return environments.flatMap((environment): ComposerBannerStackItem[] => {
    const notice = resolveServerConfigClientUpdateNotice(environment.serverConfig);
    if (!notice) return [];
    const key = JSON.stringify([
      "jones-client-notice-v2",
      environment.environmentId,
      notice.kind,
      notice.clientVersion,
      notice.serverVersion,
      notice.clientSourceSha ?? null,
      notice.serverSourceSha ?? null,
    ]);
    if (dismissed.has(key) || isVersionMismatchDismissed(key)) return [];
    return [
      {
        id: key,
        variant: "default",
        priority: "notice",
        icon: <InfoIcon />,
        title: `${environment.label}: ${notice.kind === "server-newer" ? "Client update available" : notice.kind === "client-newer" ? "Server update available" : "Preview build identity mismatch"}`,
        description: (
          <span className="break-words">
            <ClientServerBuildLabels builds={notice} />. {notice.hint}
          </span>
        ),
        // Only an older server gets an action toward this app's verified source.
        ...(notice.kind === "client-newer" && notice.clientSourceSha
          ? {
              actions: renderExactSourceAction({
                environmentId: environment.environmentId,
                label: environment.label,
                targetSource: notice.clientSourceSha,
              }),
            }
          : {}),
        dismissLabel: "Dismiss client version notice",
        onDismiss: () => {
          dismissVersionMismatch(key);
          setDismissed((previous) => new Set(previous).add(key));
        },
      },
    ];
  });
}
