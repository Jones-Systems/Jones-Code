import type { ProviderDriverKind, ServerProvider } from "@t3tools/contracts";
import { msg } from "gt-react";
import { BotIcon, type LucideIcon } from "lucide-react";
import { m, type MessageResolver } from "../../i18n/messages";
import { cn } from "../../lib/utils";
import { ProviderAccountGlyph } from "../../jones/composer/ProviderShortcutIcon";
import { subagentAccountDisplayName } from "../../jones/subagents/accountDisplayName";
import type { ProviderInstanceEntry } from "../../providerInstances";
import { ProviderInstanceIcon } from "./ProviderInstanceIcon";

export function threadRelationshipStatusLabel(
  status: string | null,
  resolve: MessageResolver = m,
): string {
  switch (status) {
    case "preparing":
    case "starting":
      return resolve(msg("Starting", { $context: "agent status" }));
    case "running":
    case "in_progress":
      return resolve(msg("Running", { $context: "agent status" }));
    case "pending":
    case "queued":
      return resolve(msg("Queued", { $context: "agent status" }));
    case "waiting":
    case "blocked":
      return resolve(msg("Waiting", { $context: "agent status" }));
    case "completed":
      return resolve(msg("Done", { $context: "agent status" }));
    case "failed":
    case "error":
      return resolve(msg("Failed", { $context: "agent status" }));
    case "cancelled":
    case "interrupted":
      return resolve(msg("Stopped", { $context: "agent status" }));
    case "rolled_back":
      return resolve(msg("Reverted", { $context: "agent status" }));
    case "resolved_native":
      return resolve(msg("Resolved (native)", { $context: "agent status" }));
    case "resolved_portable":
      return resolve(msg("Resolved (portable)", { $context: "agent status" }));
    case "consumed":
      return resolve(msg("Consumed", { $context: "agent status" }));
    case "superseded":
      return resolve(msg("Superseded", { $context: "agent status" }));
    case "idle":
      return resolve(msg("Idle", { $context: "agent status" }));
    default:
      return resolve(msg("Unknown", { $context: "agent status" }));
  }
}

export function ThreadRelationshipIcon({
  driver,
  provider,
  providerEntries = [],
  status,
  fallbackIcon: FallbackIcon = BotIcon,
}: {
  driver?: ProviderDriverKind | undefined;
  provider?: ServerProvider | undefined;
  providerEntries?: ReadonlyArray<ProviderInstanceEntry>;
  status: string | null;
  fallbackIcon?: LucideIcon;
}) {
  const iconClassName = "size-4 shrink-0 text-muted-foreground";
  const accountDisplayName = subagentAccountDisplayName(provider, driver);
  const entry =
    accountDisplayName !== undefined
      ? providerEntries.find((candidate) => candidate.instanceId === provider?.instanceId)
      : undefined;
  return (
    <span className="relative isolate inline-flex size-4 shrink-0 items-center justify-center">
      {driver && accountDisplayName !== undefined ? (
        <ProviderAccountGlyph
          driverKind={driver}
          displayName={accountDisplayName}
          acpRegistryAgentId={entry?.acpRegistryAgentId}
          acpRegistryIconUrl={entry?.acpRegistryIconUrl ?? provider?.iconUrl}
        />
      ) : driver ? (
        <ProviderInstanceIcon
          driverKind={driver}
          displayName={driver}
          size="compact"
          iconClassName={iconClassName}
          className="z-auto"
        />
      ) : (
        <FallbackIcon className={iconClassName} />
      )}
      <span
        className={cn(
          "absolute -right-1 -bottom-1 z-50 size-2 rounded-full border-2 border-card",
          status === "running" ||
            status === "in_progress" ||
            status === "pending" ||
            status === "waiting"
            ? "bg-info"
            : status === "failed" || status === "error"
              ? "bg-destructive"
              : status === "completed"
                ? "bg-success"
                : "bg-muted-foreground/45",
        )}
        aria-hidden="true"
      />
    </span>
  );
}
