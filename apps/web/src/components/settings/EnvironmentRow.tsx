import {
  type ConnectionTarget,
  connectionRouteId,
  connectionRouteLabel,
  connectionRoutes,
} from "@t3tools/client-runtime/connection";
import type { DesktopSshEnvironmentTarget, EnvironmentMachineKind } from "@t3tools/contracts";
import * as Option from "effect/Option";
import type { ReactNode } from "react";

import { cn } from "~/lib/utils";
import type { EnvironmentPresentation } from "~/state/environments";
import { isDesktopLocalConnectionTarget } from "~/connection/desktopLocal";
import { EnvironmentMachineIcon } from "../EnvironmentMachineIcon";

export function formatDesktopSshTarget(target: DesktopSshEnvironmentTarget): string {
  const authority = target.username ? `${target.username}@${target.hostname}` : target.hostname;
  return target.port ? `${authority}:${target.port}` : authority;
}

export function environmentTransportLabel(
  environment: EnvironmentPresentation,
  activeTarget: ConnectionTarget | null = null,
): string {
  const { entry } = environment;
  if (entry.target._tag === "PrimaryConnectionTarget") return "This machine";
  const routes = connectionRoutes(entry);
  if (routes.length > 1) {
    const active =
      activeTarget === null
        ? undefined
        : routes.find(
            (route) => connectionRouteId(route.target) === connectionRouteId(activeTarget),
          );
    return active === undefined
      ? connectionRouteLabel(routes[0]!)
      : `via ${connectionRouteLabel(active)}`;
  }
  if (environment.relayManaged) return "T3 Connect";
  if (isDesktopLocalConnectionTarget(entry.target)) return "WSL";
  if (
    entry.target._tag === "SshConnectionTarget" &&
    Option.isSome(entry.profile) &&
    entry.profile.value._tag === "SshConnectionProfile"
  ) {
    return `SSH ${formatDesktopSshTarget(entry.profile.value.target)}`;
  }
  return environment.displayUrl ?? "Remote link";
}

/**
 * One machine in a grouped settings list: icon, name, a single subtitle line,
 * and controls on the right. Every environment list on the Connections page
 * uses this so the lists share one rhythm. A row with content `below` keeps
 * its icon and controls on the name's line, as the header of a taller row,
 * and gives the subtitle and that content the row's full width.
 */
export function EnvironmentRow({
  kind,
  label,
  subtitle,
  below,
  detail,
  dimmed = false,
  className,
  children,
}: {
  readonly kind: EnvironmentMachineKind;
  readonly label: string;
  readonly subtitle: ReactNode;
  readonly below?: ReactNode;
  readonly detail?: ReactNode;
  readonly dimmed?: boolean;
  readonly className?: string;
  readonly children?: ReactNode;
}) {
  const header = below !== undefined;
  const name = <p className="min-w-0 truncate text-sm font-medium text-foreground">{label}</p>;
  const line = <div className="truncate text-xs text-muted-foreground">{subtitle}</div>;
  return (
    <div
      className={cn(
        "grid grid-cols-[auto_minmax(0,1fr)_auto] items-center gap-x-3 px-3 py-2.5 sm:px-4",
        dimmed && "opacity-60",
        className,
      )}
    >
      <EnvironmentMachineIcon aria-hidden kind={kind} className="size-4 text-muted-foreground" />
      {header ? (
        name
      ) : (
        <div className="min-w-0">
          {name}
          {line}
        </div>
      )}
      {/* In a header, this box is the name's line height; taller controls center on that line. */}
      <div className={cn("flex shrink-0 items-center gap-1", header && "h-5")}>{children}</div>
      {header ? (
        <div className="col-span-2 col-start-2 min-w-0">
          {line}
          {below}
        </div>
      ) : null}
      {detail ? <div className="col-span-2 col-start-2 min-w-0">{detail}</div> : null}
    </div>
  );
}
