import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { LinkedThreadLink } from "@t3tools/contracts/jones/linkedThreads";
import { useNavigate } from "@tanstack/react-router";
import { ArrowRightIcon, GitForkIcon, LinkIcon, MoreHorizontalIcon } from "lucide-react";
import { useState } from "react";
import { ThreadDetailsControl } from "../../components/chat/ThreadDetailsControl";
import { Menu, MenuItem, MenuPopup, MenuTrigger } from "../../components/ui/menu";
import { Tooltip, TooltipTrigger, TooltipPopup } from "../../components/ui/tooltip";
import { buildThreadRouteParams } from "../../threadRoutes";
import { useEnvironment } from "../../state/environments";
import { useQuietThreadShell } from "../../state/entities";
import { resolveMachineIdentity } from "../remoteAgentLineage/model";
import type { LinkedEntry } from "./model";
import type { useLinkedThreads } from "./useLinkedThreads";

type View = ReturnType<typeof useLinkedThreads>;
export function LinkedEntryActions({
  entry,
  view,
}: {
  readonly entry: LinkedEntry;
  readonly view: View;
}) {
  const [busy, setBusy] = useState(false);
  const links = entry.relations.map((r) => r.link);
  const actionable = links.filter((link) => view.canUnlink(link) || view.canAccept(link));
  if (actionable.length === 0) return null;
  const run = async (link: LinkedThreadLink, action: "accept" | "unlink") => {
    if (busy) return;
    setBusy(true);
    try {
      await view[action](link);
    } finally {
      setBusy(false);
    }
  };
  return (
    <Menu>
      <MenuTrigger
        render={
          <ThreadDetailsControl
            size="icon-xs"
            variant="ghost"
            part="icon"
            aria-label={`Link actions: ${entry.labels.join(" · ")}`}
            disabled={busy}
          />
        }
      >
        <MoreHorizontalIcon className="size-3.5" />
      </MenuTrigger>
      <MenuPopup align="end">
        {actionable.map((link) => (
          <div key={link.linkId}>
            {view.canAccept(link) ? (
              <MenuItem onClick={() => void run(link, "accept")}>Accept handoff</MenuItem>
            ) : null}
            {view.canUnlink(link) ? (
              <MenuItem onClick={() => void run(link, "unlink")}>
                {links.length > 1
                  ? `Unlink ${entry.relations.find((relation) => relation.link.linkId === link.linkId)?.label.toLowerCase() ?? link.kind}`
                  : "Unlink"}
              </MenuItem>
            ) : null}
          </div>
        ))}
      </MenuPopup>
    </Menu>
  );
}

export function LinkedEntryLabels({ entry }: { readonly entry: LinkedEntry }) {
  const acceptance = entry.relations.some((r) => r.link.acceptance === "accepted")
    ? "accepted"
    : entry.relations.some((r) => r.link.acceptance === "offered")
      ? "offered"
      : null;
  return (
    <span className="block truncate text-left text-2xs font-normal text-muted-foreground">
      {entry.labels.join(" · ")}
      {acceptance ? (
        <>
          {" "}
          ·{" "}
          <span data-handoff-acceptance={acceptance} className="rounded border border-border px-1">
            {acceptance}
          </span>
        </>
      ) : null}
    </span>
  );
}

export function LinkedThreadRow({
  entry,
  view,
  subjectEnvironmentId,
}: {
  readonly entry: LinkedEntry;
  readonly view: View;
  readonly subjectEnvironmentId: LinkedThreadLink["from"]["environmentId"];
}) {
  const navigate = useNavigate();
  const environment = useEnvironment(entry.endpoint.environmentId);
  const shell = useQuietThreadShell(
    scopeThreadRef(entry.endpoint.environmentId, entry.endpoint.threadId),
  );
  const remote = entry.endpoint.environmentId !== subjectEnvironmentId;
  const online = environment?.connection.phase === "connected";
  const title = shell?.title || entry.snapshot?.title || entry.endpoint.threadId.slice(0, 12);
  const machine = resolveMachineIdentity(
    environment?.label ?? entry.snapshot?.environmentLabel,
    environment?.serverConfig?.environment.platform.machine,
  );
  const oneSided =
    entry.relations.some((r) => r.link.reverse === "unavailable") &&
    !view.answering.has(entry.endpoint.environmentId);
  const Icon = entry.relations.some((r) => r.link.kind === "launch") ? GitForkIcon : LinkIcon;
  return (
    <li
      className="group flex min-h-10 min-w-0 items-center rounded-lg"
      data-has-continuation={
        entry.relations.some((r) => r.link.kind === "continuation") || undefined
      }
    >
      <Tooltip>
        <TooltipTrigger
          delay={200}
          render={
            <ThreadDetailsControl
              size="sm"
              variant="ghost"
              part="row"
              multiline
              disabled={!online}
              aria-label={`Open ${title}${remote ? ` on ${machine.label}` : ""}`}
              onClick={() =>
                void navigate({
                  to: "/$environmentId/$threadId",
                  params: buildThreadRouteParams(
                    scopeThreadRef(entry.endpoint.environmentId, entry.endpoint.threadId),
                  ),
                })
              }
            />
          }
        >
          <Icon className="size-4 shrink-0" />
          <span className="min-w-0 flex-1">
            <span className="block truncate text-left text-sm font-medium">{title}</span>
            <LinkedEntryLabels entry={entry} />
          </span>
          {remote ? (
            <span className="shrink-0 text-2xs text-muted-foreground" aria-label={machine.label}>
              on {machine.shortLabel}
              {!online ? " · offline" : ""}
            </span>
          ) : null}
          {oneSided ? (
            <span
              className="shrink-0 text-2xs text-muted-foreground"
              aria-label="The other environment has not recorded this link"
            >
              One-sided
            </span>
          ) : null}
          <ArrowRightIcon className="size-3 shrink-0 text-muted-foreground" />
        </TooltipTrigger>
        <TooltipPopup side="left">
          {title} · {entry.endpoint.threadId}
          {!online ? " · Connect to this environment to open the thread" : ""}
        </TooltipPopup>
      </Tooltip>
      <LinkedEntryActions entry={entry} view={view} />
    </li>
  );
}
