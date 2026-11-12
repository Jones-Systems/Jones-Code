import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { formatSubagentDisplayTitle } from "@t3tools/client-runtime/state/subagent-display";
import type {
  OrchestrationV2Subagent,
  OrchestrationV2RemoteTaskChild,
  ServerProvider,
} from "@t3tools/contracts";
import { useNavigate } from "@tanstack/react-router";
import { BotIcon, CornerLeftUpIcon } from "lucide-react";
import { ThreadDetailsControl } from "../../components/chat/ThreadDetailsControl";
import {
  ThreadRelationshipIcon,
  threadRelationshipStatusLabel,
} from "../../components/chat/ThreadRelationshipIcon";
import { Tooltip, TooltipTrigger, TooltipPopup } from "../../components/ui/tooltip";
import { buildThreadRouteParams } from "../../threadRoutes";
import { useEnvironment } from "../../state/environments";
import { useMachineLocation } from "./useMachineLocation";
import type { useParentLink } from "./useParentLink";

/**
 * A task delegated to a linked environment. It has no thread here, so it opens
 * the thread there, when this client is connected to that environment.
 */
export function RemoteSubagentRow(props: {
  readonly agent: OrchestrationV2Subagent & {
    readonly remoteChild: OrchestrationV2RemoteTaskChild;
  };
  readonly provider: ServerProvider | undefined;
}) {
  const navigate = useNavigate();
  const environment = useEnvironment(props.agent.remoteChild.environmentId);
  const { remoteChild } = props.agent;
  const machine = useMachineLocation(remoteChild.environmentId, remoteChild.label);
  const status = props.agent.status;
  const failed = status === "failed";
  const title = formatSubagentDisplayTitle(props.agent.title ?? "Subagent");
  return (
    <li className="group relative flex h-8 items-center rounded-lg">
      <Tooltip>
        <TooltipTrigger delay={200} render={<span className="flex min-w-0 flex-1" />}>
          <ThreadDetailsControl
            size="sm"
            variant="ghost"
            part="row"
            aria-label={`${title}${machine ? ` on ${machine.label}` : ""}`}
            disabled={environment?.connection.phase !== "connected"}
            onClick={() =>
              void navigate({
                to: "/$environmentId/$threadId",
                params: buildThreadRouteParams(
                  scopeThreadRef(remoteChild.environmentId, remoteChild.threadId),
                ),
              })
            }
          >
            <ThreadRelationshipIcon
              driver={props.agent.driver}
              provider={props.provider}
              fallbackIcon={BotIcon}
              status={status}
            />
            <span className="min-w-0 flex-1">
              <span className="block truncate text-left text-sm font-medium leading-4 text-foreground/85">
                {title}
              </span>
            </span>
            {/* A long machine label truncates instead of squeezing the title out of the row. */}
            <span
              className={`flex max-w-1/2 shrink-0 gap-1 text-2xs ${failed ? "text-destructive" : "text-muted-foreground"}`}
            >
              {machine ? <span className="truncate">on {machine.shortLabel}</span> : null}
              <span className="shrink-0">
                {machine ? " · " : ""}
                {threadRelationshipStatusLabel(status)}
              </span>
            </span>
          </ThreadDetailsControl>
        </TooltipTrigger>
        <TooltipPopup side="left">
          {title}.{" "}
          {environment?.connection.phase !== "connected"
            ? machine
              ? `Runs on ${machine.label}. Connect to it to open the thread there.`
              : "Connect to the local environment to open the thread."
            : machine
              ? `Open on ${machine.label}`
              : "Open delegated task"}
        </TooltipPopup>
      </Tooltip>
    </li>
  );
}

export function RemoteParentRow({
  parent,
}: {
  parent: NonNullable<ReturnType<typeof useParentLink>>;
}) {
  return (
    <ul className="m-0 list-none p-0" aria-label="Parent agent">
      <li className="flex h-8 min-w-0 items-center">
        <Tooltip>
          <TooltipTrigger render={<span className="flex min-w-0 flex-1" />}>
            <ThreadDetailsControl
              size="sm"
              variant="ghost"
              part="row"
              disabled={!parent.open}
              onClick={parent.open}
              aria-label={`Parent agent: ${parent.title}`}
              aria-description={parent.unavailableReason}
            >
              <CornerLeftUpIcon className="size-4 shrink-0" />
              <span className="min-w-0 flex-1 truncate text-left">{parent.title}</span>
            </ThreadDetailsControl>
          </TooltipTrigger>
          <TooltipPopup side="left">
            {parent.unavailableReason
              ? `${parent.title}. ${parent.unavailableReason} Delegated from a linked environment. Connect to it to open the parent thread.`
              : `Open parent agent: ${parent.title}`}
          </TooltipPopup>
        </Tooltip>
      </li>
    </ul>
  );
}
