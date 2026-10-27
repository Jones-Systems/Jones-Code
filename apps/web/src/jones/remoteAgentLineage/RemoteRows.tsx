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
import { resolveMachineIdentity } from "./model";
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
  const status = props.agent.status;
  const failed = status === "failed";
  const title = formatSubagentDisplayTitle(props.agent.title ?? "Subagent");
  return (
    <li className="group relative flex h-8 items-center rounded-lg">
      <Tooltip>
        <TooltipTrigger
          delay={200}
          render={
            <ThreadDetailsControl
              size="sm"
              variant="ghost"
              part="row"
              aria-label={`${title} on ${environment?.label ?? remoteChild.label}`}
              disabled={environment?.connection.phase !== "connected"}
              onClick={() =>
                void navigate({
                  to: "/$environmentId/$threadId",
                  params: buildThreadRouteParams(
                    scopeThreadRef(remoteChild.environmentId, remoteChild.threadId),
                  ),
                })
              }
            />
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
          <span
            className={`shrink-0 text-2xs ${failed ? "text-destructive" : "text-muted-foreground"}`}
          >
            on{" "}
            {
              resolveMachineIdentity(
                environment?.label ?? remoteChild.label,
                environment?.serverConfig?.environment.platform.machine,
              ).shortLabel
            }{" "}
            · {threadRelationshipStatusLabel(status)}
          </span>
        </TooltipTrigger>
        <TooltipPopup side="left">
          {title}.{" "}
          {environment?.connection.phase !== "connected"
            ? `Runs on ${remoteChild.label}. Connect to it to open the thread there.`
            : `Open on ${remoteChild.label}`}
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
          <TooltipTrigger
            render={
              <ThreadDetailsControl
                size="sm"
                variant="ghost"
                part="row"
                disabled={!parent.open}
                onClick={parent.open}
                aria-label={`Parent agent: ${parent.title}`}
                aria-description={parent.unavailableReason}
              />
            }
          >
            <CornerLeftUpIcon className="size-4 shrink-0" />
            <span className="min-w-0 flex-1 truncate text-left">{parent.title}</span>
          </TooltipTrigger>
          <TooltipPopup side="left">
            {parent.unavailableReason
              ? `${parent.title}. ${parent.unavailableReason}`
              : `Open parent agent: ${parent.title}`}
          </TooltipPopup>
        </Tooltip>
      </li>
    </ul>
  );
}
