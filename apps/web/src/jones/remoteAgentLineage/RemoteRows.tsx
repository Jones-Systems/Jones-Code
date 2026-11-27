import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { SideThreadOpenIntent } from "@t3tools/client-runtime/side-thread";
import { formatSubagentDisplayTitle } from "@t3tools/client-runtime/state/subagent-display";
import type {
  OrchestrationV2Subagent,
  OrchestrationV2RemoteTaskChild,
  ScopedThreadRef,
  ServerProvider,
} from "@t3tools/contracts";
import { deriveSubagentElapsedMs } from "@t3tools/shared/orchestrationTiming";
import { useNavigate } from "@tanstack/react-router";
import * as DateTime from "effect/DateTime";
import { BotIcon, CornerLeftUpIcon } from "lucide-react";
import { AgentElapsed } from "../../components/chat/AgentElapsed";
import { ThreadDetailsControl } from "../../components/chat/ThreadDetailsControl";
import {
  ThreadRelationshipIcon,
  threadRelationshipStatusLabel,
} from "../../components/chat/ThreadRelationshipIcon";
import { Tooltip, TooltipTrigger, TooltipPopup } from "../../components/ui/tooltip";
import { useSideThreadTrigger } from "../../hooks/useSideThreadTrigger";
import { openLinkedThreadRead } from "../../lib/sideChatPane";
import { buildThreadRouteParams } from "../../threadRoutes";
import { useEnvironment } from "../../state/environments";
import { useLinkedEnvironments } from "../../state/peerLinks";
import { useMachineLocation } from "./useMachineLocation";
import type { useParentLink } from "./useParentLink";

/**
 * Opens the delegated task directly when connected, or through the parent's
 * environment as a read-only view when it advertises a link to the destination.
 */
export function RemoteSubagentRow(props: {
  readonly agent: OrchestrationV2Subagent & {
    readonly remoteChild: OrchestrationV2RemoteTaskChild;
  };
  readonly parentRef: ScopedThreadRef;
  readonly provider: ServerProvider | undefined;
}) {
  const navigate = useNavigate();
  const trigger = useSideThreadTrigger();
  const { remoteChild } = props.agent;
  const viaEnvironmentId = props.parentRef.environmentId;
  const destination = useEnvironment(remoteChild.environmentId);
  const source = useEnvironment(viaEnvironmentId);
  const connected = destination?.connection.phase === "connected";
  const linkedLabel = useLinkedEnvironments(
    connected || source?.connection.phase !== "connected" ? null : viaEnvironmentId,
    remoteChild.environmentId,
  ).get(remoteChild.environmentId);
  const canRead = linkedLabel !== undefined;
  const canOpen = connected || canRead;
  const machine = useMachineLocation(remoteChild.environmentId, linkedLabel ?? remoteChild.label);
  const status = props.agent.status;
  const failed = status === "failed";
  const timing = {
    status,
    startedAt: props.agent.startedAt ? DateTime.formatIso(props.agent.startedAt) : null,
    completedAt: props.agent.completedAt ? DateTime.formatIso(props.agent.completedAt) : null,
  };
  const showElapsed = !failed && deriveSubagentElapsedMs(timing, 0) !== null;
  const title = formatSubagentDisplayTitle(props.agent.title ?? "Subagent");
  const label = machine?.label ?? destination?.label ?? remoteChild.label;
  const hint = connected
    ? machine
      ? `Open on ${machine.label}`
      : "Open delegated task"
    : canRead
      ? `Read the thread on ${label}`
      : `Runs on ${label}. Connect to it to open the thread there.`;
  const open = (intent: SideThreadOpenIntent) => {
    if (connected) {
      void navigate({
        to: "/$environmentId/$threadId",
        params: buildThreadRouteParams(
          scopeThreadRef(remoteChild.environmentId, remoteChild.threadId),
        ),
      });
    } else if (linkedLabel !== undefined) {
      openLinkedThreadRead({
        panelRef: props.parentRef,
        target: {
          viaEnvironmentId,
          environmentId: remoteChild.environmentId,
          threadId: remoteChild.threadId,
          label,
          title,
        },
        intent,
        navigate,
      });
    }
  };
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
              aria-label={`${title}${machine ? ` on ${machine.label}` : ""}`}
              aria-disabled={!canOpen || undefined}
              aria-description={!canOpen ? hint : undefined}
              {...(canOpen ? trigger(open, { panel: !connected }) : {})}
            />
          }
        >
          <ThreadRelationshipIcon
            driver={
              props.provider?.driver ??
              (props.agent.driver === "remote" ? undefined : props.agent.driver)
            }
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
            <span className="shrink-0 tabular-nums">
              {machine ? " · " : ""}
              {showElapsed ? (
                <AgentElapsed agent={timing} compact />
              ) : (
                threadRelationshipStatusLabel(status)
              )}
            </span>
          </span>
        </TooltipTrigger>
        <TooltipPopup side="left">
          <span className="block font-medium">{title}</span>
          <span className="block text-muted-foreground">{hint}</span>
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
              onClick={() => void parent.open?.()}
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
