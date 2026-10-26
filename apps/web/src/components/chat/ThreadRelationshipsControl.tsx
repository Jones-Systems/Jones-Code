import { RemoteSubagentRow, RemoteParentRow } from "../../jones/remoteAgentLineage/RemoteRows";
import { groupLineageRows, resolveMachineIdentity } from "../../jones/remoteAgentLineage/model";
import { useParentLink } from "../../jones/remoteAgentLineage/useParentLink";
import {
  LinkedThreadsSection,
  LinkedContinuationFocusContext,
} from "../../jones/linkedThreads/LinkedThreadsSection";
import { LinkedLineageGroups } from "../../jones/linkedThreads/LinkedLineageGroups";
import { buildLinkedEntries, type LinkedEntry } from "../../jones/linkedThreads/model";
import {
  LinkedThreadRow,
  LinkedEntryLabels,
  LinkedEntryActions,
} from "../../jones/linkedThreads/LinkedThreadRow";
import { LinkedThreadsActions } from "../../jones/linkedThreads/LinkedThreadsActions";
import { useLinkedThreads } from "../../jones/linkedThreads/useLinkedThreads";
import { useAtomValue } from "@effect/atom-react";
import { ThreadDetailsControl } from "./ThreadDetailsControl";
import { ThreadHoverCardPopup } from "../ThreadHoverCard";

import { SectionHeaderStatus } from "../ui/collapsible-section-header";
import { SubagentTooltipContent } from "./SubagentTooltipContent";
import { PullRequestGlyph } from "../pullRequest/pullRequestIcons";
import { scopedThreadKey, scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  deriveWorkflowGroups,
  projectedSubagentsToRuntime,
  type RuntimeSubagent,
} from "@t3tools/client-runtime/state/subagentRuntime";
import { formatSubagentDisplayTitle } from "@t3tools/client-runtime/state/subagent-display";
import { deriveProviderSubagentStatus } from "@t3tools/client-runtime/state/thread-execution";
import {
  deriveThreadRelationshipGraph,
  immediateThreadRelationships,
  isParentThreadRelationship,
  threadRelationshipRowStatus,
  orderWebThreadLineageRows,
  resolveMergeBackTargetThreadId,
  type ThreadRelationshipEdge,
} from "@t3tools/client-runtime/state/thread-relationships";
import {
  canDetachThreadProviderSession,
  resolveLatestMergeBackRun,
} from "@t3tools/client-runtime/state/thread-workflows";
import {
  isOrchestrationV2WorkActive,
  isProviderNativeSubagentThread,
  NodeId,
  type EnvironmentId,
  type OrchestrationV2Subagent,
  type OrchestrationV2ThreadShell,
  type ThreadId,
} from "@t3tools/contracts";
import { deriveSubagentElapsedMs } from "@t3tools/shared/orchestrationTiming";
import * as DateTime from "effect/DateTime";
import { useNavigate } from "@tanstack/react-router";
import {
  ArrowRightIcon,
  BotIcon,
  ChevronDownIcon,
  CornerLeftUpIcon,
  GitForkIcon,
  LoaderCircleIcon,
  PlusIcon,
  SquareIcon,
} from "lucide-react";
import { useContext, useEffect, useRef, useState, type ReactNode } from "react";

import { useArchivedThreadSnapshots } from "../../lib/archivedThreadsState";
import { buildThreadRouteParams } from "../../threadRoutes";
import {
  useProjects,
  useThreadProjection,
  useThreadRelationshipShells,
} from "../../state/entities";
import { useEnvironment, useEnvironmentProviders } from "../../state/environments";
import { threadEnvironment } from "../../state/threads";
import { orchestrationEnvironment } from "../../state/orchestration";
import { useAtomCommand } from "../../state/use-atom-command";
import { AgentElapsed } from "./AgentElapsed";
import { ThreadRelationshipIcon, threadRelationshipStatusLabel } from "./ThreadRelationshipIcon";
import { ThreadLineageWorkflowRow } from "./ThreadLineageWorkflowRow";

import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import { toastManager } from "../ui/toast";
import { cn } from "~/lib/utils";
import {
  THREAD_DETAILS_PANEL_CHEVRON_CLASS,
  THREAD_DETAILS_PANEL_ICON_CLASS,
  THREAD_DETAILS_PANEL_LABEL_CLASS,
  THREAD_DETAILS_PANEL_SPLIT_GROUP_CLASS,
  THREAD_DETAILS_PANEL_ROW_CONTENT_CLASS,
  THREAD_DETAILS_PANEL_SPLIT_SEPARATOR_CLASS,
} from "./threadDetailsPanelStyles";

const THREAD_LINEAGE_INITIAL_COUNT = 6;
const THREAD_LINEAGE_PAGE_COUNT = 12;

function resolveThreadLineageWindow<Row>(
  rows: ReadonlyArray<Row>,
  visibleCount: number,
): { readonly visibleRows: ReadonlyArray<Row>; readonly hiddenCount: number } {
  const visibleRows = rows.slice(0, visibleCount);
  return { visibleRows, hiddenCount: rows.length - visibleRows.length };
}

function ThreadLineageRowList(props: {
  readonly hiddenCount: number;
  readonly onShowMore: () => void;
  readonly children: ReactNode;
}) {
  return (
    <>
      {/* ScrollArea's max-height viewport measures badly inside this scrolling panel. */}
      <ul
        aria-label="Related threads"
        className="m-0 max-h-[13.5rem] list-none overflow-y-auto overscroll-contain p-0 has-data-workflow-expanded:max-h-[min(28rem,55dvh)]"
      >
        {props.children}
      </ul>
      {props.hiddenCount > 0 ? (
        <button
          type="button"
          onClick={props.onShowMore}
          className={`flex h-8 w-full cursor-pointer items-center rounded-lg ${THREAD_DETAILS_PANEL_ROW_CONTENT_CLASS} text-sm font-medium text-muted-foreground/70 hover:bg-black/[0.055] hover:text-foreground/80 dark:hover:bg-white/[0.075]`}
        >
          <PlusIcon aria-hidden className="size-4 shrink-0" />
          Show {Math.min(props.hiddenCount, THREAD_LINEAGE_PAGE_COUNT)} more
        </button>
      ) : null}
    </>
  );
}

function ThreadLineageGroup(props: {
  readonly label: string | null;
  readonly id: string;
  readonly rows: ReadonlyArray<LinkedEntry>;
  readonly expanded: boolean;
  readonly children: (rows: ReadonlyArray<LinkedEntry>) => ReactNode;
}) {
  const [manuallyExpanded, setManuallyExpanded] = useState(props.expanded);
  const [manualFocusRequest, setManualFocusRequest] = useState(0);
  const focusRequest = useContext(LinkedContinuationFocusContext);
  const hasContinuation = props.rows.some((entry) =>
    entry.relations.some((relation) => relation.link.kind === "continuation"),
  );
  const expanded = manuallyExpanded || (hasContinuation && focusRequest > manualFocusRequest);
  const handledFocus = useRef(0);
  const element = useRef<HTMLDivElement>(null);
  // oxlint-disable-next-line t3code/react-effect-budget -- Focus the continuation control after its asynchronous lineage becomes available for the pending focus request.
  useEffect(() => {
    if (focusRequest <= handledFocus.current || !hasContinuation) return;
    handledFocus.current = focusRequest;
    element.current?.querySelector<HTMLButtonElement>("button")?.focus();
  }, [focusRequest, hasContinuation]);
  const [visibleCount, setVisibleCount] = useState(THREAD_LINEAGE_INITIAL_COUNT);
  const { visibleRows, hiddenCount } = resolveThreadLineageWindow(props.rows, visibleCount);
  const failedCount = props.rows.filter((entry) =>
    entry.lineage.some(({ status }) => status === "failed" || status === "error"),
  ).length;
  if (props.rows.length === 0) return null;
  return (
    <div ref={element} data-linked-group={props.id}>
      {props.label ? (
        <ThreadDetailsControl
          aria-expanded={expanded}
          onClick={() => {
            setManuallyExpanded(!expanded);
            setManualFocusRequest(focusRequest);
          }}
        >
          <BotIcon aria-hidden className={THREAD_DETAILS_PANEL_ICON_CLASS} />
          <span className={THREAD_DETAILS_PANEL_LABEL_CLASS}>{props.label}</span>
          <span aria-hidden className="min-w-2 flex-1" />
          {failedCount > 0 ? <SectionHeaderStatus>{failedCount} failed</SectionHeaderStatus> : null}
          {expanded ? null : (
            <span className="text-xs font-normal tabular-nums text-muted-foreground">
              {props.rows.length}
            </span>
          )}
          <ChevronDownIcon
            aria-hidden
            className={cn(
              THREAD_DETAILS_PANEL_CHEVRON_CLASS,
              "transition-transform",
              !expanded && "rotate-180 redesign:-rotate-90",
            )}
          />
        </ThreadDetailsControl>
      ) : null}
      {expanded ? (
        <ThreadLineageRowList
          hiddenCount={hiddenCount}
          onShowMore={() => setVisibleCount((count) => count + THREAD_LINEAGE_PAGE_COUNT)}
        >
          {props.children(visibleRows)}
        </ThreadLineageRowList>
      ) : null}
    </div>
  );
}

function relationshipLabel(edge: ThreadRelationshipEdge, currentThreadId: ThreadId) {
  if (edge.kind === "transfer") return "Context transfer";
  if (edge.kind === "subagent") {
    return edge.sourceThreadId === currentThreadId ? "Subagent" : "Parent agent";
  }
  return edge.sourceThreadId === currentThreadId ? "Fork" : "Parent thread";
}

function relationshipThreadTitle(input: {
  readonly title: string;
  readonly isSubagent: boolean;
}): string {
  if (!input.isSubagent) return input.title;
  return formatSubagentDisplayTitle(input.title);
}

function currentSubagent<Agent extends RuntimeSubagent>(
  agent: Agent | undefined,
  childThread: OrchestrationV2ThreadShell | null | undefined,
): Agent | undefined {
  if (!agent || !childThread || isOrchestrationV2WorkActive(agent.status)) return agent;
  const liveStatus =
    childThread.activityRunStatus ?? (childThread.status === "queued" ? "queued" : null);
  const newerRun =
    childThread.latestRunRequestedAt &&
    DateTime.toEpochMillis(childThread.latestRunRequestedAt) >
      Date.parse(agent.completedAt ?? agent.startedAt ?? agent.updatedAt);
  if (!liveStatus && !newerRun) return agent;
  const status = liveStatus ?? childThread.status;
  const startedAt = liveStatus ? childThread.activityRunStartedAt : childThread.latestRunStartedAt;
  const completedAt = liveStatus ? null : childThread.latestRunCompletedAt;
  return {
    ...agent,
    status:
      status === "preparing" || status === "starting" || status === "queued"
        ? "pending"
        : status === "rolled_back"
          ? "interrupted"
          : status,
    startedAt: startedAt ? DateTime.formatIso(startedAt) : null,
    completedAt: completedAt ? DateTime.formatIso(completedAt) : null,
    progress: null,
    result: null,
    error: null,
  };
}

export function ThreadRelationshipsPanel(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
}) {
  const ref = scopeThreadRef(props.environmentId, props.threadId);
  const projection = useThreadProjection(ref)?.projection ?? null;
  const parentLink = useParentLink(props.environmentId, projection?.thread);
  const providersByEnvironment = useEnvironmentProviders();
  const environment = useEnvironment(props.environmentId);
  const machineLabel = resolveMachineIdentity(
    environment?.label,
    environment?.serverConfig?.environment.platform.machine,
  ).shortLabel;
  const relationshipShells = useThreadRelationshipShells(ref);
  const allProjects = useProjects();
  const archived = useArchivedThreadSnapshots([props.environmentId]);
  const archivedShells = archived.snapshots.find(
    (entry) => entry.environmentId === props.environmentId,
  )?.snapshot.threads;
  const currentShell =
    relationshipShells.find((thread) => thread.id === props.threadId) ??
    archivedShells?.find((thread) => thread.id === props.threadId);
  const currentThread = projection?.thread ?? currentShell;
  const linked = useLinkedThreads(
    ref,
    currentShell?.activityRunStatus != null ||
      currentShell?.status === "running" ||
      currentShell?.status === "queued",
    currentShell?.latestRunRequestedAt ? DateTime.formatIso(currentShell.latestRunRequestedAt) : "",
  );
  const parentThreadId =
    currentThread?.forkedFrom?.type === "run"
      ? currentThread.forkedFrom.threadId
      : currentThread?.lineage.parentThreadId;
  const parentShell = parentThreadId
    ? (relationshipShells.find((thread) => thread.id === parentThreadId) ??
      archivedShells?.find((thread) => thread.id === parentThreadId))
    : undefined;
  const parentRef =
    parentShell && isProviderNativeSubagentThread(parentShell)
      ? scopeThreadRef(props.environmentId, parentShell.id)
      : null;
  const parentProjection = useThreadProjection(parentRef)?.projection;
  const navigate = useNavigate();
  const mergeBack = useAtomCommand(threadEnvironment.mergeBack);
  const stopSession = useAtomCommand(threadEnvironment.stopSession);
  const interruptTurn = useAtomCommand(threadEnvironment.interruptTurn);
  const stopWorkflowCommand = useAtomCommand(orchestrationEnvironment.stopWorkflow);
  const canOperateWorkflow = useAtomValue(
    orchestrationEnvironment.stopWorkflow.permissionAtom(props.environmentId),
  );
  const [busyAction, setBusyAction] = useState<"merge" | "detach" | null>(null);
  const [stoppingId, setStoppingId] = useState<string | null>(null);
  // Derive graphs after hooks to preserve their compiler cache.
  const providers = providersByEnvironment.get(props.environmentId);
  const projects = allProjects.filter((project) => project.environmentId === props.environmentId);
  const parentNativeStatus = parentProjection
    ? deriveProviderSubagentStatus(parentProjection)?.status
    : undefined;
  const projected = projection?.subagents ?? [];
  const runtimeSubagents = projectedSubagentsToRuntime(projected);
  const byId = new Map(runtimeSubagents.map((agent) => [agent.id, agent]));
  const subagentsByThreadId = new Map(
    projected
      .filter((subagent) => subagent.childThreadId !== null && subagent.remoteChild === undefined)
      .map((subagent) => [
        subagent.childThreadId,
        {
          ...byId.get(subagent.id)!,
          id: subagent.id,
          threadId: subagent.threadId,
          runId: subagent.runId,
          nativeTaskRef: subagent.nativeTaskRef,
          nativeStatus: subagent.status,
          driver: subagent.driver,
          providerInstanceId: subagent.providerInstanceId,
          origin: subagent.origin,
          modelSelection: subagent.modelSelection,
        },
      ]),
  );
  const workflowGroupsById = new Map(
    deriveWorkflowGroups(runtimeSubagents).map((group) => [group.workflow.id, group]),
  );
  const shells: ReadonlyArray<OrchestrationV2ThreadShell> = [
    ...relationshipShells,
    ...(archivedShells ?? []),
  ];
  const graph = deriveThreadRelationshipGraph({
    threads: shells,
    projection:
      projection === null
        ? null
        : {
            ...projection,
            subagents: projected.filter((agent) => agent.remoteChild === undefined),
          },
  });
  const currentProject = projects.find((project) => project.id === currentThread?.projectId);
  const latestMergeBackRun = projection === null ? null : resolveLatestMergeBackRun(projection);
  const mergeTargetThreadId = resolveMergeBackTargetThreadId(projection);
  const relationshipRows = orderWebThreadLineageRows({
    graph,
    rows: immediateThreadRelationships(graph, props.threadId),
    currentThreadId: props.threadId,
    mergeTargetThreadId,
  }).map((row) => {
    const agent =
      row.edge.kind === "subagent" && !isParentThreadRelationship(row.edge, props.threadId)
        ? subagentsByThreadId.get(row.threadId)
        : undefined;
    const workflowGroup = agent === undefined ? undefined : workflowGroupsById.get(agent.id);
    return workflowGroup === undefined
      ? row
      : { ...row, edge: { ...row.edge, status: workflowGroup.workflow.status } };
  });
  const canMerge = mergeTargetThreadId !== null && latestMergeBackRun !== null;
  const canDetach = projection ? canDetachThreadProviderSession(projection) : false;

  const { related, active, previous, runningCount } = groupLineageRows(
    relationshipRows,
    projected,
    props.threadId,
  );
  const entries = buildLinkedEntries({
    subject: ref,
    related,
    active,
    previous,
    relations: linked.relations,
    snapshots: linked.snapshots,
    ...(parentLink?.remote ? { remoteParent: parentLink.ref } : {}),
  });
  if (entries.length === 0 && runningCount === 0 && !parentLink?.remote && !linked.visible)
    return null;

  const openThread = (threadId: ThreadId) => {
    void navigate({
      to: "/$environmentId/$threadId",
      params: buildThreadRouteParams(scopeThreadRef(props.environmentId, threadId)),
    });
  };

  const merge = async () => {
    if (!latestMergeBackRun || mergeTargetThreadId === null || busyAction !== null) return;
    setBusyAction("merge");
    const result = await mergeBack({
      environmentId: props.environmentId,
      input: {
        sourceThreadId: props.threadId,
        targetThreadId: mergeTargetThreadId,
        runId: latestMergeBackRun.id,
      },
    });
    setBusyAction(null);
    if (result._tag === "Success") openThread(mergeTargetThreadId);
  };

  const detach = async () => {
    if (!canDetach || busyAction !== null) return;
    setBusyAction("detach");
    await stopSession({
      environmentId: props.environmentId,
      input: { threadId: props.threadId },
    });
    setBusyAction(null);
  };

  const stopSubagent = async (
    childThreadId: ThreadId,
    agent: Pick<OrchestrationV2Subagent, "id" | "threadId">,
  ) => {
    if (stoppingId !== null) return;
    setStoppingId(childThreadId);
    const result = await interruptTurn({
      environmentId: props.environmentId,
      input: { threadId: agent.threadId, subagentId: agent.id },
    });
    setStoppingId(null);
    if (result._tag === "Failure") {
      toastManager.add({ type: "error", title: "Could not stop subagent" });
    }
  };

  const stopWorkflow = async (subagentId: string) => {
    if (stoppingId !== null) return;
    setStoppingId(subagentId);
    const result = await stopWorkflowCommand({
      environmentId: props.environmentId,
      input: { threadId: props.threadId, subagentId: NodeId.make(subagentId) },
    });
    setStoppingId(null);
    if (result._tag === "Failure") {
      toastManager.add({ type: "error", title: "Could not stop workflow" });
    }
  };

  const parentTitle =
    mergeTargetThreadId === null
      ? null
      : (graph.nodes.get(mergeTargetThreadId)?.thread?.title ?? null);

  return (
    <LinkedThreadsSection
      key={scopedThreadKey(ref)}
      runningCount={runningCount}
      count={entries.length}
      incomplete={linked.incomplete}
      continued={linked.hasSuccessor}
      onExpand={linked.onExpand}
      actions={
        <LinkedThreadsActions
          key={scopedThreadKey(ref)}
          subject={ref}
          view={linked}
          title={currentThread?.title ?? props.threadId}
          environmentLabel={environment?.label ?? "unknown environment"}
          canDetach={canDetach}
          busy={busyAction !== null}
          onDetach={() => void detach()}
        />
      }
    >
      <LinkedLineageGroups
        entries={entries}
        renderGroup={(group) => (
          <ThreadLineageGroup key={`${scopedThreadKey(ref)}:${group.id}`} {...group}>
            {(visibleRows) =>
              visibleRows.flatMap((entry) => {
                if (entry.lineage.length === 0) {
                  if (entry.remoteParent && parentLink)
                    return (
                      <li key={entry.key}>
                        <div className="flex min-w-0 items-center">
                          <div className="min-w-0 flex-1">
                            <RemoteParentRow parent={parentLink} />
                          </div>
                          <LinkedEntryActions entry={entry} view={linked} />
                        </div>
                        <LinkedEntryLabels entry={entry} />
                      </li>
                    );
                  return (
                    <LinkedThreadRow
                      key={entry.key}
                      entry={entry}
                      view={linked}
                      subjectEnvironmentId={props.environmentId}
                    />
                  );
                }
                return entry.lineage.map((lineageRow) => {
                  if (lineageRow.kind === "remote") {
                    const agent = lineageRow.agent;
                    const remoteRow = (
                      <RemoteSubagentRow
                        key={`${scopedThreadKey(scopeThreadRef(agent.remoteChild.environmentId, agent.remoteChild.threadId))}:${agent.id}`}
                        agent={agent}
                        // Its provider instance belongs to the environment it runs in.
                        provider={providersByEnvironment
                          .get(agent.remoteChild.environmentId)
                          ?.find((entry) => entry.instanceId === agent.providerInstanceId)}
                      />
                    );
                    return entry.relations.length === 0 ? (
                      remoteRow
                    ) : (
                      <li key={entry.key}>
                        <div className="flex min-w-0 items-center">
                          <ul className="m-0 min-w-0 flex-1 list-none p-0">{remoteRow}</ul>
                          <LinkedEntryActions entry={entry} view={linked} />
                        </div>
                        <LinkedEntryLabels entry={entry} />
                      </li>
                    );
                  }
                  const { threadId, edge } = lineageRow.row;
                  const node = graph.nodes.get(threadId);
                  const isSubagent = edge.kind === "subagent";
                  const isMergeTarget = threadId === mergeTargetThreadId;
                  const isParent = isParentThreadRelationship(edge, props.threadId);
                  const rowStatus =
                    (isParent && threadId === parentRef?.threadId
                      ? (node?.thread?.activityRunStatus ?? parentNativeStatus)
                      : undefined) ?? threadRelationshipRowStatus(graph, { threadId, edge });
                  const RelationshipIcon = isParent
                    ? CornerLeftUpIcon
                    : isSubagent
                      ? BotIcon
                      : GitForkIcon;
                  const relationship = relationshipLabel(edge, props.threadId);
                  const projectedAgent =
                    isSubagent && !isParent ? subagentsByThreadId.get(threadId) : undefined;
                  const workflowGroup =
                    projectedAgent === undefined
                      ? undefined
                      : workflowGroupsById.get(projectedAgent.id);
                  const agent = workflowGroup
                    ? projectedAgent
                    : currentSubagent(projectedAgent, node?.thread);
                  const failed = rowStatus === "failed" || rowStatus === "error";
                  const canStop =
                    agent &&
                    (agent.origin === "app_owned" ||
                      (agent.origin === "provider_native" &&
                        agent.driver === "claudeAgent" &&
                        agent.runId != null &&
                        agent.nativeTaskRef?.strength === "strong" &&
                        agent.nativeTaskRef.nativeId !== null &&
                        ["pending", "running", "waiting"].includes(agent.nativeStatus))) &&
                    agent.startedAt &&
                    ["pending", "running", "waiting"].includes(agent.status);
                  const threadTitle = relationshipThreadTitle({
                    title:
                      workflowGroup?.workflow.workflowName ??
                      node?.thread?.title ??
                      agent?.title ??
                      threadId,
                    isSubagent,
                  });
                  const provider = providers?.find(
                    (entry) =>
                      entry.instanceId ===
                      (agent?.providerInstanceId ?? node?.thread?.providerInstanceId),
                  );
                  const providerDriver = agent?.driver ?? provider?.driver;
                  const canStopWorkflow =
                    canOperateWorkflow &&
                    workflowGroup?.workflow.status === "running" &&
                    agent?.origin === "provider_native" &&
                    providerDriver === "claudeAgent";
                  const trailingVisibilityClass = (workflowGroup ? canStopWorkflow : canStop)
                    ? "group-hover:opacity-0 group-focus-within:opacity-0 pointer-coarse:opacity-0 [@media(hover:none)]:opacity-0"
                    : "";
                  const project = projects.find(
                    (project) => project.id === node?.thread?.projectId,
                  );
                  const relationshipHint = node?.missing
                    ? "This related thread is unavailable"
                    : `Open ${relationship.toLowerCase()} in this chat`;
                  const RelationshipPopup = agent ? ThreadHoverCardPopup : TooltipPopup;
                  const relationshipTooltip = agent ? (
                    <SubagentTooltipContent
                      title={threadTitle}
                      model={agent.model}
                      providerInstanceId={agent.providerInstanceId}
                      origin={agent.origin}
                      modelSelection={agent.modelSelection}
                      provider={provider}
                      providers={providers}
                      driver={providerDriver}
                      elapsed={<AgentElapsed agent={agent} />}
                      status={agent.status}
                      result={agent.result}
                      progress={agent.progress}
                      parentThread={currentThread ?? undefined}
                      childThread={node?.thread ?? undefined}
                      parentProject={currentProject}
                      childProject={project}
                    />
                  ) : (
                    relationshipHint
                  );
                  const relationshipContent = (
                    <>
                      <ThreadRelationshipIcon
                        driver={isSubagent && !isParent ? providerDriver : undefined}
                        provider={provider}
                        fallbackIcon={RelationshipIcon}
                        status={rowStatus}
                      />
                      <span className="min-w-0 flex-1">
                        <span className="block truncate text-left text-sm font-medium leading-4 text-foreground/85">
                          {threadTitle}
                        </span>
                        <LinkedEntryLabels entry={entry} />
                      </span>
                      {agent ? null : (
                        <ArrowRightIcon className="size-3 shrink-0 text-muted-foreground opacity-0 transition-opacity group-hover:opacity-100" />
                      )}
                      {agent && !workflowGroup ? (
                        <span
                          className={`shrink-0 text-2xs ${failed ? "text-destructive" : "text-muted-foreground"} ${trailingVisibilityClass}`}
                        >
                          on {machineLabel} · {threadRelationshipStatusLabel(rowStatus)}
                        </span>
                      ) : agent &&
                        !failed &&
                        (deriveSubagentElapsedMs(agent, 0) !== null || canStopWorkflow) ? (
                        <span
                          className={`shrink-0 text-2xs font-normal tabular-nums text-muted-foreground ${canStopWorkflow ? "min-w-6 text-end" : ""} ${trailingVisibilityClass}`}
                        >
                          <AgentElapsed agent={agent} compact />
                        </span>
                      ) : !isMergeTarget ? (
                        <span
                          className={
                            workflowGroup
                              ? "sr-only"
                              : `shrink-0 text-2xs ${failed ? "text-destructive" : "text-muted-foreground"} ${trailingVisibilityClass}`
                          }
                        >
                          {threadRelationshipStatusLabel(rowStatus)}
                        </span>
                      ) : null}
                    </>
                  );
                  const relationshipLink = (
                    <Tooltip>
                      <TooltipTrigger
                        delay={200}
                        render={
                          <ThreadDetailsControl
                            size="sm"
                            variant="ghost"
                            part={workflowGroup ? "primary" : "row"}
                            disabled={node?.missing === true}
                            onClick={() => openThread(threadId)}
                          />
                        }
                      >
                        {relationshipContent}
                      </TooltipTrigger>
                      <RelationshipPopup side="left">{relationshipTooltip}</RelationshipPopup>
                    </Tooltip>
                  );
                  if (workflowGroup && agent) {
                    return (
                      <ThreadLineageWorkflowRow
                        key={threadId}
                        group={workflowGroup}
                        providerInstanceId={agent.providerInstanceId}
                        provider={provider}
                        providers={providers}
                        driver={providerDriver}
                        onOpenThread={(memberThreadId) => openThread(memberThreadId as ThreadId)}
                        isThreadAvailable={(memberThreadId) =>
                          graph.nodes.get(memberThreadId as ThreadId)?.missing === false
                        }
                        onStop={canStopWorkflow ? () => void stopWorkflow(agent.id) : undefined}
                        stopping={stoppingId === agent.id}
                        stopDisabled={stoppingId !== null}
                        header={
                          <div
                            data-has-continuation={
                              entry.relations.some(
                                (relation) => relation.link.kind === "continuation",
                              ) || undefined
                            }
                            className="flex min-w-0 items-center"
                          >
                            {relationshipLink}
                            <LinkedEntryActions entry={entry} view={linked} />
                          </div>
                        }
                      />
                    );
                  }
                  return (
                    <li
                      key={threadId}
                      data-has-continuation={
                        entry.relations.some((relation) => relation.link.kind === "continuation") ||
                        undefined
                      }
                      className="group relative flex h-8 items-center rounded-lg"
                    >
                      {isMergeTarget ? (
                        <div className={THREAD_DETAILS_PANEL_SPLIT_GROUP_CLASS}>
                          <Tooltip>
                            <TooltipTrigger
                              delay={200}
                              render={
                                <ThreadDetailsControl
                                  size="sm"
                                  variant="ghost"
                                  part="primary"
                                  aria-label={`${threadTitle} ${threadRelationshipStatusLabel(rowStatus)}`}
                                  disabled={node?.missing === true}
                                  onClick={() => openThread(threadId)}
                                />
                              }
                            >
                              {relationshipContent}
                            </TooltipTrigger>
                            <RelationshipPopup side="left">{relationshipTooltip}</RelationshipPopup>
                          </Tooltip>
                          <span
                            aria-hidden="true"
                            className={THREAD_DETAILS_PANEL_SPLIT_SEPARATOR_CLASS}
                          />
                          <Tooltip>
                            <TooltipTrigger
                              render={
                                <ThreadDetailsControl
                                  size="sm"
                                  variant="ghost"
                                  part="secondary"
                                  aria-label={
                                    parentTitle
                                      ? `Merge back to ${parentTitle}`
                                      : "Merge back to source conversation"
                                  }
                                  disabled={!canMerge || busyAction !== null}
                                  onClick={() => void merge()}
                                >
                                  {busyAction === "merge" ? (
                                    <LoaderCircleIcon className="size-3 animate-spin" />
                                  ) : (
                                    <PullRequestGlyph.merged className="size-3" />
                                  )}
                                </ThreadDetailsControl>
                              }
                            />
                            <TooltipPopup side="left">
                              {latestMergeBackRun === null
                                ? "Complete a run in this fork before merging it back"
                                : parentTitle
                                  ? `Merge this conversation back into ${parentTitle}`
                                  : "Merge this conversation back into its source"}
                            </TooltipPopup>
                          </Tooltip>
                          <span className="shrink-0 border border-transparent ps-1 pe-2.5 text-2xs font-medium text-muted-foreground">
                            {threadRelationshipStatusLabel(rowStatus)}
                          </span>
                        </div>
                      ) : (
                        relationshipLink
                      )}
                      <LinkedEntryActions entry={entry} view={linked} />
                      {canStop && agent ? (
                        <div
                          className={`pointer-events-none absolute ${entry.relations.length > 0 ? "right-7" : "right-1"} top-1/2 -translate-y-1/2 opacity-0 group-hover:pointer-events-auto group-hover:opacity-100 group-focus-within:pointer-events-auto group-focus-within:opacity-100 pointer-coarse:pointer-events-auto pointer-coarse:opacity-100 [@media(hover:none)]:pointer-events-auto [@media(hover:none)]:opacity-100`}
                        >
                          <Tooltip>
                            <TooltipTrigger
                              render={
                                <ThreadDetailsControl
                                  size="icon-xs"
                                  variant="ghost"
                                  part="icon"
                                  tone="destructive"
                                  aria-label={`Stop subagent ${threadTitle}`}
                                  disabled={stoppingId !== null}
                                  onClick={() => void stopSubagent(threadId, agent)}
                                />
                              }
                            >
                              {stoppingId === threadId ? (
                                <LoaderCircleIcon aria-hidden className="size-3 animate-spin" />
                              ) : (
                                <SquareIcon aria-hidden className="size-3 fill-current" />
                              )}
                            </TooltipTrigger>
                            <TooltipPopup side="left">Stop subagent</TooltipPopup>
                          </Tooltip>
                        </div>
                      ) : null}
                    </li>
                  );
                });
              })
            }
          </ThreadLineageGroup>
        )}
      />
      <div className="space-y-1 px-2 pt-1 text-2xs text-muted-foreground" aria-live="polite">
        {linked.loading ? <p>Loading linked threads…</p> : null}
        {linked.truncated ? <p>Results are truncated; more links may exist.</p> : null}
        {linked.errors.map((id) => (
          <p key={id}>
            Could not read links from{" "}
            {linked.environments.find((env) => env.environmentId === id)?.label ?? id}; may hold
            more links.
          </p>
        ))}
        {linked.offline.map((env) => (
          <p key={env.environmentId}>{env.label} is offline and may hold more links.</p>
        ))}
      </div>
    </LinkedThreadsSection>
  );
}
