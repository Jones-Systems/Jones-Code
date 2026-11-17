import type {
  EnvironmentId,
  ThreadRegistryComposedSnapshot,
  ThreadRegistryWorkstreams,
  VoiceReviewAction,
  VoiceReviewDraft,
  VoiceReviewRecentList,
} from "@t3tools/contracts";
import { useEffect, useEffectEvent, useId, useRef, useState, useSyncExternalStore } from "react";
import { Alert, AlertAction, AlertDescription, AlertTitle } from "../ui/alert";
import { Button } from "../ui/button";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "../ui/empty";
import { Skeleton } from "../ui/skeleton";
import { Textarea } from "../ui/textarea";
import { useMediaQuery } from "../../hooks/useMediaQuery";
import { promptAttention } from "../../jones/workQueue/promptAttention";
import { buildPromptItems, voiceItemKey, type PromptItem } from "../../jones/workQueue/promptItems";
import { type PromptCount } from "../../jones/workQueue/promptOrganize";
import {
  draftStatus,
  promptProgressSteps,
  routingStateLabel,
  textStateLabel,
} from "../../jones/workQueue/promptPresentation";
import {
  PromptAction,
  PromptActions,
  PromptDetail,
  PromptDisclosure,
  PromptEvidence,
} from "../../jones/workQueue/PromptDetail";
import {
  filteredPromptGroups,
  PromptFilteredEmpty,
  type PromptListView,
} from "../../jones/workQueue/PromptToolbar";
import { promptLink } from "../../jones/workQueue/promptSearch";
import { PromptWorkstreams } from "../../jones/workQueue/PromptWorkstreamDetails";
import { PromptMasterDetail } from "../../jones/workQueue/PromptList";
import { PromptProgress } from "../../jones/workQueue/PromptProgress";
import { displayText, usePromptTime, type PromptRowView } from "../../jones/workQueue/PromptRow";
import { useVoiceReview } from "./useVoiceReview";
import { RoutingDiagnostics } from "./RoutingDiagnostics";
import {
  remainingSeconds,
  VoiceReviewActions,
  voiceReviewError,
  type VoiceReviewTransport,
} from "./voiceReviewActions";

const PENDING_LIMIT = 50;
const terminalStates: ReadonlySet<string> = new Set(["released", "deleted", "expired"]);
const conflictMessage = voiceReviewError({ _tag: "VoiceReviewConflictError" });

/** An immutable view of one draft's action controller, rebuilt whenever it changes. */
interface PendingDraftView {
  readonly draft: VoiceReviewDraft;
  readonly text: string;
  /** This session holds the edit handle, so unsaved text is local. */
  readonly ownsEdit: boolean;
  readonly busy: boolean;
  readonly uncertain: boolean;
  readonly error: string | null;
  readonly observedAt: number;
  readonly dirty: boolean;
}

/**
 * Owns one `VoiceReviewActions` controller per pending draft so edits, in-flight actions and
 * uncertain outcomes survive selection changes. A draft that leaves the pending list drops its
 * controller, as the per-row controllers did before.
 */
class PendingDraftStore {
  private readonly controllers = new Map<
    string,
    { readonly actions: VoiceReviewActions; readonly unsubscribe: () => void }
  >();
  private readonly listeners = new Set<() => void>();
  private views: ReadonlyMap<string, PendingDraftView> = new Map();
  constructor(private readonly transport: VoiceReviewTransport) {}
  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  getSnapshot = () => this.views;
  private publish = () => {
    this.views = new Map(
      [...this.controllers].map(([id, { actions }]) => [
        id,
        {
          draft: actions.draft,
          text: actions.text,
          ownsEdit: actions.editHandle !== null,
          busy: actions.busy,
          uncertain: actions.uncertain,
          error: actions.error,
          observedAt: actions.observedAt,
          dirty: actions.editHandle !== null || actions.busy || actions.uncertain,
        },
      ]),
    );
    for (const listener of this.listeners) listener();
  };
  sync(drafts: readonly VoiceReviewDraft[]) {
    const present = new Set(drafts.map((draft) => draft.id));
    for (const [id, controller] of this.controllers) {
      if (present.has(id)) continue;
      controller.unsubscribe();
      this.controllers.delete(id);
    }
    for (const draft of drafts) {
      const existing = this.controllers.get(draft.id);
      if (existing) {
        existing.actions.receive(draft);
        continue;
      }
      const actions = new VoiceReviewActions(draft, this.transport);
      this.controllers.set(draft.id, { actions, unsubscribe: actions.subscribe(this.publish) });
    }
    this.publish();
  }
  act(id: string, action: VoiceReviewAction, sendAfterSave = false) {
    void this.controllers.get(id)?.actions.act(action, sendAfterSave);
  }
  setText(id: string, text: string) {
    this.controllers.get(id)?.actions.setText(text);
  }
  reconcile(id: string) {
    void this.controllers.get(id)?.actions.reconcile();
  }
}

/**
 * The Pending stage for one environment: voice drafts held before release, shown as a list
 * with one detail pane. It stays mounted while other stages are open so local edits and
 * uncertain outcomes are retained, and reports them to the page leave guard.
 */
export function EnvironmentVoiceReview({
  environmentId,
  pane,
  onDirtyChange,
  unavailable = false,
  onPendingCountChange,
  listView,
  linkedKey,
  onLinkSelection,
  workstreamNamesUnavailable,
}: {
  environmentId: EnvironmentId;
  pane?: "pending" | "queued" | "sent";
  onDirtyChange?: (dirty: boolean) => void;
  unavailable?: boolean;
  onPendingCountChange?: (count: PromptCount | null) => void;
  listView?: PromptListView | undefined;
  linkedKey?: string | undefined;
  onLinkSelection?: ((key: string) => void) | undefined;
  workstreamNamesUnavailable?: boolean | undefined;
}) {
  const { fetchList, transport, review } = useVoiceReview(environmentId);
  const [drafts, setDrafts] = useState<readonly VoiceReviewDraft[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [now, setNow] = useState(() => performance.now());
  const refreshing = useRef(false);
  const fetchPending = useEffectEvent(() => fetchList("pending"));
  const [store] = useState(() => new PendingDraftStore(transport));
  const views = useSyncExternalStore(store.subscribe, store.getSnapshot, store.getSnapshot);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Own the visible pending-draft refresh subscription and ignore late results after unmount.
  useEffect(() => {
    let active = true;
    const refresh = async () => {
      if (document.visibilityState !== "visible" || refreshing.current) return;
      refreshing.current = true;
      return await (async () => {
        try {
          const pending = await fetchPending();
          if (!active) return;
          setDrafts(pending.drafts);
          setError(null);
        } catch (cause) {
          if (active) setError(voiceReviewError(cause));
        }
      })().finally(() => {
        refreshing.current = false;
        if (active) setLoading(false);
      });
    };
    const onVisibility = () => {
      if (document.visibilityState === "visible") void refresh();
    };
    void refresh();
    const poll = window.setInterval(() => void refresh(), 3000);
    const tick = window.setInterval(() => {
      if (document.visibilityState === "visible") setNow(performance.now());
    }, 1000);
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      active = false;
      window.clearInterval(poll);
      window.clearInterval(tick);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, []);
  const pending = drafts.filter((draft) => !terminalStates.has(draft.state));
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Apply committed server draft observations to the external action controllers.
  useEffect(() => {
    store.sync(drafts.filter((draft) => !terminalStates.has(draft.state)));
  }, [store, drafts]);
  const dirty = [...views.values()].some((view) => view.dirty);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Report retained local work to the page leave guard.
  useEffect(() => {
    onDirtyChange?.(dirty);
  }, [dirty, onDirtyChange]);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Release the page leave guard when this environment's pending stage unmounts.
  useEffect(() => () => onDirtyChange?.(false), [onDirtyChange]);
  const pendingCount = loading && drafts.length === 0 ? null : pending.length;
  const qualifier: PromptCount["qualifier"] =
    unavailable || error !== null ? "?" : drafts.length >= PENDING_LIMIT ? "+" : "";
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Publish the pending count to the page tabs when it changes.
  useEffect(() => {
    onPendingCountChange?.(pendingCount === null ? null : { count: pendingCount, qualifier });
  }, [pendingCount, qualifier, onPendingCountChange]);

  const items = buildPromptItems({
    drafts: pending,
    recent: [],
    snapshot: null,
    nativeEnvironmentId: null,
  }).pending;
  const blocked = unavailable || error !== null;
  const localAttention = (item: PromptItem) => {
    const view = item.draft ? views.get(item.draft.id) : undefined;
    return { uncertain: view?.uncertain ?? false, ownsEdit: view?.ownsEdit ?? false };
  };
  const groups = filteredPromptGroups(
    items,
    "pending",
    listView,
    {
      attention: (item) => promptAttention(item, localAttention(item)),
      destinationLabel: (item) =>
        displayText(item.draft?.routing_target, "Destination not observed yet"),
      workstream: () => null,
      queueStateLabel: () => "",
    },
    () => [],
  );
  const wide = useMediaQuery("md");
  const [localSelection, setSelected] = useState<string | null>(null);
  const selected = linkedKey ?? localSelection;
  const [switchTo, setSwitchTo] = useState<string | null>(null);
  const firstKey = groups[0]?.items[0]?.key ?? null;
  if (wide && selected === null && firstKey !== null) setSelected(firstKey);
  const selectedItem = items.find((item) => item.key === selected) ?? null;
  const selectedDirty =
    selectedItem?.draft !== null && selectedItem?.draft !== undefined
      ? (views.get(selectedItem.draft.id)?.dirty ?? false)
      : false;
  const commitSelection = (key: string) => {
    setSelected(key);
    setSwitchTo(null);
    onLinkSelection?.(key);
  };
  const choose = (key: string) => {
    if (key === selected) return;
    if (selectedDirty) setSwitchTo(key);
    else commitSelection(key);
  };
  const formatTime = usePromptTime();
  const rows = (item: PromptItem): PromptRowView => {
    const view = item.draft ? views.get(item.draft.id) : undefined;
    const draft = view?.draft ?? item.draft!;
    const seconds = remainingSeconds(draft, Math.max(0, now - (view?.observedAt ?? now)));
    const status = draftStatus(draft, {
      seconds,
      busy: view?.busy ?? false,
      uncertain: view?.uncertain ?? false,
      conflict: view?.error === conflictMessage,
      ownsEdit: view?.ownsEdit ?? false,
      unavailable: blocked,
    });
    const text = view?.ownsEdit ? view.text : draft.text;
    return {
      key: item.key,
      environmentId,
      threadId: null,
      fallbackTitle: displayText(draft.routing_target, "Destination not observed yet"),
      destinationCount: 1,
      status,
      preview: text ? displayText(text, textStateLabel(item.text)) : textStateLabel(item.text),
      previewMuted: !text,
      source: draft.edited ? "Voice · edited" : "Voice",
      workstreams: [],
      promptHref: onLinkSelection
        ? promptLink({ source: environmentId, stage: "pending", prompt: item.key })
        : undefined,
      time: item.time,
      dirty: view?.dirty ?? false,
      gone: false,
    };
  };
  return (
    <div hidden={pane !== undefined && pane !== "pending"} className="min-w-0 space-y-3">
      {error ? (
        <p role="alert" className="text-sm">
          {error}
        </p>
      ) : null}
      {switchTo !== null ? (
        <Alert variant="warning">
          <AlertTitle>Switch prompts?</AlertTitle>
          <AlertDescription>
            This prompt has unsaved text, an action in progress, or an outcome that is not
            confirmed. Switching keeps that work with this prompt. It does not save, resume, or
            retry anything.
          </AlertDescription>
          <AlertAction>
            <Button variant="outline" onClick={() => setSwitchTo(null)}>
              Keep reviewing
            </Button>
            <Button
              onClick={() => {
                commitSelection(switchTo);
              }}
            >
              Switch prompt
            </Button>
          </AlertAction>
        </Alert>
      ) : null}
      {qualifier !== "" && pending.length > 0 ? (
        <p className="text-xs text-muted-foreground">Searching {pending.length} loaded prompts.</p>
      ) : null}
      {loading && drafts.length === 0 ? (
        <div role="status" aria-label="Loading voice prompts…" className="space-y-1.5">
          <Skeleton className="h-16 w-full" />
          <Skeleton className="h-16 w-full" />
          <Skeleton className="h-16 w-full" />
        </div>
      ) : (
        <PromptMasterDetail
          label="Pending prompts"
          groups={groups.map((group) => ({
            key: group.key,
            label: group.label,
            rows: group.items.map(rows),
          }))}
          showGroupHeadings={groups.length > 1}
          selectedKey={selected}
          onSelect={choose}
          viewKey={listView?.key}
          empty={
            listView && items.length > 0 ? (
              <PromptFilteredEmpty view={listView} />
            ) : error ? null : (
              <Empty size="compact">
                <EmptyHeader>
                  <EmptyTitle>Nothing waiting for review.</EmptyTitle>
                  <EmptyDescription>
                    Prompts arrive from your voice source and from work queues connected to this
                    server.
                  </EmptyDescription>
                </EmptyHeader>
              </Empty>
            )
          }
          detail={
            selectedItem?.draft ? (
              <PendingPromptDetail
                key={selectedItem.key}
                item={selectedItem}
                workstreams={
                  <PromptWorkstreams
                    environmentId={environmentId}
                    lines={[]}
                    namesUnavailable={workstreamNamesUnavailable ?? false}
                  />
                }
                view={views.get(selectedItem.draft.id)}
                now={now}
                unavailable={blocked}
                store={store}
                fetchDiagnostics={review.diagnostics}
                formatTime={formatTime}
                onSelectDependency={(id) => {
                  const key = voiceItemKey(id);
                  if (items.some((item) => item.key === key)) choose(key);
                }}
                dependencyPresent={(id) => items.some((item) => item.key === voiceItemKey(id))}
              />
            ) : selected !== null ? (
              <section className="rounded-md border p-4 text-sm text-muted-foreground">
                {linkedKey
                  ? "This prompt is not in the current sample."
                  : "This prompt left Pending. It may now be under Queued."}
              </section>
            ) : (
              <section className="rounded-md border p-4 text-sm text-muted-foreground">
                Select a prompt to see where it is going and what you can do.
              </section>
            )
          }
        />
      )}
    </div>
  );
}

function PendingPromptDetail({
  item,
  workstreams,
  view,
  now,
  unavailable,
  store,
  fetchDiagnostics,
  formatTime,
  onSelectDependency,
  dependencyPresent,
}: {
  item: PromptItem;
  workstreams: import("react").ReactNode;
  view: PendingDraftView | undefined;
  now: number;
  unavailable: boolean;
  store: PendingDraftStore;
  fetchDiagnostics: Parameters<typeof RoutingDiagnostics>[0]["fetchDiagnostics"];
  formatTime: (ms: number) => string;
  onSelectDependency: (id: string) => void;
  dependencyPresent: (id: string) => boolean;
}) {
  const reasonId = useId();
  const [routingOpen, setRoutingOpen] = useState(false);
  const draft = view?.draft ?? item.draft!;
  const destination = displayText(draft.routing_target, "Destination not observed yet");
  const ownsEdit = view?.ownsEdit ?? false;
  const editing = ownsEdit && draft.state === "editing";
  const terminal = terminalStates.has(draft.state);
  const seconds = remainingSeconds(draft, Math.max(0, now - (view?.observedAt ?? now)));
  const status = draftStatus(draft, {
    seconds,
    busy: view?.busy ?? false,
    uncertain: view?.uncertain ?? false,
    conflict: view?.error === conflictMessage,
    ownsEdit,
    unavailable,
  });
  const reason = !view
    ? "Preparing prompt actions…"
    : unavailable
      ? "Server disconnected or voice review unavailable. Actions are unavailable."
      : view.busy
        ? "Working…"
        : view.uncertain
          ? "Outcome unknown. Check the current state before acting again."
          : null;
  const text = view?.text ?? draft.text ?? "";
  const validText = text.trim().length > 0 && text.length <= 100000;
  const saveReason = reason ?? (editing && !validText ? "Enter prompt text before saving." : null);
  const act = (action: VoiceReviewAction, send = false) => store.act(draft.id, action, send);
  const routing = routingStateLabel(draft.routing_state, draft.routing_target, draft.dependencies);
  const holding = draft.state === "held" || draft.state === "editing";
  return (
    <PromptDetail
      workstreams={workstreams}
      title={destination}
      status={status}
      meta={
        <span>
          Voice · captured {item.time === null ? "time not reported" : formatTime(item.time)}
        </span>
      }
      progress={
        <PromptProgress
          label={`Progress for prompt to ${destination}`}
          steps={promptProgressSteps({ ...item, draft }, null, status)}
          formatTime={formatTime}
        />
      }
      actions={
        terminal ? null : editing ? (
          <PromptActions
            destructive={
              <PromptAction
                label={`Cancel edit of prompt to ${destination}`}
                reason={reason}
                reasonId={reasonId}
                onClick={() => act("edit-cancel")}
              >
                Cancel edit · stay paused
              </PromptAction>
            }
          >
            <PromptAction
              label={`Save prompt to ${destination} and stay paused`}
              reason={saveReason}
              reasonId={reasonId}
              onClick={() => act("edit-save")}
            >
              Save · stay paused
            </PromptAction>
            <PromptAction
              label={`Save and send prompt to ${destination}`}
              variant="default"
              reason={saveReason}
              reasonId={reasonId}
              onClick={() => act("edit-save", true)}
            >
              Save and send
            </PromptAction>
          </PromptActions>
        ) : (
          <PromptActions
            destructive={
              <PromptAction
                label={`Delete prompt to ${destination} before send`}
                variant="destructive-outline"
                reason={reason}
                reasonId={reasonId}
                onClick={() => act("delete")}
              >
                Delete before send
              </PromptAction>
            }
          >
            <PromptAction
              label={`${holding ? "Pause" : "Resume"} prompt to ${destination}`}
              reason={reason}
              reasonId={reasonId}
              onClick={() => act(holding ? "pause" : "play")}
            >
              {holding ? "Pause" : "Resume"}
            </PromptAction>
            {draft.state !== "editing" ? (
              <>
                <PromptAction
                  label={`Edit prompt to ${destination}`}
                  reason={reason}
                  reasonId={reasonId}
                  onClick={() => act("edit-begin")}
                >
                  Edit
                </PromptAction>
                <PromptAction
                  label={`Send prompt to ${destination} now`}
                  variant="default"
                  reason={reason}
                  reasonId={reasonId}
                  onClick={() => act("send-now")}
                >
                  Send now
                </PromptAction>
              </>
            ) : null}
          </PromptActions>
        )
      }
      actionStatus={
        <>
          {saveReason ? <p id={reasonId}>{saveReason}</p> : null}
          {view?.error ? (
            <div className="space-y-1">
              <p role="alert">{view.error}</p>
              <Button
                size="sm"
                variant="outline"
                aria-disabled={view.busy || unavailable || undefined}
                onClick={() => {
                  if (!view.busy && !unavailable) store.reconcile(draft.id);
                }}
              >
                Check current state
              </Button>
            </div>
          ) : null}
          {ownsEdit && !editing ? (
            <p>Unsaved text is preserved. Begin a new edit before saving it.</p>
          ) : null}
        </>
      }
      routing={
        <PromptDisclosure summary="Routing details" onToggle={setRoutingOpen}>
          <p className="text-foreground">{routing.label}</p>
          {routing.detail ? <p>{routing.detail}</p> : null}
          {draft.routing_target ? (
            <p>Proposed destination: {displayText(draft.routing_target, "Not reported")}</p>
          ) : null}
          {(draft.dependencies ?? []).length > 0 ? (
            <ul className="m-0 list-none space-y-1 p-0" aria-label="Earlier prompts this waits on">
              {(draft.dependencies ?? []).map((id) => (
                <li key={id} className="flex flex-wrap items-center gap-2">
                  <bdi className="wrap-anywhere">{displayText(id, "Unnamed prompt")}</bdi>
                  {dependencyPresent(id) ? (
                    <Button size="xs" variant="ghost" onClick={() => onSelectDependency(id)}>
                      Show
                    </Button>
                  ) : (
                    <span>Not in this sample</span>
                  )}
                </li>
              ))}
            </ul>
          ) : null}
          <p>Routing is decided by the voice source. It cannot be changed here.</p>
          {routingOpen ? (
            <RoutingDiagnostics draftId={draft.id} fetchDiagnostics={fetchDiagnostics} />
          ) : null}
        </PromptDisclosure>
      }
      evidence={
        <PromptEvidence
          item={{ ...item, draft }}
          formatTime={formatTime}
          workstreamName={(id) => id}
        />
      }
    >
      {ownsEdit ? (
        <Textarea
          aria-label={`Prompt text to ${destination}`}
          value={text}
          disabled={reason !== null || !editing}
          maxLength={100000}
          onChange={(event) => store.setText(draft.id, event.target.value)}
        />
      ) : draft.text !== null ? (
        <p
          className="whitespace-pre-wrap text-sm wrap-anywhere"
          onDoubleClick={() => {
            if (reason === null && !terminal && draft.state !== "editing") act("edit-begin");
          }}
        >
          {draft.text}
        </p>
      ) : (
        <p className="text-sm text-muted-foreground">Prompt text is unavailable.</p>
      )}
    </PromptDetail>
  );
}

export interface VoiceReviewMetadata {
  readonly recent: VoiceReviewRecentList | null;
  readonly registry: ThreadRegistryComposedSnapshot | null;
  readonly workstreams: ThreadRegistryWorkstreams | null;
  /** The last recent-prompts read failed; `recent` is the previous observation, if any. */
  readonly recentFailed: boolean;
  /** The thread registry or workstream label read failed. Only corrections depend on these. */
  readonly labelsFailed: boolean;
  readonly refresh: () => Promise<void>;
  readonly review: ReturnType<typeof useVoiceReview>["review"];
}

/**
 * Recent voice prompts and their Workstream labels, polled every 10 seconds while visible. A
 * failed label read is reported separately so it disables only Workstream corrections.
 */
export function useVoiceReviewMetadata(
  environmentId: EnvironmentId,
  enabled: boolean,
): VoiceReviewMetadata {
  const { review } = useVoiceReview(environmentId);
  const [recent, setRecent] = useState<VoiceReviewRecentList | null>(null);
  const [registry, setRegistry] = useState<ThreadRegistryComposedSnapshot | null>(null);
  const [workstreams, setWorkstreams] = useState<ThreadRegistryWorkstreams | null>(null);
  const [recentFailed, setRecentFailed] = useState(false);
  const [labelsFailed, setLabelsFailed] = useState(false);
  const metadataRequest = useRef(0);
  const refresh = async () => {
    const request = ++metadataRequest.current;
    const results = await Promise.allSettled([
      review.recent(),
      review.registry(),
      review.workstreams(),
    ]);
    if (request !== metadataRequest.current) throw new Error("Metadata read superseded");
    const [prompts, threads, labels] = results;
    if (prompts.status === "fulfilled") setRecent(prompts.value);
    setRecentFailed(prompts.status === "rejected");
    setRegistry(threads.status === "fulfilled" ? threads.value : null);
    setWorkstreams(labels.status === "fulfilled" ? labels.value : null);
    setLabelsFailed(threads.status === "rejected" || labels.status === "rejected");
    if (results.some((result) => result.status === "rejected"))
      throw new Error("Metadata unavailable");
  };
  const refreshFromPoll = useEffectEvent(refresh);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Own metadata polling and visibility listeners, fencing responses when the page unmounts.
  useEffect(() => {
    if (!enabled) return;
    const poll = () => {
      if (document.visibilityState === "visible") void refreshFromPoll().catch(() => undefined);
    };
    poll();
    const timer = window.setInterval(poll, 10000);
    document.addEventListener("visibilitychange", poll);
    return () => {
      metadataRequest.current++;
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", poll);
    };
  }, [enabled]);
  return { recent, registry, workstreams, recentFailed, labelsFailed, refresh, review };
}
