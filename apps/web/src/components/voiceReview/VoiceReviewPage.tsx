import type {
  EnvironmentId,
  VoiceReviewDraft,
  VoiceReviewRecentList,
  ThreadRegistryComposedSnapshot,
  ThreadRegistryWorkstreams,
} from "@t3tools/contracts";
import { MicIcon, PauseIcon, PlayIcon } from "lucide-react";
import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from "react";
import { useEnvironments, usePrimaryEnvironmentId } from "../../state/environments";
import { isElectron } from "../../env";
import { WorkspacePageContainer } from "../WorkspacePageContainer";
import { WorkspacePageHeader } from "../WorkspacePageHeader";
import { SidebarInset } from "../ui/sidebar";
import { Button } from "../ui/button";
import { Textarea } from "../ui/textarea";
import { useVoiceReview } from "./useVoiceReview";
import { RecentVoicePrompts } from "./RecentVoicePrompts";
import { RoutingDiagnostics } from "./RoutingDiagnostics";
import {
  remainingSeconds,
  VoiceReviewActions,
  voiceReviewError,
  type VoiceReviewTransport,
} from "./voiceReviewActions";

export function VoiceReviewPage() {
  const { environments } = useEnvironments();
  const primaryId = usePrimaryEnvironmentId();
  const [requestedId, setRequestedId] = useState<EnvironmentId | null>(null);
  const selected =
    environments.find((environment) => environment.environmentId === requestedId) ??
    environments.find((environment) => environment.environmentId === primaryId) ??
    environments[0];
  return (
    <SidebarInset className="h-dvh min-h-0 overflow-hidden">
      <WorkspacePageHeader electron={isElectron}>
        <MicIcon className="size-4" />
        <h1>Queue · voice prompts</h1>
      </WorkspacePageHeader>
      <div className="min-h-0 flex-1 overflow-y-auto">
        <WorkspacePageContainer>
          <p className="text-sm text-muted-foreground">
            Review voice prompts before they are released. Submitted work shows routing and delivery
            metadata. Voice prompts release when their countdown ends. Double-click a prompt to edit
            it and pause delivery, or use Edit. Released means handed off for processing; it does
            not mean an agent has started.
          </p>
          <label className="flex items-center gap-3 text-sm">
            Environment
            <select
              aria-label="Voice review environment"
              value={selected?.environmentId ?? ""}
              onChange={(event) => {
                const environment = environments.find(
                  (item) => item.environmentId === event.target.value,
                );
                if (environment) setRequestedId(environment.environmentId);
              }}
              className="rounded-md border bg-background px-2 py-1"
            >
              {environments.map((environment) => (
                <option key={environment.environmentId} value={environment.environmentId}>
                  {environment.label}
                </option>
              ))}
            </select>
          </label>
          {selected ? (
            <EnvironmentVoiceReview
              key={selected.environmentId}
              environmentId={selected.environmentId}
            />
          ) : (
            <p>No environment is connected.</p>
          )}
        </WorkspacePageContainer>
      </div>
    </SidebarInset>
  );
}

function EnvironmentVoiceReview({ environmentId }: { environmentId: EnvironmentId }) {
  const { fetchList, transport, review } = useVoiceReview(environmentId);
  const [tab, setTab] = useState<"review" | "routing">("review");
  const [recent, setRecent] = useState<VoiceReviewRecentList | null>(null);
  const [registry, setRegistry] = useState<ThreadRegistryComposedSnapshot | null>(null);
  const [workstreams, setWorkstreams] = useState<ThreadRegistryWorkstreams | null>(null);
  const [metadataError, setMetadataError] = useState(false);
  const metadataRequest = useRef(0);
  const refreshMetadata = useCallback(async () => {
    const request = ++metadataRequest.current;
    const results = await Promise.allSettled([
      review.recent(),
      review.registry(),
      review.workstreams(),
    ]);
    if (request !== metadataRequest.current) throw new Error("Metadata read superseded");
    const [prompts, threads, labels] = results;
    if (prompts.status === "fulfilled") setRecent(prompts.value);
    if (threads.status === "fulfilled") setRegistry(threads.value);
    else setRegistry(null);
    if (labels.status === "fulfilled") setWorkstreams(labels.value);
    else setWorkstreams(null);
    const failed = results.some((result) => result.status === "rejected");
    setMetadataError(failed);
    if (failed) throw new Error("Metadata unavailable");
  }, [review]);
  useEffect(() => {
    const refresh = () => {
      if (document.visibilityState === "visible") void refreshMetadata().catch(() => undefined);
    };
    refresh();
    const poll = window.setInterval(refresh, 10000);
    document.addEventListener("visibilitychange", refresh);
    return () => {
      metadataRequest.current++;
      window.clearInterval(poll);
      document.removeEventListener("visibilitychange", refresh);
    };
  }, [refreshMetadata]);
  const [drafts, setDrafts] = useState<readonly VoiceReviewDraft[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [now, setNow] = useState(() => performance.now());
  const refreshing = useRef(false);
  useEffect(() => {
    let active = true;
    const refresh = async () => {
      if (document.visibilityState !== "visible" || refreshing.current) return;
      refreshing.current = true;
      try {
        const pending = await fetchList("pending");
        if (!active) return;
        setDrafts(pending.drafts);
        setError(null);
      } catch (cause) {
        if (active) setError(voiceReviewError(cause));
      } finally {
        refreshing.current = false;
        if (active) setLoading(false);
      }
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
  }, [fetchList]);
  const pending = drafts.filter(
    (draft) => !["released", "deleted", "expired"].includes(draft.state),
  );
  const diagnosticDrafts = [
    ...new Map(
      [...drafts, ...(recent?.entries.map((entry) => entry.draft) ?? [])].map((draft) => [
        draft.id,
        draft,
      ]),
    ).values(),
  ];
  return (
    <>
      <div className="flex gap-2" role="tablist" aria-label="Voice review views">
        <Button
          variant={tab === "review" ? "default" : "outline"}
          role="tab"
          aria-selected={tab === "review"}
          aria-controls="voice-review-panel"
          id="voice-review-tab"
          onClick={() => setTab("review")}
        >
          Review
        </Button>
        <Button
          variant={tab === "routing" ? "default" : "outline"}
          role="tab"
          aria-selected={tab === "routing"}
          aria-controls="voice-routing-panel"
          id="voice-routing-tab"
          onClick={() => setTab("routing")}
        >
          Routing
        </Button>
      </div>
      {error ? <p role="alert">{error}</p> : null}
      {loading ? <p role="status">Loading voice prompts…</p> : null}
      <div
        role="tabpanel"
        aria-labelledby="voice-review-tab"
        id="voice-review-panel"
        hidden={tab !== "review"}
      >
        <div className="grid min-w-0 gap-6 xl:grid-cols-[minmax(0,1fr)_minmax(20rem,0.85fr)]">
          <section className="flex min-w-0 flex-col gap-3" aria-label="Pending voice prompts">
            <h2 className="font-medium">Pending</h2>
            {!loading && !error && pending.length === 0 ? (
              <p className="text-sm text-muted-foreground">No pending voice prompts.</p>
            ) : null}
            {pending.map((draft) => (
              <VoiceReviewRow
                key={draft.id}
                draft={draft}
                transport={transport}
                now={now}
                unavailable={error !== null}
              />
            ))}
          </section>
          <div className="flex min-w-0 flex-col gap-3">
            {metadataError ? (
              <p role="alert" className="text-sm">
                Recent prompts or workstreams are unavailable. Previously observed prompts may be
                stale.
              </p>
            ) : null}
            {recent?.partial || registry?.partial ? (
              <p className="text-xs text-muted-foreground">
                Only part of the recent prompt and thread context is available.
              </p>
            ) : null}
            {(recent?.unavailable.length ?? 0) + (registry?.unavailable.length ?? 0) > 0 ? (
              <p className="text-xs text-muted-foreground">
                Unavailable:{" "}
                {[...(recent?.unavailable ?? []), ...(registry?.unavailable ?? [])].join(", ")}
              </p>
            ) : null}
            <RecentVoicePrompts
              entries={recent?.entries ?? []}
              registry={registry}
              workstreams={workstreams}
              transport={review}
              onRefresh={refreshMetadata}
              unavailable={metadataError || recent?.partial === true}
            />
          </div>
        </div>
      </div>
      {tab === "routing" ? (
        <div role="tabpanel" aria-labelledby="voice-routing-tab" id="voice-routing-panel">
          <RoutingDiagnostics drafts={diagnosticDrafts} fetchDiagnostics={review.diagnostics} />
        </div>
      ) : null}
    </>
  );
}

function VoiceReviewRow({
  draft,
  transport,
  now,
  unavailable,
}: {
  draft: VoiceReviewDraft;
  transport: VoiceReviewTransport;
  now: number;
  unavailable: boolean;
}) {
  const [actions] = useState(() => new VoiceReviewActions(draft, transport));
  useSyncExternalStore(actions.subscribe, actions.snapshot);
  useEffect(() => {
    actions.receive(draft);
  }, [actions, draft]);
  const act = useCallback(
    (action: Parameters<VoiceReviewActions["act"]>[0], send = false) => {
      void actions.act(action, send);
    },
    [actions],
  );
  const current = actions.draft;
  const terminal = ["released", "deleted", "expired"].includes(current.state);
  const editing = actions.editHandle !== null && current.state === "editing";
  const disabled = actions.busy || actions.uncertain || unavailable;
  const seconds = remainingSeconds(current, Math.max(0, now - actions.observedAt));
  const validText = actions.text.trim().length > 0 && actions.text.length <= 100000;
  return (
    <article className="flex items-start gap-3 rounded-lg border p-4">
      {!terminal ? (
        <Button
          variant="outline"
          size="icon"
          aria-label={
            current.state === "held" || current.state === "editing" ? "Pause prompt" : "Play prompt"
          }
          disabled={disabled}
          onClick={() =>
            act(current.state === "held" || current.state === "editing" ? "pause" : "play")
          }
        >
          {current.state === "held" || current.state === "editing" ? <PauseIcon /> : <PlayIcon />}
        </Button>
      ) : null}
      <div className="flex min-w-0 flex-1 flex-col gap-3">
        <div className="flex flex-wrap items-center gap-2 text-sm">
          <strong>{actions.uncertain ? "Uncertain" : current.state}</strong>
          {current.state === "held" && seconds !== null ? <span>Release in {seconds}s</span> : null}
          {current.state === "paused" && seconds !== null ? (
            <span>{seconds}s remaining · paused</span>
          ) : null}
          <span className="text-muted-foreground">
            {current.source_id} · revision {current.revision}
          </span>
        </div>
        {actions.editHandle !== null ? (
          <Textarea
            aria-label="Edit voice prompt"
            value={actions.text}
            disabled={disabled || !editing}
            maxLength={100000}
            onChange={(event) => actions.setText(event.target.value)}
          />
        ) : current.text !== null ? (
          <p
            className="whitespace-pre-wrap break-words text-sm"
            onDoubleClick={() => {
              if (!disabled && !terminal && current.state !== "editing") act("edit-begin");
            }}
          >
            {current.text}
          </p>
        ) : null}
        {actions.editHandle !== null && !editing ? (
          <p className="text-sm">Unsaved text is preserved. Begin a new edit before saving it.</p>
        ) : null}
        {current.state === "editing" && !editing ? (
          <p className="text-sm">
            Editing is held by another tab or session. Pause to release an abandoned edit hold.
          </p>
        ) : null}
        {current.state === "blocked" ? (
          <p className="text-sm">
            Release blocked: {current.reason ?? "unavailable"}. Correct the cause before Play or
            Send now.
          </p>
        ) : null}
        {current.state === "released" ? (
          <p className="text-sm">
            Released · command {current.command_id ?? current.id} · command status{" "}
            {current.command_status ?? "not yet observed"}. Queue intake and execution require
            separate confirmation.
          </p>
        ) : null}
        {actions.error ? (
          <p role="alert" className="text-sm">
            {actions.error}
          </p>
        ) : null}
        <div className="flex flex-wrap gap-2">
          {editing ? (
            <>
              <Button
                size="compact"
                disabled={disabled || !validText}
                onClick={() => act("edit-save")}
              >
                Save · stay paused
              </Button>
              <Button
                size="compact"
                disabled={disabled || !validText}
                onClick={() => act("edit-save", true)}
              >
                Save and send
              </Button>
              <Button
                size="compact"
                variant="outline"
                disabled={disabled}
                onClick={() => act("edit-cancel")}
              >
                Cancel edit · stay paused
              </Button>
            </>
          ) : !terminal && current.state !== "editing" ? (
            <>
              <Button
                size="compact"
                variant="outline"
                disabled={disabled}
                onClick={() => act("edit-begin")}
              >
                Edit
              </Button>
              <Button size="compact" disabled={disabled} onClick={() => act("send-now")}>
                Send now
              </Button>
            </>
          ) : null}
          {!terminal ? (
            <Button
              size="compact"
              variant="outline"
              disabled={disabled}
              onClick={() => act("delete")}
            >
              Delete before send
            </Button>
          ) : null}
          {actions.error ? (
            <Button
              size="compact"
              variant="outline"
              disabled={actions.busy}
              onClick={() => void actions.reconcile()}
            >
              Check current state
            </Button>
          ) : null}
        </div>
      </div>
    </article>
  );
}
