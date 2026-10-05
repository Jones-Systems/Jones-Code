import type { EnvironmentId, VoiceReviewDraft } from "@t3tools/contracts";
import { MicIcon, PauseIcon, PlayIcon } from "lucide-react";
import { Fragment, useCallback, useEffect, useRef, useState, useSyncExternalStore } from "react";
import { useEnvironments, usePrimaryEnvironmentId } from "../../state/environments";
import { isElectron } from "../../env";
import { WorkspacePageContainer } from "../WorkspacePageContainer";
import { WorkspacePageHeader } from "../WorkspacePageHeader";
import { SidebarInset } from "../ui/sidebar";
import { Button } from "../ui/button";
import { Textarea } from "../ui/textarea";
import { useVoiceReview } from "./useVoiceReview";
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
        <h1>Voice review</h1>
      </WorkspacePageHeader>
      <div className="min-h-0 flex-1 overflow-y-auto">
        <WorkspacePageContainer>
          <p className="text-sm text-muted-foreground">
            Voice prompts release when their countdown ends. Double-click a prompt to edit it and
            pause delivery, or use Edit. Released means handed off for processing; it does not mean
            an agent has started.
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
  const { fetchList, transport } = useVoiceReview(environmentId);
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
        const recent = await fetchList("recent");
        if (!active) return;
        const combined = new Map(pending.drafts.map((draft) => [draft.id, draft]));
        recent.drafts.forEach((draft) => {
          if (!combined.has(draft.id)) combined.set(draft.id, draft);
        });
        setDrafts([...combined.values()]);
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
  const recent = drafts.filter((draft) => ["released", "deleted", "expired"].includes(draft.state));
  return (
    <>
      {error ? <p role="alert">{error}</p> : null}
      {loading ? <p role="status">Loading voice prompts…</p> : null}
      <section className="flex flex-col gap-3" aria-label="Voice prompts">
        <h2 className="font-medium">Pending</h2>
        {!loading && !error && pending.length === 0 ? (
          <p className="text-sm text-muted-foreground">No pending voice prompts.</p>
        ) : null}
        {[...pending, ...recent].map((draft, index) => (
          <Fragment key={draft.id}>
            {index === pending.length ? (
              <h2 className="font-medium">Recent releases and removals</h2>
            ) : null}
            <VoiceReviewRow
              draft={draft}
              transport={transport}
              now={now}
              unavailable={error !== null}
            />
          </Fragment>
        ))}
      </section>
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
