/* eslint-disable shadcn/no-unknown-classes -- This isolated native peer uses the dedicated monitor.css stylesheet instead of Tailwind utilities. */
import { randomUUID } from "../../lib/utils";
import { useStableCallback } from "../../hooks/useStableCallback";
import {
  useLayoutEffect,
  useEffect,
  useRef,
  useState,
  useSyncExternalStore,
  type CSSProperties,
  type ReactNode,
  type ReactElement,
} from "react";
import { CommandId, MessageId } from "@t3tools/contracts";
import {
  isRecentMonitorCompletion,
  monitorDraftKey,
  monitorThreadKey,
} from "@t3tools/client-runtime/jones/thread-monitor";
import {
  THREAD_MONITOR_LIMITS,
  type ThreadMonitorAction,
  type ThreadMonitorRow,
  type ThreadMonitorStatus,
} from "@t3tools/contracts/jones/desktopThreadMonitor";
import { Tooltip, TooltipTrigger, TooltipPopup } from "~/components/ui/tooltip";
import { observeResize } from "~/lib/observeResize";
import { MonitorPeerStore } from "./peerStore";
import { GROUPS, refOf } from "./store";
import { captureMonitorAnchor, restoreMonitorAnchor, MonitorAnchoredScroll } from "./scrollAnchor";

const countLabel = (value: number) =>
  value >= 1_000_000_000
    ? `${Number((value / 1_000_000_000).toFixed(1))}B`
    : value >= 1_000_000
      ? `${Number((value / 1_000_000).toFixed(2))}M`
      : String(value);
const titles = { needsYou: "Needs You", working: "Working", waiting: "Waiting", done: "Done" };
const glyphRole = (status: ThreadMonitorStatus | "needsYou") =>
  status === "completed" ||
  status === "interrupted" ||
  status === "cancelled" ||
  status === "rolled_back"
    ? "done"
    : status === "checkpoint"
      ? "working"
      : status === "idle" || status === "unknown"
        ? "waiting"
        : status === "failed" || status === "limited"
          ? "approval"
          : status;
function Glyph({ status }: { status: ThreadMonitorStatus | "needsYou" | "done" }) {
  return (
    <span
      aria-hidden="true"
      className={`glyph ${status === "done" ? "done" : glyphRole(status)}`}
    />
  );
}
function MonitorTooltip({ children, content }: { children: ReactElement; content: ReactNode }) {
  return (
    <Tooltip>
      <TooltipTrigger render={children} disabled={!content} />
      <TooltipPopup data-monitor-tooltip>{content}</TooltipPopup>
    </Tooltip>
  );
}
function Icon({ children }: { children: ReactNode }) {
  return (
    <svg
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      {children}
    </svg>
  );
}
const idFor = (row: ThreadMonitorRow) => ({
  ...refOf(row),
  actionId: randomUUID(),
  commandId: CommandId.make(randomUUID()),
});
function age(row: ThreadMonitorRow, now: number) {
  const stamp = row.group === "done" ? row.completedAt : row.updatedAt;
  if (stamp === null) return "—";
  const minutes = Math.max(0, Math.floor((now - Date.parse(stamp)) / 60_000));
  return minutes === 0
    ? "now"
    : minutes < 60
      ? `${minutes}m`
      : minutes < 1440
        ? `${Math.floor(minutes / 60)}h`
        : `${Math.floor(minutes / 1440)}d`;
}
function sameThread(
  a: { environmentId: string; threadId: string },
  b: { environmentId: string; threadId: string },
) {
  return a.environmentId === b.environmentId && a.threadId === b.threadId;
}

export function MonitorUI({ store }: { readonly store: MonitorPeerStore }) {
  const snapshot = useSyncExternalStore(store.subscribe, store.getSnapshot);
  const { expanded, collapsed, quick, authoritativeViewState } = snapshot;
  const [now, setNow] = useState(Date.now);
  const root = useRef<HTMLDivElement>(null);
  const editor = useRef<HTMLFormElement>(null);
  const textarea = useRef<HTMLTextAreaElement>(null);
  const composing = useRef(false);
  const applyingViewState = useRef(false);
  const captureScheduled = useRef(false);
  const captureViewState = useRef<(() => void) | null>(null);
  const compositionEnded = useRef(0);
  const suppressExpansion = useRef(false);
  const hover = useRef<ReturnType<typeof setTimeout> | null>(null);
  const close = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [announcement, setAnnouncement] = useState("");
  const selectionKey = snapshot.selection ? monitorThreadKey(snapshot.selection) : null;
  const activeKey = selectionKey ?? quick;
  const activeRow =
    activeKey === null
      ? null
      : (snapshot.rows.get(activeKey) ?? snapshot.retainedRows.get(activeKey) ?? null);
  const scheduleViewState = useStableCallback(() => {
    if (captureScheduled.current || applyingViewState.current) return;
    captureScheduled.current = true;
    queueMicrotask(() => {
      captureScheduled.current = false;
      if (!applyingViewState.current) captureViewState.current?.();
    });
  });
  useLayoutEffect(() => {
    captureViewState.current = () => {
      if (!store.generation || !root.current) return;
      const list = root.current.querySelector<HTMLElement>(".list-scroll");
      const detailElement = root.current.querySelector<HTMLElement>(".thread-scroll");
      const previous = store.viewState;
      const scroll = (
        element: HTMLElement | null,
        fallback:
          | { scrollTop: number; anchors: ReadonlyArray<{ key: string; offset: number }> }
          | undefined,
      ) =>
        element?.getClientRects().length
          ? captureMonitorAnchor(element)
          : (fallback ?? { scrollTop: 0, anchors: [] });
      store.updateViewState({
        publisherGeneration: store.generation,
        collapsed: {
          needsYou: collapsed.needsYou === true,
          working: collapsed.working === true,
          waiting: collapsed.waiting === true,
          done: collapsed.done === true,
        },
        list: scroll(list, previous?.list),
        detail: scroll(detailElement, previous?.detail),
        editor:
          activeRow && store.live(activeRow)
            ? {
                ref: refOf(activeRow),
                quick: selectionKey === null,
                focused: document.activeElement === textarea.current,
                selectionStart: textarea.current?.selectionStart ?? 0,
                selectionEnd: textarea.current?.selectionEnd ?? 0,
              }
            : null,
      });
    };
    scheduleViewState();
    return () => {
      captureViewState.current = null;
    };
  }, [store, collapsed, activeRow, selectionKey, scheduleViewState]);
  useLayoutEffect(() => {
    const saved = authoritativeViewState;
    if (!saved || !root.current) return;
    applyingViewState.current = true;
    const frame = requestAnimationFrame(() => {
      const list = root.current?.querySelector<HTMLElement>(".list-scroll");
      const detailElement = root.current?.querySelector<HTMLElement>(".thread-scroll");
      if (list) {
        list.scrollTop = saved.list.scrollTop;
        restoreMonitorAnchor(list, saved.list);
      }
      if (detailElement) {
        detailElement.scrollTop = saved.detail.scrollTop;
        restoreMonitorAnchor(detailElement, saved.detail);
      }
      if (saved.editor?.focused && store.live(saved.editor.ref) && textarea.current) {
        textarea.current.focus({ preventScroll: true });
        const length = textarea.current.value.length;
        textarea.current.setSelectionRange(
          Math.min(length, saved.editor.selectionStart),
          Math.min(length, saved.editor.selectionEnd),
        );
      }
      applyingViewState.current = false;
    });
    return () => {
      cancelAnimationFrame(frame);
      applyingViewState.current = false;
    };
    // Local captures never replace the authoritative replay snapshot.
  }, [store, authoritativeViewState]);
  const draft = activeRow
    ? (snapshot.drafts.get(monitorDraftKey(activeRow)) ?? store.draft(activeRow))
    : null;
  const busy = (row: ThreadMonitorRow, state = snapshot) =>
    [...state.results.values()].some(
      (result) =>
        sameThread(row, result) && (result.status === "pending" || result.status === "unknown"),
    );
  const currentRow = (row: ThreadMonitorRow) => store.rows.get(monitorThreadKey(row)) ?? row;
  const unavailable = (row: ThreadMonitorRow, state = snapshot) =>
    !store.live(row)
      ? "This environment lifetime has ended"
      : busy(row, state)
        ? "An action is pending or unknown. Reconcile its exact identity first."
        : state.environments.get(row.environmentId)?.transport !== "connected" ||
            state.environments.get(row.environmentId)?.freshness !== "live"
          ? "Waiting for current environment data"
          : null;
  const dispatch = (row: ThreadMonitorRow, action: ThreadMonitorAction) => {
    const reason = unavailable(row, store.getSnapshot());
    if (reason && action.type !== "open" && action.type !== "reconcile") {
      setAnnouncement(reason);
      return;
    }
    store.action(action);
  };
  const send = () => {
    if (!activeRow || !draft || composing.current) return;
    const row = currentRow(activeRow);
    const reason =
      unavailable(row, store.getSnapshot()) ??
      (!row.actions.reply.available ? row.actions.reply.reason : null) ??
      (!draft.text.trim()
        ? "Write a reply first"
        : draft.text.length > THREAD_MONITOR_LIMITS.draftCodeUnits
          ? "Reply exceeds the text limit"
          : null);
    if (reason) {
      setAnnouncement(reason);
      return;
    }
    dispatch(row, {
      ...idFor(row),
      type: "send",
      messageId: MessageId.make(randomUUID()),
      text: draft.text,
      draftRevision: draft.revision,
    });
  };
  const setOpen = (value: boolean, explicit = false) => {
    if (value && suppressExpansion.current && !explicit) return;
    if (explicit) suppressExpansion.current = false;
    store.setExpanded(value);
  };
  const back = () => {
    const key = selectionKey;
    store.release();
    queueMicrotask(() => {
      const target = [
        ...(root.current?.querySelectorAll<HTMLButtonElement>("[data-row-open]") ?? []),
      ].find((element) => element.dataset.rowOpen === key);
      target?.focus({ preventScroll: true });
    });
  };
  const settle = (row: ThreadMonitorRow) => {
    const latest = currentRow(row);
    const reason =
      unavailable(latest, store.getSnapshot()) ??
      (!latest.actions.settle.available ? latest.actions.settle.reason : null);
    if (reason) {
      setAnnouncement(reason);
      return;
    }
    dispatch(latest, { ...idFor(latest), type: "settle", expectedUpdatedAt: latest.updatedAt });
  };
  useEffect(() => {
    const refresh = () => setNow(Date.now());
    const timer = setInterval(refresh, 60_000);
    const stopStore = store.subscribe(refresh);
    document.addEventListener("visibilitychange", refresh);
    window.addEventListener("focus", refresh);
    window.addEventListener("pageshow", refresh);
    return () => {
      clearInterval(timer);
      stopStore();
      document.removeEventListener("visibilitychange", refresh);
      window.removeEventListener("focus", refresh);
      window.removeEventListener("pageshow", refresh);
    };
  }, [store]);
  useEffect(
    () => () => {
      if (hover.current) clearTimeout(hover.current);
      if (close.current) clearTimeout(close.current);
    },
    [],
  );
  const positionEditor = useStableCallback(() => {
    const form = editor.current;
    const panel = root.current;
    if (!form || !panel) return;
    panel.style.setProperty("--editor-height", `${form.getBoundingClientRect().height}px`);
    if (selectionKey !== null || activeKey === null) return;
    const anchor = [...panel.querySelectorAll<HTMLElement>("[data-editor-anchor]")].find(
      (element) => element.dataset.editorAnchor === activeKey,
    );
    if (!anchor || anchor.getClientRects().length === 0) {
      form.style.visibility = "hidden";
      return;
    }
    form.style.visibility = "visible";
    const rect = anchor.getBoundingClientRect();
    const bounds = panel.getBoundingClientRect();
    form.style.top = `${rect.top - bounds.top}px`;
    form.style.left = `${rect.left - bounds.left}px`;
    form.style.width = `${rect.width}px`;
  });
  useLayoutEffect(() => {
    positionEditor();
  });
  useLayoutEffect(() => {
    const form = editor.current;
    const panel = root.current;
    if (!form || !panel) return;
    const stopResize = observeResize([form, panel], positionEditor);
    panel.addEventListener("scroll", positionEditor, true);
    window.addEventListener("resize", positionEditor);
    return () => {
      stopResize();
      panel.removeEventListener("scroll", positionEditor, true);
      window.removeEventListener("resize", positionEditor);
    };
  }, [positionEditor]);
  const focusQuick = (key: string) => {
    if (!editor.current?.contains(document.activeElement) || quick === key) store.setQuick(key);
  };
  const rowHTML = (row: ThreadMonitorRow, retained = false) => {
    const key = monitorThreadKey(row);
    const environment = snapshot.environments.get(row.environmentId);
    const rowDraft = snapshot.drafts.get(monitorDraftKey(row));
    const reason = unavailable(row) ?? row.actions.settle.reason;
    return (
      <article
        key={key}
        className={`row${quick === key && !selectionKey ? " reply-open" : ""}`}
        data-status={glyphRole(row.status)}
        data-anchor={`row:${key}`}
        onMouseEnter={() => {
          if (hover.current) clearTimeout(hover.current);
          hover.current = setTimeout(() => focusQuick(key), 140);
        }}
        onMouseLeave={() => {
          if (hover.current) clearTimeout(hover.current);
        }}
        onFocusCapture={() => {
          if (!suppressExpansion.current) focusQuick(key);
        }}
      >
        <button
          type="button"
          className="row-main"
          data-monitor-thread-id={row.threadId}
          data-monitor-environment-id={row.environmentId}
          data-row-open={key}
          onClick={() => {
            store.select(row);
          }}
          aria-label={`${row.title}. ${row.statusLabel}. Open thread detail`}
        >
          <Glyph status={row.status} />
          <MonitorTooltip content={row.title}>
            <span className="row-title">{row.title}</span>
          </MonitorTooltip>
          <MonitorTooltip content={row.completedAt ?? row.updatedAt}>
            <time className="row-age" dateTime={row.completedAt ?? row.updatedAt}>
              {age(row, now)}
            </time>
          </MonitorTooltip>
          <span className="row-meta">
            {row.projectLabel}
            {row.projectLabel ? " · " : ""}
            {environment?.label ?? "Environment unavailable"}
          </span>
          <span className="row-line">
            <span className="state-label">{row.statusLabel}</span>
            {retained
              ? row.group === "done" && !isRecentMonitorCompletion(row.completedAt, now)
                ? " · Outside the last-hour window"
                : " · Outside the current list"
              : environment?.freshness !== "live"
                ? " · Last known"
                : null}
            {rowDraft?.text ? <span className="row-draft">Draft</span> : null}
          </span>
        </button>
        <MonitorTooltip content={reason ?? "Settle thread; keeps conversation history"}>
          <button
            type="button"
            className="row-settle settle-trigger"
            aria-label={`Settle ${row.title}`}
            aria-disabled={!!reason}
            onClick={() => settle(row)}
          >
            Settle
          </button>
        </MonitorTooltip>
        <div
          data-editor-anchor={key}
          className="editor-anchor"
          style={{ height: quick === key && !selectionKey ? "var(--editor-height,76px)" : 0 }}
        />
      </article>
    );
  };
  const selected = selectionKey ? activeRow : null;
  const detail = snapshot.detail;
  const replyReason = activeRow
    ? (unavailable(activeRow) ??
      (!activeRow.actions.reply.available ? activeRow.actions.reply.reason : null))
    : "Select a thread";
  const latestResults = [...snapshot.results.values()]
    .filter((result) => result.status !== "pending")
    .slice(-3);
  const detailUnavailable = !selected
    ? null
    : (unavailable(selected) ??
      detail?.unavailableReason ??
      (detail ? null : "Loading current detail"));
  return (
    <div
      ref={root}
      className="monitor-root"
      onScrollCapture={scheduleViewState}
      onFocusCapture={scheduleViewState}
      onBlurCapture={scheduleViewState}
      data-thread-monitor
      data-expanded={expanded}
      onMouseEnter={() => {
        if (close.current) clearTimeout(close.current);
        if (!expanded && !suppressExpansion.current) {
          hover.current = setTimeout(() => setOpen(true), 140);
        }
      }}
      onMouseLeave={() => {
        if (hover.current) clearTimeout(hover.current);
        suppressExpansion.current = false;
        if (!root.current?.contains(document.activeElement) && !draft?.text)
          close.current = setTimeout(() => setOpen(false), 380);
      }}
      onKeyDown={(event) => {
        if (event.nativeEvent.isComposing || composing.current) return;
        if (
          (event.key === "ArrowDown" || event.key === "ArrowUp") &&
          event.target instanceof HTMLElement &&
          !event.target.matches("textarea,input") &&
          event.target.closest(".row") &&
          selectionKey === null
        ) {
          const buttons = [
            ...(root.current?.querySelectorAll<HTMLButtonElement>(".row-main") ?? []),
          ].filter((button) => button.getClientRects().length > 0);
          const current = buttons.findIndex(
            (button) => button.closest(".row") === (event.target as HTMLElement).closest(".row"),
          );
          const next =
            buttons[
              Math.max(
                0,
                Math.min(buttons.length - 1, current + (event.key === "ArrowDown" ? 1 : -1)),
              )
            ];
          if (next) {
            event.preventDefault();
            next.focus({ preventScroll: true });
            next.scrollIntoView({ block: "nearest" });
          }
        }
        if (event.key === "Escape") {
          event.preventDefault();
          event.stopPropagation();
          if (selectionKey) back();
          else {
            suppressExpansion.current = true;
            store.setQuick(null);
            setOpen(false);
            root.current
              ?.querySelector<HTMLButtonElement>(".notch")
              ?.focus({ preventScroll: true });
          }
        }
      }}
    >
      <button
        type="button"
        className="notch"
        aria-expanded={expanded}
        hidden={expanded}
        aria-label={`Open thread monitor: ${snapshot.counts.needsYou} need you, ${snapshot.counts.working} working, ${snapshot.counts.waiting} waiting, ${snapshot.counts.done} done in the last hour`}
        data-attention={snapshot.counts.needsYou > 0}
        onClick={() => setOpen(true, true)}
        onFocus={() => {
          if (!suppressExpansion.current) setOpen(true);
        }}
      >
        {GROUPS.map((group) => (
          <span
            key={group}
            className="notch-line"
            data-status={group === "needsYou" ? "needsYou" : group}
          >
            <Glyph status={group === "needsYou" ? "needsYou" : group === "done" ? "done" : group} />
            <span>{snapshot.countsReady ? countLabel(snapshot.counts[group]) : "…"}</span>
          </span>
        ))}
      </button>
      <div className="monitor" hidden={!expanded}>
        <div className="count-strip" data-monitor-counts aria-label="Thread counts">
          {GROUPS.map((group) => (
            <span key={group} data-status={group === "needsYou" ? "needsYou" : group}>
              <Glyph
                status={group === "needsYou" ? "needsYou" : group === "done" ? "done" : group}
              />
              <span className="count-name">{titles[group]}</span>
              <strong>{snapshot.countsReady ? countLabel(snapshot.counts[group]) : "…"}</strong>
            </span>
          ))}
        </div>
        <div className="list-view" hidden={selectionKey !== null}>
          <header className="panel-header">
            <span className="brand">
              <Icon>
                <path d="M2 3h12v10H2zM5 3v10" />
              </Icon>
              Jones Code
            </span>
            <span className="header-count">
              {snapshot.countsReady
                ? `${countLabel(GROUPS.reduce((count, group) => count + snapshot.counts[group], 0))} threads`
                : "Updating counts"}
            </span>
            <button
              className="icon-button"
              type="button"
              aria-label="Collapse thread monitor"
              onClick={() => {
                suppressExpansion.current = true;
                setOpen(false);
              }}
            >
              <Icon>
                <path d="m6 3 5 5-5 5" />
              </Icon>
            </button>
            <MonitorTooltip content="Show it again with the toolbar toggle in Jones Code">
              <button
                className="icon-button"
                type="button"
                aria-label="Hide thread monitor"
                onClick={() => {
                  void store.dispatch({ type: "hide" });
                }}
              >
                <Icon>
                  <path d="m4 4 8 8m0-8-8 8" />
                </Icon>
              </button>
            </MonitorTooltip>
          </header>
          <MonitorAnchoredScroll>
            {snapshot.error ? (
              <div className="offline-note" role="status">
                {snapshot.error}
              </div>
            ) : null}
            {[...snapshot.environments.values()]
              .filter(
                (environment) =>
                  environment.transport !== "connected" || environment.freshness !== "live",
              )
              .map((environment) => (
                <div key={environment.environmentId} className="offline-note">
                  <strong>{environment.label}</strong> ·{" "}
                  {environment.reason ?? "Waiting for current data"}. Drafts are kept.
                </div>
              ))}
            {GROUPS.map((group) => {
              const rows = [...snapshot.rows.values()]
                .filter(
                  (row) =>
                    row.group === group &&
                    (group !== "done" || isRecentMonitorCompletion(row.completedAt, now)),
                )
                .sort(
                  (a, b) =>
                    Date.parse(b.updatedAt) - Date.parse(a.updatedAt) ||
                    monitorThreadKey(a).localeCompare(monitorThreadKey(b)),
                );
              const hasDraft = [...snapshot.drafts.values()].some(
                (draft) => draft.text.length > 0 && rows.some((row) => sameThread(row, draft)),
              );
              return (
                <section
                  className="group"
                  key={group}
                  data-group={group}
                  data-monitor-section={group}
                >
                  <button
                    type="button"
                    className="section-toggle"
                    aria-expanded={!collapsed[group]}
                    aria-controls={`monitor-group-${group}`}
                    data-anchor={`section:${group}`}
                    onClick={(event) => {
                      if (!collapsed[group] && activeRow?.group === group && !selectionKey)
                        event.currentTarget.focus();
                      store.toggleGroup(group);
                    }}
                  >
                    <Icon>
                      <path d="m4 6 4 4 4-4" />
                    </Icon>
                    <span>{titles[group]}</span>
                    <span className="section-count">
                      {snapshot.countsReady ? countLabel(snapshot.counts[group]) : "…"}
                    </span>
                    {hasDraft ? <span className="section-draft">· draft</span> : null}
                    {group === "done" ? <span className="section-extra">last hour</span> : null}
                  </button>
                  <div id={`monitor-group-${group}`} hidden={collapsed[group]}>
                    {rows.map((row) => rowHTML(row))}
                    {snapshot.countsReady && snapshot.counts[group] === 0 ? (
                      <div className="group-empty">No threads here</div>
                    ) : null}
                    {snapshot.counts[group] > rows.length ? (
                      <button
                        type="button"
                        className="show-more"
                        onClick={() => {
                          void store.dispatch({ type: "requestMore", group });
                        }}
                      >
                        Show {Math.min(50, snapshot.counts[group] - rows.length)} more{" "}
                        <span className="sr-only">in {titles[group]}</span>
                      </button>
                    ) : null}
                  </div>
                </section>
              );
            })}
            {activeRow &&
            (!snapshot.rows.has(monitorThreadKey(activeRow)) ||
              (activeRow.group === "done" &&
                !isRecentMonitorCompletion(activeRow.completedAt, now))) &&
            store.live(activeRow) ? (
              <section className="retained-editor">
                <div className="offline-note">
                  {activeRow.group === "done" &&
                  !isRecentMonitorCompletion(activeRow.completedAt, now)
                    ? "This completion is outside the last hour. Your editor is kept."
                    : "This thread is outside the current list. Your draft is kept."}
                </div>
                {rowHTML(activeRow, true)}
              </section>
            ) : null}
            {snapshot.environments.size === 0 ? (
              <div className="empty-state">
                <strong>Waiting for Jones Code</strong>Connected environments appear here when their
                current state is ready.
              </div>
            ) : null}
          </MonitorAnchoredScroll>
          <footer className="panel-footer">
            <span>Across {snapshot.environments.size} environments</span>
            <span>Newest activity first</span>
          </footer>
        </div>
        <div className="thread-view" data-monitor-detail hidden={selectionKey === null}>
          <header className="thread-header">
            <div className="thread-actions">
              <button className="back" type="button" onClick={back}>
                <Icon>
                  <path d="m10 4-4 4 4 4" />
                </Icon>
                Threads
              </button>
              {selected ? (
                <MonitorTooltip
                  content={
                    unavailable(selected) ?? selected.actions.settle.reason ?? "Settle thread"
                  }
                >
                  <button
                    type="button"
                    className="button"
                    aria-disabled={!!(unavailable(selected) ?? selected.actions.settle.reason)}
                    onClick={() => settle(selected)}
                  >
                    Settle
                  </button>
                </MonitorTooltip>
              ) : null}
              <MonitorTooltip content="Show it again with the toolbar toggle in Jones Code">
                <button
                  className="icon-button"
                  type="button"
                  aria-label="Hide thread monitor"
                  onClick={() => {
                    void store.dispatch({ type: "hide" });
                  }}
                >
                  <Icon>
                    <path d="m4 4 8 8m0-8-8 8" />
                  </Icon>
                </button>
              </MonitorTooltip>
            </div>
            <h1 className="thread-title">{selected?.title ?? "Thread unavailable"}</h1>
            {selected ? (
              <div className="thread-meta">
                <span className="status-chip" data-status={glyphRole(selected.status)}>
                  <Glyph status={selected.status} />
                  {selected.statusLabel}
                </span>
                <span>
                  {selected.projectLabel} ·{" "}
                  {snapshot.environments.get(selected.environmentId)?.label}
                </span>
              </div>
            ) : null}
          </header>
          <MonitorAnchoredScroll className="thread-scroll">
            {snapshot.error ? (
              <div className="offline-note" role="status">
                {snapshot.error}
              </div>
            ) : null}
            {detailUnavailable ? (
              <div className="offline-note" role="status">
                {detailUnavailable}
              </div>
            ) : null}
            {detail?.entries.map((entry) => (
              <article
                className={`message ${entry.role}`}
                data-anchor={`message:${entry.id}`}
                key={entry.id}
              >
                <div className="who">
                  {entry.role === "user" ? "You" : entry.role === "assistant" ? "Agent" : "System"}
                  {entry.excerpt ? " · excerpt" : ""}
                </div>
                <p>{entry.text}</p>
              </article>
            ))}
            {selected && detail?.request ? (
              <RequestControls
                store={store}
                row={selected}
                disabledReason={detailUnavailable}
                dispatch={(action) => dispatch(selected, action)}
              />
            ) : null}
            {selected && detail?.plan ? (
              <section className="request" data-status="plan">
                <div className="request-label">Plan ready</div>
                <pre className="plan-text">{detail.plan.markdown}</pre>
                <MonitorTooltip content={detailUnavailable ?? detail.plan.availability.reason}>
                  <button
                    type="button"
                    className="button primary"
                    aria-disabled={!!detailUnavailable || !detail.plan.availability.available}
                    onClick={() => {
                      if (!detailUnavailable && detail.plan?.availability.available)
                        dispatch(selected, {
                          ...idFor(selected),
                          type: "usePlan",
                          planId: detail.plan.planId,
                          messageId: MessageId.make(randomUUID()),
                        });
                    }}
                  >
                    Use this plan
                  </button>
                </MonitorTooltip>
                {detail.plan.availability.reason ? (
                  <small>{detail.plan.availability.reason}</small>
                ) : null}
              </section>
            ) : null}
            {selected ? (
              <button
                type="button"
                className="button open-in-jones"
                onClick={() => dispatch(selected, { ...idFor(selected), type: "open" })}
              >
                Open in Jones Code <span aria-hidden="true">↗</span>
              </button>
            ) : null}
            {selected?.actions.settle.reason ? (
              <div className="settle-reason">{selected.actions.settle.reason}</div>
            ) : null}
          </MonitorAnchoredScroll>
          <div
            className="detail-composer-space"
            style={{ height: activeRow ? "var(--editor-height,100px)" : 0 }}
          />
        </div>
        <form
          data-monitor-reply
          ref={editor}
          className={`editor-host ${selectionKey ? "thread-composer" : "reply-form"}`}
          hidden={!activeRow}
          style={
            selectionKey
              ? ({
                  top: "auto",
                  bottom: 0,
                  left: 0,
                  width: "100%",
                  visibility: "visible",
                } as CSSProperties)
              : undefined
          }
          onSubmit={(event) => {
            event.preventDefault();
            send();
          }}
        >
          <div className="reply-box">
            <textarea
              onSelect={scheduleViewState}
              onKeyUp={scheduleViewState}
              onPointerUp={scheduleViewState}
              ref={textarea}
              rows={selectionKey ? 2 : 1}
              aria-label={activeRow ? `Reply to ${activeRow.title}` : "Reply"}
              aria-describedby="monitor-reply-hint"
              placeholder="Reply to this thread…"
              value={draft?.text ?? ""}
              onChange={(event) => {
                if (draft)
                  store.edit({ ...draft, revision: draft.revision + 1, text: event.target.value });
              }}
              onCompositionStart={() => {
                composing.current = true;
              }}
              onCompositionEnd={() => {
                composing.current = false;
                compositionEnded.current = performance.now();
              }}
              onKeyDown={(event) => {
                if (
                  event.key === "Enter" &&
                  !event.shiftKey &&
                  !event.nativeEvent.isComposing &&
                  !composing.current &&
                  event.nativeEvent.keyCode !== 229 &&
                  performance.now() - compositionEnded.current > 30
                ) {
                  event.preventDefault();
                  send();
                }
              }}
            />
            <button
              type="submit"
              className="send"
              aria-disabled={!!replyReason || !draft?.text.trim()}
              aria-describedby="monitor-reply-hint"
            >
              Send
            </button>
          </div>
          <div className="reply-hint" id="monitor-reply-hint">
            {replyReason ?? "Enter to send · Shift+Enter for a new line"}
          </div>
        </form>
        {latestResults.length > 0 ? (
          <div className="receipts" aria-live="polite">
            {latestResults.map((result) => {
              const original = snapshot.actions.get(result.actionId);
              const row = snapshot.retainedRows.get(monitorThreadKey(result));
              return (
                <div className="receipt" key={result.actionId}>
                  <span>
                    {result.status === "accepted"
                      ? result.settleCommandId
                        ? "Thread settled. History is kept."
                        : "Command accepted. Work may still be pending."
                      : result.reason}
                  </span>
                  {result.status === "unknown" && row ? (
                    <button
                      type="button"
                      onClick={() => {
                        store.select(row);
                        store.action({
                          ...idFor(row),
                          type: "reconcile",
                          originalActionId: result.actionId,
                          originalCommandId: result.commandId,
                          ...(original?.messageId === undefined
                            ? {}
                            : { messageId: original.messageId }),
                        });
                      }}
                    >
                      Reconcile
                    </button>
                  ) : null}
                  {result.status === "accepted" && result.settleCommandId && row ? (
                    <MonitorTooltip
                      content={
                        row.actions.restore.reason ??
                        "Restores supported thread placement only; execution effects are not reversed"
                      }
                    >
                      <button
                        type="button"
                        aria-disabled={!row.actions.restore.available}
                        onClick={() => {
                          if (row.actions.restore.available)
                            dispatch(row, {
                              ...idFor(row),
                              type: "restore",
                              settleCommandId: result.settleCommandId!,
                            });
                        }}
                      >
                        Restore thread
                      </button>
                    </MonitorTooltip>
                  ) : null}
                  <button
                    type="button"
                    aria-label="Dismiss command notice"
                    onClick={() => {
                      if (result.status !== "unknown") {
                        store.results.delete(result.actionId);
                        store.changed();
                      }
                    }}
                  >
                    ×
                  </button>
                </div>
              );
            })}
          </div>
        ) : null}
      </div>
      <div className="sr-only" role="status" aria-live="polite">
        {announcement}
      </div>
    </div>
  );
}

function RequestControls({
  store,
  row,
  disabledReason,
  dispatch,
}: {
  store: MonitorPeerStore;
  row: ThreadMonitorRow;
  disabledReason: string | null;
  dispatch: (action: ThreadMonitorAction) => void;
}) {
  const snapshot = useSyncExternalStore(store.subscribe, store.getSnapshot);
  const request = snapshot.detail?.request;
  if (!request) return null;
  const reason = disabledReason ?? request.availability.reason;
  if (request.kind === "approval")
    return (
      <section className="request" data-status="approval">
        <div className="request-label">Pending approval</div>
        <p>{request.prompt}</p>
        <div className="request-options">
          {request.options
            .filter((option) => option.decision === "accept" || option.decision === "decline")
            .map((option) => (
              <MonitorTooltip key={option.decision} content={reason ?? option.warning}>
                <button
                  type="button"
                  className={`button${option.decision === "accept" ? " primary" : ""}`}
                  aria-disabled={!!reason || !request.availability.available}
                  onClick={() => {
                    if (
                      !reason &&
                      request.availability.available &&
                      (option.decision === "accept" || option.decision === "decline")
                    )
                      dispatch({
                        ...idFor(row),
                        type: "respondToApproval",
                        requestId: request.requestId,
                        decision: option.decision,
                      });
                  }}
                >
                  {option.decision === "accept" ? "Approve once" : "Deny"}
                  {option.warning ? " ⚠" : null}
                </button>
              </MonitorTooltip>
            ))}
        </div>
        {request.options
          .filter((option) => option.decision === "accept" || option.decision === "decline")
          .map((option) =>
            option.warning ? (
              <small key={option.decision}>
                {option.label}: {option.warning}
              </small>
            ) : null,
          )}
        {reason ? <small>{reason}</small> : null}
        {!request.options.some(
          (option) => option.decision === "accept" || option.decision === "decline",
        ) ? (
          <small>No once/deny decision is offered. Open this request in Jones Code.</small>
        ) : null}
      </section>
    );
  const draft =
    snapshot.drafts.get(monitorDraftKey(row, request.requestId)) ??
    store.draft(row, request.requestId);
  const answers = draft.answers;
  const complete = request.questions.every((question) => {
    const answer = answers[question.id];
    return typeof answer === "string" ? true : Array.isArray(answer) && answer.length > 0;
  });
  const edit = (id: string, answer: string | ReadonlyArray<string>) =>
    store.edit({ ...draft, revision: draft.revision + 1, answers: { ...answers, [id]: answer } });
  return (
    <section className="request" data-status="input">
      <div className="request-label">Awaiting input</div>
      {request.questions.map((question) => {
        const answer = answers[question.id];
        const selected = typeof answer === "string" ? [answer] : (answer ?? []);
        const custom =
          typeof answer === "string" && !question.options.some((option) => option.value === answer)
            ? answer
            : "";
        return (
          <fieldset key={`${request.requestId}:${question.id}`}>
            <legend>{question.header}</legend>
            <p>{question.question}</p>
            <div className="request-options">
              {question.options.map((option) => (
                <label className="question-option" key={option.value}>
                  <input
                    type={question.multiSelect ? "checkbox" : "radio"}
                    name={`${request.requestId}:${question.id}`}
                    checked={selected.includes(option.value)}
                    disabled={!!reason || !request.availability.available}
                    onChange={() =>
                      edit(
                        question.id,
                        question.multiSelect
                          ? selected.includes(option.value)
                            ? selected.filter((value) => value !== option.value)
                            : [
                                ...selected.filter((value) =>
                                  question.options.some((option) => option.value === value),
                                ),
                                option.value,
                              ]
                          : option.value,
                      )
                    }
                  />
                  <span>
                    {option.label}
                    {option.description ? <small>{option.description}</small> : null}
                  </span>
                </label>
              ))}
            </div>
            {question.allowCustomAnswer ? (
              <textarea
                rows={2}
                aria-label={`Custom answer: ${question.header}`}
                value={custom}
                disabled={!!reason || !request.availability.available}
                onChange={(event) => edit(question.id, event.target.value)}
                placeholder="Your answer…"
              />
            ) : null}
          </fieldset>
        );
      })}
      <button
        type="button"
        className="button primary"
        aria-disabled={!!reason || !complete || !request.availability.available}
        onClick={() => {
          if (!reason && complete && request.availability.available)
            dispatch({
              ...idFor(row),
              type: "respondToUserInput",
              requestId: request.requestId,
              answers,
              draftRevision: draft.revision,
            });
        }}
      >
        Submit answers
      </button>
      {reason ? (
        <small>{reason}</small>
      ) : (
        <small>
          Answers apply to these exact questions. Plain replies do not answer a request.
        </small>
      )}
    </section>
  );
}
