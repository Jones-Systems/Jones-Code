import { useEffect, useState } from "react";
import { randomUUID } from "../../lib/utils";
import type { EnvironmentId } from "@t3tools/contracts";
import {
  bindingKey,
  type Access,
  type Duration,
  type Item,
  type Mutation,
  type GroupView,
  type ReviewGroup,
} from "@t3tools/client-runtime/jones/remoteAdmission/controller";
import { Button } from "../../components/ui/button";
import { admissionPresentation, admissionStatus } from "./presentation";
import { RequestMarkdown } from "./RequestMarkdown";
import { DurationSelect, ReviewControls } from "./ReviewControls";
import { useFrozenRows, useReview } from "./reviewHooks";
import { requestHref, threadHref } from "./navigation";
import {
  AccessExpiry,
  RequestEnvironmentLabel,
  RequestThreadTitle,
  RequestThreadStatus,
} from "./RequestThreadStatus";

export function ReviewPage({
  environmentId,
  label,
}: {
  environmentId: EnvironmentId;
  label: string;
}) {
  const state = useReview(environmentId);
  const { page, capabilities, controller } = state;
  const initial = new URLSearchParams(window.location.search);
  const matchesReceiver = initial.get("receiver") === environmentId;
  const [selected, setSelected] = useState<string | null>(
    matchesReceiver ? initial.get("request") : null,
  );
  const [accessSelection, setAccessSelection] = useState({
    id: matchesReceiver ? initial.get("access") : null,
    revision: state.accessSelectionRevision,
  });
  const accessId =
    state.selectedAccess &&
    accessSelection.revision !== state.accessSelectionRevision &&
    accessSelection.id === state.selectedAccessFromId
      ? state.selectedAccess.id
      : accessSelection.id;
  // A restore/readback can replace the selected predecessor with its successor.
  // Reconcile this component's selection before commit, without a second effect render.
  if (accessSelection.revision !== state.accessSelectionRevision) {
    setAccessSelection({ id: accessId, revision: state.accessSelectionRevision });
  }
  const setAccessId = (id: string | null) => {
    setAccessSelection({ id, revision: state.accessSelectionRevision });
  };
  const [reviewBlocked, setReviewBlocked] = useState(false);
  const requested = matchesReceiver ? initial.get("request") : null;
  const digest = initial.get("binding");
  const requestedAccess = matchesReceiver ? initial.get("access") : null;
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Reconcile the mounted exact request route with the subscribed owner controller; lookup preserves off-page binding and uncertainty checks.
  useEffect(() => {
    if (requested) void controller.lookup(requested, digest ?? undefined).catch(() => undefined);
  }, [controller, requested, digest]);
  const openAccess = (id: string) => {
    setAccessId(id);
    void controller.lookupAccess(id).catch(() => undefined);
  };
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Reconcile the mounted exact access route with the subscribed controller, including retained restore-command readback.
  useEffect(() => {
    if (requestedAccess) void controller.lookupAccess(requestedAccess).catch(() => undefined);
  }, [controller, requestedAccess]);
  const frozen = useFrozenRows(page?.records ?? [], (item) => bindingKey(item.record));
  const frozenAccess = useFrozenRows(page?.access ?? [], (entry) => entry.id);
  const frozenDiagnostics = useFrozenRows(page?.diagnostics ?? [], (entry) => entry.id);
  const item =
    [
      ...(page?.records ?? []),
      ...Object.values(state.groups).flatMap((group) => group.page?.records ?? []),
    ].find((entry) => entry.record.id === selected) ??
    (state.selectedItem?.record.id === selected ? state.selectedItem : null);
  const access =
    state.selectedAccess?.id === accessId
      ? state.selectedAccess
      : (page?.access.find((entry) => entry.id === accessId) ?? null);
  const disabled = item
    ? state.policyBarrier
      ? "Evaluator settings are changing."
      : state.pending.has(bindingKey(item.record))
        ? "Recording this decision…"
        : state.uncertain.has(bindingKey(item.record))
          ? "Outcome unknown. Do not resend."
          : state.loading || state.loadFailed
            ? "Refresh server state before deciding."
            : item.record.envelope.receiverPolicy.revision !== page?.policyRevision
              ? "Evaluator settings changed. Refresh this request before approving."
              : null
    : null;
  const mutate = (input: Mutation) => {
    void controller.mutate(input);
  };
  const choose = (entry: Item) => {
    setSelected(entry.record.id);
    setAccessId(null);
    setReviewBlocked(false);
    void controller.lookup(entry.record.id, entry.record.bindingDigest).catch(() => undefined);
  };
  return (
    <section className="min-w-0 space-y-3" aria-label={`Requests on ${label}`}>
      <div className="flex items-center justify-between gap-2">
        <h2 className="text-sm font-medium">{label}</h2>
        <Button
          size="sm"
          variant="outline"
          onClick={() => {
            void controller.load();
          }}
        >
          Refresh
        </Button>
      </div>
      <div role="tablist" aria-label="Request views" className="flex gap-2">
        {(["current", "blocked", "done"] as const).map((view) => (
          <Button
            key={view}
            role="tab"
            aria-selected={state.view === view}
            variant={state.view === view ? "secondary" : "ghost"}
            size="sm"
            onClick={() => {
              setSelected(null);
              setAccessId(null);
              void controller.load(view);
            }}
          >
            {view === "current" ? "Current" : view === "blocked" ? "Blocked" : "Done"}
            {page && view !== "done"
              ? ` · ${view === "current" ? page.counts.currentHeldAdmissions : page.counts.blockedHeldAdmissions}`
              : ""}
          </Button>
        ))}
      </div>
      {page ? (
        <div className="flex flex-wrap gap-2">
          {(
            [
              ["unresolved", `Unresolved · ${page.counts.unresolvedAdmissions}`],
              ["inflight", `In flight · ${page.counts.inFlightAdmissions}`],
              ["access", `Access · ${page.counts.activeAccessScopes}`],
            ] as const
          ).map(([view, title]) => (
            <Button
              key={view}
              size="sm"
              variant="ghost"
              onClick={() => {
                setSelected(null);
                setAccessId(null);
                void controller.load(view);
              }}
            >
              {title}
            </Button>
          ))}
        </div>
      ) : null}
      {state.error ? (
        <p role="alert" className="text-sm text-muted-foreground">
          {state.error}
        </p>
      ) : null}
      {state.loading ? (
        <p role="status" className="text-xs text-muted-foreground">
          Loading requests…
        </p>
      ) : null}
      {state.view === "done" && page ? (
        <p className="text-xs text-muted-foreground">{page.recordCount} retained records</p>
      ) : null}
      <div className="grid min-w-0 gap-4 md:grid-cols-[minmax(14rem,2fr)_minmax(0,3fr)]">
        <div
          className="min-w-0 space-y-2"
          onPointerEnter={() => {
            frozen.handlers.onPointerEnter();
            frozenAccess.handlers.onPointerEnter();
            frozenDiagnostics.handlers.onPointerEnter();
          }}
          onPointerLeave={() => {
            frozen.handlers.onPointerLeave();
            frozenAccess.handlers.onPointerLeave();
            frozenDiagnostics.handlers.onPointerLeave();
          }}
          onFocusCapture={() => {
            frozen.handlers.onFocusCapture();
            frozenAccess.handlers.onFocusCapture();
            frozenDiagnostics.handlers.onFocusCapture();
          }}
          onBlurCapture={(event) => {
            frozen.handlers.onBlurCapture(event);
            frozenAccess.handlers.onBlurCapture(event);
            frozenDiagnostics.handlers.onBlurCapture(event);
          }}
        >
          {frozen.rows.map((entry) => (
            <CompactRequest
              key={bindingKey(entry.record)}
              environmentId={environmentId}
              item={entry}
              selected={selected === entry.record.id}
              disabled={frozen.isGone(entry)}
              onSelect={() => choose(entry)}
            />
          ))}
          {frozenAccess.rows.map((entry) => (
            <CompactAccess
              key={entry.id}
              environmentId={environmentId}
              access={entry}
              disabled={frozenAccess.isGone(entry)}
              onSelect={() => {
                openAccess(entry.id);
                setSelected(null);
              }}
            />
          ))}
          {page && page.records.length === 0 && page.access.length === 0 ? (
            <p className="text-sm text-muted-foreground">No records in this view.</p>
          ) : null}
          {state.view === "blocked" && page?.diagnosticCounts ? (
            <p className="text-xs text-muted-foreground">
              {page.diagnosticCounts.logicalEvents} logical events ·{" "}
              {page.diagnosticCounts.repeatArrivals} repeat arrivals ·{" "}
              {page.diagnosticCounts.summaryRows} summary rows
            </p>
          ) : null}
          {state.view === "blocked"
            ? frozenDiagnostics.rows.map((event) => (
                <div key={event.id} className="rounded-md border p-3 text-xs">
                  <p>
                    {event.reason} · {event.repeats} repeats
                  </p>
                  {event.requestId ? (
                    <a
                      className="underline"
                      aria-disabled={frozenDiagnostics.isGone(event)}
                      onClick={(action) => {
                        if (frozenDiagnostics.isGone(event)) action.preventDefault();
                      }}
                      href={requestHref(environmentId, event.requestId)}
                    >
                      Review exact held message ↗
                    </a>
                  ) : (
                    <p className="text-muted-foreground">
                      Diagnostic only · retained message unavailable
                    </p>
                  )}
                </div>
              ))
            : null}
          {page?.nextCursor ? (
            <Button
              variant="outline"
              size="sm"
              disabled={state.loading}
              onClick={() => {
                void controller.load(state.view, true);
              }}
            >
              Load older records
            </Button>
          ) : null}
          {state.view === "current" ? (
            <>
              <details open className="mt-4 border-t pt-3">
                <summary className="cursor-pointer text-sm font-medium">In progress</summary>
                <CurrentGroup
                  mode="active"
                  environmentId={environmentId}
                  group={state.groups.access}
                  selected={selected}
                  choose={choose}
                  chooseAccess={(id) => {
                    openAccess(id);
                    setSelected(null);
                  }}
                  load={(older) => {
                    void controller.loadGroup("access", older);
                  }}
                />
              </details>
              <details open className="mt-4 border-t pt-3">
                <summary className="cursor-pointer text-sm font-medium">
                  Awaiting confirmation
                </summary>
                <CurrentGroup
                  mode="pending"
                  environmentId={environmentId}
                  group={state.groups.access}
                  selected={selected}
                  choose={choose}
                  chooseAccess={(id) => {
                    openAccess(id);
                    setSelected(null);
                  }}
                  load={(older) => {
                    void controller.loadGroup("access", older);
                  }}
                />
                {(["inflight", "unresolved"] as const).map((view) => (
                  <CurrentGroup
                    key={view}
                    mode={view}
                    environmentId={environmentId}
                    group={state.groups[view]}
                    selected={selected}
                    choose={choose}
                    chooseAccess={(id) => {
                      openAccess(id);
                      setSelected(null);
                    }}
                    load={(older) => {
                      void controller.loadGroup(view, older);
                    }}
                  />
                ))}
              </details>
            </>
          ) : null}
        </div>
        <div className="flex min-h-72 min-w-0 flex-col rounded-md border p-4">
          {access ? (
            <AccessDetail
              key={`${access.id}:${access.generation}:${access.state}:${state.accessSelectionRevision}`}
              environmentId={environmentId}
              access={access}
              uncertain={state.uncertain.has(`access:${access.id}`)}
              reviewAccessReadback={() => controller.lookupAccess(access.id).catch(() => undefined)}
              disabled={
                state.policyBarrier ||
                state.loading ||
                state.loadFailed ||
                state.pending.has(`access:${access.id}`) ||
                state.uncertain.has(`access:${access.id}`)
              }
              canRestore={capabilities?.features.has("grantRestore") ?? false}
              canEdit={capabilities?.features.has("grants") ?? false}
              mutate={mutate}
            />
          ) : accessId ? (
            <div>
              <p className="text-sm text-muted-foreground">
                Exact access record unavailable. Refresh its current state before editing.
              </p>
              <Button size="compact" variant="outline" onClick={() => openAccess(accessId)}>
                Refresh access
              </Button>
            </div>
          ) : item ? (
            <>
              <div className="flex flex-wrap items-center gap-2">
                <h3 className="min-w-0 flex-1 text-sm font-medium wrap-anywhere">
                  <RequestThreadTitle
                    environmentId={environmentId}
                    threadId={item.threadId}
                    fallback={admissionPresentation(item.record).title ?? "Incoming request"}
                  />
                </h3>
                <RequestThreadStatus environmentId={environmentId} threadId={item.threadId} />
              </div>
              <p className="mt-1 text-xs text-muted-foreground">
                {admissionPresentation(item.record).source}
                {admissionPresentation(item.record).project
                  ? ` · ${admissionPresentation(item.record).project!.label}`
                  : ""}
              </p>
              <div className="my-4">
                <RequestMarkdown>{admissionPresentation(item.record).message}</RequestMarkdown>
              </div>
              {item.reason ? (
                <p className="mb-4 text-xs text-muted-foreground">{item.reason}</p>
              ) : null}
              {item.blocked && !reviewBlocked ? (
                <div className="mt-auto flex flex-wrap justify-between gap-2">
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={!capabilities?.features.has("blockedReview")}
                    onClick={() => setReviewBlocked(true)}
                  >
                    Review This Message
                  </Button>
                  <Button
                    size="sm"
                    disabled={!item.accessId || !capabilities?.features.has("managedAccess")}
                    onClick={() => {
                      if (item.accessId) openAccess(item.accessId);
                    }}
                  >
                    Manage Access
                  </Button>
                </div>
              ) : item.record.status === "held" ? (
                <ReviewControls
                  key={bindingKey(item.record)}
                  item={item}
                  disabled={disabled}
                  grantSupported={capabilities?.features.has("grants") ?? false}
                  mutate={mutate}
                />
              ) : (
                <p className="text-xs text-muted-foreground">
                  {item.outcome === "unknown"
                    ? "Outcome unknown · do not resend"
                    : item.outcome === "awaiting"
                      ? "Awaiting confirmation"
                      : admissionStatus(item.record)}
                </p>
              )}
              <div className="mt-3 flex gap-2">
                {item.canRecheck ? (
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={!capabilities?.features.has("recoveredResult") || Boolean(disabled)}
                    onClick={() =>
                      mutate({
                        action: "recheck",
                        id: item.record.id,
                        bindingDigest: item.record.bindingDigest,
                        commandId: randomUUID(),
                      })
                    }
                  >
                    Recheck
                  </Button>
                ) : null}
                {item.canRetry ? (
                  <RetryControls
                    key={`${bindingKey(item.record)}:${item.attempt}`}
                    item={item}
                    disabled={!capabilities?.features.has("launchRetry") || Boolean(disabled)}
                    grants={capabilities?.features.has("grants") ?? false}
                    mutate={mutate}
                  />
                ) : null}
              </div>
              <details className="mt-4 text-xs">
                <summary>Bound request details</summary>
                <p className="my-2">
                  Source thread (reported):{" "}
                  {admissionPresentation(item.record).thread?.label ?? "Not reported"}
                </p>
                <pre className="overflow-auto whitespace-pre-wrap wrap-anywhere">
                  {JSON.stringify(item.record, null, 2)}
                </pre>
              </details>
            </>
          ) : (
            <p className="text-sm text-muted-foreground">Select a request or access record.</p>
          )}
        </div>
      </div>
    </section>
  );
}
function CompactRequest({
  environmentId,
  item,
  selected,
  disabled,
  onSelect,
}: {
  environmentId: EnvironmentId;
  item: Item;
  selected: boolean;
  disabled: boolean;
  onSelect: () => void;
}) {
  const view = admissionPresentation(item.record);
  return (
    <article
      className={`flex min-w-0 items-start gap-2 rounded-md border p-3 ${selected ? "border-ring bg-accent" : ""}`}
    >
      <button
        type="button"
        className="min-w-0 flex-1 text-left"
        aria-disabled={disabled}
        onClick={() => {
          if (!disabled) onSelect();
        }}
      >
        <span className="block text-sm font-medium wrap-anywhere">
          <RequestThreadTitle
            environmentId={environmentId}
            threadId={item.threadId}
            fallback={view.title ?? "Incoming request"}
          />
        </span>
        <span className="text-xs text-muted-foreground">
          {view.source}
          {view.project ? ` · ${view.project.label}` : ""}
        </span>
        <span className="block text-xs text-muted-foreground">
          {disabled
            ? "Resolved · refresh for current state"
            : item.outcome === "unknown"
              ? "Outcome unknown"
              : admissionStatus(item.record)}
        </span>
      </button>
      <div className="flex flex-wrap items-center justify-end gap-1">
        <span className="rounded bg-muted px-1.5 py-0.5 text-xs text-muted-foreground">
          {view.operation}
        </span>
        <a
          href={requestHref(environmentId, item.record.id, item.record.bindingDigest)}
          aria-label="Open exact request"
          className="p-2 text-3xs"
        >
          ↗
        </a>
        <RequestThreadStatus environmentId={environmentId} threadId={item.threadId} />
      </div>
    </article>
  );
}
function CompactAccess({
  environmentId,
  access,
  disabled,
  onSelect,
}: {
  environmentId: EnvironmentId;
  access: Access;
  disabled: boolean;
  onSelect: () => void;
}) {
  return (
    <article className="flex min-w-0 items-center gap-2 rounded-md border p-3">
      <button
        type="button"
        className="min-w-0 flex-1 text-left text-sm wrap-anywhere"
        aria-disabled={disabled}
        onClick={() => {
          if (!disabled) onSelect();
        }}
      >
        <RequestThreadTitle
          environmentId={environmentId}
          threadId={access.destinationThreadId}
          fallback={access.title}
        />
        <span className="block text-xs text-muted-foreground">
          {disabled
            ? "Changed · refresh for current state"
            : access.state === "pending"
              ? "Awaiting confirmation"
              : access.state === "revoked"
                ? "Access revoked"
                : "Active access"}
        </span>
        <span className="block text-xs text-muted-foreground">
          <RequestEnvironmentLabel environmentId={access.origin} /> ·{" "}
          <AccessExpiry expiresAt={access.expiresAt} />
        </span>
      </button>
      <RequestThreadStatus environmentId={environmentId} threadId={access.destinationThreadId} />
      {access.destinationThreadId ? (
        <a
          aria-label="Open exact thread"
          className="p-2 text-xs"
          href={threadHref(environmentId, access.destinationThreadId)}
        >
          ↗
        </a>
      ) : null}
    </article>
  );
}
function CurrentGroup({
  mode,
  environmentId,
  group,
  selected,
  choose,
  chooseAccess,
  load,
}: {
  mode: "active" | "pending" | Exclude<GroupView, "access">;
  environmentId: EnvironmentId;
  group: ReviewGroup;
  selected: string | null;
  choose: (item: Item) => void;
  chooseAccess: (id: string) => void;
  load: (older: boolean) => void;
}) {
  const title =
    mode === "active"
      ? "Active access"
      : mode === "pending"
        ? "Pending access"
        : mode === "inflight"
          ? "In-flight requests"
          : "Unresolved requests";
  const frozen = useFrozenRows(group.page?.records ?? [], (item) => bindingKey(item.record));
  const frozenAccess = useFrozenRows(
    (group.page?.access ?? []).filter((entry) => entry.state === mode),
    (entry) => entry.id,
  );
  const accessGroup = mode === "active" || mode === "pending";
  return (
    <section aria-label={title} className="mt-3 space-y-2">
      <h4 className="text-xs text-muted-foreground">{title}</h4>
      {group.loading ? (
        <p role="status" className="text-xs text-muted-foreground">
          Loading {title.toLowerCase()}…
        </p>
      ) : null}
      {group.error ? (
        <p role="alert" className="text-xs text-muted-foreground">
          {group.error}
        </p>
      ) : null}
      <div {...(accessGroup ? frozenAccess.handlers : frozen.handlers)} className="space-y-2">
        {accessGroup
          ? frozenAccess.rows.map((entry) => (
              <CompactAccess
                key={entry.id}
                environmentId={environmentId}
                access={entry}
                disabled={Boolean(group.error) || frozenAccess.isGone(entry)}
                onSelect={() => chooseAccess(entry.id)}
              />
            ))
          : frozen.rows.map((entry) => (
              <CompactRequest
                key={bindingKey(entry.record)}
                environmentId={environmentId}
                item={entry}
                selected={selected === entry.record.id}
                disabled={Boolean(group.error) || frozen.isGone(entry)}
                onSelect={() => choose(entry)}
              />
            ))}
      </div>
      {!group.loading &&
      group.page &&
      (accessGroup ? frozenAccess.rows.length === 0 : frozen.rows.length === 0) ? (
        <p className="text-xs text-muted-foreground">No {title.toLowerCase()} on this page.</p>
      ) : null}
      <div className="flex flex-wrap gap-2">
        <Button size="compact" variant="ghost" disabled={group.loading} onClick={() => load(false)}>
          Refresh {title.toLowerCase()}
        </Button>
        {group.page?.nextCursor ? (
          <Button
            size="compact"
            variant="outline"
            disabled={group.loading}
            onClick={() => load(true)}
          >
            Load more {title.toLowerCase()}
          </Button>
        ) : null}
      </div>
    </section>
  );
}
function AccessDetail({
  environmentId,
  access,
  disabled,
  canRestore,
  canEdit,
  mutate,
  reviewAccessReadback,
  uncertain,
}: {
  uncertain: boolean;
  reviewAccessReadback: () => Promise<unknown>;
  environmentId: EnvironmentId;
  access: Access;
  disabled: boolean;
  canRestore: boolean;
  canEdit: boolean;
  mutate: (input: Mutation) => void;
}) {
  const [duration, setDuration] = useState("");
  return (
    <>
      <h3 className="text-sm font-medium wrap-anywhere">
        <RequestThreadTitle
          environmentId={environmentId}
          threadId={access.destinationThreadId}
          fallback={access.title}
        />
      </h3>
      <RequestThreadStatus environmentId={environmentId} threadId={access.destinationThreadId} />
      <p className="mt-1 text-xs text-muted-foreground">
        <RequestEnvironmentLabel environmentId={access.origin} />
      </p>
      <p className="my-4 text-xs">
        {access.state === "revoked"
          ? "Access revoked · "
          : access.state === "pending"
            ? "Awaiting confirmation · "
            : ""}
        <AccessExpiry expiresAt={access.expiresAt} />
      </p>
      {access.destinationThreadId ? (
        <div className="flex flex-wrap items-center gap-2">
          <DurationSelect value={duration} onChange={setDuration} disabled={disabled || !canEdit} />
          <Button
            size="sm"
            disabled={
              disabled || !duration || !canEdit || (access.state === "revoked" && !canRestore)
            }
            onClick={() =>
              mutate({
                action: access.state === "revoked" ? "restore" : "update",
                id: access.id,
                expectedGeneration: access.generation,
                durationSeconds: Number(duration) as Duration,
                commandId: randomUUID(),
              })
            }
          >
            {access.state === "revoked" ? "Restore access" : "Update access"}
          </Button>
        </div>
      ) : null}
      {uncertain ? (
        <p role="status" className="mt-3 text-xs text-muted-foreground">
          Outcome unknown. Do not repeat this access action. Refresh access to read the original
          command’s current successor. Missing or pruned evidence remains uncertain.
        </p>
      ) : null}
      {disabled ? (
        <Button
          size="compact"
          variant="outline"
          onClick={() => {
            void reviewAccessReadback();
          }}
        >
          Refresh access
        </Button>
      ) : null}
      <p className="mt-3 text-xs text-muted-foreground">
        {!canEdit
          ? "Access editing is unavailable on this receiver. "
          : access.state === "revoked" && !canRestore
            ? "Restore is unavailable on this receiver. "
            : ""}
        Choose a duration, then apply it. Restoring access does not release held messages.
      </p>
      <div className="mt-auto flex justify-end pt-6">
        <Button
          size="sm"
          variant="destructive"
          disabled={disabled || !canEdit || access.state === "revoked"}
          onClick={() =>
            mutate({
              action: "revoke",
              id: access.id,
              expectedGeneration: access.generation,
              commandId: randomUUID(),
            })
          }
        >
          Revoke access
        </Button>
      </div>
    </>
  );
}

function RetryControls({
  item,
  disabled,
  grants,
  mutate,
}: {
  item: Item;
  disabled: boolean;
  grants: boolean;
  mutate: (input: Mutation) => void;
}) {
  const [duration, setDuration] = useState(grants && item.grantEligible ? "" : "once");
  return (
    <div className="flex flex-wrap gap-2">
      {grants && item.grantEligible ? (
        <DurationSelect value={duration} onChange={setDuration} once disabled={disabled} />
      ) : null}
      <Button
        size="compact"
        variant="outline"
        disabled={disabled || !duration}
        onClick={() =>
          mutate({
            action: "retry",
            id: item.record.id,
            bindingDigest: item.record.bindingDigest,
            commandId: randomUUID(),
            ...(grants && item.grantEligible && duration !== "once"
              ? { durationSeconds: Number(duration) as Duration }
              : {}),
          })
        }
      >
        Retry launch
      </Button>
    </div>
  );
}
