import { useEffect, useRef, useState } from "react";
import { Link, useLinkProps } from "@tanstack/react-router";
import { randomUUID } from "../../lib/utils";
import type { EnvironmentId } from "@t3tools/contracts";
import {
  bindingKey,
  type Access,
  type Duration,
  type Item,
  type Mutation,
  type View,
} from "@t3tools/client-runtime/jones/remoteAdmission/controller";
import { Button } from "../../components/ui/button";
import { admissionPresentation } from "./presentation";
import { useCopyToClipboard } from "../../hooks/useCopyToClipboard";
import {
  requestOutcomeLabel,
  requestOutcomeReason,
  recoveryDetailsText,
} from "./outcomePresentation";
import { RequestMarkdown } from "./RequestMarkdown";
import { DurationSelect, ReviewControls } from "./ReviewControls";
import { useFrozenRows, useReview } from "./reviewHooks";
import { reviewSupportKnownMissing, useReviewSupportKnownMissing } from "./reviewRuntime";
import { requestLink, threadLink, type RequestsSearch } from "./navigation";
import {
  AccessExpiry,
  RequestEnvironmentLabel,
  RequestThreadTitle,
  RequestThreadStatus,
} from "./RequestThreadStatus";

const requestViews = [
  ["current", "Current", "currentHeldAdmissions"],
  ["blocked", "Blocked", "blockedHeldAdmissions"],
  ["inflight", "In flight", "inFlightAdmissions"],
  ["unresolved", "Unresolved", "unresolvedAdmissions"],
  ["access", "Access", "activeAccessScopes"],
  ["done", "Done", null],
] as const;

interface RequestSelection {
  id: string;
  digest?: string | null;
  fromLink?: true;
}
type ExactLookup = "loading" | "unavailable" | { digest: string };

const lookupExactRequest = (
  controller: ReturnType<typeof useReview>["controller"],
  selection: RequestSelection,
): Promise<ExactLookup> =>
  selection.digest === null
    ? Promise.resolve("unavailable")
    : controller.lookup(selection.id, selection.digest).then(
        (item) => (item ? { digest: item.record.bindingDigest } : "unavailable"),
        () => "unavailable",
      );

export function ReviewPage({
  environmentId,
  label,
  search,
}: {
  environmentId: EnvironmentId;
  label: string;
  search?: RequestsSearch | undefined;
}) {
  const state = useReview(environmentId);
  const { page, capabilities, controller } = state;
  const supportMissing = useReviewSupportKnownMissing(environmentId);
  const requestedAccess = search?.access;
  const requested = requestedAccess ? undefined : search?.request;
  const digest = requested ? search?.binding : undefined;
  const routeKey = JSON.stringify({ request: requested, binding: digest, access: requestedAccess });
  const [previousRouteKey, setPreviousRouteKey] = useState(routeKey);
  const linkedSelection: RequestSelection | null = requested
    ? { id: requested, ...(digest === undefined ? {} : { digest }), fromLink: true }
    : null;
  const [selection, setSelection] = useState(linkedSelection);
  const [lookupRevision, setLookupRevision] = useState(0);
  const [requestRead, setRequestRead] = useState<{ revision: number; result: ExactLookup }>();
  const [linkedAccess, setLinkedAccess] = useState(requestedAccess);
  const [accessRead, setAccessRead] = useState<{ revision: number; loaded: boolean }>();
  const [accessSelection, setAccessSelection] = useState({
    id: requestedAccess ?? null,
    revision: state.accessSelectionRevision,
  });
  const [reviewBlocked, setReviewBlocked] = useState(false);
  if (routeKey !== previousRouteKey) {
    // Fence the old selection before this render can expose any decision controls.
    setPreviousRouteKey(routeKey);
    setSelection(linkedSelection);
    setLookupRevision(lookupRevision + 1);
    setLinkedAccess(requestedAccess);
    setAccessSelection({ id: requestedAccess ?? null, revision: state.accessSelectionRevision });
    setReviewBlocked(false);
  }
  const exactLookup = requestRead?.revision === lookupRevision ? requestRead.result : "loading";
  const exactAccess = accessRead?.revision === lookupRevision ? accessRead.loaded : undefined;
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
    setLinkedAccess(undefined);
    setAccessSelection({ id, revision: state.accessSelectionRevision });
  };
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- An exact route must finish its own lookup before it can show a cached request or expose decisions.
  useEffect(() => {
    if (!selection?.fromLink) return;
    let current = true;
    void lookupExactRequest(controller, selection).then((readback) => {
      if (current) setRequestRead({ revision: lookupRevision, result: readback });
    });
    return () => {
      current = false;
    };
  }, [controller, selection, lookupRevision]);
  const openAccess = (id: string) => {
    setAccessId(id);
    void controller.lookupAccess(id).catch(() => undefined);
  };
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Exact access must complete readback before a route can expose editing controls.
  useEffect(() => {
    if (!linkedAccess) return;
    let current = true;
    void controller.lookupAccess(linkedAccess).then(
      () => {
        if (current) setAccessRead({ revision: lookupRevision, loaded: true });
      },
      () => {
        if (current) setAccessRead({ revision: lookupRevision, loaded: false });
      },
    );
    return () => {
      current = false;
    };
  }, [controller, linkedAccess, lookupRevision]);
  const selectedDigest = selection?.fromLink
    ? typeof exactLookup === "object"
      ? exactLookup.digest
      : undefined
    : selection?.digest;
  const matchesSelection = (entry: Item) =>
    entry.record.id === selection?.id && entry.record.bindingDigest === selectedDigest;
  const lookedUpItem =
    state.selectedItem && matchesSelection(state.selectedItem) ? state.selectedItem : null;
  const item = capabilities?.canDecide
    ? (page?.records.find(matchesSelection) ?? lookedUpItem)
    : null;
  const itemView = item ? admissionPresentation(item.record) : null;
  const selectedAccess = state.selectedAccess?.id === accessId ? state.selectedAccess : null;
  const access = linkedAccess
    ? exactAccess && state.selectedAccessFromId === linkedAccess
      ? selectedAccess
      : null
    : (selectedAccess ?? page?.access.find((entry) => entry.id === accessId) ?? null);
  const disabled = item
    ? state.policyBarrier
      ? "Evaluator settings are changing."
      : state.pending.has(bindingKey(item.record))
        ? "Recording this decision…"
        : state.uncertain.has(bindingKey(item.record))
          ? "Outcome unknown. Do not resend."
          : supportMissing
            ? "Request review V2 is unsupported on this receiver."
            : state.loading || state.loadFailed
              ? "Refresh server state before deciding."
              : item.record.envelope.receiverPolicy.revision !== page?.policyRevision
                ? "Evaluator settings changed. Refresh this request before approving."
                : null
    : null;
  const mutate = (input: Mutation) => {
    if (!reviewSupportKnownMissing(environmentId)) void controller.mutate(input);
  };
  const choose = (entry: Item) => {
    const { id, bindingDigest } = entry.record;
    setSelection({ id, digest: bindingDigest });
    setAccessId(null);
    setReviewBlocked(false);
    void controller.lookup(id, bindingDigest).catch(() => undefined);
  };
  const clearSelection = () => {
    setSelection(null);
    setAccessId(null);
    setReviewBlocked(false);
  };
  const changeView = (view: View) => {
    if (view === state.view) return;
    clearSelection();
    void controller.load(view);
  };
  const busy = state.policyBarrier || state.pending.size > 0;
  const hasSelection = accessId !== null || selection !== null;
  const viewsRef = useRef<HTMLElement>(null);
  const detailRef = useRef<HTMLElement>(null);
  const backRef = useRef<HTMLDivElement>(null);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- When the narrow list is hidden, move DOM focus into its replacement panel.
  useEffect(() => {
    if (hasSelection && backRef.current?.getClientRects().length) detailRef.current?.focus();
  }, [hasSelection]);
  return (
    <section className="@container min-w-0 space-y-4" aria-label={`Requests on ${label}`}>
      <div className="flex flex-wrap items-center justify-between gap-2 border-b pb-3">
        <h2 className="min-w-0 text-base font-semibold wrap-anywhere">{label}</h2>
        <Button
          size="sm"
          variant="outline"
          disabled={busy || state.loading}
          onClick={() => {
            void controller.load();
          }}
        >
          Refresh
        </Button>
      </div>
      <nav ref={viewsRef} aria-label="Request views" className="flex flex-wrap items-center gap-1">
        {requestViews.map(([view, title, count]) => (
          <Button
            key={view}
            aria-pressed={state.view === view}
            variant={state.view === view ? "secondary" : "ghost"}
            size="sm"
            disabled={busy}
            onClick={() => changeView(view)}
          >
            {title}
            {page && count && (
              <span className="text-xs tabular-nums text-muted-foreground">
                {page.counts[count]}
                {view === "access" ? " active" : ""}
              </span>
            )}
          </Button>
        ))}
      </nav>
      {supportMissing || state.error ? (
        <p role="alert" className="rounded-lg border border-border bg-muted/30 px-3 py-2 text-sm">
          {supportMissing ? "Request review V2 is unsupported on this receiver." : state.error}
        </p>
      ) : null}
      {state.loading ? (
        <p role="status" className="text-xs text-muted-foreground">
          Loading requests…
        </p>
      ) : null}
      {state.view === "done" && page ? (
        <p className="text-xs text-muted-foreground">
          {page.recordCount} completed {page.recordCount === 1 ? "request" : "requests"}
        </p>
      ) : null}
      <div className="grid min-w-0 items-start gap-4 @min-[46rem]:grid-cols-[minmax(0,2fr)_minmax(0,3fr)]">
        <div className={hasSelection ? "hidden min-w-0 @min-[46rem]:block" : "min-w-0"}>
          <ReviewRows
            key={state.view}
            environmentId={environmentId}
            state={state}
            supportMissing={supportMissing}
            selectedItem={item}
            accessId={accessId}
            choose={choose}
            chooseAccess={(id) => {
              openAccess(id);
              setSelection(null);
            }}
          />
        </div>
        <section
          ref={detailRef}
          tabIndex={-1}
          aria-label="Request details"
          className={`min-w-0 flex-col rounded-lg border p-4 @min-[46rem]:sticky @min-[46rem]:top-4 @min-[46rem]:max-h-[calc(100dvh-5rem)] @min-[46rem]:overflow-y-auto ${hasSelection ? "flex" : "hidden @min-[46rem]:flex"}`}
        >
          {hasSelection ? (
            <div ref={backRef} className="mb-4 @min-[46rem]:hidden">
              <Button
                size="compact"
                variant="outline"
                onClick={() => {
                  clearSelection();
                  viewsRef.current
                    ?.querySelector<HTMLButtonElement>('[aria-pressed="true"]')
                    ?.focus();
                }}
              >
                Back to requests
              </Button>
            </div>
          ) : null}
          {access ? (
            <AccessDetail
              key={`${access.id}:${access.generation}:${access.state}:${state.accessSelectionRevision}`}
              environmentId={environmentId}
              access={access}
              uncertain={state.uncertain.has(`access:${access.id}`)}
              reviewAccessReadback={() => controller.lookupAccess(access.id).catch(() => undefined)}
              disabled={
                supportMissing ||
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
              {linkedAccess && exactAccess === undefined ? (
                <p role="status" className="text-sm text-muted-foreground">
                  Loading exact access…
                </p>
              ) : (
                <>
                  <p className="text-sm text-muted-foreground">
                    Exact access record unavailable. Refresh its current state before editing.
                  </p>
                  <Button
                    size="compact"
                    variant="outline"
                    onClick={() => {
                      if (linkedAccess) {
                        setLookupRevision((revision) => revision + 1);
                      } else {
                        openAccess(accessId);
                      }
                    }}
                  >
                    Refresh access
                  </Button>
                </>
              )}
            </div>
          ) : item && itemView ? (
            <>
              <div className="flex flex-wrap items-center gap-2">
                <h3 className="min-w-0 flex-1 text-sm font-medium wrap-anywhere">
                  <RequestThreadTitle
                    environmentId={environmentId}
                    threadId={item.threadId}
                    fallback={itemView.title ?? "Incoming request"}
                  />
                </h3>
                <RequestThreadStatus environmentId={environmentId} threadId={item.threadId} />
              </div>
              <p className="mt-1 text-xs text-muted-foreground wrap-anywhere">
                {itemView.source}
                {itemView.project ? ` · ${itemView.project.label}` : ""}
              </p>
              <p className="mt-1 text-xs text-muted-foreground wrap-anywhere">
                {itemView.operation}
              </p>
              <div className="my-4">
                <RequestMarkdown>{itemView.message}</RequestMarkdown>
              </div>
              {requestOutcomeReason(item) ? (
                <p className="mb-4 text-xs text-muted-foreground">
                  Reason:{" "}
                  {item.launchResult
                    ? requestOutcomeReason(item)?.replaceAll("_", " ")
                    : requestOutcomeReason(item)}
                </p>
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
                <p className="text-xs text-muted-foreground">{requestOutcomeLabel(item)}</p>
              )}
              <LaunchEvidence item={item} />
              {item.launchResult ||
              item.outcome ||
              (item.record.envelope.operation === "t3_thread_launch" &&
                item.record.status === "dispatched") ? (
                <RecoveryDetails
                  key={`${bindingKey(item.record)}:${item.attempt}`}
                  receiver={environmentId}
                  item={item}
                />
              ) : null}
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
                  Source thread (reported): {itemView.thread?.label ?? "Not reported"}
                </p>
                <pre className="max-h-72 overflow-auto whitespace-pre-wrap wrap-anywhere">
                  {JSON.stringify(
                    {
                      record: item.record,
                      launchResult: item.launchResult ?? null,
                      originalLaunchResult: item.originalLaunchResult ?? null,
                    },
                    null,
                    2,
                  )}
                </pre>
              </details>
            </>
          ) : selection ? (
            <div className="space-y-2">
              {selection.fromLink && exactLookup === "loading" ? (
                <p role="status" className="text-sm text-muted-foreground">
                  Loading exact request…
                </p>
              ) : (
                <>
                  <p role="status" className="text-sm text-muted-foreground">
                    Exact request unavailable.
                  </p>
                  {selection.fromLink ? (
                    <Button
                      size="compact"
                      variant="outline"
                      onClick={() => setLookupRevision((revision) => revision + 1)}
                    >
                      Refresh request
                    </Button>
                  ) : null}
                </>
              )}
            </div>
          ) : (
            <>
              <h3 className="text-sm font-medium">Request details</h3>
              <p className="mt-1 text-sm text-muted-foreground">
                Select a request or access record.
              </p>
            </>
          )}
        </section>
      </div>
    </section>
  );
}
function ReviewRows({
  environmentId,
  state,
  supportMissing,
  selectedItem,
  accessId,
  choose,
  chooseAccess,
}: {
  environmentId: EnvironmentId;
  state: ReturnType<typeof useReview>;
  supportMissing: boolean;
  selectedItem: Item | null;
  accessId: string | null;
  choose: (item: Item) => void;
  chooseAccess: (id: string) => void;
}) {
  const { page, view, controller } = state;
  const records = useFrozenRows(page?.records ?? [], (item) => bindingKey(item.record));
  const access = useFrozenRows(page?.access ?? [], (entry) => entry.id);
  const diagnostics = useFrozenRows(page?.diagnostics ?? [], (entry) => entry.id);
  const frozen = view === "access" ? access : records;
  const showDiagnostics = view === "blocked" && diagnostics.rows.length > 0;
  return (
    <div className="min-w-0 space-y-2">
      <div className="space-y-2" {...frozen.handlers}>
        {records.rows.map((entry) => (
          <CompactRequest
            key={bindingKey(entry.record)}
            environmentId={environmentId}
            item={entry}
            selected={
              selectedItem !== null && bindingKey(selectedItem.record) === bindingKey(entry.record)
            }
            disabled={records.isGone(entry)}
            onSelect={() => choose(entry)}
          />
        ))}
        {access.rows.map((entry) => (
          <CompactAccess
            key={entry.id}
            environmentId={environmentId}
            access={entry}
            selected={entry.id === accessId}
            disabled={access.isGone(entry)}
            onSelect={() => chooseAccess(entry.id)}
          />
        ))}
      </div>
      {page &&
      !supportMissing &&
      !state.loadFailed &&
      records.rows.length === 0 &&
      access.rows.length === 0 &&
      !showDiagnostics ? (
        <p className="rounded-lg border border-dashed px-3 py-4 text-sm text-muted-foreground">
          {view === "current"
            ? "No requests need review."
            : view === "access"
              ? "No access records."
              : "No requests in this view."}
        </p>
      ) : null}
      {showDiagnostics ? (
        <div className="space-y-2" {...diagnostics.handlers}>
          {page?.diagnosticCounts ? (
            <p className="text-xs text-muted-foreground">
              {page.diagnosticCounts.logicalEvents} blocked events ·{" "}
              {page.diagnosticCounts.repeatArrivals} repeat arrivals ·{" "}
              {page.diagnosticCounts.summaryRows} summaries
            </p>
          ) : null}
          {diagnostics.rows.map((event) => (
            <div key={event.id} className="rounded-lg border p-3 text-xs wrap-anywhere">
              <p>
                {event.reason} · {event.repeats} repeats
              </p>
              {event.requestId ? (
                <Link
                  className="underline"
                  aria-disabled={diagnostics.isGone(event)}
                  onClick={(action) => {
                    if (diagnostics.isGone(event)) action.preventDefault();
                  }}
                  {...requestLink(environmentId, event.requestId)}
                >
                  Review exact held message ↗
                </Link>
              ) : (
                <p className="text-muted-foreground">Retained message unavailable</p>
              )}
            </div>
          ))}
        </div>
      ) : null}
      {page?.nextCursor ? (
        <Button
          variant="outline"
          size="sm"
          disabled={state.loading || state.policyBarrier || state.pending.size > 0}
          onClick={() => {
            void controller.load(view, true);
          }}
        >
          Load older records
        </Button>
      ) : null}
    </div>
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
  const outcome = disabled ? "Resolved · refresh for current state" : requestOutcomeLabel(item);
  return (
    <article
      className={`flex min-w-0 items-start rounded-lg border ${selected ? "border-ring bg-accent" : "hover:bg-muted/40"}`}
    >
      <button
        type="button"
        className="min-w-0 flex-1 rounded-lg px-3 py-2.5 text-left focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-ring"
        aria-current={selected || undefined}
        aria-disabled={disabled}
        onClick={() => {
          if (!disabled) onSelect();
        }}
      >
        <span className="line-clamp-2 text-sm/5 font-medium wrap-anywhere">
          <RequestThreadTitle
            environmentId={environmentId}
            threadId={item.threadId}
            fallback={view.title ?? "Incoming request"}
          />
        </span>
        <span className="mt-0.5 block truncate text-xs text-muted-foreground">
          {view.source}
          {view.project ? ` · ${view.project.label}` : ""}
        </span>
        <span className="mt-1.5 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs">
          <span className="text-muted-foreground">{outcome}</span>
          <RequestThreadStatus environmentId={environmentId} threadId={item.threadId} />
        </span>
        <span className="mt-1 block truncate text-xs text-muted-foreground">{view.operation}</span>
      </button>
      <Link
        {...requestLink(environmentId, item.record.id, item.record.bindingDigest)}
        aria-label="Open exact request"
        className="m-1 grid size-8 shrink-0 place-items-center rounded-md text-sm text-muted-foreground hover:bg-muted hover:text-foreground"
      >
        ↗
      </Link>
    </article>
  );
}
function CompactAccess({
  environmentId,
  access,
  selected,
  disabled,
  onSelect,
}: {
  environmentId: EnvironmentId;
  access: Access;
  selected: boolean;
  disabled: boolean;
  onSelect: () => void;
}) {
  return (
    <article
      className={`flex min-w-0 items-start rounded-lg border ${selected ? "border-ring bg-accent" : "hover:bg-muted/40"}`}
    >
      <button
        type="button"
        className="min-w-0 flex-1 rounded-lg px-3 py-2.5 text-left focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-ring"
        aria-current={selected || undefined}
        aria-disabled={disabled}
        onClick={() => {
          if (!disabled) onSelect();
        }}
      >
        <span className="line-clamp-2 text-sm/5 font-medium wrap-anywhere">
          <RequestThreadTitle
            environmentId={environmentId}
            threadId={access.destinationThreadId}
            fallback={access.title}
          />
        </span>
        <span className="mt-0.5 block text-xs text-muted-foreground wrap-anywhere">
          <RequestEnvironmentLabel environmentId={access.origin} /> ·{" "}
          <AccessExpiry expiresAt={access.expiresAt} />
        </span>
        <span className="mt-1.5 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs">
          <span className="text-muted-foreground">
            {disabled
              ? "Changed · refresh for current state"
              : access.state === "pending"
                ? "Awaiting confirmation"
                : access.state === "revoked"
                  ? "Access revoked"
                  : "Active access"}
          </span>
          <RequestThreadStatus
            environmentId={environmentId}
            threadId={access.destinationThreadId}
          />
        </span>
      </button>
      {access.destinationThreadId ? (
        <Link
          aria-label="Open exact thread"
          className="m-1 grid size-8 shrink-0 place-items-center rounded-md text-sm text-muted-foreground hover:bg-muted hover:text-foreground"
          {...threadLink(environmentId, access.destinationThreadId)}
        >
          ↗
        </Link>
      ) : null}
    </article>
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

function LaunchEvidence({ item }: { item: Item }) {
  const result = item.launchResult;
  if (!result) return null;
  return (
    <div className="mt-3 space-y-2 text-xs">
      <dl className="grid grid-cols-2 gap-x-3 gap-y-1" aria-label="Recorded launch stages">
        <dt>Creation</dt>
        <dd>{result.stages.creation}</dd>
        <dt>Availability</dt>
        <dd>{result.stages.availability}</dd>
        <dt>Preparation</dt>
        <dd>{result.stages.preparation}</dd>
        <dt>Initial message</dt>
        <dd>{result.stages.initialMessage}</dd>
        <dt>Provider progress</dt>
        <dd>{result.stages.providerProgress}</dd>
        <dt>Recorded at</dt>
        <dd className="wrap-anywhere">{result.recordedAt}</dd>
      </dl>
      {result.outcome === "not_created" ? (
        <p className="text-muted-foreground">Preparatory work may have occurred.</p>
      ) : null}
    </div>
  );
}
function RecoveryDetails({ receiver, item }: { receiver: EnvironmentId; item: Item }) {
  const [error, setError] = useState<string | null>(null);
  const { href } = useLinkProps(requestLink(receiver, item.record.id, item.record.bindingDigest));
  const { copyToClipboard, isCopied } = useCopyToClipboard({
    target: "recovery details",
    onCopy: () => setError(null),
    onError: () => setError("Could not copy recovery details. Try again."),
  });
  return (
    <div className="mt-3 space-y-1">
      <Button
        size="compact"
        variant="outline"
        onClick={() => {
          if (href)
            copyToClipboard(
              recoveryDetailsText(receiver, item, new URL(href, window.location.href).href),
              undefined,
            );
        }}
      >
        {isCopied ? "Recovery details copied" : "Copy recovery details"}
      </Button>
      {error ? (
        <p role="alert" className="text-xs text-muted-foreground">
          {error}
        </p>
      ) : null}
    </div>
  );
}
