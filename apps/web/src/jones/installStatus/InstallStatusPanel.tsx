import { type Ref, useId, useRef, useState } from "react";
import type { EnvironmentId } from "@t3tools/contracts";
import type { JonesInstallPlanSelection } from "@t3tools/contracts/jones/installPlan";
import {
  CheckIcon,
  ChevronDownIcon,
  ChevronLeftIcon,
  ChevronRightIcon,
  Clock3Icon,
  LaptopIcon,
  TriangleAlertIcon,
  XIcon,
} from "lucide-react";
import { Button } from "../../components/ui/button";
import { Checkbox } from "../../components/ui/checkbox";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import {
  deviceSelectionLocked,
  deviceStatusActive,
  operationOutstanding,
} from "@t3tools/client-runtime/jones/install-plan";
import { cn } from "../../lib/utils";
import { useInstallPlan } from "./useInstallPlan";
import { installStatusStore, type InstallStatusStore } from "./panelUi";
import {
  deviceAttention,
  deviceHeardAt,
  deviceLiveness,
  packageCheckLine,
  panelPresentation,
} from "./presentation";
import { Elapsed, HeardAgo } from "./Elapsed";
import { ProgressSteps } from "./ProgressSteps";
import { desktopCanPreviewInstallPlan, selectionFor } from "./entrypoints";
import { installPlanServices, requestInstallPlan } from "./services";
import {
  inspectPanelCleanup,
  performPanelAction,
  recoverPanelCleanup,
  type CleanupInspection,
  type PanelAction,
} from "./actions";
import { finishPlan } from "./finishPlan";
import { abandonDevice } from "./abandonDevice";
import { ManualInstallClosure } from "./ManualInstallClosure";
import { INSTALL_PLAN_GO, INSTALL_PLAN_WAIT, lastUpdateLabel } from "./copy";

interface EditDraft {
  readonly planId: string;
  readonly revision: number;
  readonly selected: ReadonlySet<EnvironmentId>;
}

export function InstallStatusPanel({
  store = installStatusStore,
}: {
  readonly store?: InstallStatusStore;
}) {
  const snapshot = useInstallPlan(store);
  const [draft, setDraft] = useState<EditDraft | null>(null);
  const [detail, setDetail] = useState<EnvironmentId | "laptop" | null>(null);
  const allDevicesButton = useRef<HTMLButtonElement>(null);
  const [pending, setPending] = useState<ReadonlySet<string>>(new Set());
  const [cleanupInspection, setCleanupInspection] = useState<{
    readonly key: string;
    readonly inspection: CleanupInspection;
    readonly recovery?: "released" | "released-plan-unconfirmed" | "unproved";
  } | null>(null);
  const descriptionId = useId();
  const view = panelPresentation(snapshot, snapshot.updatedAt);
  if (!snapshot.open || snapshot.native === null) return null;
  if (view?.plan == null)
    return snapshot.native.readOnly === undefined && snapshot.error === null ? null : (
      <section
        aria-label="Install status"
        className="shrink-0 rounded-lg border border-sidebar-border p-2 text-xs text-muted-foreground"
      >
        <h2>Install status</h2>
        <p role="status">{snapshot.native.readOnly?.message ?? snapshot.error}</p>
      </section>
    );
  const plan = view.plan;
  const editing = draft !== null && draft.planId === plan.planId && !view.readOnly;
  const selected = editing
    ? draft!.selected
    : new Set(plan.devices.filter((d) => d.membership === "selected").map((d) => d.environmentId));
  const complete = view.headerDoneVisible && !editing;
  const laptopId = plan.laptop.environmentId;
  const devices = new Map(
    snapshot.devices.filter((d) => d.environmentId !== laptopId).map((d) => [d.environmentId, d]),
  );
  const ids = [...new Set([...plan.devices.map((d) => d.environmentId), ...devices.keys()])];
  const choices = ids.map((environmentId) => {
    const device = devices.get(environmentId);
    const member = plan.devices.find((d) => d.environmentId === environmentId);
    const selection = device === undefined ? undefined : selectionFor(device);
    const locked = member !== undefined && deviceSelectionLocked(plan, member);
    const reason = locked
      ? "Open device details for recovery options."
      : selection === undefined
        ? (device?.observation.message ??
          (device?.canOperate === false
            ? "This session cannot change updates."
            : "Fresh operations and enrollment capability are required."))
        : undefined;
    const row = view.rows.find((r) => r.environmentId === environmentId);
    const active = deviceStatusActive(device?.observation, member);
    const health = snapshot.pollHealth[environmentId];
    const liveness = deviceLiveness(device, health, snapshot.updatedAt);
    return {
      environmentId,
      active,
      liveness,
      attention: deviceAttention(row?.progress, liveness, snapshot.updatedAt),
      heardAt: deviceHeardAt(device, health),
      checkLine: packageCheckLine(row, device, active, snapshot.updatedAt),
      label: member?.label ?? device?.label ?? "Device",
      selection,
      locked,
      reason,
      row,
      device,
    };
  });
  const perform = async (key: string, work: () => Promise<void>) => {
    if (pending.has(key)) return;
    setPending((old) => new Set([...old, key]));
    store.update({ error: null });
    await Promise.resolve()
      .then(work)
      .catch((error: unknown) => {
        store.update({
          error:
            error instanceof Error
              ? error.message
              : "The request is not confirmed. Read status before continuing.",
        });
      })
      .finally(() => {
        setPending((old) => new Set([...old].filter((item) => item !== key)));
      });
  };
  const action = (environmentId: EnvironmentId | "laptop", value: PanelAction) =>
    void perform(`${environmentId}:${value}`, () =>
      performPanelAction(
        environmentId,
        value,
        !view.preview && value === "check" ? plan.planId : undefined,
      ),
    );
  const editDone = () =>
    void perform("edit", async () => {
      if (!editing) return;
      const select: JonesInstallPlanSelection[] = choices.flatMap((c) =>
        selected.has(c.environmentId) && c.selection !== undefined ? [c.selection] : [],
      );
      if (view.preview) store.update({ previewSelection: select });
      else {
        const binding = installPlanServices();
        const result = await requestInstallPlan(
          {
            type: "edit",
            planId: plan.planId,
            expectedRevision: draft!.revision,
            select,
            deselect: plan.devices
              .filter((d) => d.membership === "selected" && !selected.has(d.environmentId))
              .map((d) => d.environmentId),
          },
          binding,
        );
        if (result.refused !== undefined) throw new Error(result.refused.message);
        const locked = result.applied?.filter((d) => d.result === "locked-started") ?? [];
        if (locked.length > 0)
          store.update({
            error: locked
              .map(
                (d) =>
                  `${choices.find((c) => c.environmentId === d.environmentId)?.label ?? "Device"} started before the edit applied. Open device details for recovery options.`,
              )
              .join(" "),
          });
      }
      setDraft(null);
    });
  const collapse = () => {
    setDraft(null);
    setDetail(null);
    store.collapse(!snapshot.collapsed);
  };
  const rowClass =
    "flex min-h-8 w-full min-w-0 items-center gap-2 rounded-md px-2 text-left text-xs hover:bg-sidebar-row-hover focus-visible:outline-2 focus-visible:outline-ring";
  const availableLaptop = view.laptop?.actions;
  const focusedDevice =
    detail === "laptop" || choices.some((choice) => choice.environmentId === detail)
      ? detail
      : null;
  const overview = focusedDevice === null;
  const gateClosed = plan.laptop.gateClosed !== undefined;
  const canCheckLaptop =
    !view.readOnly ||
    (plan.cancelled === true &&
      view.laptop?.phase === "held" &&
      snapshot.native.current === null &&
      snapshot.native.readOnly === undefined);
  const hasClosureHistory =
    snapshot.native.last?.manualClosure !== undefined || snapshot.native.last?.cancelled === true;
  const updatePreview =
    view.readOnly &&
    hasClosureHistory &&
    snapshot.native.current === null &&
    snapshot.native.laptop.environmentId !== undefined &&
    desktopCanPreviewInstallPlan(snapshot.desktop);
  const summary = view.readOnly
    ? plan.cancelled === true
      ? "Update plan cancelled"
      : lastUpdateLabel(plan.dismissedAt ?? plan.createdAt)
    : view.preview
      ? "Laptop first"
      : gateClosed
        ? "Laptop changed"
        : view.summary;
  const returnToOverview = () => {
    setDetail(null);
    allDevicesButton.current?.focus();
  };
  return (
    <section
      aria-label="Install status"
      data-install-status
      data-plan-id={plan.planId}
      data-plan-preview={view.preview || undefined}
      className="flex max-h-[min(26rem,45dvh)] min-h-0 shrink-0 flex-col rounded-xl border border-sidebar-border bg-sidebar text-sidebar-foreground"
      onKeyDown={(event) => {
        if (event.key === "Escape") {
          setDraft(null);
          if (!overview) returnToOverview();
        }
      }}
    >
      <header
        className="grid min-h-9 shrink-0 grid-cols-[auto_minmax(0,1fr)_2.5rem_3rem] items-center gap-1 px-2"
        data-install-header
      >
        <h2 className="whitespace-nowrap text-xs font-medium" data-install-title>
          Install status
        </h2>
        <Tooltip>
          <TooltipTrigger
            render={<span tabIndex={0} className="truncate text-2xs text-muted-foreground" />}
          >
            {summary}
          </TooltipTrigger>
          <TooltipPopup>{summary}</TooltipPopup>
        </Tooltip>
        <span
          data-install-done-slot
          aria-hidden={!complete}
          inert={!complete}
          style={{ visibility: complete ? "visible" : "hidden" }}
          className="flex w-10 justify-end"
        >
          <Button
            variant="ghost"
            size="micro"
            aria-label="Done with update plan"
            disabled={pending.has("dismiss")}
            onClick={() => void perform("dismiss", () => finishPlan(plan.planId))}
          >
            Done
          </Button>
        </span>
        <span className="flex w-12 items-center justify-end">
          <Button
            variant="ghost-muted"
            size="icon-micro"
            aria-label={snapshot.collapsed ? "Expand Install status" : "Collapse Install status"}
            aria-expanded={!snapshot.collapsed}
            data-install-chevron
            onClick={collapse}
          >
            <ChevronDownIcon className={cn("size-3.5", snapshot.collapsed && "-rotate-90")} />
          </Button>
          <Button
            variant="ghost-muted"
            size="icon-micro"
            aria-label="Hide Install status"
            onClick={() => {
              setDraft(null);
              setDetail(null);
              store.update({ open: false });
            }}
          >
            <XIcon className="size-3.5" />
          </Button>
        </span>
      </header>
      {!snapshot.collapsed ? (
        <div className="min-h-0 overflow-y-auto overscroll-contain px-1 pb-1" data-install-body>
          {!editing ? (
            <nav aria-label="Install status views" className="flex flex-wrap gap-0.5 px-1 pb-1">
              <DeviceViewButton
                buttonRef={allDevicesButton}
                label="All"
                accessibleLabel="All devices"
                selected={overview}
                onClick={() => setDetail(null)}
              />
              <DeviceViewButton
                label="Laptop"
                selected={focusedDevice === "laptop"}
                onClick={() => setDetail("laptop")}
              />
              {choices.map(({ environmentId, label }) => (
                <DeviceViewButton
                  key={environmentId}
                  label={label}
                  selected={focusedDevice === environmentId}
                  onClick={() => setDetail(environmentId)}
                />
              ))}
            </nav>
          ) : null}
          {!overview ? (
            <div className="px-1">
              <Button variant="ghost-muted" size="micro" onClick={returnToOverview}>
                <ChevronLeftIcon aria-hidden className="size-3" />
                Back to all devices
              </Button>
            </div>
          ) : null}
          {overview && !editing && hasClosureHistory && (view.preview || updatePreview) ? (
            <div className="flex min-h-7 items-center px-2 pb-1">
              <Button variant="ghost-muted" size="micro" onClick={() => store.open(view.preview)}>
                {view.preview ? "Last update" : "Back to available update"}
              </Button>
            </div>
          ) : null}
          {!editing && !view.preview ? (
            <ManualInstallClosure store={store} plan={plan} readOnly={view.readOnly} />
          ) : null}
          {overview || focusedDevice === "laptop" ? (
            <>
              <button
                type="button"
                className={rowClass}
                disabled={editing}
                onClick={() => (overview ? setDetail("laptop") : returnToOverview())}
                aria-expanded={focusedDevice === "laptop"}
                aria-label="Laptop update details"
              >
                <LaptopIcon className="size-3.5 shrink-0 text-muted-foreground" />
                <span>Laptop</span>
                <span
                  className={cn(
                    "ml-auto min-w-0 text-right text-muted-foreground",
                    overview && "truncate",
                  )}
                >
                  {view.laptop?.text}
                  {view.laptop?.progress?.active ? (
                    <>
                      {" · "}
                      <Elapsed
                        since={view.laptop.progress.stepStartedAt}
                        skewMs={view.laptop.progress.skewMs}
                      />
                    </>
                  ) : null}
                </span>
                {overview && !editing ? (
                  <ChevronRightIcon className="size-3 shrink-0 text-muted-foreground" />
                ) : null}
              </button>
              {!view.readOnly &&
              !editing &&
              (availableLaptop?.download || availableLaptop?.install || availableLaptop?.stop) ? (
                <div className="flex flex-wrap gap-1 px-2 pb-2">
                  {availableLaptop?.download ? (
                    <Button
                      size="micro"
                      variant="outline"
                      disabled={pending.has("laptop:download")}
                      onClick={() => action("laptop", "download")}
                    >
                      {view.laptop?.downloadLabel}
                    </Button>
                  ) : null}
                  {availableLaptop?.install ? (
                    <Button
                      size="micro"
                      variant="outline"
                      disabled={pending.has("laptop:install")}
                      onClick={() => action("laptop", "install")}
                    >
                      Install on Laptop
                    </Button>
                  ) : null}
                  {availableLaptop?.stop ? (
                    <Button
                      size="micro"
                      variant="outline"
                      disabled={pending.has("laptop:stop")}
                      onClick={() => action("laptop", "stop")}
                    >
                      {pending.has("laptop:stop") ? "Requesting stop…" : "Stop download"}
                    </Button>
                  ) : null}
                </div>
              ) : null}
              {focusedDevice === "laptop" ? (
                <div
                  aria-label="Laptop update status"
                  className="space-y-1 px-2 pb-2 text-2xs text-muted-foreground"
                >
                  {view.laptop?.progress ? <ProgressSteps progress={view.laptop.progress} /> : null}
                  {view.laptop?.operationId ? (
                    <OperationId value={view.laptop.operationId} />
                  ) : null}
                  {plan.laptop.operations.at(-1)?.result?.code ? (
                    <p>Code: {plan.laptop.operations.at(-1)!.result!.code}</p>
                  ) : null}
                  {plan.laptop.operations.at(-1)?.result?.message ? (
                    <p>{plan.laptop.operations.at(-1)!.result!.message}</p>
                  ) : null}
                  {view.laptop?.phase === "held" ? (
                    <p>
                      {plan.cancelled === true
                        ? "Check the Laptop outcome to update this history entry."
                        : plan.laptop.operations.at(-1)?.step === "install"
                          ? "Check the running Laptop build. If it matches the plan and passes verification, the other devices can continue."
                          : "Check whether the previous download has finished or stopped."}
                    </p>
                  ) : null}
                  {availableLaptop?.abandon ? (
                    <Button
                      variant="outline"
                      size="micro"
                      disabled={pending.has("laptop:abandon-stage")}
                      onClick={() => action("laptop", "abandon-stage")}
                    >
                      {plan.laptop.stageAbandonment?.status === "pending"
                        ? "Abandoning staged update…"
                        : plan.laptop.stageAbandonment?.status === "unconfirmed"
                          ? "Retry abandoning staged update"
                          : "Abandon staged update"}
                    </Button>
                  ) : null}
                  {!view.readOnly &&
                  plan.laptop.stageAbandonment?.status === "retired" &&
                  availableLaptop?.download !== true &&
                  installPlanServices()?.services.bridge.checkForUpdate !== undefined ? (
                    <Button
                      variant="ghost-muted"
                      size="micro"
                      disabled={pending.has("laptop:check-build")}
                      onClick={() => action("laptop", "check-build")}
                    >
                      Check for builds
                    </Button>
                  ) : null}
                  {canCheckLaptop ? (
                    <Button
                      variant="ghost-muted"
                      size="micro"
                      disabled={pending.has("laptop:check")}
                      onClick={() => action("laptop", "check")}
                    >
                      {pending.has("laptop:check")
                        ? "Checking outcome…"
                        : view.laptop?.phase === "held"
                          ? "Check update outcome"
                          : "Refresh now"}
                    </Button>
                  ) : null}
                </div>
              ) : null}
            </>
          ) : null}
          {overview ? (
            <div
              data-install-gate
              className="flex min-h-8 items-center gap-1.5 border-t border-dashed border-sidebar-border px-2 text-2xs text-muted-foreground"
            >
              <Clock3Icon className="size-3 shrink-0" />
              <span className="min-w-0 flex-1 truncate">
                {plan.cancelled === true
                  ? "Queued updates cancelled"
                  : gateClosed
                    ? "Other devices won’t start"
                    : plan.laptop.gate !== undefined
                      ? INSTALL_PLAN_GO
                      : INSTALL_PLAN_WAIT}
              </span>
              {!view.readOnly ? (
                <span className="flex shrink-0 items-center">
                  {editing ? (
                    <>
                      <Button
                        variant="ghost-muted"
                        size="micro"
                        onClick={() =>
                          setDraft({
                            ...draft!,
                            selected: new Set(
                              choices
                                .filter((c) =>
                                  c.locked
                                    ? selected.has(c.environmentId)
                                    : c.selection !== undefined,
                                )
                                .map((c) => c.environmentId),
                            ),
                          })
                        }
                      >
                        All
                      </Button>
                      <Button
                        variant="ghost-muted"
                        size="micro"
                        onClick={() =>
                          setDraft({
                            ...draft!,
                            selected: new Set(
                              choices
                                .filter((c) => c.locked && selected.has(c.environmentId))
                                .map((c) => c.environmentId),
                            ),
                          })
                        }
                      >
                        None
                      </Button>
                      <Button
                        variant="ghost-muted"
                        size="micro"
                        disabled={pending.has("edit")}
                        onClick={editDone}
                      >
                        Done
                      </Button>
                      <Button
                        variant="ghost-muted"
                        size="micro"
                        disabled={pending.has("edit")}
                        onClick={() => setDraft(null)}
                      >
                        Cancel
                      </Button>
                    </>
                  ) : (
                    <Button
                      variant="ghost-muted"
                      size="micro"
                      onClick={() =>
                        setDraft({
                          planId: plan.planId,
                          revision: plan.revision,
                          selected: new Set(selected),
                        })
                      }
                    >
                      Edit
                    </Button>
                  )}
                </span>
              ) : null}
            </div>
          ) : null}
          <ul className="m-0 list-none p-0">
            {choices.map((choice) => {
              const { environmentId, label, row, device, attention } = choice;
              if (!overview && focusedDevice !== environmentId) return null;
              const included = selected.has(environmentId);
              const silent =
                attention.kind === "unreachable" || attention.kind === "restart-silent";
              const text = editing
                ? included
                  ? "Included"
                  : "Not in plan"
                : row &&
                    plan.cancelled === true &&
                    ["queued", "not-started", "staged", "fenced"].includes(row.phase)
                  ? "Skipped for this plan"
                  : row?.phase === "not-started" && gateClosed
                    ? "Not started · queue closed"
                    : (row?.text ??
                      (choice.reason === undefined
                        ? "Not in plan"
                        : `Can't join · ${choice.reason}`));
              const lineText =
                !editing &&
                silent &&
                row?.attemptText === undefined &&
                row?.sourceText === undefined
                  ? attention.text
                  : text;
              const timer =
                !editing && !silent && row?.attemptText === undefined && row?.progress?.active
                  ? row.progress
                  : undefined;
              const open = focusedDevice === environmentId;
              const updated = row?.phase === "done" || row?.phase === "updated-elsewhere";
              const held = row?.phase === "held" || row?.phase === "unknown";
              const operations = plan.devices.find(
                (d) => d.environmentId === environmentId,
              )?.operations;
              const operation = operations?.find(operationOutstanding) ?? operations?.at(-1);
              const receipt = operation?.receipt;
              const setupUnconfirmed =
                operation?.step === "directive" && operation.directive === undefined;
              const launcherUnconfirmed =
                operation?.step === "install" && receipt?.code === "launcher-unconfirmed";
              const inspectKey =
                receipt?.phase === "held" &&
                receipt.code === "cleanup-unconfirmed" &&
                receipt.jobId !== undefined &&
                receipt.action.kind !== "install"
                  ? `${plan.planId}:${environmentId}:${receipt.operationId}:${receipt.updatedAt}:cleanup`
                  : null;
              const cleanup = cleanupInspection?.key === inspectKey ? cleanupInspection : null;
              const canCheckArchived =
                snapshot.native?.readOnly === undefined &&
                ((plan.cancelled === true && (inspectKey !== null || setupUnconfirmed)) ||
                  launcherUnconfirmed);
              return (
                <li key={environmentId}>
                  <div className="flex min-w-0 items-center gap-1">
                    {editing ? (
                      <Checkbox
                        aria-label={`Include ${label}`}
                        checked={included}
                        disabled={choice.locked || (!included && choice.selection === undefined)}
                        aria-describedby={
                          choice.reason === undefined
                            ? undefined
                            : `${descriptionId}-${environmentId}`
                        }
                        className="ml-2"
                        onCheckedChange={(checked) => {
                          const next = new Set(selected);
                          if (checked) next.add(environmentId);
                          else next.delete(environmentId);
                          setDraft({ ...draft!, selected: next });
                        }}
                      />
                    ) : null}
                    <Tooltip>
                      <TooltipTrigger
                        render={
                          <button
                            type="button"
                            className={rowClass}
                            disabled={pending.has("edit")}
                          />
                        }
                        aria-label={`${label} update details`}
                        aria-expanded={open}
                        onClick={() => {
                          if (editing) setDraft(null);
                          if (open) returnToOverview();
                          else setDetail(environmentId);
                        }}
                      >
                        {held ? (
                          <TriangleAlertIcon aria-hidden className="size-3 shrink-0 text-warning" />
                        ) : updated && !editing ? (
                          <CheckIcon aria-hidden className="size-3 shrink-0 text-success" />
                        ) : (
                          <span
                            aria-hidden
                            data-liveness={choice.liveness.kind}
                            data-attention={attention.kind}
                            className={cn(
                              "size-1.5 shrink-0 rounded-full border border-muted-foreground",
                              !included && "bg-muted-foreground opacity-50",
                              !editing &&
                                row?.activeWork !== undefined &&
                                !silent &&
                                "border-foreground bg-foreground opacity-100",
                              silent && "border-warning bg-warning opacity-100",
                            )}
                          />
                        )}
                        <span
                          className={cn("min-w-0 truncate", !included && "text-muted-foreground")}
                        >
                          {label}
                        </span>
                        <span
                          className={cn(
                            "ml-auto min-w-0 flex-1 text-right text-2xs text-muted-foreground",
                            !open && "truncate",
                            silent && "text-warning",
                          )}
                          data-row-text
                        >
                          {lineText}
                          {timer !== undefined ? (
                            <>
                              {" · "}
                              <Elapsed since={timer.stepStartedAt} skewMs={timer.skewMs} />
                            </>
                          ) : null}
                        </span>
                        {!open ? (
                          <ChevronRightIcon className="size-3 shrink-0 text-muted-foreground" />
                        ) : null}
                      </TooltipTrigger>
                      <TooltipPopup>
                        {editing && choice.reason !== undefined ? choice.reason : lineText}
                      </TooltipPopup>
                    </Tooltip>
                  </div>
                  {overview && timer !== undefined && timer.steps.length > 0 ? (
                    <div
                      role="progressbar"
                      aria-label={`${label} update steps`}
                      aria-valuemin={0}
                      aria-valuemax={timer.steps.length}
                      aria-valuenow={timer.steps.filter((step) => step.status === "done").length}
                      aria-valuetext={`Step ${timer.stepIndex} of ${timer.stepCount}: ${timer.stepLabel}`}
                      className="mx-3 mb-1 ml-7 flex h-0.5 gap-0.5"
                    >
                      {timer.steps.map((step) => (
                        <span
                          key={step.key}
                          className={cn(
                            "flex-1 rounded-full bg-sidebar-border",
                            step.status === "done" && "bg-muted-foreground",
                            step.status === "current" && "bg-info/80",
                          )}
                        />
                      ))}
                    </div>
                  ) : null}
                  {!editing && (attention.kind === "quiet" || attention.kind === "stalled") ? (
                    <p
                      className="flex items-center gap-1 px-3 pb-1 text-2xs text-muted-foreground"
                      role="status"
                      data-attention-line={attention.kind}
                    >
                      {attention.kind === "stalled" ? (
                        <TriangleAlertIcon className="size-3 shrink-0 text-warning/70" />
                      ) : null}
                      {attention.text}
                    </p>
                  ) : null}
                  {!editing && row?.sourceText ? (
                    <p className="px-3 pb-1 text-2xs text-muted-foreground">{row.sourceText}</p>
                  ) : null}
                  {editing && choice.reason !== undefined ? (
                    <p
                      id={`${descriptionId}-${environmentId}`}
                      className="px-2 pb-1 text-2xs text-muted-foreground"
                    >
                      {choice.reason}
                    </p>
                  ) : null}
                  {open ? (
                    <div
                      className="space-y-1 px-3 pb-2 text-2xs text-muted-foreground"
                      aria-label={`${label} update status`}
                    >
                      {row?.progress ? <ProgressSteps progress={row.progress} /> : null}
                      {row?.attemptText ? <p>{row.attemptText}</p> : null}
                      {device?.observation.freshness !== "fresh" ? (
                        <p>{device?.observation.message ?? "Fresh status unavailable."}</p>
                      ) : null}
                      {choice.checkLine === undefined ? null : <p>{choice.checkLine}</p>}
                      {device === undefined ? null : <HeardAgo at={choice.heardAt} />}
                      {row?.operationId ? <OperationId value={row.operationId} /> : null}
                      {receipt?.code ? <p>Code: {receipt.code}</p> : null}
                      {receipt?.message ? <p>{receipt.message}</p> : null}
                      {receipt?.stop?.result === "too-late" ? (
                        <p>The download completed before Stop. Review installation separately.</p>
                      ) : receipt?.stop?.result === "not-stoppable-phase" ? (
                        <p>This phase cannot be stopped. Read the operation outcome.</p>
                      ) : null}
                      {held ? (
                        <p>
                          {setupUnconfirmed
                            ? plan.cancelled === true
                              ? "The host had yet to confirm setup when this plan was cancelled."
                              : "The host has yet to confirm its update setup. The plan is still at setup for this device."
                            : receipt?.code === "cleanup-unconfirmed"
                              ? "Inspect cleanup to check whether the disk image from this attempt is still attached."
                              : "The host has yet to confirm the outcome of this attempt. Check the operation receipt. If the hold remains, open Connections for this host."}
                        </p>
                      ) : null}
                      {cleanup !== null ? (
                        <p role="status">
                          {cleanup.recovery === "unproved"
                            ? "Cleanup release is unconfirmed. Check the update outcome before trying again."
                            : cleanup.recovery === "released"
                              ? "The cleanup hold was released."
                              : cleanup.recovery === "released-plan-unconfirmed"
                                ? "The host released the cleanup hold. Check the update outcome to refresh this plan."
                                : cleanup.inspection.status === "eligible"
                                  ? "Inspection confirmed the prior disk image is absent. The update remains on hold."
                                  : "Inspection could not confirm cleanup. The update remains on hold."}
                        </p>
                      ) : null}
                      {!view.readOnly || canCheckArchived ? (
                        <div className="flex flex-wrap gap-1">
                          {inspectKey !== null && receipt !== undefined && device?.canOperate ? (
                            <Button
                              variant="outline"
                              size="micro"
                              disabled={pending.has(inspectKey)}
                              onClick={() =>
                                void perform(inspectKey, async () => {
                                  setCleanupInspection(null);
                                  const inspection = await inspectPanelCleanup(
                                    environmentId,
                                    receipt.operationId,
                                  );
                                  setCleanupInspection({ key: inspectKey, inspection });
                                })
                              }
                            >
                              {pending.has(inspectKey) ? "Inspecting cleanup…" : "Inspect cleanup"}
                            </Button>
                          ) : null}
                          {inspectKey !== null &&
                          receipt !== undefined &&
                          device?.canOperate &&
                          cleanup?.inspection.recoveryToken !== undefined ? (
                            <Button
                              variant="outline"
                              size="micro"
                              disabled={pending.has(inspectKey)}
                              onClick={() => {
                                const token = cleanup.inspection.recoveryToken;
                                if (token === undefined) return;
                                void perform(inspectKey, async () => {
                                  const recovery = await recoverPanelCleanup(
                                    environmentId,
                                    receipt,
                                    token,
                                  );
                                  if (recovery !== "cancelled")
                                    setCleanupInspection({
                                      key: inspectKey,
                                      inspection: { status: "eligible" },
                                      recovery,
                                    });
                                });
                              }}
                            >
                              Recover cleanup
                            </Button>
                          ) : null}
                          {row?.actions.remove ? (
                            <Button
                              variant="outline"
                              size="micro"
                              disabled={pending.has(`${environmentId}:remove`)}
                              onClick={() => action(environmentId, "remove")}
                            >
                              {pending.has(`${environmentId}:remove`)
                                ? "Removing from queue…"
                                : "Remove from queue"}
                            </Button>
                          ) : null}
                          {row?.actions.download ? (
                            <Button
                              variant="outline"
                              size="micro"
                              disabled={pending.has(`${environmentId}:download`)}
                              onClick={() => action(environmentId, "download")}
                            >
                              {row.downloadLabel}
                            </Button>
                          ) : null}
                          {row?.actions.install ? (
                            <Button
                              variant="outline"
                              size="micro"
                              disabled={pending.has(`${environmentId}:install`)}
                              onClick={() => action(environmentId, "install")}
                            >
                              {row.installLabel}
                            </Button>
                          ) : null}
                          {row?.actions.stop ? (
                            <Button
                              variant="outline"
                              size="micro"
                              disabled={pending.has(`${environmentId}:stop`)}
                              onClick={() => action(environmentId, "stop")}
                            >
                              {pending.has(`${environmentId}:stop`)
                                ? "Requesting stop…"
                                : `Stop download on ${label}`}
                            </Button>
                          ) : null}
                          {row?.actions.abandon ? (
                            <Button
                              variant="outline"
                              size="micro"
                              disabled={pending.has(`${environmentId}:abandon`)}
                              onClick={() =>
                                void perform(`${environmentId}:abandon`, () =>
                                  abandonDevice(environmentId, plan.planId),
                                )
                              }
                            >
                              {pending.has(`${environmentId}:abandon`)
                                ? "Abandoning update…"
                                : plan.devices.find(
                                      (device) => device.environmentId === environmentId,
                                    )?.abandoning
                                  ? "Resume abandonment"
                                  : "Abandon update"}
                            </Button>
                          ) : null}
                          {row?.actions.end ? (
                            <Button
                              variant="ghost-muted"
                              size="micro"
                              disabled={pending.has(`${environmentId}:end`)}
                              onClick={() => action(environmentId, "end")}
                            >
                              {pending.has(`${environmentId}:end`)
                                ? "Stopping update…"
                                : "Stop update"}
                            </Button>
                          ) : null}
                          <Button
                            variant="ghost-muted"
                            size="micro"
                            disabled={pending.has(`${environmentId}:check`)}
                            onClick={() => action(environmentId, "check")}
                          >
                            {pending.has(`${environmentId}:check`)
                              ? "Checking…"
                              : setupUnconfirmed
                                ? "Check setup status"
                                : held || launcherUnconfirmed
                                  ? "Check update outcome"
                                  : "Refresh now"}
                          </Button>
                        </div>
                      ) : null}
                    </div>
                  ) : null}
                </li>
              );
            })}
          </ul>
          {!editing &&
          !view.readOnly &&
          !view.preview &&
          snapshot.native.view.cancelPlan === 1 &&
          (overview || focusedDevice === "laptop") ? (
            <div className="border-t border-dashed border-sidebar-border px-2 py-1">
              <Button
                size="micro"
                variant="ghost-muted"
                disabled={pending.has("cancel-plan")}
                onClick={() =>
                  void perform("cancel-plan", async () => {
                    const binding = installPlanServices();
                    if (binding === undefined) throw new Error("Install status is not connected.");
                    if (
                      !(await binding.services.confirm(
                        "Cancel this update plan? Queued updates will be dropped. Any setup already sent can still reach the host. Install history and cleanup holds will be kept.",
                      ))
                    )
                      return;
                    const result = await requestInstallPlan(
                      {
                        type: "cancel-plan",
                        planId: plan.planId,
                        expectedRevision: plan.revision,
                      },
                      binding,
                    );
                    if (result.refused !== undefined) throw new Error(result.refused.message);
                    if (
                      result.state.current !== null ||
                      result.state.last?.planId !== plan.planId ||
                      result.state.last.cancelled !== true
                    )
                      throw new Error(
                        "Cancellation could not be confirmed. Refresh Install status.",
                      );
                    setDetail(null);
                    store.open(true);
                  })
                }
              >
                Cancel update plan
              </Button>
            </div>
          ) : null}
          {snapshot.error ? (
            <p role="status" className="px-2 py-1 text-2xs text-muted-foreground">
              {snapshot.error}
            </p>
          ) : null}
        </div>
      ) : null}
    </section>
  );
}

function DeviceViewButton({
  buttonRef,
  label,
  accessibleLabel = label,
  selected,
  onClick,
}: {
  readonly buttonRef?: Ref<HTMLButtonElement>;
  readonly label: string;
  readonly accessibleLabel?: string;
  readonly selected: boolean;
  readonly onClick: () => void;
}) {
  return (
    <span className="grid min-w-0 max-w-24 grid-cols-[minmax(0,1fr)]">
      <Button
        ref={buttonRef}
        variant={selected ? "secondary" : "ghost-muted"}
        size="micro"
        aria-label={accessibleLabel}
        aria-pressed={selected}
        title={accessibleLabel}
        onClick={onClick}
      >
        <span className="min-w-0 truncate">{label}</span>
      </Button>
    </span>
  );
}

function OperationId({ value }: { readonly value: string }) {
  return (
    <label className="block">
      Operation{" "}
      <input
        aria-label="Operation ID"
        readOnly
        value={value}
        className="w-full select-all rounded border border-sidebar-border bg-transparent px-1 font-mono text-3xs"
        onFocus={(event) => event.currentTarget.select()}
      />
    </label>
  );
}
