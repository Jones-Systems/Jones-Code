import { type Ref, useEffect, useId, useRef, useState } from "react";
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
  allowsOperationRecovery,
  deviceSelectionLocked,
  deviceStatusActive,
  isProvisionalUnsupportedReceipt,
  operationOutstanding,
  orphanReceiptCheck,
} from "@t3tools/client-runtime/jones/install-plan";
import { cn } from "../../lib/utils";
import { useInstallPlan, useInstallPlanBinding } from "./useInstallPlan";
import { installStatusStore, type InstallStatusSnapshot, type InstallStatusStore } from "./panelUi";
import {
  deviceAttention,
  deviceHeardAt,
  deviceLiveness,
  packageCheckLine,
  panelPresentation,
} from "./presentation";
import { Elapsed, HeardAgo } from "./Elapsed";
import { ProgressSteps } from "./ProgressSteps";
import { desktopCanPreviewInstallPlan, selectionFor, targetForDesktop } from "./entrypoints";
import {
  confirmClosedPlan,
  installPlanServices,
  nativePlanIssue,
  planEnrollmentFor,
  readTargetPlan,
  readWritableTarget,
  requestInstallPlan,
  requireWritablePlan,
  type InstallPlanBinding,
} from "./services";
import {
  canFenceArchivedOperation,
  canStopOperation,
  checkOrphanedPlanCommand,
  fencePanelOperation,
  inspectPanelCleanup,
  latestPanelOperation,
  performPanelAction,
  recoverPanelCleanup,
  stopPanelDownload,
  type CleanupInspection,
  type PanelAction,
  type PanelActionTarget,
} from "./actions";
import { finishPlan } from "./finishPlan";
import { abandonDevice, deviceAbandonTarget } from "./abandonDevice";
import { ManualInstallClosure } from "./ManualInstallClosure";
import {
  INSTALL_PLAN_GO,
  INSTALL_PLAN_WAIT,
  LAPTOP_REOPEN_TO_CHECK_INSTALL,
  lastUpdateLabel,
  operationAdmissionMessage,
} from "./copy";

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
  const view = panelPresentation(snapshot, snapshot.updatedAt);
  const active = useInstallPlanBinding();
  const binding = active?.store === store ? active : undefined;
  const selection = snapshot.historySelection;
  const historyId = snapshot.showLast ? snapshot.historyPlanId : null;
  const documentRevision = snapshot.native?.documentRevision;
  const historyRevision =
    snapshot.native?.recoveryHistory?.find((entry) => entry.planId === historyId)?.revision ??
    (snapshot.native?.last?.planId === historyId ? snapshot.native.last.revision : undefined);
  const needsHistory =
    snapshot.open &&
    historyId !== null &&
    snapshot.native?.view.recoveryHistory === 1 &&
    snapshot.native.readOnly === undefined &&
    (snapshot.historyPlan === null ||
      (historyRevision === undefined
        ? snapshot.historyDocumentRevision !== documentRevision
        : historyRevision !== snapshot.historyPlan.revision));
  const requestedRevision = needsHistory ? (historyRevision ?? documentRevision ?? 0) : null;
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Archive detail is fetched for this exact desk, selection and durable revision.
  useEffect(() => {
    if (requestedRevision === null || binding === undefined || historyId === null) return;
    let cancelled = false;
    const showError = (error: string) => {
      if (!cancelled && installPlanServices() === binding && store.historySelection() === selection)
        store.update({ error });
    };
    void readTargetPlan(binding, { planId: historyId, archived: true })
      .then(({ result, plan }) => {
        const issue = nativePlanIssue(result);
        if (issue !== undefined || plan === null)
          showError(issue ?? "This saved update is unavailable.");
      })
      .catch(() => showError("Install recovery history could not be read from Laptop."));
    return () => {
      cancelled = true;
    };
  }, [binding, store, historyId, requestedRevision, selection]);

  if (!snapshot.open || snapshot.native === null) return null;
  if (view?.plan == null) {
    const message = snapshot.native.readOnly?.message ?? snapshot.nativeRefusal ?? snapshot.error;
    if (
      message === null &&
      historyId === null &&
      !snapshot.native.recoveryHistory?.length &&
      !snapshot.native.orphanedCommands?.length
    )
      return null;
    return (
      <section
        aria-label="Install status"
        className="shrink-0 rounded-lg border border-sidebar-border p-2 text-xs text-muted-foreground"
      >
        <h2>Install status</h2>
        <HistoryNavigation store={store} snapshot={snapshot} binding={binding} preview={false} />
        {message !== null || needsHistory ? (
          <p role="status">{message ?? "Reading recovery history…"}</p>
        ) : null}
      </section>
    );
  }
  return (
    <InstallStatusPlan
      key={`${selection}:${view.plan.planId}`}
      store={store}
      snapshot={snapshot}
      binding={binding}
      view={view}
      historyRefreshing={needsHistory && historyRevision !== view.plan.revision}
    />
  );
}

function InstallStatusPlan({
  store,
  snapshot,
  binding,
  view,
  historyRefreshing,
}: {
  readonly store: InstallStatusStore;
  readonly snapshot: InstallStatusSnapshot;
  readonly binding: InstallPlanBinding | undefined;
  readonly view: NonNullable<ReturnType<typeof panelPresentation>>;
  readonly historyRefreshing: boolean;
}) {
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
  if (view.plan === null || snapshot.native === null) return null;
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
  const memberIds = plan.devices.map((d) => d.environmentId);
  const ids = view.archived ? memberIds : [...new Set([...memberIds, ...devices.keys()])];
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
    if (binding === undefined || installPlanServices() !== binding) {
      store.update({ error: "Install status connection changed. Refresh Install status." });
      return;
    }
    const selection = store.historySelection();
    setPending((old) => new Set([...old, key]));
    store.update({ error: null });
    await Promise.resolve()
      .then(work)
      .catch((error: unknown) => {
        if (store.historySelection() !== selection || installPlanServices() !== binding) return;
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
  const planTarget = {
    planId: plan.planId,
    archived: view.archived,
    expectedRevision: plan.revision,
  };
  const targetFor = (environmentId: EnvironmentId | "laptop"): PanelActionTarget => {
    const laptop = environmentId === "laptop";
    const operationId = laptop
      ? view.laptop?.operationId
      : view.rows.find((r) => r.environmentId === environmentId)?.operationId;
    const stagedHandle = laptop
      ? snapshot.desktop?.jones?.stagedHandle
      : devices.get(environmentId)?.observation.state?.stagedHandle;
    return {
      ...planTarget,
      operationId: operationId ?? null,
      ...(laptop ? { laptopEnvironmentId: laptopId } : {}),
      ...(stagedHandle === undefined ? {} : { stagedHandle }),
      ...(view.preview ? { preview: plan.target } : {}),
    };
  };
  const action = (environmentId: EnvironmentId | "laptop", value: PanelAction) =>
    void perform(`${environmentId}:${value}`, () =>
      performPanelAction(environmentId, value, targetFor(environmentId), binding),
    );
  const stop = (kind: "laptop" | "device", environmentId: EnvironmentId, operationId: string) =>
    void perform(`${kind === "laptop" ? "laptop" : environmentId}:stop`, () =>
      stopPanelDownload({ ...planTarget, kind, environmentId, operationId }, binding),
    );
  const editDone = () =>
    void perform("edit", async () => {
      if (!editing) return;
      const select: JonesInstallPlanSelection[] = choices.flatMap((c) =>
        selected.has(c.environmentId) && c.selection !== undefined ? [c.selection] : [],
      );
      if (view.preview) {
        if (binding === undefined) throw new Error("Install status is not connected.");
        const read = await requestInstallPlan({ type: "read" }, binding);
        requireWritablePlan(binding, read);
        const desktopTarget = targetForDesktop(store.get().desktop);
        if (
          read.state.current !== null ||
          desktopTarget === undefined ||
          desktopTarget.sourceSha !== plan.target.sourceSha ||
          desktopTarget.sourceTree !== plan.target.sourceTree ||
          read.state.laptop.environmentId !== laptopId
        )
          throw new Error("The available update changed. Review the devices again.");
        store.update({ previewSelection: select });
      } else {
        if (binding === undefined) throw new Error("Install status is not connected.");
        await readWritableTarget(binding, { ...planTarget, expectedRevision: draft!.revision });
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
        const issue = nativePlanIssue(result);
        if (issue !== undefined) throw new Error(issue);
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
  const laptopOperationId = view.laptop?.operationId;
  const gateIssue = view.laptop?.gateIssue;
  const gatePaused = gateIssue?.phase === "gatePaused";
  const reopenToCheckInstall = view.laptop?.reopenToCheckInstall === true;
  const focusedDevice =
    detail === "laptop" || choices.some((choice) => choice.environmentId === detail)
      ? detail
      : null;
  const overview = focusedDevice === null;
  const canCheckLaptop = !view.readOnly || view.archived || view.deskReadOnly;
  const archivedRecovery = view.archived && snapshot.native.view.recoveryHistory === 1;
  const canRecoverHistory = archivedRecovery && !view.deskReadOnly && !historyRefreshing;
  const archivedLaptopStop =
    archivedRecovery &&
    plan.laptop.operations.at(-1)?.step === "download" &&
    [undefined, "running"].includes(plan.laptop.operations.at(-1)?.result?.status);
  const pendingLaptopAbandonment = plan.laptop.stageAbandonment;
  const archivedLaptopAbandon =
    archivedRecovery &&
    pendingLaptopAbandonment !== undefined &&
    pendingLaptopAbandonment.status !== "retired";
  const summary = view.archived
    ? plan.cancelled === true
      ? "Scheduling stopped"
      : lastUpdateLabel(plan.dismissedAt ?? plan.supersededAt ?? plan.createdAt)
    : view.preview
      ? "Laptop first"
      : (gateIssue?.title ?? view.summary);
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
            onClick={() =>
              void perform("dismiss", () => finishPlan(plan.planId, plan.revision, binding))
            }
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
          {overview && !editing ? (
            <HistoryNavigation
              store={store}
              snapshot={snapshot}
              binding={binding}
              preview={view.preview}
              displayedPlan={plan.planId}
            />
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
                  {view.archived && view.laptop?.phase === "preview"
                    ? "No update recorded"
                    : view.laptop?.text}
                  {view.archived && view.laptop?.hasRecoveryDuty ? " · Recovery pending" : null}
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
                  {availableLaptop?.stop && laptopOperationId !== undefined ? (
                    <Button
                      size="micro"
                      variant="outline"
                      disabled={pending.has("laptop:stop")}
                      onClick={() => stop("laptop", laptopId, laptopOperationId)}
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
                  {view.archived ? (
                    <RunningBuild
                      version={
                        snapshot.native.laptop.activeVersion ?? snapshot.desktop?.currentVersion
                      }
                      source={
                        snapshot.native.laptop.activeSource ??
                        snapshot.desktop?.jones?.installedSource?.sha
                      }
                      fresh={snapshot.native.readOnly === undefined}
                    />
                  ) : gateIssue !== undefined ? (
                    <p>{gateIssue.reason}</p>
                  ) : null}
                  {reopenToCheckInstall ? (
                    <p>{LAPTOP_REOPEN_TO_CHECK_INSTALL}</p>
                  ) : view.laptop?.phase === "held" ? (
                    <p>
                      {view.archived
                        ? "Check the Laptop outcome to update this history entry."
                        : plan.laptop.operations.at(-1)?.step === "install"
                          ? "Check the running Laptop build. If it matches the plan and passes verification, the other devices can continue."
                          : "Check whether the previous download has finished or stopped."}
                    </p>
                  ) : null}
                  {archivedLaptopStop && laptopOperationId !== undefined ? (
                    <Button
                      variant="outline"
                      size="micro"
                      disabled={!canRecoverHistory || pending.has("laptop:stop")}
                      onClick={() => stop("laptop", laptopId, laptopOperationId)}
                    >
                      Stop this download
                    </Button>
                  ) : null}
                  {availableLaptop?.abandon || archivedLaptopAbandon ? (
                    <Button
                      variant="outline"
                      size="micro"
                      disabled={
                        pending.has("laptop:abandon-stage") || (view.archived && !canRecoverHistory)
                      }
                      onClick={() => {
                        const creator = plan.laptop.operations.findLast(
                          (op) => op.step === "download",
                        );
                        const operationId = archivedLaptopAbandon
                          ? pendingLaptopAbandonment?.operationId
                          : creator?.operationId;
                        const stagedHandle = archivedLaptopAbandon
                          ? pendingLaptopAbandonment?.handle
                          : creator?.createdStagedHandle;
                        if (operationId === undefined || stagedHandle === undefined) return;
                        void perform("laptop:abandon-stage", () =>
                          performPanelAction(
                            "laptop",
                            "abandon-stage",
                            {
                              ...planTarget,
                              operationId,
                              stagedHandle,
                              laptopEnvironmentId: laptopId,
                            },
                            binding,
                          ),
                        );
                      }}
                    >
                      {archivedLaptopAbandon
                        ? "Resume staged update retirement"
                        : plan.laptop.stageAbandonment?.status === "pending"
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
                        : view.laptop?.phase === "held" ||
                            reopenToCheckInstall ||
                            (view.archived && view.laptop?.hasRecoveryDuty)
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
                {view.archived
                  ? "Scheduling for this plan is closed"
                  : gatePaused
                    ? "Other devices paused · waiting for Laptop verification"
                    : gateIssue !== undefined
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
                : view.archived && row?.setupUnconfirmed
                  ? "Setup unconfirmed"
                  : row?.phase === "not-started" && gateIssue?.phase === "gateClosed"
                    ? "Not started · queue closed"
                    : (row?.text ??
                      (choice.reason === undefined
                        ? "Not in plan"
                        : `Can't join · ${choice.reason}`));
              const lineText =
                !editing &&
                silent &&
                !row?.launcherOutcomeUnconfirmed &&
                !(view.archived && row?.hasRecoveryDuty) &&
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
              const held =
                row?.phase === "held" ||
                row?.phase === "unknown" ||
                (row?.launcherOutcomeUnconfirmed === true && !row.settled);
              const member = plan.devices.find((d) => d.environmentId === environmentId);
              const operations = member?.operations;
              const operation = view.archived
                ? member === undefined
                  ? undefined
                  : latestPanelOperation(member, true)
                : operations?.find((op) => op.operationId === row?.operationId);
              const receipt = operation?.receipt;
              const enrollment =
                member === undefined ? "unknown" : planEnrollmentFor(member, device);
              const receiptMessage = operationAdmissionMessage(receipt?.code) ?? receipt?.message;
              const routeMessage =
                enrollment === "route-changed" &&
                member?.membership === "selected" &&
                receipt?.code !== "admission-route-changed"
                  ? operationAdmissionMessage("admission-route-changed")
                  : undefined;
              const operationId = operation?.operationId;
              const setupUnconfirmed =
                operation?.step === "directive" && operationOutstanding(operation);
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
              const cleanupTarget =
                inspectKey !== null && receipt?.jobId !== undefined
                  ? {
                      ...planTarget,
                      environmentId,
                      operationId: receipt.operationId,
                      jobId: receipt.jobId,
                    }
                  : undefined;
              const stopArchived =
                archivedRecovery && operation !== undefined && canStopOperation(operation);
              const fenceArchived =
                archivedRecovery &&
                operation !== undefined &&
                canFenceArchivedOperation(operation, snapshot.updatedAt);
              const abandonTarget = deviceAbandonTarget(plan, environmentId, view.archived);
              const resumeArchived = archivedRecovery && abandonTarget?.resume === true;
              const canInspect = !view.archived || archivedRecovery || plan.cancelled === true;
              const hostRecoveryReason = view.deskReadOnly
                ? (snapshot.native?.readOnly?.message ??
                  snapshot.nativeRefusal ??
                  "Install status is read-only.")
                : historyRefreshing
                  ? "Reading updated recovery history."
                  : device?.canOperate === false
                    ? "This session cannot change updates."
                    : device?.observation.freshness !== "fresh" ||
                        device.observation.state?.environmentId !== environmentId
                      ? "Fresh status for this device is required."
                      : device.observation.state.capability.operations !== 1
                        ? "This device does not currently support update operations."
                        : undefined;
              const routeUnconfirmed =
                "The original device route is unconfirmed. Refresh this device.";
              const recoveryReason =
                hostRecoveryReason ??
                (view.archived && member !== undefined && enrollment !== "match"
                  ? routeUnconfirmed
                  : undefined);
              const stopReason =
                hostRecoveryReason ??
                (!allowsOperationRecovery(enrollment, "stop") ? routeUnconfirmed : undefined);
              const fenceReason = setupUnconfirmed
                ? (recoveryReason ??
                  (snapshot.native?.view.directiveFence !== 1 ||
                  device?.fleetStatus?.fencePlanDirective !== 1
                    ? "The host and Laptop must support fencing this original setup request."
                    : undefined))
                : (hostRecoveryReason ??
                  (!allowsOperationRecovery(enrollment, "fence")
                    ? routeUnconfirmed
                    : operation?.request.kind === "operation-start" &&
                        operation.request.input.admission !== undefined &&
                        device?.observation.state?.capability.boundAdmission !== 1
                      ? operationAdmissionMessage("admission-binding-required")
                      : undefined));
              const recoveryDescription = `${descriptionId}-recovery-${environmentId}`;
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
                          {view.archived && row?.hasRecoveryDuty ? " · Recovery pending" : null}
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
                      {view.archived ? (
                        <RunningBuild
                          version={device?.observation.state?.currentVersion}
                          source={device?.observation.state?.installedSource?.sha}
                          fresh={device?.observation.freshness === "fresh"}
                        />
                      ) : null}
                      {row?.queueReason !== undefined && row.queueReason !== lineText ? (
                        <p>{row.queueReason}</p>
                      ) : null}
                      {device?.observation.freshness !== "fresh" ? (
                        <p>{device?.observation.message ?? "Fresh status unavailable."}</p>
                      ) : null}
                      {choice.checkLine === undefined ? null : <p>{choice.checkLine}</p>}
                      {device === undefined ? null : <HeardAgo at={choice.heardAt} />}
                      {row?.operationId ? <OperationId value={row.operationId} /> : null}
                      {receipt?.code ? <p>Code: {receipt.code}</p> : null}
                      {receiptMessage ? <p>{receiptMessage}</p> : null}
                      {routeMessage ? <p>{routeMessage}</p> : null}
                      {receipt !== undefined && isProvisionalUnsupportedReceipt(receipt) ? (
                        <p>Check the exact operation receipt to confirm what the host recorded.</p>
                      ) : null}
                      {operation?.directiveFence === undefined ? null : (
                        <p>
                          {operation.directiveFence.outcome === "fenced"
                            ? "The original setup request is fenced."
                            : "The host confirmed that later setup replaced the original directive."}
                        </p>
                      )}
                      {receipt?.stop?.result === "too-late" ? (
                        <p>The download completed before Stop. Review installation separately.</p>
                      ) : receipt?.stop?.result === "not-stoppable-phase" ? (
                        <p>This phase cannot be stopped. Read the operation outcome.</p>
                      ) : null}
                      {held || (view.archived && setupUnconfirmed) ? (
                        <p>
                          {setupUnconfirmed
                            ? view.archived
                              ? "Check the original setup or fence the request to prevent a delayed send."
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
                              ? "The host released the cleanup hold. The exact receipt is saved in this plan."
                              : cleanup.recovery === "released-plan-unconfirmed"
                                ? "The host released the cleanup hold. Check the update outcome to refresh this plan."
                                : cleanup.inspection.status === "eligible"
                                  ? "Inspection confirmed the prior disk image is absent. The update remains on hold."
                                  : "Inspection could not confirm cleanup. The update remains on hold."}
                        </p>
                      ) : null}
                      {recoveryReason !== undefined &&
                      (resumeArchived || (cleanupTarget !== undefined && canInspect)) ? (
                        <p id={recoveryDescription}>{recoveryReason}</p>
                      ) : null}
                      {(row?.actions.stop || stopArchived) && stopReason !== undefined ? (
                        <p id={`${recoveryDescription}-stop`}>{stopReason}</p>
                      ) : null}
                      {fenceArchived && fenceReason !== undefined ? (
                        <p id={`${recoveryDescription}-fence`}>{fenceReason}</p>
                      ) : null}
                      {!view.readOnly || view.archived || view.deskReadOnly ? (
                        <div className="flex flex-wrap gap-1">
                          {inspectKey !== null && cleanupTarget !== undefined && canInspect ? (
                            <Button
                              variant="outline"
                              size="micro"
                              disabled={pending.has(inspectKey) || recoveryReason !== undefined}
                              aria-describedby={
                                recoveryReason === undefined ? undefined : recoveryDescription
                              }
                              onClick={() =>
                                void perform(inspectKey, async () => {
                                  setCleanupInspection(null);
                                  const inspection = await inspectPanelCleanup(
                                    cleanupTarget,
                                    binding,
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
                          cleanupTarget !== undefined &&
                          canInspect &&
                          cleanup?.inspection.recoveryToken !== undefined ? (
                            <Button
                              variant="outline"
                              size="micro"
                              disabled={pending.has(inspectKey) || recoveryReason !== undefined}
                              aria-describedby={
                                recoveryReason === undefined ? undefined : recoveryDescription
                              }
                              onClick={() => {
                                const token = cleanup.inspection.recoveryToken;
                                if (token === undefined) return;
                                void perform(inspectKey, async () => {
                                  const recovery = await recoverPanelCleanup(
                                    cleanupTarget,
                                    receipt,
                                    token,
                                    binding,
                                  );
                                  if (recovery !== "cancelled") {
                                    setCleanupInspection({
                                      key: inspectKey,
                                      inspection: { status: "eligible" },
                                      recovery,
                                    });
                                    if (
                                      recovery !== "unproved" &&
                                      installPlanServices() === binding
                                    )
                                      store.update({
                                        error:
                                          recovery === "released"
                                            ? "The host released the cleanup hold. The exact receipt is saved in this plan."
                                            : "The host released the cleanup hold. Saving the exact receipt in this plan is unconfirmed.",
                                      });
                                  }
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
                          {(row?.actions.stop || stopArchived) && operationId !== undefined ? (
                            <Button
                              variant="outline"
                              size="micro"
                              disabled={
                                pending.has(`${environmentId}:stop`) || stopReason !== undefined
                              }
                              aria-describedby={
                                stopReason !== undefined ? `${recoveryDescription}-stop` : undefined
                              }
                              onClick={() => stop("device", environmentId, operationId)}
                            >
                              {pending.has(`${environmentId}:stop`)
                                ? "Requesting stop…"
                                : `Stop download on ${label}`}
                            </Button>
                          ) : null}
                          {fenceArchived && operationId !== undefined ? (
                            <Button
                              variant="outline"
                              size="micro"
                              disabled={
                                fenceReason !== undefined || pending.has(`${environmentId}:fence`)
                              }
                              aria-describedby={
                                fenceReason === undefined
                                  ? undefined
                                  : `${recoveryDescription}-fence`
                              }
                              onClick={() =>
                                void perform(`${environmentId}:fence`, () =>
                                  fencePanelOperation(
                                    { ...planTarget, archived: true, environmentId, operationId },
                                    binding,
                                  ),
                                )
                              }
                            >
                              {setupUnconfirmed ? "Fence original setup" : "Fence original request"}
                            </Button>
                          ) : null}
                          {(row?.actions.abandon || resumeArchived) &&
                          abandonTarget !== undefined ? (
                            <Button
                              variant="outline"
                              size="micro"
                              disabled={
                                pending.has(`${environmentId}:abandon`) ||
                                (view.archived && recoveryReason !== undefined)
                              }
                              aria-describedby={
                                view.archived && recoveryReason !== undefined
                                  ? recoveryDescription
                                  : undefined
                              }
                              onClick={() =>
                                void perform(`${environmentId}:abandon`, () =>
                                  abandonDevice(abandonTarget, binding),
                                )
                              }
                            >
                              {pending.has(`${environmentId}:abandon`)
                                ? "Abandoning update…"
                                : abandonTarget.resume
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
                            onClick={() =>
                              void perform(`${environmentId}:check`, () =>
                                performPanelAction(
                                  environmentId,
                                  "check",
                                  {
                                    ...targetFor(environmentId),
                                    operationId: operationId ?? null,
                                  },
                                  binding,
                                ),
                              )
                            }
                          >
                            {pending.has(`${environmentId}:check`)
                              ? "Checking…"
                              : setupUnconfirmed
                                ? "Check setup status"
                                : held ||
                                    launcherUnconfirmed ||
                                    (view.archived && operationId !== undefined)
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
          !view.archived &&
          !view.preview &&
          snapshot.native.view.cancelPlan === 1 &&
          (overview || focusedDevice === "laptop") ? (
            <div className="border-t border-dashed border-sidebar-border px-2 py-1">
              <Button
                size="micro"
                variant="ghost-muted"
                disabled={pending.has("cancel-plan") || view.deskReadOnly}
                onClick={() =>
                  void perform("cancel-plan", async () => {
                    if (binding === undefined) throw new Error("Install status is not connected.");
                    const selection = store.historySelection();
                    await readWritableTarget(binding, planTarget);
                    if (
                      !(await binding.services.confirm(
                        "Stop scheduling this plan? Already-issued work may finish. You can check its outcomes and recovery in Install status.",
                      ))
                    )
                      return;
                    await readWritableTarget(binding, planTarget);
                    const result = await requestInstallPlan(
                      {
                        type: "cancel-plan",
                        planId: plan.planId,
                        expectedRevision: plan.revision,
                      },
                      binding,
                    ).catch(() => undefined);
                    await confirmClosedPlan(
                      binding,
                      plan.planId,
                      (closed) =>
                        closed.cancelled === true &&
                        (closed.cancelledFromRevision === undefined ||
                          closed.cancelledFromRevision === plan.revision),
                      result,
                    );
                    if (store.historySelection() === selection) {
                      store.update({ previewSelection: [], error: null });
                      store.open(true, plan.planId);
                    }
                  })
                }
              >
                Stop scheduling this plan
              </Button>
            </div>
          ) : null}
          {snapshot.error || snapshot.native.readOnly || snapshot.nativeRefusal ? (
            <p role="status" className="px-2 py-1 text-2xs text-muted-foreground">
              {snapshot.native.readOnly?.message ?? snapshot.nativeRefusal ?? snapshot.error}
            </p>
          ) : null}
        </div>
      ) : null}
    </section>
  );
}

function HistoryNavigation({
  store,
  snapshot,
  binding,
  displayedPlan,
  preview,
}: {
  readonly store: InstallStatusStore;
  readonly snapshot: InstallStatusSnapshot;
  readonly binding: InstallPlanBinding | undefined;
  readonly displayedPlan?: string;
  readonly preview: boolean;
}) {
  const [checking, setChecking] = useState<string | null>(null);
  const { native, desktop, showLast } = snapshot;
  if (native === null) return null;
  const last = native.last;
  const history = native.recoveryHistory ?? [];
  const orphans = native.orphanedCommands ?? [];
  const canPreview = native.current === null && desktopCanPreviewInstallPlan(desktop);
  const otherPlan =
    native.current !== null ? native.current.planId !== displayedPlan : canPreview && !preview;
  if (
    !otherPlan &&
    (last === null || last.planId === displayedPlan) &&
    history.length === 0 &&
    orphans.length === 0
  )
    return null;
  const select = (planId?: string) => {
    store.update({ error: null });
    store.open(planId !== undefined, planId);
  };
  return (
    <div
      className="space-y-1 px-2 pb-1 text-2xs text-muted-foreground"
      aria-label="Update plan history"
    >
      {showLast && otherPlan ? (
        <Button variant="ghost-muted" size="micro" onClick={() => select()}>
          {native.current !== null ? "Back to current update" : "Back to available update"}
        </Button>
      ) : null}
      {last !== null && last.planId !== displayedPlan ? (
        <Button variant="ghost-muted" size="micro" onClick={() => select(last.planId)}>
          Last update
        </Button>
      ) : null}
      {history.length > 0 ? (
        <details className="space-y-1">
          <summary className="cursor-pointer rounded-sm px-1 focus-visible:outline-2 focus-visible:outline-ring">
            Recovery history
          </summary>
          {history.map((entry) => {
            const pending = [
              ...(entry.pendingLaptop ? ["Laptop"] : []),
              ...entry.pendingDevices.map((device) => device.label),
            ];
            return (
              <Button
                key={entry.planId}
                variant={displayedPlan === entry.planId ? "secondary" : "ghost-muted"}
                size="micro"
                className="h-auto w-full justify-start whitespace-normal text-left"
                aria-label={`Recovery for ${entry.targetVersion}`}
                aria-pressed={displayedPlan === entry.planId}
                onClick={() => select(entry.planId)}
              >
                <span>
                  {entry.targetVersion} · {pending.join(", ")}
                </span>
              </Button>
            );
          })}
        </details>
      ) : null}
      {orphans.length > 0 ? (
        <details className="space-y-1">
          <summary className="cursor-pointer rounded-sm px-1 focus-visible:outline-2 focus-visible:outline-ring">
            Recovery for commands from missing plans
          </summary>
          {orphans.map((entry) => {
            const id = entry.command.commandId;
            const label =
              snapshot.devices.find(
                ({ environmentId }) => environmentId === entry.command.environmentId,
              )?.label ?? entry.command.environmentId;
            const checkable =
              native.view.orphanRecovery === 1 && orphanReceiptCheck(entry) !== undefined;
            return (
              <div key={id} className="space-y-1 px-1">
                <p>
                  {label} · {entry.command.action} · {entry.command.targetSource.slice(0, 7)}
                </p>
                <OperationId value={entry.operationId ?? id} />
                {checkable ? (
                  <Button
                    variant="ghost-muted"
                    size="micro"
                    disabled={
                      checking !== null || binding === undefined || native.readOnly !== undefined
                    }
                    onClick={() => {
                      if (binding === undefined) return;
                      setChecking(id);
                      store.update({ error: null });
                      void checkOrphanedPlanCommand(entry, binding)
                        .catch((cause: unknown) => {
                          if (installPlanServices() === binding)
                            store.update({
                              error:
                                cause instanceof Error
                                  ? cause.message
                                  : "The command remains on hold.",
                            });
                        })
                        .finally(() => setChecking(null));
                    }}
                  >
                    {checking === id ? "Checking…" : `Check retained receipt on ${label}`}
                  </Button>
                ) : (
                  <p>
                    Laptop no longer has this plan’s exact operation binding. Updates on this device
                    remain on hold.
                  </p>
                )}
              </div>
            );
          })}
        </details>
      ) : null}
    </div>
  );
}

function RunningBuild({
  version,
  source,
  fresh,
}: {
  readonly version: string | undefined;
  readonly source: string | undefined;
  readonly fresh: boolean;
}) {
  if (version === undefined) return <p>Running build unverified</p>;
  return (
    <p>
      {fresh ? "Running build" : "Last reported running build"}: {version}
      {source === undefined ? "" : ` (${source.slice(0, 7)})`}
    </p>
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
