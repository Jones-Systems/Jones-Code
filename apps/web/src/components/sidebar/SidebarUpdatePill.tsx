import { classifiedCheckFeedback } from "../../jones/installStatus/checkFeedback";
import {
  canOpenInstallStatusPill,
  routeInstallStatusPill,
} from "../../jones/installStatus/entrypoints";
import { useInstallPlan } from "../../jones/installStatus/useInstallPlan";
import {
  type DesktopBridge,
  type DesktopUpdateState,
  managedUpdatesDenial,
} from "@t3tools/contracts";
import type {
  JonesUpdateBuildList,
  JonesUpdateProvenance,
} from "@t3tools/contracts/jones/jonesUpdates";
import { TriangleAlertIcon } from "lucide-react";
import { type ComponentProps, useId, useRef, useState } from "react";
import { flushSync } from "react-dom";
import { settlePromise, squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import { isElectron } from "../../env";
import { useMediaQuery } from "../../hooks/useMediaQuery";
import { cn } from "../../lib/utils";
import { ensureLocalApi } from "../../localApi";
import { useDesktopUpdateState } from "../../state/desktopUpdate";
import { usePrimaryManagedPolicy } from "../../state/managedPolicy";
import { stackedThreadToast, toastManager } from "../ui/toast";
import {
  canCheckForUpdate,
  getArm64IntelBuildWarningDescription,
  getDesktopUpdateActionError,
  getDesktopUpdateButtonTooltip,
  getDesktopUpdateDownloadedVersion,
  getDesktopUpdateIdleTooltip,
  getDesktopUpdateInstallConfirmationMessage,
  isDesktopUpdateButtonDisabled,
  resolveDesktopUpdateButtonAction,
  shouldShowArm64IntelBuildWarning,
  shouldToastDesktopUpdateActionResult,
} from "../desktopUpdate.logic";
import {
  downloadLocalDesktopUpdate,
  installLocalDesktopUpdate,
} from "../../jones/updates/localDesktopUpdate";
import { showDesktopUpdateDownloadedToast } from "../desktopUpdate.toast";
import { useCheckProviderUpdates } from "../ProviderUpdatesAction";
import { Alert, AlertDescription, AlertTitle } from "../ui/alert";
import { Popover, PopoverCreateHandle, PopoverPopup, PopoverTrigger } from "../ui/popover";
import { SidebarMenuItem } from "../ui/sidebar";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import {
  DesktopUpdateStatusIcon,
  type DesktopUpdateStatusIconState,
  shouldContinueDesktopUpdateCheckAnimation,
  shouldShowDesktopUpdateCheckIcon,
} from "./DesktopUpdateStatusIcon";
import { SidebarUpdateReleaseNotes } from "./SidebarUpdateReleaseNotes";
import { SidebarUpdateCapsule } from "./SidebarUpdateCapsule";
import { UpdateEverythingDialog } from "./UpdateEverythingDialog";
import type { UpdateEverythingRun, UpdateRunLocalState } from "./updateEverything.logic";
import {
  clearSavedUpdateEverythingResult,
  saveUpdateEverythingResult,
  useUpdateEverythingRun,
} from "./updateEverythingRun";
import { type ReportLocalUpdateError, useUpdateEverything } from "./useUpdateEverything";

type SidebarUpdatePopoverChangeDetails = Parameters<
  NonNullable<ComponentProps<typeof Popover>["onOpenChange"]>
>[1];
type SidebarUpdatePopoverHandle = ReturnType<typeof PopoverCreateHandle>;

export function resolveSidebarUpdateCheckFeedback(state: DesktopUpdateState): {
  type: "info" | "error";
  title: string;
  description?: string;
} | null {
  if (state.status !== "idle" && state.status !== "up-to-date" && state.status !== "error") {
    return null;
  }
  const classified = classifiedCheckFeedback(state);
  if (classified !== null) return classified;
  if (state.jones?.phase === "building") {
    return {
      type: "info",
      title: "Update still being qualified",
      description: state.jones.message ?? state.message ?? "The update is still being qualified.",
    };
  }
  if (state.jones?.phase === "blocked") {
    return {
      type: "error",
      title: "Update check blocked",
      description: state.jones.message ?? state.message ?? "The update check is blocked.",
    };
  }
  if (state.status === "error") {
    return {
      type: "error",
      title: "Could not check for updates",
      description: state.message ?? "Update check failed.",
    };
  }
  if (state.status === "up-to-date") {
    return { type: "info", title: "No new update available" };
  }
  return null;
}

export function checkForSidebarUpdate(
  bridge: Pick<DesktopBridge, "checkForUpdate">,
): Promise<void> {
  return bridge
    .checkForUpdate()
    .then((result) => {
      if (result.checked) {
        const feedback = resolveSidebarUpdateCheckFeedback(result.state);
        if (feedback) toastManager.add(stackedThreadToast(feedback));
        return;
      }
      toastManager.add(
        stackedThreadToast({
          type: "error",
          title: "Could not check for updates",
          description: result.state.message ?? "Automatic updates are not available in this build.",
        }),
      );
    })
    .catch((error) => {
      toastManager.add(
        stackedThreadToast({
          type: "error",
          title: "Could not check for updates",
          description: error instanceof Error ? error.message : "Update check failed.",
        }),
      );
    });
}

function shouldUseSidebarUpdateReleaseNotesPopover(
  showUpdateDetails: boolean,
  state: DesktopUpdateState | null,
): boolean {
  return showUpdateDetails && (state?.releaseNotes.length ?? 0) > 0;
}

function handleSidebarUpdateReleaseNotesPopoverOpenChange(
  _open: boolean,
  details: Pick<SidebarUpdatePopoverChangeDetails, "reason" | "cancel">,
): void {
  if (details.reason === "trigger-press") details.cancel();
}

function openSidebarUpdateReleaseNotesPopoverOnForwardTab(
  event: { readonly key: string; readonly shiftKey: boolean },
  handle: Pick<SidebarUpdatePopoverHandle, "open">,
  triggerId: string,
): void {
  if (event.key !== "Tab" || event.shiftKey) return;
  // Hover-open popovers do not manage focus. Promote this one before native Tab runs.
  flushSync(() => handle.open(triggerId));
}

function resolveSidebarUpdatePresentation({
  action,
  showsEverything,
  run,
  isDownloading,
  showCheckIcon,
}: {
  readonly action: ReturnType<typeof resolveDesktopUpdateButtonAction>;
  readonly showsEverything: boolean;
  readonly run: UpdateEverythingRun | null;
  readonly isDownloading: boolean;
  readonly showCheckIcon: boolean;
}) {
  const showUpdateDetails = action !== "none" || showsEverything || run !== null || isDownloading;
  const iconStatus: DesktopUpdateStatusIconState = showCheckIcon
    ? "checking"
    : isDownloading
      ? "downloading"
      : run !== null
        ? run.localVersion !== null && action === "install"
          ? "downloaded"
          : "everything"
        : showsEverything
          ? "everything"
          : action === "install"
            ? "downloaded"
            : action === "download"
              ? "available"
              : "idle";

  return {
    iconStatus,
    showUpdateDetails,
    showUpdateIconState: showUpdateDetails && !showCheckIcon,
  } as const;
}

export function SidebarUpdateArchitectureWarning() {
  return isElectron ? <SidebarUpdateArchitectureWarningContent /> : null;
}

function SidebarUpdateArchitectureWarningContent() {
  const state = useDesktopUpdateState();
  const visible = shouldShowArm64IntelBuildWarning(state);
  const description = state && visible ? getArm64IntelBuildWarningDescription(state) : null;

  if (!visible || !description) return null;

  return (
    <Alert variant="warning">
      <TriangleAlertIcon />
      <AlertTitle>Intel build on Apple Silicon</AlertTitle>
      <AlertDescription>{description}</AlertDescription>
    </Alert>
  );
}

const toastUpdateError: ReportLocalUpdateError = (title, description) => {
  toastManager.add(stackedThreadToast({ type: "error", title, description }));
};

export async function downloadLocalUpdate(
  bridge: DesktopBridge,
  state: DesktopUpdateState | null,
  {
    notifyDownloaded,
    reportError = toastUpdateError,
  }: { readonly notifyDownloaded: boolean; readonly reportError?: ReportLocalUpdateError },
): Promise<boolean> {
  try {
    const result = await downloadLocalDesktopUpdate(bridge, state);
    if (result.completed && notifyDownloaded) {
      showDesktopUpdateDownloadedToast(bridge, result.state);
    }
    const actionError = shouldToastDesktopUpdateActionResult(result)
      ? getDesktopUpdateActionError(result)
      : null;
    if (actionError) reportError("Could not download update", actionError);
    return result.completed;
  } catch (error) {
    reportError(
      "Could not start update download",
      error instanceof Error ? error.message : "An unexpected error occurred.",
    );
    return false;
  }
}

export async function installLocalUpdate(
  bridge: Pick<
    DesktopBridge,
    "getUpdateState" | "installUpdate" | "openExternal" | "fleetUpdates" | "installPlan"
  >,
  reportError: ReportLocalUpdateError = toastUpdateError,
  expectedVersion?: string,
): Promise<boolean> {
  try {
    // Update everything installs after its own download finishes, so the rendered
    // state can predate the staged selection that the installer requires.
    const current = await bridge.getUpdateState();
    const result =
      current.jones !== undefined
        ? await installLocalDesktopUpdate(bridge, current)
        : expectedVersion === undefined
          ? await bridge.installUpdate()
          : await bridge.installUpdate(expectedVersion);
    const actionError = getDesktopUpdateActionError(result);
    if (actionError) {
      reportError("Could not install update", actionError);
      return false;
    }

    if (!result.accepted) {
      showDesktopUpdateDownloadedToast(bridge, result.state);
      return false;
    }
    return true;
  } catch (error) {
    reportError(
      "Could not install update",
      error instanceof Error ? error.message : "An unexpected error occurred.",
    );
    return false;
  }
}

export function SidebarUpdatePill() {
  return <SidebarUpdateControl />;
}

export function JonesBuildReview({
  build,
  list,
  onSelect,
}: {
  readonly build: JonesUpdateProvenance;
  readonly list: JonesUpdateBuildList | undefined;
  readonly onSelect: (build: JonesUpdateProvenance) => void;
}) {
  const selectId = useId();
  const available = list?.builds ?? [];
  const choices = available.some((item) => item.artifactId === build.artifactId)
    ? available
    : [build, ...available];
  return (
    <div className="flex flex-col gap-1.5 rounded-md border p-3">
      <label className="text-sm font-medium" htmlFor={selectId}>
        Jones Code build
      </label>
      <select
        id={selectId}
        className="min-w-0 rounded-md border bg-background px-2 py-1.5 text-sm"
        value={build.artifactId}
        onChange={(event) => {
          const choice = choices.find((item) => item.artifactId === Number(event.target.value));
          if (choice !== undefined) onSelect(choice);
        }}
      >
        {choices.map((item) => (
          <option key={item.artifactId} value={item.artifactId}>
            {item.version ?? item.sourceSha} · {item.sourceSha.slice(0, 8)}
          </option>
        ))}
      </select>
      <p className="text-muted-foreground text-xs">
        {list === undefined
          ? "Checking for other qualified builds…"
          : list.unavailable
            ? "The build list could not be fully checked."
            : choices.length === 1
              ? "No other qualified builds found."
              : "Recent qualified builds."}
        {" Download prepares the Laptop. Installation is reviewed separately."}
      </p>
    </div>
  );
}

function SidebarUpdateControl() {
  const state = useDesktopUpdateState();
  const installPlan = useInstallPlan();
  // The desktop updates this machine, whose policy the primary environment reports.
  const updatesDenial = managedUpdatesDenial(usePrimaryManagedPolicy());
  const [isActionPending, setIsActionPending] = useState(false);
  const [providerCheck, setProviderCheck] = useState<"button" | "dialog" | null>(null);
  const isCheckingProviders = providerCheck === "button";
  const [isConfirmingEverything, setIsConfirmingEverything] = useState(false);
  const [reviewedBuild, setReviewedBuild] = useState<JonesUpdateProvenance>();
  const [buildList, setBuildList] = useState<JonesUpdateBuildList>();
  const reviewGeneration = useRef(0);
  const confirmationInFlight = useRef(false);
  const [checkAnimationKey, setCheckAnimationKey] = useState(0);
  const [releaseNotesPopoverHandle] = useState(() => PopoverCreateHandle());
  const suppressReleaseNotesFocusOpen = useRef(false);
  const releaseNotesPopupRef = useRef<HTMLDivElement>(null);
  const releaseNotesTriggerId = useId();
  const prefersReducedMotion = useMediaQuery("(prefers-reduced-motion: reduce)");

  const updateStatus = state?.status;
  const [isCheckAnimationLatched, setIsCheckAnimationLatched] = useState(
    () => !prefersReducedMotion && updateStatus === "checking",
  );
  const [latchInputs, setLatchInputs] = useState({ prefersReducedMotion, updateStatus });
  if (
    latchInputs.prefersReducedMotion !== prefersReducedMotion ||
    latchInputs.updateStatus !== updateStatus
  ) {
    setLatchInputs({ prefersReducedMotion, updateStatus });
    if (prefersReducedMotion) {
      setIsCheckAnimationLatched(false);
    } else if (updateStatus === "checking") {
      setIsCheckAnimationLatched(true);
    }
  }

  const action = state ? resolveDesktopUpdateButtonAction(state) : "none";
  const isDownloading = state?.status === "downloading";
  const localUpdateVersion =
    state && action !== "none" ? getDesktopUpdateDownloadedVersion(state) : null;
  const everything = useUpdateEverything(localUpdateVersion);
  const checkProviderUpdates = useCheckProviderUpdates();
  const bridge = typeof window === "undefined" ? undefined : window.desktopBridge;
  const offeredBuild =
    action === "download" &&
    state?.jones?.provenance?.version !== undefined &&
    bridge?.installPlan !== undefined &&
    bridge.listJonesUpdateBuilds !== undefined &&
    bridge.selectJonesUpdateBuild !== undefined
      ? state.jones.provenance
      : undefined;
  const hasUpdates = everything.hasUpdates || offeredBuild !== undefined;
  const runsEverything = hasUpdates && !isDownloading;
  const run = useUpdateEverythingRun();
  const isRunningEverything = run !== null;
  const showCheckIcon = shouldShowDesktopUpdateCheckIcon({
    isAnimationLatched: isCheckAnimationLatched,
    isChecking: state?.status === "checking" || isCheckingProviders,
    prefersReducedMotion,
  });
  const { iconStatus, showUpdateDetails, showUpdateIconState } = resolveSidebarUpdatePresentation({
    action,
    showsEverything: runsEverything,
    run,
    isDownloading,
    showCheckIcon,
  });
  const idleTooltip =
    !showUpdateDetails && !showCheckIcon ? getDesktopUpdateIdleTooltip(state, updatesDenial) : null;
  const opensInstallStatus = canOpenInstallStatusPill(installPlan.native, state, hasUpdates);
  const tooltip = opensInstallStatus
    ? "Open Install status"
    : runsEverything
      ? offeredBuild === undefined
        ? everything.summary
        : `Review Jones Code ${offeredBuild.version} · ${offeredBuild.sourceSha.slice(0, 8)}`
      : showUpdateDetails
        ? state
          ? getDesktopUpdateButtonTooltip(state)
          : "Update available"
        : showCheckIcon
          ? "Checking for updates…"
          : (idleTooltip?.title ?? "Check for updates");
  const disabled = showCheckIcon
    ? true
    : runsEverything
      ? false
      : showUpdateDetails
        ? isDesktopUpdateButtonDisabled(state)
        : !canCheckForUpdate(state);
  const isInteractionDisabled =
    !opensInstallStatus && (disabled || isActionPending || isRunningEverything);
  const showReleaseNotesPopover =
    !isRunningEverything && shouldUseSidebarUpdateReleaseNotesPopover(showUpdateDetails, state);

  const [releaseNotesOpen, setReleaseNotesOpen] = useState(false);
  const [releaseNotesTriggerFocusVisible, setReleaseNotesTriggerFocusVisible] = useState(false);
  // Release notes close when they stop applying, and open when they start
  // applying while the trigger already holds keyboard focus.
  const [releaseNotesShownBefore, setReleaseNotesShownBefore] = useState(showReleaseNotesPopover);
  if (releaseNotesShownBefore !== showReleaseNotesPopover) {
    setReleaseNotesShownBefore(showReleaseNotesPopover);
    setReleaseNotesOpen(showReleaseNotesPopover && releaseNotesTriggerFocusVisible);
  }

  const handleAction = async () => {
    const bridge = window.desktopBridge;
    if (bridge?.installPlan) {
      const route = await routeInstallStatusPill(bridge, state, hasUpdates).catch(() => {
        toastUpdateError(
          "Install status unavailable",
          "The native plan could not be read. No update was started.",
        );
        return "opened" as const;
      });
      if (route === "opened") return;
      if (route === "last-and-check") {
        void checkForSidebarUpdate(bridge);
        return;
      }
    }
    if (isInteractionDisabled) return;

    if (runsEverything) {
      const generation = ++reviewGeneration.current;
      setReviewedBuild(offeredBuild);
      setBuildList(undefined);
      setIsConfirmingEverything(true);
      if (offeredBuild !== undefined && bridge?.listJonesUpdateBuilds !== undefined) {
        void bridge
          .listJonesUpdateBuilds()
          .catch(() => ({ builds: [], unavailable: true }))
          .then((list) => {
            if (reviewGeneration.current === generation) setBuildList(list);
          });
      }
      // Machines that still cache an older latest version add their rows while the dialog is open.
      setProviderCheck("dialog");
      void checkProviderUpdates({ quiet: true }).finally(() => setProviderCheck(null));
      return;
    }

    if (!bridge || !state) return;
    setIsActionPending(true);

    if (action === "download") {
      void downloadLocalUpdate(bridge, state, { notifyDownloaded: true }).finally(() =>
        setIsActionPending(false),
      );
      return;
    }

    if (action === "install") {
      let confirmed = false;
      try {
        confirmed = await ensureLocalApi().dialogs.confirm(
          getDesktopUpdateInstallConfirmationMessage(state),
        );
      } catch (error) {
        setIsActionPending(false);
        toastManager.add(
          stackedThreadToast({
            type: "error",
            title: "Could not confirm update",
            description: error instanceof Error ? error.message : "Update confirmation failed.",
          }),
        );
        return;
      }
      if (!confirmed) {
        setIsActionPending(false);
        return;
      }
      void installLocalDesktopUpdate(bridge, state)
        .then((result) => {
          if (!shouldToastDesktopUpdateActionResult(result)) return;
          const actionError = getDesktopUpdateActionError(result);
          if (!actionError) return;
          toastManager.add(
            stackedThreadToast({
              type: "error",
              title: "Could not install update",
              description: actionError,
            }),
          );
        })
        .catch((error) => {
          toastManager.add(
            stackedThreadToast({
              type: "error",
              title: "Could not install update",
              description: error instanceof Error ? error.message : "An unexpected error occurred.",
            }),
          );
        })
        .finally(() => setIsActionPending(false));
      return;
    }

    if (!prefersReducedMotion) {
      setIsCheckAnimationLatched(true);
      setCheckAnimationKey((key) => key + 1);
    }
    const appCheck = checkForSidebarUpdate(bridge);
    setProviderCheck("button");
    const providersChecked = checkProviderUpdates().finally(() => setProviderCheck(null));
    void Promise.allSettled([appCheck, providersChecked]).then(() => setIsActionPending(false));
  };

  const runEverything = async (providersOnly: boolean) => {
    if (confirmationInFlight.current) return;
    const bridge = window.desktopBridge;
    const runCombined = async () => {
      if (reviewedBuild !== undefined) {
        if ((await bridge?.selectJonesUpdateBuild?.(reviewedBuild)) !== true)
          throw new Error(
            "The reviewed build could not be reverified. Open Updates and review again.",
          );
        await everything.run({ reviewedBuild });
        return;
      }
      await everything.run({
        ...(bridge && action === "download"
          ? {
              downloadLocal: (reportError) =>
                downloadLocalUpdate(bridge, state, { notifyDownloaded: false, reportError }),
            }
          : {}),
        ...(bridge && action !== "none"
          ? { installLocal: (reportError) => installLocalUpdate(bridge, reportError) }
          : {}),
      });
    };
    const update = providersOnly ? everything.updateProvidersOnly : runCombined;
    if (update === undefined) return;
    confirmationInFlight.current = true;
    setIsConfirmingEverything(false);
    setIsActionPending(true);
    const result = await settlePromise(async () => {
      await update();
    });
    confirmationInFlight.current = false;
    setIsActionPending(false);
    if (result._tag === "Failure") {
      const error = squashAtomCommandFailure(result);
      toastUpdateError(
        providersOnly ? "Could not update providers" : "Could not update everything",
        error instanceof Error ? error.message : "An unexpected error occurred.",
      );
    }
  };

  const handleCheckAnimationIteration = () => {
    setIsCheckAnimationLatched(
      shouldContinueDesktopUpdateCheckAnimation({
        isChecking: state?.status === "checking" || isCheckingProviders,
        prefersReducedMotion,
      }),
    );
  };

  if (!isElectron && !runsEverything && !isRunningEverything) return null;

  const statusIcon = (
    <DesktopUpdateStatusIcon
      key={showCheckIcon ? checkAnimationKey : iconStatus}
      downloadPercent={state?.downloadPercent ?? null}
      isCheckAnimating={showCheckIcon && !prefersReducedMotion}
      onCheckAnimationIteration={handleCheckAnimationIteration}
      status={iconStatus}
    />
  );
  const confirmDialog = (
    <UpdateEverythingDialog
      open={isConfirmingEverything}
      onOpenChange={(open) => {
        if (!open) reviewGeneration.current++;
        setIsConfirmingEverything(open);
      }}
      version={everything.version}
      providerCount={everything.providerCount}
      rows={everything.rows}
      restarts={everything.restarts}
      checking={providerCheck === "dialog"}
      onConfirm={() => void runEverything(false)}
      {...(reviewedBuild === undefined
        ? {}
        : {
            title: "Review updates",
            confirmLabel: "Download selected build",
            buildReview: (
              <JonesBuildReview
                build={reviewedBuild}
                list={buildList}
                onSelect={setReviewedBuild}
              />
            ),
          })}
      {...(everything.updateProvidersOnly === undefined
        ? {}
        : { onProvidersOnly: () => void runEverything(true) })}
    />
  );

  if (run !== null && !opensInstallStatus) {
    const local: UpdateRunLocalState =
      run.localVersion === null
        ? null
        : state?.status === "downloading"
          ? { status: "downloading", percent: state.downloadPercent }
          : { status: "waiting" };
    return (
      <SidebarMenuItem className="ml-auto flex flex-1">
        <SidebarUpdateCapsule
          run={run}
          local={local}
          icon={statusIcon}
          {...(run.localVersion === null && action === "install" && window.desktopBridge
            ? {
                onRestart: async () => {
                  saveUpdateEverythingResult({
                    localVersion: localUpdateVersion,
                    updatedServers: 0,
                    updatedProviders: 0,
                    failures: [],
                    restarting: [],
                  });
                  if (!(await installLocalUpdate(window.desktopBridge!))) {
                    clearSavedUpdateEverythingResult();
                  }
                },
              }
            : {})}
        />
        {confirmDialog}
      </SidebarMenuItem>
    );
  }

  const updateButton = (
    <button
      type="button"
      aria-label={idleTooltip?.detail ? `${tooltip}: ${idleTooltip.detail}` : tooltip}
      aria-disabled={isInteractionDisabled || undefined}
      className={cn(
        "inline-flex size-8 items-center justify-center rounded-full outline-hidden ring-ring transition-colors focus-visible:ring-2 focus-visible:ring-inset",
        isInteractionDisabled ? "cursor-not-allowed" : "cursor-pointer",
        showUpdateIconState
          ? cn(
              "bg-sidebar-control-surface text-sidebar-foreground",
              !isInteractionDisabled && "hover:bg-sidebar-row-hover",
            )
          : cn(
              "text-(--sidebar-icon-color)",
              !isInteractionDisabled && "hover:bg-sidebar-row-hover hover:text-sidebar-foreground",
            ),
        disabled && !showUpdateIconState && "opacity-60",
      )}
      onClick={handleAction}
      onBlur={() => {
        suppressReleaseNotesFocusOpen.current = false;
        setReleaseNotesTriggerFocusVisible(false);
      }}
      onFocus={(event) => {
        const focusVisible = event.currentTarget.matches(":focus-visible");
        setReleaseNotesTriggerFocusVisible(focusVisible);
        if (!showReleaseNotesPopover || !focusVisible) return;
        if (suppressReleaseNotesFocusOpen.current) {
          suppressReleaseNotesFocusOpen.current = false;
          return;
        }
        flushSync(() => releaseNotesPopoverHandle.open(releaseNotesTriggerId));
      }}
      onKeyDown={(event) => {
        if (!showReleaseNotesPopover) return;
        openSidebarUpdateReleaseNotesPopoverOnForwardTab(
          event,
          releaseNotesPopoverHandle,
          releaseNotesTriggerId,
        );
      }}
    >
      {statusIcon}
    </button>
  );

  return (
    <SidebarMenuItem className="ml-auto shrink-0">
      <Popover
        handle={releaseNotesPopoverHandle}
        open={releaseNotesOpen && showReleaseNotesPopover}
        triggerId={releaseNotesTriggerId}
        onOpenChange={(open, details) => {
          if (open && !showReleaseNotesPopover) {
            details.cancel();
            return;
          }
          handleSidebarUpdateReleaseNotesPopoverOpenChange(open, details);
          if (!details.isCanceled) setReleaseNotesOpen(open);
        }}
      >
        <Tooltip disabled={showReleaseNotesPopover}>
          <TooltipTrigger
            id={releaseNotesTriggerId}
            render={
              <PopoverTrigger
                {...(!showReleaseNotesPopover
                  ? {
                      "aria-controls": undefined,
                      "aria-expanded": undefined,
                      "aria-haspopup": undefined,
                    }
                  : {})}
                closeDelay={150}
                handle={releaseNotesPopoverHandle}
                id={releaseNotesTriggerId}
                openOnHover={showReleaseNotesPopover}
                render={updateButton}
              />
            }
          />
          {!showReleaseNotesPopover ? (
            <TooltipPopup
              align="center"
              side="top"
              variant={showUpdateDetails ? "glass" : "default"}
            >
              {idleTooltip?.detail ? (
                <div className="text-left">
                  <div className="font-medium">{idleTooltip.title}</div>
                  <div className="text-muted-foreground">{idleTooltip.detail}</div>
                </div>
              ) : (
                <span className="whitespace-pre-line">{tooltip}</span>
              )}
            </TooltipPopup>
          ) : null}
        </Tooltip>
        {showReleaseNotesPopover && state ? (
          <PopoverPopup
            align="center"
            aria-label="Update release notes"
            initialFocus={false}
            onKeyDownCapture={(event) => {
              if (
                event.key === "Escape" &&
                releaseNotesPopupRef.current?.contains(document.activeElement)
              ) {
                suppressReleaseNotesFocusOpen.current = true;
              }
            }}
            ref={releaseNotesPopupRef}
            side="top"
            tooltipStyle
          >
            <SidebarUpdateReleaseNotes
              shell={window.desktopBridge}
              state={state}
              tooltip={tooltip}
            />
          </PopoverPopup>
        ) : null}
      </Popover>
      {confirmDialog}
    </SidebarMenuItem>
  );
}
