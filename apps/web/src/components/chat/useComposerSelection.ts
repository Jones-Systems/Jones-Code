import {
  type ComposerModelSelectOptions,
  withShortcutEffort,
} from "../../jones/composer/composerModelEffortShortcuts";
import { matchesProviderModelLock } from "../../jones/composer/ProviderInstanceShortcuts";
import { type ProviderInstanceEntry, isProviderInstancePickerReady } from "../../providerInstances";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import type { UnifiedSettings } from "@t3tools/contracts";
import { resolveSelectableModel } from "@t3tools/shared/model";
import type { Dispatch } from "react";
import type { ComposerThreadTarget, DraftSessionState } from "../../composerDraftStore";
import { useStableCallback } from "../../hooks/useStableCallback";
import { useApplyThreadModelPick } from "./useApplyThreadModelPick";
import {
  type EnvironmentId,
  type ModelSelection,
  ProviderInstanceId,
  type ServerProvider,
  type ThreadId,
  RuntimeMode,
} from "@t3tools/contracts";
import type { deriveThreadStructureRuntime } from "@t3tools/client-runtime/state/thread-execution";
import { type SetStateAction } from "react";
import { useNavigate } from "@tanstack/react-router";
import { toastManager } from "../ui/toast";
import {
  getAppModelOptionsForInstance,
  resolveAppModelSelectionForInstance,
} from "../../modelSelection";
import {
  draftModelSelectionApplies,
  resolveNewDraftStartFromOrigin,
  threadHasStarted,
} from "../../lib/chatThreadActions";
import { type DraftThreadEnvMode, useComposerDraftStore } from "../../composerDraftStore";
import {
  getStartedThreadModelChangeBlockReason,
  deriveLockedProvider,
  resolveComposerPickerModelSelection,
  makeModelDisabledReason,
} from "../ChatView.logic";
import { useMessages } from "gt-react";
import { cancelHeldComposerSend } from "./preparingComposerImages";
import { composerTargetKey } from "../../composerDraftStore";

interface ComposerSelectionInput {
  readonly providerInstanceEntries: ReadonlyArray<ProviderInstanceEntry>;
  readonly lockedContinuationGroupKey: string | null;
  readonly isEnvironmentChanging: boolean;
  readonly activeEnvironmentUnavailable: boolean;
  readonly isRevertingCheckpoint: boolean;
  readonly environmentId: EnvironmentId;
  readonly composerDraftTarget: ComposerThreadTarget;
  readonly draftThread: DraftSessionState | null;
  readonly activeThread: EnvironmentThreadShell | undefined;
  readonly activeRuntime: ReturnType<typeof deriveThreadStructureRuntime>;
  readonly activeProjectSettings: {
    readonly settings: { readonly newWorktreesStartFromOrigin: boolean };
  };
  readonly verifiedDefault?: boolean;
  readonly settings: UnifiedSettings;
  readonly providerStatuses: ReadonlyArray<ServerProvider>;
  readonly lockedProvider: ReturnType<typeof deriveLockedProvider>;
  readonly supportsProviderSwitchingViaHandoff: boolean;
  readonly multipleModelSelections: ReadonlyArray<ModelSelection> | null;
  readonly runtimeMode: RuntimeMode;
  readonly isLocalDraftThread: boolean;
  readonly canOverrideServerThreadEnvMode: boolean;
  readonly setPendingServerThreadEnvModeEntry: Dispatch<
    SetStateAction<{ readonly threadId: ThreadId; readonly mode: DraftThreadEnvMode } | null>
  >;
  readonly setPendingServerThreadStartFromOriginByThreadId: Dispatch<
    SetStateAction<Record<string, boolean>>
  >;
  readonly scheduleComposerFocus: () => void;
}

/**
 * The composer's choices for the next turn: provider and model, runtime mode,
 * and where a new thread runs (local or a new worktree, from the remote's tip or not).
 */
export function useComposerSelection(input: ComposerSelectionInput) {
  const {
    providerInstanceEntries,
    lockedContinuationGroupKey,
    isEnvironmentChanging,
    activeEnvironmentUnavailable,
    isRevertingCheckpoint,
    environmentId,
    composerDraftTarget,
    draftThread,
    activeThread,
    activeRuntime,
    activeProjectSettings,
    verifiedDefault,
    settings,
    providerStatuses,
    lockedProvider,
    supportsProviderSwitchingViaHandoff,
    multipleModelSelections,
    runtimeMode,
    isLocalDraftThread,
    canOverrideServerThreadEnvMode,
    setPendingServerThreadEnvModeEntry,
    setPendingServerThreadStartFromOriginByThreadId,
    scheduleComposerFocus,
  } = input;
  const messages = useMessages();
  const navigate = useNavigate();
  const setStickyComposerModelSelection = useComposerDraftStore(
    (store) => store.setStickyModelSelection,
  );
  const setComposerDraftModelSelection = useComposerDraftStore((store) => store.setModelSelection);
  const setComposerDraftRuntimeMode = useComposerDraftStore((store) => store.setRuntimeMode);
  const setDraftThreadContext = useComposerDraftStore((store) => store.setDraftThreadContext);
  const applyThreadModelPick = useApplyThreadModelPick();

  const handleRuntimeModeChange = (mode: RuntimeMode) => {
    if (mode === runtimeMode) return;
    cancelHeldComposerSend(composerTargetKey(composerDraftTarget));
    setComposerDraftRuntimeMode(composerDraftTarget, mode);
    if (isLocalDraftThread) {
      setDraftThreadContext(composerDraftTarget, { runtimeMode: mode });
    }
    scheduleComposerFocus();
  };
  const handleRuntimeModeChangeStable = useStableCallback(handleRuntimeModeChange);

  const openProviderSetup = (instanceId: ProviderInstanceId) => {
    void navigate({
      to: "/settings/providers",
      search: { environmentId, instanceId },
    });
  };
  const openProviderSetupStable = useStableCallback(openProviderSetup);

  // Keyed on the fields it reads: the thread and runtime objects change with
  // every streamed event, and a new function re-renders the whole model picker.
  const hasStartedSession = activeRuntime !== null;
  const runtimeProviderInstanceId = activeRuntime?.providerInstanceId ?? null;
  const currentModelSelection = activeThread?.modelSelection ?? null;
  const getModelDisabledReason = makeModelDisabledReason(
    currentModelSelection
      ? {
          hasStartedSession,
          supportsProviderSwitchingViaHandoff,
          currentModelSelection,
          currentProviderInstanceId: runtimeProviderInstanceId,
        }
      : null,
    messages,
  );

  const onProviderModelSelect = (
    instanceId: ProviderInstanceId,
    model: string,
    options?: ComposerModelSelectOptions,
  ) => {
    if (
      !activeThread ||
      isEnvironmentChanging ||
      activeEnvironmentUnavailable ||
      isRevertingCheckpoint
    )
      return;
    // Look up the configured instance so model normalization and custom
    // model lookup stay scoped to that exact instance. Unknown instance ids
    // are rejected by returning early; the server remains authoritative too.
    const configuredEntry = providerInstanceEntries.find(
      (entry) => entry.instanceId === instanceId,
    );
    if (!configuredEntry || !isProviderInstancePickerReady(configuredEntry)) {
      if (options?.focusComposer !== false) scheduleComposerFocus();
      return;
    }
    const entry = configuredEntry.snapshot;
    const resolvedDriverKind = entry.driver;
    if (
      !supportsProviderSwitchingViaHandoff &&
      lockedProvider !== null &&
      resolvedDriverKind !== null &&
      resolvedDriverKind !== lockedProvider
    ) {
      if (options?.focusComposer !== false) scheduleComposerFocus();
      return;
    }
    if (
      !supportsProviderSwitchingViaHandoff &&
      !matchesProviderModelLock(
        configuredEntry,
        lockedProvider,
        lockedContinuationGroupKey,
        activeRuntime?.providerInstanceId ?? activeThread.modelSelection.instanceId,
      )
    ) {
      if (options?.focusComposer !== false) scheduleComposerFocus();
      return;
    }
    // The picker lists an unavailable model only as the active selection, so re-picking keeps it.
    const resolvedModel = resolveAppModelSelectionForInstance(
      instanceId,
      settings,
      providerStatuses,
      model,
      { preserveUnavailableSelection: true },
    );
    const storedDraft = useComposerDraftStore.getState().getComposerDraft(composerDraftTarget);
    const draft = draftModelSelectionApplies(storedDraft, threadHasStarted(activeThread))
      ? storedDraft
      : null;
    const shownInstanceId =
      draft?.activeProvider ??
      activeRuntime?.providerInstanceId ??
      activeThread.modelSelection.instanceId;
    const shownSelection =
      draft?.modelSelectionByProvider[shownInstanceId] ?? activeThread.modelSelection;
    if (
      !resolvedModel ||
      (!options?.effort &&
        instanceId === shownInstanceId &&
        shownSelection.instanceId === instanceId &&
        shownSelection.model === resolvedModel)
    ) {
      // Re-picking the shown model keeps its options and saves it again if that save failed.
      if (resolvedModel && !isLocalDraftThread) applyThreadModelPick(composerDraftTarget);
      if (options?.focusComposer !== false) scheduleComposerFocus();
      return;
    }
    // Resolve remembered traits first; a shortcut supplies its explicit effort
    // before the complete model selection is persisted.
    const store = useComposerDraftStore.getState();
    const pickerDraft = store.getComposerDraft(composerDraftTarget);
    const currentInstance = pickerDraft?.activeProvider ?? activeThread.modelSelection.instanceId;
    const currentSelection =
      pickerDraft?.modelSelectionByProvider?.[currentInstance] ?? activeThread.modelSelection;
    const rememberedOptions = store.stickyOptionsByModelByProvider[instanceId]?.[resolvedModel];
    const selection = { instanceId, model: resolvedModel };
    const storedSelection = activeThread.modelSelection;
    const returnsToStoredModel =
      storedSelection.instanceId === instanceId &&
      (shownInstanceId !== instanceId || storedSelection.model !== resolvedModel) &&
      (storedSelection.model === resolvedModel ||
        resolveSelectableModel(
          resolvedDriverKind,
          storedSelection.model,
          getAppModelOptionsForInstance(settings, configuredEntry, storedSelection.model),
        ) === resolvedModel);
    const pickerSelection =
      returnsToStoredModel && storedSelection.options?.length
        ? { ...selection, options: storedSelection.options }
        : resolveComposerPickerModelSelection({
            ...selection,
            currentSelection,
            rememberedOptions,
          });
    const nextModelSelection = options?.effort
      ? withShortcutEffort(pickerSelection, resolvedModel, entry.models, options.effort)
      : pickerSelection;
    if (!nextModelSelection) return;
    const modelChangeBlockReason = getStartedThreadModelChangeBlockReason({
      hasStartedSession: activeRuntime !== null,
      supportsProviderSwitchingViaHandoff,
      currentModelSelection: activeThread.modelSelection,
      currentProviderInstanceId: activeRuntime?.providerInstanceId ?? null,
      nextModelSelection,
    });
    if (modelChangeBlockReason) {
      toastManager.add({
        type: "warning",
        title: modelChangeBlockReason.title,
        description: modelChangeBlockReason.description,
      });
      if (options?.focusComposer !== false) scheduleComposerFocus();
      return;
    }
    setComposerDraftModelSelection(composerDraftTarget, nextModelSelection, {
      explicit: true,
      replaceOptions: true,
    });
    setStickyComposerModelSelection(nextModelSelection);
    if (!isLocalDraftThread) applyThreadModelPick(composerDraftTarget);
    if (options?.focusComposer !== false) scheduleComposerFocus();
  };
  const onProviderModelSelectStable = useStableCallback(onProviderModelSelect);
  const onEnvModeChange = (mode: DraftThreadEnvMode) => {
    if (multipleModelSelections !== null) return;
    cancelHeldComposerSend(composerTargetKey(composerDraftTarget));
    if (canOverrideServerThreadEnvMode) {
      if (activeThread) setPendingServerThreadEnvModeEntry({ threadId: activeThread.id, mode });
      scheduleComposerFocus();
      return;
    }
    if (isLocalDraftThread) {
      setDraftThreadContext(composerDraftTarget, {
        envMode: mode,
        workspaceExplicit: true,
        startFromOrigin: resolveNewDraftStartFromOrigin({
          envMode: mode,
          newWorktreesStartFromOrigin:
            !verifiedDefault && activeProjectSettings.settings.newWorktreesStartFromOrigin,
        }),
        ...(mode !== "local" && draftThread?.worktreePath ? { worktreePath: null } : {}),
      });
    }
    scheduleComposerFocus();
  };
  const onEnvModeChangeStable = useStableCallback(onEnvModeChange);

  // Reads only the id: the shell changes on every streamed event, and a new handler
  // re-renders the branch toolbar and thread details panel with it.
  const activeThreadId = activeThread?.id;
  const onStartFromOriginChange = (nextStartFromOrigin: boolean) => {
    cancelHeldComposerSend(composerTargetKey(composerDraftTarget));
    if (canOverrideServerThreadEnvMode && activeThreadId !== undefined) {
      setPendingServerThreadStartFromOriginByThreadId((current) =>
        current[activeThreadId] === nextStartFromOrigin
          ? current
          : { ...current, [activeThreadId]: nextStartFromOrigin },
      );
      return;
    }
    if (isLocalDraftThread) {
      setDraftThreadContext(composerDraftTarget, {
        startFromOrigin: nextStartFromOrigin,
        workspaceExplicit: true,
      });
    }
  };
  return {
    getModelDisabledReason,
    handleRuntimeModeChangeStable,
    onEnvModeChangeStable,
    onProviderModelSelectStable,
    onStartFromOriginChange,
    openProviderSetupStable,
  };
}
