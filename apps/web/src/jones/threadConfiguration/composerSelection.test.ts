import { afterEach, describe, expect, it, onTestFinished } from "vite-plus/test";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import {
  EnvironmentId,
  ProjectId,
  ThreadId,
  ProviderDriverKind,
  ProviderInstanceId,
  OrchestrationV2Command,
} from "@t3tools/contracts";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { EMPTY_SHELL_STATE } from "@t3tools/client-runtime/state/shell";
import {
  useComposerDraftStore,
  DraftId,
  subscribeComposerDraftModelReconciliation,
} from "../../composerDraftStore";
import {
  buildLocalDraftThread,
  resolveComposerProviderSelection,
} from "../../components/ChatView.logic";
import { deriveProviderInstanceEntries } from "../../providerInstances";
import {
  acknowledgeComposerSelection,
  composerSelectionForDispatch,
  requiredComposerProvider,
} from "./composerSelection";

const decodeCommand = Schema.decodeUnknownSync(OrchestrationV2Command);

describe("web composer selection dispatch contract", () => {
  const initialState = useComposerDraftStore.getState();
  afterEach(() => useComposerDraftStore.setState(initialState, true));
  const selection = {
    instanceId: ProviderInstanceId.make("compatible-b"),
    model: "gpt-6-astra",
    options: [{ id: "reasoningEffort", value: "high" }],
  };
  it.each([undefined, false, true])(
    "carries selection values separately from picker intent %s",
    (explicit) => {
      const fields = composerSelectionForDispatch(selection, {
        activeProvider: selection.instanceId,
        modelSelectionByProvider: { [selection.instanceId]: selection },
        ...(explicit === undefined ? {} : { modelSelectionExplicit: explicit }),
      });
      const wire = decodeCommand({
        type: "message.dispatch",
        commandId: "composer",
        threadId: "thread",
        messageId: "follow-up",
        text: "continue",
        attachments: [],
        createdBy: "user",
        creationSource: "web",
        ...fields,
        deliveryIntent: "auto",
        dispatchMode: { type: "start_immediately" },
      });
      expect(wire).toMatchObject({
        modelSelection: selection,
        modelSelectionExplicit: explicit === true,
        deliveryIntent: "auto",
      });
    },
  );
  it("consumes an accepted picker choice so the next follow-up inherits defaults", () => {
    const target = DraftId.make("self-config-composer");
    const store = useComposerDraftStore.getState();
    store.setModelSelection(target, selection, { explicit: true });
    const submitted = store.getComposerDraft(target);
    expect(composerSelectionForDispatch(selection, submitted).modelSelectionExplicit).toBe(true);
    store.clearComposerContent(target);
    acknowledgeComposerSelection(target, submitted);
    expect(
      composerSelectionForDispatch(selection, store.getComposerDraft(target))
        .modelSelectionExplicit,
    ).toBe(false);
    store.setModelSelection(target, selection, { explicit: true });
    expect(
      composerSelectionForDispatch(selection, store.getComposerDraft(target))
        .modelSelectionExplicit,
    ).toBe(true);
  });
  it("preserves a newer picker choice made while the accepted send is in flight", () => {
    const target = DraftId.make("self-config-newer-composer");
    const store = useComposerDraftStore.getState();
    store.setModelSelection(target, selection, { explicit: true });
    const submitted = store.getComposerDraft(target);
    const next = { ...selection, model: "different-model" };
    store.setModelSelection(target, next, { explicit: true });
    acknowledgeComposerSelection(target, submitted);
    expect(composerSelectionForDispatch(next, store.getComposerDraft(target))).toEqual({
      modelSelection: next,
      modelSelectionExplicit: true,
    });
  });
  it.each(["model", "options"] as const)(
    "preserves equal explicit %s reselection across the older acknowledgement",
    (action) => {
      const target = scopeThreadRef(EnvironmentId.make("equal-pick"), ThreadId.make("thread"));
      const store = useComposerDraftStore.getState();
      store.setModelSelection(target, selection, { explicit: true });
      const first = store.getComposerDraft(target)!;
      if (action === "model") store.setModelSelection(target, selection, { explicit: true });
      else
        store.setProviderModelOptions(target, ProviderDriverKind.make("codex"), selection.options, {
          instanceId: selection.instanceId,
          model: selection.model,
        });
      const repeated = store.getComposerDraft(target)!;
      expect(repeated.modelSelectionByProvider).not.toBe(first.modelSelectionByProvider);
      acknowledgeComposerSelection(target, first);
      expect(store.getComposerDraft(target)).toBe(repeated);
      expect(composerSelectionForDispatch(selection, repeated).modelSelectionExplicit).toBe(true);
      acknowledgeComposerSelection(target, repeated);
      expect(
        composerSelectionForDispatch(selection, store.getComposerDraft(target))
          .modelSelectionExplicit,
      ).toBe(false);
      const passive = store.getComposerDraft(target);
      store.setModelSelection(target, selection, { replaceOptions: true });
      expect(store.getComposerDraft(target)).toBe(passive);
    },
  );

  it("keeps explicit intent through equal live-shell emissions and a concurrent saved default", () => {
    const target = scopeThreadRef(EnvironmentId.make("live-pick"), ThreadId.make("thread"));
    const store = useComposerDraftStore.getState();
    const thread = buildLocalDraftThread(
      target.threadId,
      {
        ...target,
        projectId: ProjectId.make("project"),
        logicalProjectKey: "live-pick",
        createdAt: "2026-11-02T20:00:00Z",
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        envMode: "local",
        startFromOrigin: false,
      },
      selection,
    );
    const tasks: Array<() => void> = [];
    const registry = AtomRegistry.make({
      scheduleTask: (task) => {
        tasks.push(task);
        return () => {};
      },
    });
    onTestFinished(() => registry.dispose());
    const shellAtom = Atom.make(thread);
    const stateAtom = Atom.make({
      ...EMPTY_SHELL_STATE,
      status: "live" as const,
      snapshot: Option.some({
        schemaVersion: 1 as const,
        snapshotSequence: 1,
        projects: [],
        threads: [thread.source],
        archivedThreads: [],
      }),
    });
    store.setModelSelection(target, selection, { explicit: true });
    const submitted = store.getComposerDraft(target);
    const shellResultAtom = Atom.map(stateAtom, AsyncResult.success);
    const noPendingThreadIdsAtom = Atom.make((): ReadonlySet<ThreadId> => new Set());
    onTestFinished(
      subscribeComposerDraftModelReconciliation({
        registry,
        threadRefsAtom: Atom.make([target]),
        threadShellAtom: () => shellAtom,
        shellStateAtom: () => shellResultAtom,
        pendingThreadIdsAtom: () => noPendingThreadIdsAtom,
      }),
    );
    const flush = () => {
      for (let task = tasks.shift(); task; task = tasks.shift()) task();
    };
    flush();
    expect(store.getComposerDraft(target)).toBe(submitted);
    const saved = { ...thread, modelSelection: { ...selection, model: "new-saved-model" } };
    registry.set(shellAtom, saved);
    registry.set(stateAtom, {
      ...registry.get(stateAtom),
      snapshot: Option.some({
        schemaVersion: 1 as const,
        snapshotSequence: 2,
        projects: [],
        threads: [{ ...thread.source, modelSelection: saved.modelSelection }],
        archivedThreads: [],
      }),
    });
    flush();
    expect(
      composerSelectionForDispatch(selection, store.getComposerDraft(target))
        .modelSelectionExplicit,
    ).toBe(true);
    acknowledgeComposerSelection(target, submitted);
    expect(
      composerSelectionForDispatch(saved.modelSelection, store.getComposerDraft(target))
        .modelSelectionExplicit,
    ).toBe(false);
  });

  it("requires the unavailable saved account until an explicit compatible replacement, preserving new draft fallback", () => {
    const savedId = ProviderInstanceId.make("codex-saved");
    const replacementId = ProviderInstanceId.make("codex-replacement");
    const saved = { instanceId: savedId, model: "model" };
    const target = DraftId.make("unavailable-saved");
    const entries = deriveProviderInstanceEntries(
      [savedId, replacementId].map((instanceId) => ({
        instanceId,
        driver: ProviderDriverKind.make("codex"),
        enabled: instanceId !== savedId,
        installed: true,
        version: null,
        status: "ready" as const,
        auth: { status: "authenticated" as const },
        checkedAt: "2026-11-02T20:00:00Z",
        models: [],
        slashCommands: [],
        skills: [],
      })),
    );
    const store = useComposerDraftStore.getState();
    store.setPrompt(target, "keep my draft");
    const resolve = (isLocalDraftThread: boolean) =>
      resolveComposerProviderSelection({
        entries,
        candidateInstanceIds: [savedId, replacementId],
        lockedProvider: ProviderDriverKind.make("codex"),
        lockedInstanceId: savedId,
        requiredInstanceId: requiredComposerProvider(
          store.getComposerDraft(target),
          saved,
          isLocalDraftThread,
        ),
      });
    expect(resolve(false)).toMatchObject({
      selectedProviderEntry: undefined,
      unavailableProviderInstanceId: savedId,
    });
    expect(resolve(true).selectedProviderEntry?.instanceId).toBe(replacementId);
    store.setModelSelection(target, { ...saved, instanceId: replacementId }, { explicit: true });
    expect(resolve(false).selectedProviderEntry?.instanceId).toBe(replacementId);
    expect(store.getComposerDraft(target)?.prompt).toBe("keep my draft");
    const incompatible = entries.map((entry) => ({
      ...entry,
      continuationGroupKey: entry.instanceId,
    }));
    expect(
      resolveComposerProviderSelection({
        entries: incompatible,
        candidateInstanceIds: [replacementId],
        lockedProvider: ProviderDriverKind.make("codex"),
        lockedInstanceId: savedId,
        requiredInstanceId: replacementId,
      }).selectedProviderEntry,
    ).toBeUndefined();
    expect(
      resolveComposerProviderSelection({
        entries: entries.map((entry) => ({
          ...entry,
          driverKind: ProviderDriverKind.make("antigravity"),
        })),
        candidateInstanceIds: [replacementId],
        lockedProvider: ProviderDriverKind.make("antigravity"),
        lockedInstanceId: savedId,
        requiredInstanceId: replacementId,
      }).selectedProviderEntry,
    ).toBeUndefined();
  });
  it("does not turn an inherited saved selection into a picker change when no draft exists", () => {
    expect(composerSelectionForDispatch(selection, undefined)).toEqual({
      modelSelection: selection,
      modelSelectionExplicit: false,
    });
  });
});
