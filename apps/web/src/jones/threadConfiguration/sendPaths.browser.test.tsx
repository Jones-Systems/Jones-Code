import {
  scopeProjectRef,
  scopeThreadRef,
  scopedThreadKey,
} from "@t3tools/client-runtime/environment";
import {
  ORCHESTRATION_V2_WS_METHODS,
  OrchestrationV2DispatchCommandError,
  PlanId,
  ProviderDriverKind,
  type ThreadId,
  type OrchestrationV2Command,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";
import { createMemoryHistory } from "@tanstack/react-router";
import type * as Context from "effect/Context";
import { AsyncResult } from "effect/reactivity";
import * as Option from "effect/Option";
import { useEffect } from "react";
// oxlint-disable-next-line t3code/require-disposable-render -- Boot subscriptions can hold async act open; _rootCleanup owns this synchronous mount and unmount.
import { createRoot } from "react-dom/client";
import { expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import "~/index.css";
import { AppRoot } from "../../AppRoot";
import { useComposerDraftStore } from "../../composerDraftStore";
import type { ComposerHandleRef } from "../../composerHandleContext";
import {
  useImplementPlanInNewThread,
  usePlanFollowUpSend,
} from "../../components/chat/useProposedPlanSend";
import { useThreadLifecycle } from "../../components/chat/useThreadLifecycle";
import { __setClientSettingsForTests } from "../../hooks/useSettings";
import { getRouter } from "../../router";
import { AppAtomRegistryProvider, appAtomRegistry } from "../../rpc/atomRegistry";
import { readProject, readThreadShell } from "../../state/entities";
import { environmentThreadDetails } from "../../state/threads";
import { usePendingMessages } from "../pendingSends/hooks";
import { pendingSends } from "../pendingSends/store";
import { planCreation, createPlanCreationController } from "../pendingSends/planCreation";
import { PlanCreationStatus } from "../pendingSends/PlanCreationStatus";
import { planCreationStorage, planCreationStorageKey } from "../pendingSends/planCreationStorage";
import type { PlanCreationSlot } from "@t3tools/client-runtime/jones/planCreationOperation";
import { toastManager } from "../../components/ui/toast";
import { act, render, type Screen } from "../../../test/render";
import {
  ENVIRONMENT_ID,
  PROJECT_ID,
  deferFakeSends,
  installFakeEnvironment,
  threadWithHistory,
  type FakeDriver,
} from "../../../test/fakeEnvironment";

const creationGate = vi.hoisted(() => ({
  intercept: undefined as
    | undefined
    | ((controls: {
        input: Extract<OrchestrationV2Command, { type: "thread.create" }>;
        accept: () => void;
        interrupt: () => void;
        reject: (status?: "accepted" | "rejected") => void;
      }) => void),
  messageRejection: false,
  pendingCreations: new Set<() => void>(),
}));

vi.mock("../../../../../packages/client-runtime/src/connection/driver.ts", async (original) => {
  const [actual, { fakeDriver }, Layer, Effect, Stream] = await Promise.all([
    original<{ readonly ConnectionDriver: Context.Key<unknown, FakeDriver> }>(),
    import("../../../test/fakeEnvironment"),
    import("effect/Layer"),
    import("effect/Effect"),
    import("effect/Stream"),
  ]);
  // The shared render fixture stores committed user items but leaves messages empty.
  const withUserMessages = (
    projection: OrchestrationV2ThreadProjection,
  ): OrchestrationV2ThreadProjection => ({
    ...projection,
    messages: [
      ...projection.messages,
      ...projection.turnItems.flatMap((item) =>
        item.type === "user_message" &&
        !projection.messages.some((message) => message.id === item.messageId)
          ? [
              {
                id: item.messageId,
                threadId: item.threadId,
                runId: item.runId,
                nodeId: item.nodeId,
                role: "user" as const,
                text: item.text,
                attachments: item.attachments,
                streaming: false,
                createdBy: item.createdBy,
                creationSource: item.creationSource,
                createdAt: item.startedAt ?? item.updatedAt,
                updatedAt: item.updatedAt,
              },
            ]
          : [],
      ),
    ],
  });
  return {
    ...actual,
    layer: Layer.succeed(actual.ConnectionDriver, {
      ...fakeDriver,
      connect: (...args: Parameters<typeof fakeDriver.connect>) =>
        Effect.map(fakeDriver.connect(...args), (lease) => ({
          ...lease,
          session: {
            ...lease.session,
            client: new Proxy(lease.session.client, {
              get(target, method, receiver) {
                if (method === ORCHESTRATION_V2_WS_METHODS.getThreadProjection)
                  return (input: Parameters<(typeof target)[typeof method]>[0]) =>
                    Effect.map(target[method](input), withUserMessages);
                if (method === ORCHESTRATION_V2_WS_METHODS.subscribeThread)
                  return (input: Parameters<(typeof target)[typeof method]>[0]) =>
                    Stream.map(target[method](input), (item) =>
                      item.kind === "snapshot"
                        ? { ...item, projection: withUserMessages(item.projection) }
                        : item,
                    );
                if (method === ORCHESTRATION_V2_WS_METHODS.dispatchCommand)
                  return (input: OrchestrationV2Command) => {
                    if (input.type === "thread.create" && creationGate.intercept) {
                      return Effect.callback<
                        { sequence: number },
                        OrchestrationV2DispatchCommandError
                      >((resume) => {
                        const interrupt = () => {
                          creationGate.pendingCreations.delete(interrupt);
                          resume(Effect.interrupt);
                        };
                        creationGate.pendingCreations.add(interrupt);
                        creationGate.intercept?.({
                          input,
                          accept: () => {
                            creationGate.pendingCreations.delete(interrupt);
                            resume(Effect.succeed({ sequence: 0 }));
                          },
                          interrupt,
                          reject: (status) => {
                            creationGate.pendingCreations.delete(interrupt);
                            resume(
                              Effect.fail(
                                new OrchestrationV2DispatchCommandError({
                                  commandId: input.commandId,
                                  commandType: input.type,
                                  message: "Synthetic create response failed",
                                  ...(status
                                    ? {
                                        deliveryEvidence: {
                                          kind: "dispatch",
                                          commandId: input.commandId,
                                          commandType: input.type,
                                          threadId: input.threadId,
                                          status,
                                        },
                                      }
                                    : {}),
                                }),
                              ),
                            );
                          },
                        });
                      });
                    }
                    if (input.type === "message.dispatch" && creationGate.messageRejection)
                      return Effect.fail(
                        new OrchestrationV2DispatchCommandError({
                          commandId: input.commandId,
                          commandType: input.type,
                          message: "Synthetic confirmed message rejection",
                          deliveryEvidence: {
                            kind: "dispatch",
                            commandId: input.commandId,
                            commandType: input.type,
                            threadId: input.threadId,
                            status: "rejected",
                          },
                        }),
                      );
                    return target[method](input);
                  };
                return Reflect.get(target, method, receiver);
              },
            }),
          },
        })),
    }),
  };
});
vi.mock(import("../../rpc/requestLatencyState"), async (original) => ({
  ...(await original()),
  trackRpcRequestSent: () => {},
}));

const cases = [
  ["plan", false],
  ["plan", true],
  ["new-plan", false],
  ["new-plan", true],
  ["standalone", false],
  ["standalone", true],
] as const;
// The app registry keeps its shell subscription between cases, so every case boots
// against the same roster rather than replacing it with an unseen thread ID.
const recoveryCases = [
  "reload-false",
  "reload-true",
  "storage",
  "create-rejected",
  "message-rejected",
  "navigation",
] as const;
const fixtures = [
  ...cases.map(([path, explicit]) => threadWithHistory(`intent-${path}-${explicit}`, 1)),
  ...recoveryCases.map((name) => threadWithHistory(`plan-${name}`, 1)),
];

it.each(cases)(
  "%s captures explicitness %s before delayed settings and protects the newer pick on acknowledgement",
  async (path, explicit) => {
    let sending: Promise<unknown> | undefined;
    const journalKey = planCreationStorageKey(ENVIRONMENT_ID);
    const savedJournal = localStorage.getItem(journalKey);
    localStorage.removeItem(journalKey);
    await using _journalCleanup = {
      [Symbol.asyncDispose]: async () => {
        for (const interrupt of creationGate.pendingCreations) interrupt();
        await Promise.allSettled(sending ? [sending] : []);
        if (savedJournal === null) localStorage.removeItem(journalKey);
        else localStorage.setItem(journalKey, savedJournal);
      },
    };
    using _pendingCleanup = {
      [Symbol.dispose]: () => {
        for (const entry of pendingSends.getSnapshot()) pendingSends.dismiss(entry.key);
      },
    };
    const previous = useComposerDraftStore.getState();
    using _draftCleanup = {
      [Symbol.dispose]: () => useComposerDraftStore.setState(previous, true),
    };
    const fixture = fixtures.find((thread) => thread.id === `intent-${path}-${explicit}`)!;
    using _environment = installFakeEnvironment(fixtures);
    using delivery = deferFakeSends();
    __setClientSettingsForTests({
      ...DEFAULT_CLIENT_SETTINGS,
      onboardingCompletedAt: "2026-01-01T00:00:00.000Z",
    });
    await page.viewport(1400, 900);
    const router = getRouter(
      createMemoryHistory({ initialEntries: [`/${ENVIRONMENT_ID}/${fixture.id}`] }),
    );
    await router.load();
    const container = document.createElement("div");
    container.style.cssText = "height: 900px; width: 1400px;";
    document.body.append(container);
    const root = createRoot(container);
    using _rootCleanup = {
      [Symbol.dispose]: () => {
        root.unmount();
        container.remove();
      },
    };
    root.render(<AppRoot router={router} />);
    await expect.element(page.getByText(`Answer 0 for ${fixture.id}.`)).toBeVisible();
    await expect.element(page.getByTestId("composer-editor")).toBeVisible();
    const target = scopeThreadRef(ENVIRONMENT_ID, fixture.id);
    await vi.waitFor(() => expect(readThreadShell(target)).not.toBeNull());
    const thread = readThreadShell(target)!;
    const routeThreadKey = scopedThreadKey(target);
    const drafts = useComposerDraftStore.getState();
    if (explicit) drafts.setModelSelection(target, thread.modelSelection, { explicit: true });
    let releaseSettings!: () => void;
    let reachedSettings!: () => void;
    const settingsEntered = new Promise<void>((resolve) => {
      reachedSettings = resolve;
    });
    const settings = new Promise<ReturnType<typeof AsyncResult.success<void>>>((resolve) => {
      releaseSettings = () => resolve(AsyncResult.success(undefined));
    });
    let interruptCreation: (() => void) | undefined;
    using _creationCleanup = {
      [Symbol.dispose]: () => {
        interruptCreation?.();
        creationGate.intercept = undefined;
        releaseSettings();
      },
    };
    if (path === "new-plan")
      creationGate.intercept = ({ accept, interrupt }) => {
        releaseSettings = () => {
          interruptCreation = undefined;
          accept();
        };
        interruptCreation = interrupt;
        reachedSettings();
      };
    const persistSettings = () => {
      reachedSettings();
      return settings;
    };
    const getSendContext: NonNullable<ComposerHandleRef["current"]>["getSendContext"] = () => ({
      answeringPendingUserInput: false,
      prompt: "",
      images: [],
      files: [],
      terminalContexts: [],
      previewAnnotations: [],
      reviewComments: [],
      threadContexts: [],
      shownPendingApprovalIds: [],
      selectedModelSelection: thread.modelSelection,
      selectedProvider: ProviderDriverKind.make("codex"),
      selectedModel: thread.modelSelection.model,
      selectedModelOptionsForDispatch: null,
      multipleModelSelections: null,
      selectedProviderModels: [],
      selectedPromptEffort: null,
      providerAvailable: true,
      interactionModeEnabled: true,
      interactionMode: "default",
    });
    const composerRef = { current: { getSendContext } } as ComposerHandleRef;
    let submitPlan!: ReturnType<typeof usePlanFollowUpSend>;
    let submitNewPlan!: ReturnType<typeof useImplementPlanInNewThread>;
    let submitStandalone!: ReturnType<typeof useThreadLifecycle>["sendStandaloneCommand"];
    function Probe() {
      const [, setMessages] = usePendingMessages(ENVIRONMENT_ID, fixture.id);
      const plan = usePlanFollowUpSend({
        environmentId: ENVIRONMENT_ID,
        routeThreadKey,
        activeThread: thread,
        activeThreadBranch: thread.branch,
        activeProposedPlan: null,
        isServerThread: true,
        isSendBusy: false,
        isConnecting: false,
        composerRef,
        composerDraftTarget: target,
        beginLocalDispatch: vi.fn(),
        resetLocalDispatch: vi.fn(),
        runtimeMode: "full-access",
        localCheckoutBranchMismatch: null,
        setOptimisticUserMessages: setMessages,
        setThreadError: vi.fn(),
        anchorNewTurn: vi.fn(),
        persistThreadSettingsForNextTurn: persistSettings,
      });
      const newPlan = useImplementPlanInNewThread({
        environmentId: ENVIRONMENT_ID,
        routeThreadKey,
        activeThread: thread,
        activeThreadBranch: thread.branch,
        activeProposedPlan: {
          id: PlanId.make("source-plan"),
          createdAt: "2026-11-02T20:00:00Z",
          updatedAt: "2026-11-02T20:00:00Z",
          runId: null,
          planMarkdown: "# Captured plan",
          status: "active",
        },
        isServerThread: true,
        isSendBusy: false,
        isConnecting: false,
        composerRef,
        composerDraftTarget: target,
        beginLocalDispatch: vi.fn(),
        resetLocalDispatch: vi.fn(),
        activeProject: readProject(scopeProjectRef(ENVIRONMENT_ID, PROJECT_ID)),
        activeEnvironmentUnavailable: false,
        defaultRuntimeMode: "full-access",
      });
      const standalone = useThreadLifecycle({
        environmentId: ENVIRONMENT_ID,
        routeThreadKey,
        currentRouteThreadKeyRef: { current: routeThreadKey },
        canOperateThread: true,
        clientSettingsHydrated: true,
        activeThread: thread,
        activeThreadId: thread.id,
        activeThreadRef: target,
        activeThreadKey: routeThreadKey,
        activeThreadShell: thread,
        activeLatestRun: thread.latestRun,
        activeProjectCwd: null,
        serverConfig: null,
        runtimeMode: "full-access",
        isWorking: false,
        pendingBackgroundTasks: [],
        activePendingApproval: null,
        activePendingUserInput: null,
        localCheckoutBranchMismatch: null,
        gitStatusQuery: { refresh: vi.fn() },
        nowMinute: "2026-11-02T20:00",
        composerRef,
        composerDraftTarget: target,
        sendInFlightRef: { current: false },
        setOptimisticUserMessages: setMessages,
        setThreadError: vi.fn(),
        clearUsageLimitsFor: vi.fn(),
        beginLocalDispatch: vi.fn(),
        resetLocalDispatch: vi.fn(),
        scheduleComposerFocus: vi.fn(),
        scrollToEnd: vi.fn(),
        persistThreadSettingsForNextTurn: persistSettings,
      }).sendStandaloneCommand;
      useEffect(() => {
        submitPlan = plan;
        submitNewPlan = newPlan;
        submitStandalone = standalone;
      }, [plan, newPlan, standalone]);
      return null;
    }
    await using _probe = await render(
      <AppAtomRegistryProvider>
        <Probe />
      </AppAtomRegistryProvider>,
    );
    sending =
      path === "plan"
        ? submitPlan({ text: "implement", interactionMode: "default" })
        : path === "new-plan"
          ? submitNewPlan()
          : submitStandalone("/goal resume", "Failed to resume");
    await settingsEntered;
    const next = explicit
      ? thread.modelSelection
      : { ...thread.modelSelection, model: "newer-explicit-model" };
    await act(() => drafts.setModelSelection(target, next, { explicit: true }));
    const newer = drafts.getComposerDraft(target);
    releaseSettings();
    await vi.waitFor(() => expect(delivery.requests).toHaveLength(1));
    expect(delivery.requests[0]?.payload).toMatchObject({
      type: "message.dispatch",
      modelSelection: thread.modelSelection,
      modelSelectionExplicit: explicit,
    });
    if (path === "new-plan")
      expect(delivery.requests[0]?.payload).toMatchObject({
        sourcePlanRef: { threadId: thread.id, planId: "source-plan" },
      });
    await act(async () => {
      await delivery.requests[0]!.commit();
      delivery.requests[0]!.acknowledge();
    });
    await sending;
    expect(drafts.getComposerDraft(target)?.modelSelectionByProvider).toBe(
      newer?.modelSelectionByProvider,
    );
    expect(drafts.getComposerDraft(target)?.modelSelectionExplicit).toBe(true);
  },
);

async function withPlanAction(
  name: (typeof recoveryCases)[number],
  run: (fixture: {
    submit: () => Promise<void>;
    resume: (slot: PlanCreationSlot) => Promise<void>;
    hidePlan: () => void;
    remount: () => Promise<void>;
    refresh: () => Promise<void>;
    screen: () => Screen;
    target: ReturnType<typeof scopeThreadRef>;
    thread: NonNullable<ReturnType<typeof readThreadShell>>;
    delivery: ReturnType<typeof deferFakeSends>;
    getSendContext: ReturnType<
      typeof vi.fn<NonNullable<ComposerHandleRef["current"]>["getSendContext"]>
    >;
  }) => Promise<void>,
) {
  const sends = new Set<Promise<void>>();
  const track = (send: Promise<void>) => {
    sends.add(send);
    void send.then(
      () => sends.delete(send),
      () => sends.delete(send),
    );
    return send;
  };
  const previous = useComposerDraftStore.getState();
  const storageKey = planCreationStorageKey(ENVIRONMENT_ID);
  const saved = localStorage.getItem(storageKey);
  localStorage.removeItem(storageKey);
  await using _stateCleanup = {
    [Symbol.asyncDispose]: async () => {
      for (const interrupt of creationGate.pendingCreations) interrupt();
      await Promise.allSettled(sends);
      creationGate.intercept = undefined;
      creationGate.messageRejection = false;
      for (const entry of pendingSends.getSnapshot()) pendingSends.dismiss(entry.key);
      useComposerDraftStore.setState(previous, true);
      if (saved === null) localStorage.removeItem(storageKey);
      else localStorage.setItem(storageKey, saved);
    },
  };
  using _environment = installFakeEnvironment(fixtures);
  using delivery = deferFakeSends();
  __setClientSettingsForTests({
    ...DEFAULT_CLIENT_SETTINGS,
    onboardingCompletedAt: "2026-01-01T00:00:00Z",
  });
  const fixture = fixtures.find((thread) => thread.id === `plan-${name}`)!;
  const router = getRouter(
    createMemoryHistory({ initialEntries: [`/${ENVIRONMENT_ID}/${fixture.id}`] }),
  );
  await router.load();
  const container = document.createElement("div");
  document.body.append(container);
  const root = createRoot(container);
  using _rootCleanup = {
    [Symbol.dispose]: () => {
      root.unmount();
      container.remove();
    },
  };
  root.render(<AppRoot router={router} />);
  await expect.element(page.getByText(`Answer 0 for ${fixture.id}.`)).toBeVisible();
  const target = scopeThreadRef(ENVIRONMENT_ID, fixture.id);
  await vi.waitFor(() => expect(readThreadShell(target)).not.toBeNull());
  const thread = readThreadShell(target)!;
  const getSendContext = vi.fn<NonNullable<ComposerHandleRef["current"]>["getSendContext"]>(() => ({
    answeringPendingUserInput: false,
    prompt: "",
    images: [],
    files: [],
    terminalContexts: [],
    previewAnnotations: [],
    reviewComments: [],
    threadContexts: [],
    shownPendingApprovalIds: [],
    selectedModelSelection: thread.modelSelection,
    selectedProvider: ProviderDriverKind.make("codex"),
    selectedModel: thread.modelSelection.model,
    selectedModelOptionsForDispatch: null,
    multipleModelSelections: null,
    selectedProviderModels: [],
    selectedPromptEffort: null,
    providerAvailable: true,
    interactionModeEnabled: true,
    interactionMode: "default",
  }));
  const composerRef = { current: { getSendContext: () => getSendContext() } } as ComposerHandleRef;
  let submit!: ReturnType<typeof useImplementPlanInNewThread>;
  let planAvailable = true;
  function Probe() {
    const send = useImplementPlanInNewThread({
      environmentId: ENVIRONMENT_ID,
      routeThreadKey: scopedThreadKey(target),
      activeThread: thread,
      activeThreadBranch: thread.branch,
      activeProposedPlan: planAvailable
        ? {
            id: PlanId.make("durable-plan"),
            createdAt: "2026-11-05T16:00:00Z",
            updatedAt: "2026-11-05T16:00:00Z",
            runId: null,
            planMarkdown: "# Durable implementation",
            status: "active",
          }
        : null,
      isServerThread: true,
      isSendBusy: false,
      isConnecting: false,
      composerRef,
      composerDraftTarget: target,
      beginLocalDispatch: () => {},
      resetLocalDispatch: () => {},
      activeProject: readProject(scopeProjectRef(ENVIRONMENT_ID, PROJECT_ID)),
      activeEnvironmentUnavailable: false,
      defaultRuntimeMode: "full-access",
    });
    useEffect(() => {
      submit = send;
    }, [send]);
    return (
      <PlanCreationStatus
        environmentId={ENVIRONMENT_ID}
        sourceThreadId={thread.id}
        disabled={false}
        onResume={(slot) => track(send(slot))}
      />
    );
  }
  let screen = await render(
    <AppAtomRegistryProvider>
      <Probe />
    </AppAtomRegistryProvider>,
  );
  await using _probeCleanup = { [Symbol.asyncDispose]: () => screen[Symbol.asyncDispose]() };
  await run({
    submit: () => track(submit()),
    resume: (slot) => track(submit(slot)),
    hidePlan: () => {
      planAvailable = false;
    },
    screen: () => screen,
    target,
    thread,
    delivery,
    getSendContext,
    refresh: () =>
      screen.rerender(
        <AppAtomRegistryProvider>
          <Probe key="refreshed" />
        </AppAtomRegistryProvider>,
      ),
    remount: async () => {
      await screen[Symbol.asyncDispose]();
      screen = await render(
        <AppAtomRegistryProvider>
          <Probe />
        </AppAtomRegistryProvider>,
      );
    },
  });
}

it.each([false, true])(
  "resumes a committed create with lost reply after remount/reload; explicit %s",
  async (explicit) => {
    await withPlanAction(explicit ? "reload-true" : "reload-false", async (f) => {
      const drafts = useComposerDraftStore.getState();
      if (explicit) drafts.setModelSelection(f.target, f.thread.modelSelection, { explicit: true });
      const creates: Extract<OrchestrationV2Command, { type: "thread.create" }>[] = [];
      const committedCreates = new Map<string, ThreadId>();
      let release!: () => void;
      let reached!: () => void;
      const entered = new Promise<void>((resolve) => {
        reached = resolve;
      });
      creationGate.intercept = ({ input, reject }) => {
        creates.push(input);
        committedCreates.set(input.commandId, input.threadId);
        release = () => reject();
        reached();
      };
      const first = f.submit();
      await entered;
      const next = explicit
        ? f.thread.modelSelection
        : { ...f.thread.modelSelection, model: "newer-pick" };
      drafts.setModelSelection(f.target, next, { explicit: true });
      drafts.setPrompt(f.target, "newer source draft");
      const newer = drafts.getComposerDraft(f.target);
      release();
      await first;
      expect(f.delivery.requests).toHaveLength(0);
      expect(pendingSends.getSnapshot()[0]).toMatchObject({
        delivery: "uncertain",
        issued: false,
        durable: true,
      });
      // The shell subscription has not published the committed target yet.
      expect(readThreadShell(scopeThreadRef(ENVIRONMENT_ID, creates[0]!.threadId))).toBeNull();
      await f.remount();
      await expect.element(f.screen().getByText("Plan delivery needs confirmation")).toBeVisible();
      for (const entry of pendingSends.getSnapshot()) pendingSends.dismiss(entry.key);
      const reloaded = createPlanCreationController(planCreationStorage, pendingSends);
      const runSpy = vi.spyOn(planCreation, "run").mockImplementation(reloaded.run);
      using _runCleanup = { [Symbol.dispose]: () => runSpy.mockRestore() };
      const context = f.getSendContext();
      f.getSendContext.mockImplementation(() => ({ ...context, providerAvailable: false }));
      const callsBeforeReplay = f.getSendContext.mock.calls.length;
      creationGate.intercept = ({ input, accept }) => {
        creates.push(input);
        committedCreates.set(input.commandId, input.threadId);
        accept();
      };
      const replay = f.submit();
      await vi.waitFor(() => expect(f.delivery.requests).toHaveLength(1));
      const message = f.delivery.requests[0]!.payload;
      expect(message).toMatchObject({
        type: "message.dispatch",
        threadId: creates[0]!.threadId,
        modelSelection: f.thread.modelSelection,
        modelSelectionExplicit: explicit,
        sourcePlanRef: { threadId: f.thread.id, planId: "durable-plan" },
      });
      await act(async () => {
        await f.delivery.requests[0]!.commit();
        f.delivery.requests[0]!.acknowledge();
      });
      await replay;
      expect(creates).toHaveLength(2);
      expect(creates[1]).toEqual(creates[0]);
      expect(committedCreates.size).toBe(1);
      expect(new Set(committedCreates.values()).size).toBe(1);
      expect(f.getSendContext.mock.calls).toHaveLength(callsBeforeReplay);
      expect(drafts.getComposerDraft(f.target)?.modelSelectionByProvider).toBe(
        newer?.modelSelectionByProvider,
      );
      expect(drafts.getComposerDraft(f.target)?.modelSelectionExplicit).toBe(true);
      expect(drafts.getComposerDraft(f.target)?.prompt).toBe("newer source draft");
      const target = scopeThreadRef(ENVIRONMENT_ID, creates[0]!.threadId);
      await vi.waitFor(() => {
        const projection = Option.getOrNull(
          appAtomRegistry.get(environmentThreadDetails.stateAtom(target)).data,
        );
        expect(projection?.runs).toHaveLength(1);
        expect(projection?.messages.filter((row) => row.role === "user")).toHaveLength(1);
      });
      const saved = await planCreationStorage.read({
        environmentId: ENVIRONMENT_ID,
        sourceThreadId: f.thread.id,
        sourcePlanId: PlanId.make("durable-plan"),
        action: "implement-in-new-thread",
      });
      f.hidePlan();
      await f.remount();
      await expect.element(f.screen().getByText("Implementation thread saved")).toBeVisible();
      await f.resume(saved!);
      expect(creates).toHaveLength(2);
      expect(f.delivery.requests).toHaveLength(1);
    });
  },
);

it("does not issue create when the platform storage write fails", async () => {
  await withPlanAction("storage", async (f) => {
    const creates = vi.fn();
    creationGate.intercept = creates;
    const originalSetItem = Storage.prototype.setItem;
    const setItem = vi.spyOn(Storage.prototype, "setItem").mockImplementation(function (
      this: Storage,
      key,
      value,
    ) {
      if (key === planCreationStorageKey(ENVIRONMENT_ID))
        throw new Error("Synthetic quota failure");
      originalSetItem.call(this, key, value);
    });
    using _storageCleanup = { [Symbol.dispose]: () => setItem.mockRestore() };
    useComposerDraftStore.getState().setPrompt(f.target, "keep this draft");
    await f.submit();
    expect(creates).not.toHaveBeenCalled();
    expect(f.delivery.requests).toHaveLength(0);
    expect(useComposerDraftStore.getState().getComposerDraft(f.target)?.prompt).toBe(
      "keep this draft",
    );
  });
});

it("preserves a rejected create before allowing a new edited action", async () => {
  await withPlanAction("create-rejected", async (f) => {
    const creates: Extract<OrchestrationV2Command, { type: "thread.create" }>[] = [];
    creationGate.intercept = ({ input, reject }) => {
      creates.push(input);
      reject("rejected");
    };
    await f.submit();
    expect(f.delivery.requests).toHaveLength(0);
    useComposerDraftStore
      .getState()
      .setModelSelection(
        f.target,
        { ...f.thread.modelSelection, model: "edited-model" },
        { explicit: true },
      );
    await f.submit();
    expect(creates).toHaveLength(2);
    expect(creates[1]!.commandId).not.toBe(creates[0]!.commandId);
    expect(creates[1]!.threadId).not.toBe(creates[0]!.threadId);
    const document = JSON.parse(localStorage.getItem(planCreationStorageKey(ENVIRONMENT_ID))!);
    expect(document.history).toHaveLength(1);
    expect(document.history[0]).toMatchObject({
      phase: "create-rejected",
      create: { commandId: creates[0]!.commandId },
    });
  });
});

it("keeps the accepted target and prompt when its first message is rejected", async () => {
  await withPlanAction("message-rejected", async (f) => {
    let created!: ThreadId;
    const create = vi.fn<NonNullable<typeof creationGate.intercept>>(({ input, accept }) => {
      created = input.threadId;
      accept();
    });
    creationGate.intercept = create;
    creationGate.messageRejection = true;
    await f.submit();
    const target = scopeThreadRef(ENVIRONMENT_ID, created);
    expect(useComposerDraftStore.getState().getComposerDraft(target)?.prompt).toContain(
      "Durable implementation",
    );
    expect(pendingSends.getSnapshot()[0]).toMatchObject({
      creationAccepted: true,
      delivery: "failed",
    });
    await f.remount();
    await f.submit();
    expect(create).toHaveBeenCalledTimes(1);
    expect(f.delivery.requests).toHaveLength(0);
  });
});

it("reports accepted navigation failure and reopens terminal linkage without creating again", async () => {
  await withPlanAction("navigation", async (f) => {
    const create = vi.fn<NonNullable<typeof creationGate.intercept>>(({ accept }) => accept());
    creationGate.intercept = create;
    const navigate = vi
      .spyOn(f.screen().router!, "navigate")
      .mockRejectedValueOnce(new Error("Synthetic navigation failure"));
    const toast = vi.spyOn(toastManager, "add");
    using _spyCleanup = {
      [Symbol.dispose]: () => {
        navigate.mockRestore();
        toast.mockRestore();
      },
    };
    await f.refresh();
    const sending = f.submit();
    await vi.waitFor(() => expect(f.delivery.requests).toHaveLength(1));
    await act(async () => {
      await f.delivery.requests[0]!.commit();
      f.delivery.requests[0]!.acknowledge();
    });
    await sending;
    expect(toast).toHaveBeenCalledWith(
      expect.objectContaining({ title: "Thread started; could not open it" }),
    );
    await f.remount();
    await f.submit();
    expect(create).toHaveBeenCalledTimes(1);
    expect(f.delivery.requests).toHaveLength(1);
  });
});
