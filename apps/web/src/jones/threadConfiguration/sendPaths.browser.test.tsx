import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import {
  ORCHESTRATION_V2_WS_METHODS,
  ProviderDriverKind,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";
import { createMemoryHistory } from "@tanstack/react-router";
import type * as Context from "effect/Context";
import { AsyncResult } from "effect/reactivity";
import { useEffect } from "react";
// oxlint-disable-next-line t3code/require-disposable-render -- Boot subscriptions can hold async act open; _rootCleanup owns this synchronous mount and unmount.
import { createRoot } from "react-dom/client";
import { expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import "~/index.css";
// Load the real routed view before the scenario starts its synthetic transport.
import "../../components/ThreadRouteView";
import { AppRoot } from "../../AppRoot";
import { useComposerDraftStore } from "../../composerDraftStore";
import type { ComposerHandleRef } from "../../composerHandleContext";
import { useThreadLifecycle } from "../../components/chat/useThreadLifecycle";
import { __setClientSettingsForTests } from "../../hooks/useSettings";
import { getRouter } from "../../router";
import { AppAtomRegistryProvider } from "../../rpc/atomRegistry";
import { readThreadShell } from "../../state/entities";
import { usePendingMessages } from "../pendingSends/hooks";
import { pendingSends } from "../pendingSends/store";
import { act, render } from "../../../test/render";
import {
  ENVIRONMENT_ID,
  deferFakeSends,
  installFakeEnvironment,
  threadWithHistory,
  type FakeDriver,
} from "../../../test/fakeEnvironment";

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

// App modules can subscribe before a test starts. Admit real primary HTTP only
// after this file has installed its synthetic descriptor/session/transport.
const primaryFixtureAdmission = vi.hoisted(() => {
  let admit!: () => void;
  const ready = new Promise<void>((resolve) => {
    admit = resolve;
  });
  return { ready, admit };
});
vi.mock(import("~/environments/primary/httpLayer"), async (original) => {
  const [actual, Effect, Layer] = await Promise.all([
    original(),
    import("effect/Effect"),
    import("effect/Layer"),
  ]);
  return {
    ...actual,
    layer: Layer.unwrap(
      Effect.promise(() => primaryFixtureAdmission.ready).pipe(
        Effect.map(() => actual.layerForCurrentOrigin()),
      ),
    ),
  };
});

const explicitCases = [false, true] as const;
const fixtures = explicitCases.map((explicit) =>
  threadWithHistory(`intent-standalone-${explicit}`, 1),
);

it.each(explicitCases)(
  "a standalone command captures explicitness %s before delayed settings and protects the newer pick on acknowledgement",
  async (explicit) => {
    let sending: Promise<unknown> | undefined;
    await using _sendCleanup = {
      [Symbol.asyncDispose]: async () => {
        await Promise.allSettled(sending ? [sending] : []);
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
    const fixture = fixtures.find((thread) => thread.id === `intent-standalone-${explicit}`)!;
    using _environment = installFakeEnvironment(fixtures);
    primaryFixtureAdmission.admit();
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
    using _settingsCleanup = { [Symbol.dispose]: () => releaseSettings() };
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
      environmentContexts: [],
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
    let submitStandalone!: ReturnType<typeof useThreadLifecycle>["sendStandaloneCommand"];
    function Probe() {
      const [, setMessages] = usePendingMessages(ENVIRONMENT_ID, fixture.id);
      const standalone = useThreadLifecycle({
        environmentId: ENVIRONMENT_ID,
        routeThreadKey,
        currentRouteThreadKeyRef: { current: routeThreadKey },
        canOperateThread: true,
        clientSettingsHydrated: true,
        activeThread: thread,
        activeThreadId: thread.id,
        activeThreadRef: target,
        threadDetailRef: target,
        turnItems: [],
        activeThreadKey: routeThreadKey,
        activeThreadShell: thread,
        limitRecoveryHoldsSnooze: false,
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
        scrollToEnd: vi.fn(),
        persistThreadSettingsForNextTurn: persistSettings,
      }).sendStandaloneCommand;
      useEffect(() => {
        submitStandalone = standalone;
      }, [standalone]);
      return null;
    }
    await using _probe = await render(
      <AppAtomRegistryProvider>
        <Probe />
      </AppAtomRegistryProvider>,
    );
    sending = submitStandalone("/goal resume", "Failed to resume");
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
