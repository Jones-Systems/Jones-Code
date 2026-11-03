import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  ORCHESTRATION_V2_WS_METHODS,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ThreadShell,
  type ServerConfig,
} from "@t3tools/contracts";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";
import { createMemoryHistory } from "@tanstack/react-router";
import type * as Context from "effect/Context";
// oxlint-disable-next-line t3code/require-disposable-render -- Boot subscriptions can hold async act open; _rootCleanup owns this synchronous mount and unmount.
import { createRoot } from "react-dom/client";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import "~/index.css";
import { AppRoot } from "../../AppRoot";
import { useComposerDraftStore } from "../../composerDraftStore";
import { __setClientSettingsForTests } from "../../hooks/useSettings";
import { getRouter } from "../../router";
import { readThreadShell } from "../../state/entities";
import { act } from "../../../test/render";
import {
  ENVIRONMENT_ID,
  deferFakeSends,
  installFakeEnvironment,
  threadWithHistory,
  type FakeDriver,
} from "../../../test/fakeEnvironment";
import { pendingSends } from "../pendingSends/store";

function disabledSavedProvider(config: ServerConfig): ServerConfig {
  const codex = { ...config.providers[0]!, continuation: { groupKey: "synthetic-shared" } };
  return {
    ...config,
    providers: [codex, { ...codex, instanceId: ProviderInstanceId.make("codex-replacement") }],
    settings: {
      ...config.settings,
      providerInstances: {
        [codex.instanceId]: { driver: codex.driver, enabled: false },
        [ProviderInstanceId.make("codex-replacement")]: { driver: codex.driver, enabled: true },
      },
    },
  };
}

const detachedProviderThreadId = ProviderThreadId.make("disabled-saved-account-provider-thread");

function withDetachedProviderThreadShell(
  thread: OrchestrationV2ThreadShell,
): OrchestrationV2ThreadShell {
  return { ...thread, activeProviderThreadId: detachedProviderThreadId };
}

// A completed run alone cannot prove portable handoff. Retain its native thread
// after session detachment so the real picker can verify that B is selectable.
function withDetachedProviderThread(
  projection: OrchestrationV2ThreadProjection,
): OrchestrationV2ThreadProjection {
  return {
    ...projection,
    thread: { ...projection.thread, activeProviderThreadId: detachedProviderThreadId },
    providerThreads: [
      {
        id: detachedProviderThreadId,
        driver: ProviderDriverKind.make("codex"),
        providerInstanceId: projection.thread.modelSelection.instanceId,
        providerSessionId: null,
        appThreadId: projection.thread.id,
        ownerNodeId: null,
        nativeThreadRef: {
          driver: ProviderDriverKind.make("codex"),
          nativeId: "synthetic-disabled-saved-conversation",
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "not_loaded",
        firstRunOrdinal: projection.runs[0]?.ordinal ?? null,
        lastRunOrdinal: projection.runs.at(-1)?.ordinal ?? null,
        handoffIds: [],
        forkedFrom: null,
        pendingBackgroundTasks: [],
        contextUsage: null,
        nativeMetadata: null,
        goal: null,
        createdAt: projection.thread.createdAt,
        updatedAt: projection.updatedAt,
      },
    ],
  };
}

vi.mock("../../../../../packages/client-runtime/src/connection/driver.ts", async (original) => {
  const [actual, { fakeDriver }, Layer, Effect, Stream] = await Promise.all([
    original<{ readonly ConnectionDriver: Context.Key<unknown, FakeDriver> }>(),
    import("../../../test/fakeEnvironment"),
    import("effect/Layer"),
    import("effect/Effect"),
    import("effect/Stream"),
  ]);
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
                  return (
                    input: Parameters<
                      (typeof target)[typeof ORCHESTRATION_V2_WS_METHODS.getThreadProjection]
                    >[0],
                  ) => Effect.map(target[method](input), withDetachedProviderThread);
                if (method === ORCHESTRATION_V2_WS_METHODS.subscribeThread)
                  return (
                    input: Parameters<
                      (typeof target)[typeof ORCHESTRATION_V2_WS_METHODS.subscribeThread]
                    >[0],
                  ) =>
                    Stream.map(target[method](input), (event) =>
                      event.kind === "snapshot"
                        ? { ...event, projection: withDetachedProviderThread(event.projection) }
                        : event,
                    );
                if (method === ORCHESTRATION_V2_WS_METHODS.subscribeShell)
                  return (
                    input: Parameters<
                      (typeof target)[typeof ORCHESTRATION_V2_WS_METHODS.subscribeShell]
                    >[0],
                  ) =>
                    Stream.map(target[method](input), (event) => {
                      if (event.kind === "snapshot")
                        return {
                          ...event,
                          snapshot: {
                            ...event.snapshot,
                            threads: event.snapshot.threads.map(withDetachedProviderThreadShell),
                          },
                        };
                      if (event.kind === "thread.updated")
                        return { ...event, thread: withDetachedProviderThreadShell(event.thread) };
                      return event;
                    });
                return Reflect.get(target, method, receiver);
              },
            }),
            initialConfig: Effect.map(lease.session.initialConfig, disabledSavedProvider),
            subscribeServerConfig: (
              input: Parameters<typeof lease.session.subscribeServerConfig>[0],
            ) =>
              lease.session
                .subscribeServerConfig(input)
                .pipe(
                  Stream.map((event) =>
                    "config" in event
                      ? { ...event, config: disabledSavedProvider(event.config) }
                      : event,
                  ),
                ),
          },
        })),
    }),
  };
});
vi.mock(import("../../rpc/requestLatencyState"), async (original) => ({
  ...(await original()),
  trackRpcRequestSent: () => {},
}));

it("shows saved disabled A as unavailable, retains the draft, and sends to B only after explicit selection", async () => {
  using _pendingCleanup = {
    [Symbol.dispose]: () => {
      for (const entry of pendingSends.getSnapshot()) pendingSends.dismiss(entry.key);
    },
  };
  const previous = useComposerDraftStore.getState();
  using _draftCleanup = { [Symbol.dispose]: () => useComposerDraftStore.setState(previous, true) };
  const thread = threadWithHistory("disabled-saved-account", 1);
  using _environment = installFakeEnvironment([thread]);
  using delivery = deferFakeSends();
  __setClientSettingsForTests({
    ...DEFAULT_CLIENT_SETTINGS,
    onboardingCompletedAt: "2026-01-01T00:00:00.000Z",
  });
  await page.viewport(1400, 900);
  const router = getRouter(
    createMemoryHistory({ initialEntries: [`/${ENVIRONMENT_ID}/${thread.id}`] }),
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
  await expect.element(page.getByText(`Answer 0 for ${thread.id}.`)).toBeVisible();
  const target = scopeThreadRef(ENVIRONMENT_ID, thread.id);
  await vi.waitFor(() => expect(readThreadShell(target)).not.toBeNull());
  await expect.element(page.getByText("Provider unavailable", { exact: true })).toBeVisible();
  await userEvent.click(page.getByTestId("composer-editor"));
  await userEvent.type(page.getByTestId("composer-editor"), "Retain this account-sensitive draft");
  await userEvent.keyboard("{Enter}");
  expect(delivery.requests).toEqual([]);
  await expect
    .element(page.getByTestId("composer-editor"))
    .toHaveTextContent("Retain this account-sensitive draft");
  const picker = container.querySelector<HTMLElement>("[data-chat-provider-model-picker]");
  expect(picker).not.toBeNull();
  await page.elementLocator(picker!).click();
  const replacement = document.querySelector<HTMLButtonElement>(
    '[data-model-picker-provider="codex-replacement"] button',
  );
  expect(replacement).not.toBeNull();
  await page.elementLocator(replacement!).click();
  const replacementModel = page.getByRole("option", { name: /GPT-5.4/ });
  await expect.element(replacementModel).toBeEnabled();
  expect(delivery.requests).toEqual([]);
  await replacementModel.click();
  await vi.waitFor(() =>
    expect(useComposerDraftStore.getState().getComposerDraft(target)).toMatchObject({
      activeProvider: ProviderInstanceId.make("codex-replacement"),
      modelSelectionExplicit: true,
    }),
  );
  await userEvent.click(page.getByTestId("composer-editor"));
  await userEvent.keyboard("{Enter}");
  await vi.waitFor(() => expect(delivery.requests).toHaveLength(1));
  expect(delivery.requests[0]?.payload).toMatchObject({
    type: "message.dispatch",
    modelSelectionExplicit: true,
    modelSelection: { instanceId: "codex-replacement", model: "gpt-5.4" },
  });
  await act(async () => {
    await delivery.requests[0]!.commit();
    delivery.requests[0]!.acknowledge();
  });
});
