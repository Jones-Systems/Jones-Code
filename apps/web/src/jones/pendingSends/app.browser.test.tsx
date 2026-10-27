import { scopeProjectRef, scopeThreadRef } from "@t3tools/client-runtime/environment";
import { runAtomCommand } from "@t3tools/client-runtime/state/runtime";
import {
  MessageId,
  ORCHESTRATION_V2_WS_METHODS,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import { DraftId, useComposerDraftStore } from "../../composerDraftStore";
import { createMemoryHistory } from "@tanstack/react-router";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";
import type * as Context from "effect/Context";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
// oxlint-disable-next-line t3code/require-disposable-render -- The uncreated-thread subscription holds async act open; _rootCleanup owns this synchronous mount and unmount.
import { createRoot } from "react-dom/client";
import "~/index.css";
import { AppRoot } from "~/AppRoot";
import { __setClientSettingsForTests } from "~/hooks/useSettings";
import { getRouter } from "~/router";
import {
  ENVIRONMENT_ID,
  PROJECT_ID,
  deferFakeSends,
  installFakeEnvironment,
  threadWithHistory,
  type FakeDriver,
} from "../../../test/fakeEnvironment";
import { act, render } from "../../../test/render";
import { pendingSends } from "./store";
import { pendingNewThread } from "./newThread";
import { sendPendingTurn } from "./send";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { readThreadShell } from "../../state/entities";
import { threadEnvironment } from "../../state/threads";

// The shared fake leaves unknown-thread subscriptions empty. Release this one only after
// commit so it reads the actual synthetic projection without inventing acceptance evidence.
const pendingTargetStream = vi.hoisted(() => {
  let release!: () => void;
  const ready = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { ready, release };
});

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
                if (method !== ORCHESTRATION_V2_WS_METHODS.subscribeThread)
                  return Reflect.get(target, method, receiver);
                return (
                  input: Parameters<
                    (typeof target)[typeof ORCHESTRATION_V2_WS_METHODS.subscribeThread]
                  >[0],
                ) =>
                  input.threadId === "pending-fanout-target"
                    ? Stream.unwrap(
                        Effect.map(
                          Effect.promise(() => pendingTargetStream.ready),
                          () => target[ORCHESTRATION_V2_WS_METHODS.subscribeThread](input),
                        ),
                      )
                    : target[ORCHESTRATION_V2_WS_METHODS.subscribeThread](input);
              },
            }),
          },
        })),
    }),
  };
});
vi.mock(import("~/rpc/requestLatencyState"), async (original) => ({
  ...(await original()),
  trackRpcRequestSent: () => {},
}));

it("opens an uncreated pending target, keeps Retry, then replaces it with one authoritative row", async () => {
  using _pendingCleanup = {
    [Symbol.dispose]: () => {
      for (const entry of pendingSends.getSnapshot()) pendingSends.dismiss(entry.key);
    },
  };
  const previousDrafts = useComposerDraftStore.getState();
  using _restoreDrafts = { [Symbol.dispose]: () => useComposerDraftStore.setState(previousDrafts) };
  const thread = threadWithHistory("send-feedback", 1);
  const other = threadWithHistory("send-feedback-other", 1);
  using _environment = installFakeEnvironment([thread, other]);
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
  document.body.append(container);
  const root = createRoot(container);
  using _rootCleanup = {
    [Symbol.dispose]: () => {
      root.unmount();
      container.remove();
    },
  };
  root.render(<AppRoot router={router} />);
  await expect.element(page.getByTestId("composer-editor")).toBeVisible();
  const targetThreadId = ThreadId.make("pending-fanout-target");
  const targetRef = scopeThreadRef(ENVIRONMENT_ID, targetThreadId);
  const messageId = MessageId.make("pending-fanout-message");
  const text = "Unique uncreated target message";
  const createdAt = "2026-03-17T19:12:28.000Z";
  const template = readThreadShell(scopeThreadRef(ENVIRONMENT_ID, thread.id))!;
  const input = {
    threadId: targetThreadId,
    message: { messageId, role: "user" as const, text, attachments: [] },
    modelSelection: template.modelSelection,
    runtimeMode: template.runtimeMode,
    interactionMode: template.interactionMode,
    createdAt,
    bootstrap: {
      createThread: {
        projectId: PROJECT_ID,
        title: "Uncreated fanout target",
        modelSelection: template.modelSelection,
        runtimeMode: template.runtimeMode,
        interactionMode: template.interactionMode,
        branch: null,
        worktreePath: null,
        createdAt,
      },
    },
  };
  const sending = sendPendingTurn({
    environmentId: ENVIRONMENT_ID,
    input,
    prepare: () =>
      runAtomCommand(
        appAtomRegistry,
        threadEnvironment.prepareTurn,
        {
          environmentId: ENVIRONMENT_ID,
          input,
        },
        { reportFailure: false },
      ),
    dispatch: (prepared) =>
      runAtomCommand(
        appAtomRegistry,
        threadEnvironment.dispatchPreparedTurn,
        {
          environmentId: ENVIRONMENT_ID,
          input: prepared,
        },
        { reportFailure: false },
      ),
  });
  pendingSends.attachThread(
    ENVIRONMENT_ID,
    targetThreadId,
    pendingNewThread(
      template,
      targetThreadId,
      "Uncreated fanout target",
      template.modelSelection,
      createdAt,
    ),
  );
  await vi.waitFor(() => expect(delivery.requests).toHaveLength(1));
  await router.navigate({
    to: "/$environmentId/$threadId",
    params: {
      environmentId: ENVIRONMENT_ID,
      threadId: targetThreadId,
    },
  });
  await expect.element(page.getByText(text, { exact: true })).toBeVisible();
  expect(router.state.location.pathname).toBe(`/${ENVIRONMENT_ID}/${targetThreadId}`);
  expect(readThreadShell(targetRef)).toBeNull();
  delivery.requests[0]!.reject();
  await sending;
  await expect
    .element(page.getByRole("button", { name: "Retry sending this message", exact: true }))
    .toBeVisible();
  await page.getByRole("button", { name: "Retry sending this message", exact: true }).click();
  await vi.waitFor(() => expect(delivery.requests).toHaveLength(2));
  expect(delivery.requests[1]!.payload).toEqual(delivery.requests[0]!.payload);
  await delivery.requests[1]!.commit();
  pendingTargetStream.release();
  delivery.requests[1]!.acknowledge();
  await vi.waitFor(() => expect(pendingSends.getSnapshot()).toEqual([]));
  await expect.element(page.getByText(text, { exact: true })).toBeVisible();
  expect(readThreadShell(targetRef)).not.toBeNull();
  expect([...container.querySelectorAll("p")].filter((p) => p.textContent === text)).toHaveLength(
    1,
  );
  expect(router.state.location.pathname).toBe(`/${ENVIRONMENT_ID}/${targetThreadId}`);
});

it("keeps a deferred send through navigation and reconciles commit before lost ACK", async () => {
  using _pendingCleanup = {
    [Symbol.dispose]: () => {
      for (const entry of pendingSends.getSnapshot()) pendingSends.dismiss(entry.key);
    },
  };
  const previousDrafts = useComposerDraftStore.getState();
  using _restoreDrafts = { [Symbol.dispose]: () => useComposerDraftStore.setState(previousDrafts) };
  const thread = threadWithHistory("send-feedback", 1);
  const other = threadWithHistory("send-feedback-other", 1);
  using _environment = installFakeEnvironment([thread, other]);
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
  await using _screen = await render(<AppRoot router={router} />, { router: false });
  await expect.element(page.getByTestId("composer-editor")).toBeVisible();
  await userEvent.click(page.getByTestId("composer-editor"));
  await userEvent.type(page.getByTestId("composer-editor"), "Unique deferred send");
  await userEvent.keyboard("{Enter}");
  await expect.element(page.getByText("Sending", { exact: true })).toBeVisible();
  await vi.waitFor(() => expect(delivery.requests).toHaveLength(1));
  const request = delivery.requests[0]!;
  const originalPayload = request.payload;
  await act(() =>
    router.navigate({
      to: "/$environmentId/$threadId",
      params: { environmentId: ENVIRONMENT_ID, threadId: other.id },
    }),
  );
  await act(() => request.commit());
  await act(() => request.loseAcknowledgement());
  await act(() =>
    router.navigate({
      to: "/$environmentId/$threadId",
      params: { environmentId: ENVIRONMENT_ID, threadId: thread.id },
    }),
  );
  await expect.element(page.getByText("Unique deferred send", { exact: true })).toBeVisible();
  expect(
    pendingSends.getSnapshot().some((entry) => entry.message.text === "Unique deferred send"),
  ).toBe(false);
  expect(delivery.requests).toHaveLength(1);
  expect(request.payload).toBe(originalPayload);
});

it("retries an uncertain send only on request with the identical payload", async () => {
  using _pendingCleanup = {
    [Symbol.dispose]: () => {
      for (const entry of pendingSends.getSnapshot()) pendingSends.dismiss(entry.key);
    },
  };
  const previousDrafts = useComposerDraftStore.getState();
  using _restoreDrafts = { [Symbol.dispose]: () => useComposerDraftStore.setState(previousDrafts) };
  const thread = threadWithHistory("send-feedback", 1);
  const other = threadWithHistory("send-feedback-other", 1);
  using _environment = installFakeEnvironment([thread, other]);
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
  await using _screen = await render(<AppRoot router={router} />, { router: false });
  await userEvent.click(page.getByTestId("composer-editor"));
  await userEvent.type(page.getByTestId("composer-editor"), "Unique uncertain send");
  await userEvent.keyboard("{Enter}");
  await vi.waitFor(() => expect(delivery.requests).toHaveLength(1));
  await act(() => delivery.requests[0]!.reject());
  await expect.element(page.getByText("Waiting for confirmation", { exact: true })).toBeVisible();
  expect(delivery.requests).toHaveLength(1);
  await page.getByRole("button", { name: "Retry sending this message", exact: true }).click();
  await vi.waitFor(() => expect(delivery.requests).toHaveLength(2));
  expect(delivery.requests[1]!.payload).toEqual(delivery.requests[0]!.payload);
  await act(async () => {
    await delivery.requests[1]!.commit();
    delivery.requests[1]!.acknowledge();
  });
  await vi.waitFor(() => expect(pendingSends.getSnapshot()).toEqual([]));
});

it("shows a first send locally and reconciles lost launch acknowledgement", async () => {
  using _pendingCleanup = {
    [Symbol.dispose]: () => {
      for (const entry of pendingSends.getSnapshot()) pendingSends.dismiss(entry.key);
    },
  };
  const previousDrafts = useComposerDraftStore.getState();
  using _restoreDrafts = { [Symbol.dispose]: () => useComposerDraftStore.setState(previousDrafts) };
  const thread = threadWithHistory("send-feedback", 1);
  const other = threadWithHistory("send-feedback-other", 1);
  using _environment = installFakeEnvironment([thread, other]);
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
  await using screen = await render(<AppRoot router={router} />, { router: false });
  const draftId = DraftId.make("feedback-new-draft");
  const newThreadId = ThreadId.make("feedback-new-thread");
  const drafts = useComposerDraftStore.getState();
  await act(() => {
    drafts.setLogicalProjectDraftThreadId(
      "feedback-new",
      scopeProjectRef(ENVIRONMENT_ID, PROJECT_ID),
      draftId,
      {
        threadId: newThreadId,
        envMode: "local",
        environmentSelection: "manual",
        runtimeMode: "full-access",
        interactionMode: "default",
      },
    );
    drafts.setModelSelection(draftId, {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5.4",
    });
  });
  await act(() => router.navigate({ to: "/draft/$draftId", params: { draftId } }));
  await userEvent.click(page.getByTestId("composer-editor"));
  await userEvent.type(page.getByTestId("composer-editor"), "Unique new-thread send");
  await userEvent.keyboard("{Enter}");
  await expect.element(page.getByText("Sending", { exact: true })).toBeVisible();
  await vi.waitFor(() => expect(delivery.requests).toHaveLength(1));
  expect(delivery.requests[0]!.method).toBe("orchestration.launchThread");
  expect(pendingSends.getSnapshot().filter((entry) => entry.threadId === newThreadId)).toHaveLength(
    1,
  );
  expect(screen.container.querySelectorAll('[data-testid="sidebar-draft-row"]')).toHaveLength(0);
  await act(async () => {
    await delivery.requests[0]!.commit();
    delivery.requests[0]!.loseAcknowledgement();
  });
  await expect
    .element(page.getByRole("paragraph").filter({ hasText: "Unique new-thread send" }))
    .toBeVisible();
  await vi.waitFor(() => expect(pendingSends.getSnapshot()).toEqual([]));
});
