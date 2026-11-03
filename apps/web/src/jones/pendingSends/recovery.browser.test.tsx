import { scopeProjectRef, scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  ORCHESTRATION_V2_WS_METHODS,
  OrchestrationV2DispatchCommandError,
  OrchestrationV2ThreadLaunchError,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2Command,
  type OrchestrationV2ThreadLaunchInput,
} from "@t3tools/contracts";
import { initialMessageCommandId } from "@t3tools/contracts/jones/deliveryEvidence";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";
import { createMemoryHistory } from "@tanstack/react-router";
import type * as Context from "effect/Context";
// oxlint-disable-next-line t3code/require-disposable-render -- The delayed target subscription holds async act open; _rootCleanup owns this synchronous mount and unmount.
import { createRoot } from "react-dom/client";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import "~/index.css";
import { AppRoot } from "../../AppRoot";
import { DraftId, useComposerDraftStore } from "../../composerDraftStore";
import { __setClientSettingsForTests } from "../../hooks/useSettings";
import { getRouter } from "../../router";
import { readThreadShell } from "../../state/entities";
import { act } from "../../../test/render";
import {
  ENVIRONMENT_ID,
  PROJECT_ID,
  deferFakeSends,
  installFakeEnvironment,
  threadWithHistory,
  type FakeDriver,
} from "../../../test/fakeEnvironment";
import { pendingSends } from "./store";

// Supply exact server error facts at the RPC boundary; commit/rejection remains
// controlled by the synthetic transport, never inferred from the displayed shell.
vi.mock("../../../../../packages/client-runtime/src/connection/driver.ts", async (original) => {
  const [actual, { fakeDriver }, Layer, Effect] = await Promise.all([
    original<{ readonly ConnectionDriver: Context.Key<unknown, FakeDriver> }>(),
    import("../../../test/fakeEnvironment"),
    import("effect/Layer"),
    import("effect/Effect"),
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
                if (method === ORCHESTRATION_V2_WS_METHODS.dispatchCommand)
                  return (input: OrchestrationV2Command) =>
                    Effect.mapError(
                      target[method](input),
                      () =>
                        new OrchestrationV2DispatchCommandError({
                          commandId: input.commandId,
                          commandType: input.type,
                          message: "Confirmed synthetic rejection",
                          deliveryEvidence: {
                            kind: "dispatch",
                            commandId: input.commandId,
                            commandType: input.type,
                            threadId: "threadId" in input ? (input.threadId ?? null) : null,
                            status: "rejected",
                          },
                        }),
                    );
                if (method === ORCHESTRATION_V2_WS_METHODS.launchThread)
                  return (input: OrchestrationV2ThreadLaunchInput) =>
                    Effect.mapError(target[method](input), (error) =>
                      input.threadId && input.initialMessage?.messageId
                        ? new OrchestrationV2ThreadLaunchError({
                            commandId: input.commandId,
                            projectId: input.projectId,
                            message: "Created; first message rejected",
                            deliveryEvidence: {
                              kind: "launch",
                              commandId: input.commandId,
                              projectId: input.projectId,
                              creation: { status: "accepted", threadId: input.threadId },
                              initialMessage: {
                                status: "rejected",
                                commandId: initialMessageCommandId(input.commandId),
                                messageId: input.initialMessage.messageId,
                              },
                            },
                          })
                        : error,
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

it.each([false, true])(
  "restores editable rejection and sends a deliberate new identity into the proven target; launch: %s",
  async (launch) => {
    using _pendingCleanup = {
      [Symbol.dispose]: () => {
        for (const entry of pendingSends.getSnapshot()) pendingSends.dismiss(entry.key);
      },
    };
    const previous = useComposerDraftStore.getState();
    using _draftCleanup = {
      [Symbol.dispose]: () => useComposerDraftStore.setState(previous, true),
    };
    const existing = threadWithHistory("confirmed-rejection", 1);
    using _environment = installFakeEnvironment([existing]);
    using delivery = deferFakeSends();
    __setClientSettingsForTests({
      ...DEFAULT_CLIENT_SETTINGS,
      onboardingCompletedAt: "2026-01-01T00:00:00.000Z",
    });
    await page.viewport(1400, 900);
    const router = getRouter(
      createMemoryHistory({ initialEntries: [`/${ENVIRONMENT_ID}/${existing.id}`] }),
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
    await expect.element(page.getByText(`Answer 0 for ${existing.id}.`)).toBeVisible();
    await expect.element(page.getByTestId("composer-editor")).toBeVisible();
    const targetId = launch ? ThreadId.make("rejected-initial-message") : existing.id;
    if (launch) {
      const draftId = DraftId.make("rejected-initial-draft");
      await act(() =>
        useComposerDraftStore
          .getState()
          .setLogicalProjectDraftThreadId(
            "rejected-initial",
            scopeProjectRef(ENVIRONMENT_ID, PROJECT_ID),
            draftId,
            {
              threadId: targetId,
              envMode: "local",
              environmentSelection: "manual",
              runtimeMode: "full-access",
              interactionMode: "default",
            },
          ),
      );
      await act(() =>
        useComposerDraftStore.getState().setModelSelection(draftId, {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5.4",
        }),
      );
      await router.navigate({ to: "/draft/$draftId", params: { draftId } });
      await vi.waitFor(() => expect(router.state.location.pathname).toBe(`/draft/${draftId}`));
    }
    await userEvent.click(page.getByTestId("composer-editor"));
    await userEvent.type(page.getByTestId("composer-editor"), "Rejected message to edit");
    await userEvent.keyboard("{Enter}");
    await vi.waitFor(() => expect(delivery.requests).toHaveLength(1));
    const original = delivery.requests[0]!;
    expect(original.method).toBe(
      launch ? "orchestration.launchThread" : "orchestration.dispatchCommand",
    );
    original.reject();
    await expect.element(page.getByText("Couldn't send", { exact: true })).toBeVisible();
    await expect
      .element(page.getByTestId("composer-editor"))
      .toHaveTextContent("Rejected message to edit");
    const originalMessageId =
      "messageId" in original.payload
        ? original.payload.messageId
        : "initialMessage" in original.payload
          ? original.payload.initialMessage?.messageId
          : undefined;
    if (!originalMessageId) throw new Error("Expected captured message identity");
    expect(pendingSends.get(ENVIRONMENT_ID, targetId, originalMessageId)).toMatchObject({
      delivery: "failed",
      working: false,
      retry: undefined,
    });
    expect(
      useComposerDraftStore.getState().getComposerDraft(scopeThreadRef(ENVIRONMENT_ID, targetId))
        ?.prompt,
    ).toBe("Rejected message to edit");
    if (launch) {
      await vi.waitFor(() =>
        expect(router.state.location.pathname).toBe(`/${ENVIRONMENT_ID}/${targetId}`),
      );
      expect(readThreadShell(scopeThreadRef(ENVIRONMENT_ID, targetId))).toBeNull();
    }
    await userEvent.click(page.getByTestId("composer-editor"));
    await userEvent.keyboard("{Enter}");
    await vi.waitFor(() => expect(delivery.requests).toHaveLength(2));
    const corrected = delivery.requests[1]!;
    expect(corrected.method).toBe("orchestration.dispatchCommand");
    expect(corrected.payload.threadId).toBe(targetId);
    expect(corrected.payload.commandId).not.toBe(original.payload.commandId);
    if ("messageId" in corrected.payload) {
      expect(corrected.payload.messageId).not.toBe(originalMessageId);
    }
    if (!("messageId" in corrected.payload)) throw new Error("Expected corrected message identity");
    const correctedMessageId = corrected.payload.messageId;
    await corrected.commit();
    corrected.acknowledge();
    await vi.waitFor(() => {
      const pending = pendingSends.get(ENVIRONMENT_ID, targetId, correctedMessageId);
      expect(pending === undefined || pending.delivery === "accepted").toBe(true);
    });
    expect(
      delivery.requests.filter((request) => request.method === "orchestration.launchThread"),
    ).toHaveLength(launch ? 1 : 0);
  },
);

it.each(["failed", "superseded"] as const)(
  "retains the recovered launch draft when proven-target navigation is %s",
  async (outcome) => {
    using _pendingCleanup = {
      [Symbol.dispose]: () => {
        for (const entry of pendingSends.getSnapshot()) pendingSends.dismiss(entry.key);
      },
    };
    const previous = useComposerDraftStore.getState();
    using _draftCleanup = {
      [Symbol.dispose]: () => useComposerDraftStore.setState(previous, true),
    };
    using _environment = installFakeEnvironment([threadWithHistory("recovery-navigation", 1)]);
    using delivery = deferFakeSends();
    __setClientSettingsForTests({
      ...DEFAULT_CLIENT_SETTINGS,
      onboardingCompletedAt: "2026-01-01T00:00:00.000Z",
    });
    const draftId = DraftId.make(`recovery-navigation-${outcome}`);
    const targetId = ThreadId.make(`recovery-navigation-target-${outcome}`);
    const targetRef = scopeThreadRef(ENVIRONMENT_ID, targetId);
    useComposerDraftStore.getState().setLogicalProjectDraftThreadId(
      `recovery-navigation-${outcome}`,
      scopeProjectRef(ENVIRONMENT_ID, PROJECT_ID),
      draftId,
      {
        threadId: targetId,
        envMode: "local",
        environmentSelection: "manual",
        runtimeMode: "full-access",
        interactionMode: "default",
      },
    );
    useComposerDraftStore.getState().setModelSelection(draftId, {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5.4",
    });
    await page.viewport(1400, 900);
    const router = getRouter(
      createMemoryHistory({ initialEntries: [`/draft/${draftId}`] }),
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
    await expect.element(page.getByTestId("composer-editor")).toBeVisible();
    await userEvent.click(page.getByTestId("composer-editor"));
    await userEvent.type(page.getByTestId("composer-editor"), "Keep the rejected draft");
    await userEvent.keyboard("{Enter}");
    await vi.waitFor(() => expect(delivery.requests).toHaveLength(1));
    const original = delivery.requests[0]!;
    expect(original.method).toBe("orchestration.launchThread");
    let resolveNavigation!: () => void;
    let rejectNavigation!: (error: Error) => void;
    const navigationResult = new Promise<void>((resolve, reject) => {
      resolveNavigation = resolve;
      rejectNavigation = reject;
    });
    // Hold only the recovery navigation at the router boundary; later user navigation is real.
    using navigation = vi.spyOn(router, "navigate").mockReturnValueOnce(navigationResult);
    using _navigationCleanup = {
      [Symbol.dispose]: () => {
        if (navigation.mock.calls.length > 0) {
          rejectNavigation(new Error("Disposed recovery navigation"));
        }
      },
    };
    original.reject();
    await vi.waitFor(() =>
      expect(navigation).toHaveBeenCalledWith(
        expect.objectContaining({
          to: "/$environmentId/$threadId",
          params: { environmentId: ENVIRONMENT_ID, threadId: targetId },
        }),
      ),
    );
    await expect.element(page.getByText("Couldn't send", { exact: true })).toBeVisible();
    await expect
      .element(page.getByTestId("composer-editor"))
      .toHaveTextContent("Keep the rejected draft");
    expect(router.state.location.pathname).toBe(`/draft/${draftId}`);
    const recoveredSession = useComposerDraftStore.getState().getDraftSession(draftId);
    expect(recoveredSession).not.toBeNull();

    let newerDraftPath: string | undefined;
    if (outcome === "failed") {
      rejectNavigation(new Error("Synthetic navigation failure"));
      await expect(navigationResult).rejects.toThrow("Synthetic navigation failure");
    } else {
      await userEvent.click(page.getByRole("button", { name: "New thread", exact: true }));
      await vi.waitFor(() => {
        expect(router.state.location.pathname).toMatch(/^\/draft\//);
        expect(router.state.location.pathname).not.toBe(`/draft/${draftId}`);
      });
      newerDraftPath = router.state.location.pathname;
      await expect
        .element(page.getByTestId("composer-editor"))
        .not.toHaveTextContent("Keep the rejected draft");
      await userEvent.click(page.getByTestId("composer-editor"));
      await userEvent.type(page.getByTestId("composer-editor"), "Newer composer stays mine");
      resolveNavigation();
      await navigationResult;
    }

    expect(useComposerDraftStore.getState().getDraftSession(draftId)).toBe(recoveredSession);
    expect(useComposerDraftStore.getState().getComposerDraft(draftId)?.prompt).toBe(
      "Keep the rejected draft",
    );
    expect(router.state.location.pathname).toBe(newerDraftPath ?? `/draft/${draftId}`);
    await expect
      .element(page.getByTestId("composer-editor"))
      .toHaveTextContent(
        outcome === "failed" ? "Keep the rejected draft" : "Newer composer stays mine",
      );
    expect(readThreadShell(targetRef)).toBeNull();
    expect(delivery.requests).toHaveLength(1);
    expect(pendingSends.getSnapshot()).toEqual([
      expect.objectContaining({
        threadId: targetId,
        delivery: "failed",
        working: false,
        retry: undefined,
      }),
    ]);
  },
);
