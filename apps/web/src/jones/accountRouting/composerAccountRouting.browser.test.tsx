// The picker model list is virtualized and needs the app's layout styles.
import "~/index.css";
import "~/components/ChatView";
import { scopeProjectRef, scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  ORCHESTRATION_V2_WS_METHODS,
  OrchestrationV2ThreadLaunchError,
  ProviderInstanceId,
  ThreadId,
  WS_METHODS,
} from "@t3tools/contracts";
import { initialMessageCommandId } from "@t3tools/contracts/jones/deliveryEvidence";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";
import { createMemoryHistory } from "@tanstack/react-router";
import type * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { AppRoot } from "../../AppRoot";
import {
  COMPOSER_DRAFT_STORAGE_KEY,
  DraftId,
  useComposerDraftStore,
} from "../../composerDraftStore";
import { __setClientSettingsForTests } from "../../hooks/useSettings";
import { isMacPlatform } from "../../lib/utils";
import { getRouter } from "../../router";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { readThreadShell } from "../../state/entities";
import { serverEnvironment } from "../../state/server";
import { ensureWorktreeLayoutApproved } from "../../worktreeLayoutApproval";
import {
  CLAUDE_PROVIDER,
  CODEX_PROVIDER,
  ENVIRONMENT_ID,
  PROJECT_ID,
  answerRpc,
  deferFakeSends,
  installFakeEnvironment,
  refreshFakeShell,
  updateFakeServerConfig,
  threadWithHistory,
  type FakeDriver,
} from "../../../test/fakeEnvironment";
import { act, render } from "../../../test/render";
import { pendingSends } from "../pendingSends/store";
import { AUTO_ACCOUNT_ROUTING } from "./composerAccountRouting";

vi.mock("../../../../../packages/client-runtime/src/connection/driver.ts", async (original) => {
  const [actual, { fakeDriver }, Layer] = await Promise.all([
    original<{ readonly ConnectionDriver: Context.Key<unknown, FakeDriver> }>(),
    import("../../../test/fakeEnvironment"),
    import("effect/Layer"),
  ]);
  return { ...actual, layer: Layer.succeed(actual.ConnectionDriver, fakeDriver) };
});

vi.mock(import("~/rpc/requestLatencyState"), async (original) => ({
  ...(await original()),
  trackRpcRequestSent: () => {},
}));

vi.mock(import("../../worktreeLayoutApproval"), async (original) => {
  const actual = await original();
  return { ...actual, ensureWorktreeLayoutApproved: vi.fn(actual.ensureWorktreeLayoutApproved) };
});

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

it("opts a fresh draft into account routing, keeps edits, and displays the server's concrete pick", async () => {
  const previousDrafts = useComposerDraftStore.getState();
  using _restoreDrafts = {
    [Symbol.dispose]: () => useComposerDraftStore.setState(previousDrafts, true),
  };
  using _pendingCleanup = {
    [Symbol.dispose]: () => {
      for (const entry of pendingSends.getSnapshot()) pendingSends.dismiss(entry.key);
    },
  };
  const bound = threadWithHistory("account-routing-bound", 1);
  const secondCodex = ProviderInstanceId.make("codex-second");
  const secondClaude = ProviderInstanceId.make("claude-second");
  using _environment = installFakeEnvironment([bound], {
    providers: [
      CODEX_PROVIDER,
      CLAUDE_PROVIDER,
      { ...CODEX_PROVIDER, instanceId: secondCodex, displayName: "Codex Second" },
      { ...CLAUDE_PROVIDER, instanceId: secondClaude, displayName: "Claude Second" },
    ],
    settings: {
      providerInstances: {
        [secondCodex]: { driver: "codex", enabled: true, config: {} },
        [secondClaude]: { driver: "claudeAgent", enabled: true, config: {} },
      },
    },
  });
  primaryFixtureAdmission.admit();
  using delivery = deferFakeSends();
  __setClientSettingsForTests({
    ...DEFAULT_CLIENT_SETTINGS,
    onboardingCompletedAt: "2026-01-01T00:00:00.000Z",
  });
  await page.viewport(1400, 900);

  const router = getRouter(
    createMemoryHistory({ initialEntries: [`/${ENVIRONMENT_ID}/${bound.id}`] }),
  );
  await router.load();
  await using _screen = await render(<AppRoot router={router} />, { router: false });
  await expect.element(page.getByText("Answer 0 for account-routing-bound.")).toBeVisible();

  const draftId = DraftId.make("account-routing-draft");
  const threadId = ThreadId.make("account-routing-thread");
  const threadRef = scopeThreadRef(ENVIRONMENT_ID, threadId);
  const draft = () => useComposerDraftStore.getState().getComposerDraft(draftId);
  const initialModel = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" };
  await act(() => {
    previousDrafts.setLogicalProjectDraftThreadId(
      "account-routing",
      scopeProjectRef(ENVIRONMENT_ID, PROJECT_ID),
      draftId,
      {
        threadId,
        envMode: "local",
        environmentSelection: "manual",
        runtimeMode: "full-access",
        interactionMode: "default",
      },
    );
    previousDrafts.setModelSelection(draftId, initialModel);
  });
  await act(() => router.navigate({ to: "/draft/$draftId", params: { draftId } }));
  const picker = () => {
    const trigger = document.querySelector<HTMLElement>("[data-chat-provider-model-picker]");
    if (!trigger) throw new Error("Composer model picker is unavailable");
    return page.elementLocator(trigger);
  };
  const codexAuto = page.getByRole("button", { name: "Codex Auto", exact: true });
  const claudeAuto = page.getByRole("button", { name: "Claude Auto", exact: true });
  const updateCapabilities = (enabled: boolean, claudeAvailable = true) =>
    act(() =>
      updateFakeServerConfig((config) => ({
        ...config,
        environment: {
          ...config.environment,
          capabilities: { ...config.environment.capabilities, accountAutoRouting: enabled },
        },
        providers: config.providers.map((provider) =>
          provider.instanceId === secondClaude
            ? { ...provider, installed: claudeAvailable }
            : provider,
        ),
      })),
    );
  await picker().click();
  await expect.element(page.getByPlaceholder("Search models...")).toBeVisible();
  await expect.element(codexAuto).not.toBeInTheDocument();
  await expect.element(claudeAuto).not.toBeInTheDocument();

  await updateCapabilities(true, false);
  await expect.element(codexAuto).toBeVisible();
  await expect.element(claudeAuto).not.toBeInTheDocument();
  await updateCapabilities(true);
  await expect.element(claudeAuto).toBeVisible();
  await claudeAuto.click();
  await expect.element(picker()).toHaveAccessibleName(/Claude Auto/);
  expect(draft()).toMatchObject({
    activeProvider: "claudeAgent",
    accountRouting: { mode: "auto" },
  });

  await codexAuto.click();
  await page.getByRole("option", { name: /GPT-5\.5/ }).click();
  await expect.element(picker()).toHaveAccessibleName(/Codex Auto.*GPT-5\.5/);
  expect(draft()).toMatchObject({
    activeProvider: "codex",
    modelSelectionByProvider: { codex: { instanceId: "codex", model: "gpt-5.5" } },
    accountRouting: { mode: "auto" },
  });
  expect(useComposerDraftStore.getState().stickyActiveProvider).toBe(
    previousDrafts.stickyActiveProvider,
  );

  // Picking the same model on a concrete account's tab is an explicit manual choice.
  await page.getByRole("button", { name: "Codex", exact: true }).click();
  await page.getByRole("option", { name: /GPT-5\.5/ }).click();
  expect(draft()?.accountRouting).toBeUndefined();
  await codexAuto.click();
  await userEvent.keyboard("{Escape}");

  const editor = page.getByTestId("composer-editor");
  await userEvent.click(editor);
  await userEvent.type(editor, "Send with automatic account selection");
  await updateCapabilities(false);
  const capability = () =>
    appAtomRegistry.get(serverEnvironment.configValueAtom(ENVIRONMENT_ID))?.environment.capabilities
      .accountAutoRouting;
  await vi.waitFor(() => expect(capability()).toBe(false));
  await userEvent.keyboard("{Enter}");
  await expect
    .element(page.getByText(/Automatic account selection is unavailable here/))
    .toBeVisible();
  expect(delivery.requests).toHaveLength(0);
  expect(draft()).toMatchObject({
    prompt: "Send with automatic account selection",
    accountRouting: { mode: "auto" },
  });

  await updateCapabilities(true);
  await vi.waitFor(() => expect(capability()).toBe(true));
  await act(() =>
    previousDrafts.setDraftThreadContext(draftId, {
      envMode: "worktree",
      branch: "main",
      workspaceExplicit: true,
    }),
  );
  await userEvent.click(editor);
  await userEvent.keyboard("{Enter}");
  await expect
    .element(page.getByText(/Automatic account selection needs an existing workspace/))
    .toBeVisible();
  expect(ensureWorktreeLayoutApproved).not.toHaveBeenCalled();
  expect(delivery.requests).toHaveLength(0);
  expect(draft()).toMatchObject({
    prompt: "Send with automatic account selection",
    accountRouting: { mode: "auto" },
  });

  await act(() =>
    previousDrafts.setDraftThreadContext(draftId, { envMode: "local", branch: null }),
  );
  await userEvent.click(editor);
  await userEvent.keyboard("{Enter}");
  await vi.waitFor(() => expect(delivery.requests).toHaveLength(1));
  const request = delivery.requests[0]!;
  expect(request.method).toBe(ORCHESTRATION_V2_WS_METHODS.launchThread);
  expect(request.payload).toMatchObject({
    threadId,
    modelSelection: { instanceId: "codex", model: "gpt-5.5" },
    accountRouting: { mode: "auto" },
    workspaceStrategy: { type: "root" },
    initialMessage: { text: "Send with automatic account selection" },
  });

  await act(() => previousDrafts.setPrompt(draftId, "Follow-up typed while sending"));
  const acceptedModel = { instanceId: secondCodex, model: "gpt-5.5" };
  await act(async () => {
    await request.commit(acceptedModel);
    request.acknowledge();
  });
  await vi.waitFor(() => {
    expect(readThreadShell(threadRef)?.modelSelection).toEqual(acceptedModel);
    expect(useComposerDraftStore.getState().getComposerDraft(threadRef)).toMatchObject({
      prompt: "Follow-up typed while sending",
      activeProvider: null,
    });
    expect(
      useComposerDraftStore.getState().getComposerDraft(threadRef)?.accountRouting,
    ).toBeUndefined();
  });
  await expect.element(picker()).toHaveAccessibleName(/GPT-5\.5/);
  await expect.element(picker()).not.toHaveAccessibleName(/Codex Auto/);
  await picker().click();
  await expect.element(page.getByPlaceholder("Search models...")).toBeVisible();
  await expect.element(codexAuto).not.toBeInTheDocument();
  await expect.element(claudeAuto).not.toBeInTheDocument();
});

const recoveryOutcomes = ["accepted", "refused", "storage failure"] as const;

it.each(recoveryOutcomes)("recovers a background Auto launch with %s", async (outcome) => {
  const initialDrafts = useComposerDraftStore.getState();
  const initialStorage = localStorage.getItem(COMPOSER_DRAFT_STORAGE_KEY);
  using _cleanup = {
    [Symbol.dispose]: () => {
      for (const entry of pendingSends.getSnapshot()) pendingSends.dismiss(entry.key);
      useComposerDraftStore.setState(initialDrafts, true);
      window.dispatchEvent(new Event("beforeunload"));
      if (initialStorage === null) localStorage.removeItem(COMPOSER_DRAFT_STORAGE_KEY);
      else localStorage.setItem(COMPOSER_DRAFT_STORAGE_KEY, initialStorage);
    },
  };
  const bound = threadWithHistory("account-routing-reload-bound", 1);
  const secondCodex = ProviderInstanceId.make("codex-second");
  using _environment = installFakeEnvironment([bound], {
    providers: [
      CODEX_PROVIDER,
      { ...CODEX_PROVIDER, instanceId: secondCodex, displayName: "Codex Second" },
    ],
    settings: {
      providerInstances: { [secondCodex]: { driver: "codex", enabled: true, config: {} } },
    },
  });
  primaryFixtureAdmission.admit();
  using delivery = deferFakeSends();
  await updateFakeServerConfig((config) => ({
    ...config,
    environment: {
      ...config.environment,
      capabilities: {
        ...config.environment.capabilities,
        accountAutoRouting: true,
        inlineMessageContext: true,
      },
    },
  }));
  await refreshFakeShell();
  __setClientSettingsForTests({
    ...DEFAULT_CLIENT_SETTINGS,
    onboardingCompletedAt: "2026-01-01T00:00:00.000Z",
  });
  await page.viewport(1400, 900);

  const draftId = DraftId.make("account-routing-reload-draft");
  const threadId = ThreadId.make("account-routing-reload-thread");
  const threadRef = scopeThreadRef(ENVIRONMENT_ID, threadId);
  const prompt = "Resume this background Auto launch";
  const nextPrompt = "Independent next draft";
  const terminalContext = {
    id: "auto-reload-terminal",
    threadId,
    createdAt: "2026-03-17T19:12:28.000Z",
    terminalId: "synthetic-terminal",
    terminalLabel: "Synthetic terminal",
    lineStart: 4,
    lineEnd: 4,
    text: "Rich context must survive the dropped reply.",
  };
  const editor = page.getByTestId("composer-editor");
  const recoveryRow = page.getByRole("button", { name: /Auto launch pending/ });
  const retry = page.getByRole("button", { name: "Retry sending this message" });
  const router = getRouter(
    createMemoryHistory({ initialEntries: [`/${ENVIRONMENT_ID}/${bound.id}`] }),
  );
  await router.load();
  await using screen = await render(<AppRoot router={router} />, { router: false });
  await expect.element(page.getByText("Answer 0 for account-routing-reload-bound.")).toBeVisible();
  await vi.waitFor(() =>
    expect(
      appAtomRegistry.get(serverEnvironment.configValueAtom(ENVIRONMENT_ID))?.environment
        .capabilities.accountAutoRouting,
    ).toBe(true),
  );
  await act(() => {
    initialDrafts.setLogicalProjectDraftThreadId(
      "account-routing-reload",
      scopeProjectRef(ENVIRONMENT_ID, PROJECT_ID),
      draftId,
      {
        threadId,
        envMode: "local",
        environmentSelection: "manual",
        runtimeMode: "full-access",
        interactionMode: "default",
      },
    );
    initialDrafts.setModelSelection(
      draftId,
      { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.5" },
      { accountRouting: AUTO_ACCOUNT_ROUTING, explicit: true },
    );
  });
  await act(() => router.navigate({ to: "/draft/$draftId", params: { draftId } }));
  await editor.click();
  await userEvent.type(editor, prompt);
  if (outcome !== "accepted") {
    await act(() => initialDrafts.setTerminalContexts(draftId, [terminalContext]));
  }
  using _verifyUpload =
    outcome === "refused"
      ? answerRpc(WS_METHODS.assetsCreateUrl, () =>
          Effect.succeed({ relativeUrl: "/upload", expiresAt: 0 }),
        )
      : undefined;
  if (outcome === "refused") {
    using _uploadUrl = answerRpc(WS_METHODS.attachmentsCreateUploadUrl, () =>
      Effect.succeed({ attachmentId: "auto-reload-upload", relativeUrl: "/upload", expiresAt: 0 }),
    );
    const status = vi.spyOn(XMLHttpRequest.prototype, "status", "get").mockReturnValue(200);
    const upload = vi.spyOn(XMLHttpRequest.prototype, "send").mockImplementation(function (
      this: XMLHttpRequest,
    ) {
      queueMicrotask(() => this.dispatchEvent(new ProgressEvent("load")));
    });
    try {
      const png = Uint8Array.from(
        atob(
          "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=",
        ),
        (byte) => byte.charCodeAt(0),
      );
      const clipboardData = new DataTransfer();
      clipboardData.items.add(new File([png], "auto-reload.png", { type: "image/png" }));
      editor
        .element()
        .dispatchEvent(
          new ClipboardEvent("paste", { clipboardData, bubbles: true, cancelable: true }),
        );
      await expect
        .element(page.getByRole("button", { name: "Preview auto-reload.png" }))
        .toBeEnabled();
      await vi.waitFor(() => expect(upload).toHaveBeenCalledTimes(1));
      await vi.waitFor(() =>
        expect(
          useComposerDraftStore.getState().getComposerDraft(draftId)?.persistedAttachments,
        ).toMatchObject([{ name: "auto-reload.png" }]),
      );
    } finally {
      upload.mockRestore();
      status.mockRestore();
    }
  }

  const mod = isMacPlatform(navigator.platform) ? "Meta" : "Control";
  if (outcome === "storage failure") {
    const add = IDBObjectStore.prototype.add;
    let beforeCommit: { promotedTo: unknown; pending: unknown; requests: number } | undefined;
    const abortWrite = vi.spyOn(IDBObjectStore.prototype, "add").mockImplementation(function (
      this: IDBObjectStore,
      value: unknown,
      key?: IDBValidKey,
    ) {
      const request = add.call(this, value, key);
      if (this.transaction.db.name === "jones:auto-launch-recovery") {
        request.addEventListener("success", () => {
          const session = useComposerDraftStore.getState().getDraftSession(draftId);
          beforeCommit = {
            promotedTo: session?.promotedTo,
            pending: session?.pendingAutoLaunch,
            requests: delivery.requests.length,
          };
          window.dispatchEvent(new Event("beforeunload"));
          this.transaction.abort();
        });
      }
      return request;
    });
    try {
      await userEvent.keyboard(`{${mod}>}{Enter}{/${mod}}`);
      await expect
        .element(page.getByText("Could not save Auto launch recovery. The message was not sent."))
        .toBeVisible();
      expect(beforeCommit).toEqual({ promotedTo: null, pending: undefined, requests: 0 });
      expect(delivery.requests).toHaveLength(0);
      expect(router.state.location.pathname).toBe(`/draft/${draftId}`);
      await useComposerDraftStore.persist.rehydrate();
      expect(useComposerDraftStore.getState().getDraftSession(draftId)).toMatchObject({
        promotedTo: null,
      });
      expect(useComposerDraftStore.getState().getComposerDraft(draftId)).toMatchObject({
        prompt: expect.stringContaining(prompt),
        terminalContexts: [terminalContext],
      });
      await expect.element(recoveryRow).not.toBeInTheDocument();
    } finally {
      abortWrite.mockRestore();
    }
    return;
  }

  await userEvent.keyboard(`{${mod}>}{Enter}{/${mod}}`);
  await vi.waitFor(() => expect(delivery.requests).toHaveLength(1));
  const first = delivery.requests[0]!;
  expect(first.method).toBe(ORCHESTRATION_V2_WS_METHODS.launchThread);
  expect(first.payload).toMatchObject({ accountRouting: { mode: "auto" }, threadId });
  let refusal: OrchestrationV2ThreadLaunchError | undefined;
  if (outcome === "refused") {
    expect(first.payload).toMatchObject({
      initialMessage: {
        attachments: [expect.objectContaining({ type: "image", id: "auto-reload-upload" })],
        context: {
          records: expect.arrayContaining([
            expect.objectContaining({ kind: "terminal", text: terminalContext.text }),
          ]),
        },
      },
    });
    if (!("initialMessage" in first.payload)) throw new Error("Expected a launch request");
    const { commandId, projectId, initialMessage } = first.payload;
    const messageId = initialMessage?.messageId;
    if (!messageId) throw new Error("Expected the original message ID");
    refusal = new OrchestrationV2ThreadLaunchError({
      commandId,
      projectId,
      message: "Original Auto launch refusal",
      deliveryEvidence: {
        kind: "launch",
        commandId,
        projectId,
        creation: { status: "rejected" },
        initialMessage: {
          status: "rejected",
          commandId: initialMessageCommandId(commandId),
          messageId,
        },
      },
    });
  }
  expect(localStorage.getItem(COMPOSER_DRAFT_STORAGE_KEY)).toContain(first.payload.commandId);
  await vi.waitFor(() => {
    expect(router.state.location.pathname).toMatch(/^\/draft\//);
    expect(router.state.location.pathname).not.toBe(`/draft/${draftId}`);
  });
  const nextPath = router.state.location.pathname;
  const nextDraftId = DraftId.make(nextPath.split("/").at(-1)!);
  await editor.click();
  await userEvent.type(editor, nextPrompt);
  await act(() => first.loseAcknowledgement());
  expect(readThreadShell(threadRef)).toBeNull();
  await expect.element(recoveryRow).toBeVisible();
  await expect.element(recoveryRow).not.toHaveAccessibleName(/Codex/);
  await recoveryRow.click();
  await vi.waitFor(() => expect(router.state.location.pathname).toBe(`/draft/${draftId}`));
  await expect.element(retry).toBeVisible();

  await screen.unmount();
  window.dispatchEvent(new Event("beforeunload"));
  for (const entry of pendingSends.getSnapshot()) pendingSends.dismiss(entry.key);
  await useComposerDraftStore.persist.rehydrate();
  const reloadedRouter = getRouter(createMemoryHistory({ initialEntries: [nextPath] }));
  await reloadedRouter.load();
  await using _reloaded = await render(<AppRoot router={reloadedRouter} />, { router: false });
  await expect.element(editor).toHaveTextContent(nextPrompt);
  await recoveryRow.click();
  await expect.element(page.getByText("Waiting for confirmation", { exact: true })).toBeVisible();
  await expect.element(retry).toBeVisible();
  expect(delivery.requests).toHaveLength(1);

  await retry.click();
  await vi.waitFor(() => expect(delivery.requests).toHaveLength(2));
  const retried = delivery.requests[1]!;
  expect(retried.method).toBe(first.method);
  expect(retried.payload).toEqual(first.payload);
  if (refusal) {
    await act(() => retried.reject(refusal));
    await vi.waitFor(() => {
      expect(useComposerDraftStore.getState().getComposerDraft(draftId)).toMatchObject({
        prompt: expect.stringContaining(prompt),
        terminalContexts: [terminalContext],
        images: [expect.objectContaining({ name: "auto-reload.png" })],
      });
      expect(useComposerDraftStore.getState().getDraftSession(draftId)?.pendingAutoLaunch).toBe(
        undefined,
      );
    });
    expect(readThreadShell(threadRef)).toBeNull();
  } else {
    const acceptedModel = { instanceId: secondCodex, model: "gpt-5.5" };
    await act(async () => {
      await retried.commit(acceptedModel);
      retried.acknowledge();
    });
    await vi.waitFor(() => {
      expect(readThreadShell(threadRef)?.modelSelection).toEqual(acceptedModel);
      expect(useComposerDraftStore.getState().getDraftSession(draftId)).toBeNull();
    });
  }
  await expect.element(recoveryRow).not.toBeInTheDocument();
  expect(useComposerDraftStore.getState().getComposerDraft(nextDraftId)?.prompt).toBe(nextPrompt);
  expect(localStorage.getItem(COMPOSER_DRAFT_STORAGE_KEY)).not.toContain(first.payload.commandId);
});
