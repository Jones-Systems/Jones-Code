// The picker model list is virtualized and needs the app's layout styles.
import "~/index.css";
import "~/components/ChatView";
import { scopeProjectRef, scopeThreadRef } from "@t3tools/client-runtime/environment";
import { ORCHESTRATION_V2_WS_METHODS, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";
import { createMemoryHistory } from "@tanstack/react-router";
import type * as Context from "effect/Context";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { AppRoot } from "../../AppRoot";
import { DraftId, useComposerDraftStore } from "../../composerDraftStore";
import { __setClientSettingsForTests } from "../../hooks/useSettings";
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
  deferFakeSends,
  installFakeEnvironment,
  updateFakeServerConfig,
  threadWithHistory,
  type FakeDriver,
} from "../../../test/fakeEnvironment";
import { act, render } from "../../../test/render";
import { pendingSends } from "../pendingSends/store";

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
