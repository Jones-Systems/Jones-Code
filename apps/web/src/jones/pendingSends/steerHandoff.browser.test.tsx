import { createMemoryHistory } from "@tanstack/react-router";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";
import type * as Context from "effect/Context";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import "~/index.css";
import { AppRoot } from "~/AppRoot";
import { __setClientSettingsForTests } from "~/hooks/useSettings";
import { getRouter } from "~/router";
import { useComposerDraftStore } from "../../composerDraftStore";
import {
  ENVIRONMENT_ID,
  deferFakeSends,
  emitThreadEvent,
  installFakeEnvironment,
  runningRun,
  threadWithHistory,
  userItem,
  type FakeDriver,
} from "../../../test/fakeEnvironment";
import { act, render } from "../../../test/render";
import { pendingSends } from "./store";

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

it("hands a steer queued by the server to the queue and then its original timeline message", async () => {
  using _pendingCleanup = {
    [Symbol.dispose]: () => {
      for (const entry of pendingSends.getSnapshot()) pendingSends.dismiss(entry.key);
    },
  };
  const previousDrafts = useComposerDraftStore.getState();
  using _restoreDrafts = { [Symbol.dispose]: () => useComposerDraftStore.setState(previousDrafts) };
  const history = threadWithHistory("steer-fallback", 1);
  const active = runningRun(history.id, "active-steer", 2, "active-message");
  using _environment = installFakeEnvironment([{ ...history, runs: [...history.runs, active] }]);
  using delivery = deferFakeSends();
  __setClientSettingsForTests({
    ...DEFAULT_CLIENT_SETTINGS,
    onboardingCompletedAt: "2026-01-01T00:00:00.000Z",
  });
  await page.viewport(1400, 900);
  const router = getRouter(
    createMemoryHistory({ initialEntries: [`/${ENVIRONMENT_ID}/${history.id}`] }),
  );
  await router.load();
  await using _screen = await render(<AppRoot router={router} />, { router: false });
  const text = "Follow-up queued while the provider starts";
  await userEvent.click(page.getByTestId("composer-editor"));
  await userEvent.type(page.getByTestId("composer-editor"), text);
  await userEvent.keyboard("{Enter}");
  await vi.waitFor(() => expect(delivery.requests).toHaveLength(1));
  const request = delivery.requests[0]!;
  if (!("type" in request.payload) || request.payload.type !== "message.dispatch")
    throw new Error("Expected message dispatch");
  expect(request.payload.deliveryIntent).toBe("steer");
  const { messageId } = request.payload;
  const queued = {
    ...runningRun(history.id, "deferred-steer", 3, messageId),
    status: "queued" as const,
    startedAt: null,
    queuePosition: 1,
    queuedToolBoundaryEligible: true,
  };
  const message = {
    id: messageId,
    threadId: history.id,
    runId: queued.id,
    nodeId: null,
    role: "user" as const,
    text,
    attachments: [],
    streaming: false,
    createdBy: "user" as const,
    creationSource: "web" as const,
    createdAt: queued.requestedAt,
    updatedAt: queued.requestedAt,
  };
  await act(async () => {
    await emitThreadEvent(history.id, { type: "run.updated", payload: queued });
    await emitThreadEvent(history.id, { type: "message.updated", payload: message });
  });
  await vi.waitFor(() => expect(pendingSends.getSnapshot()).toEqual([]));
  await expect.element(page.getByText(text, { exact: true })).toBeVisible();
  expect(page.getByText(text, { exact: true }).elements()).toHaveLength(1);
  request.loseAcknowledgement();
  await act(async () => {
    await emitThreadEvent(history.id, {
      type: "run.updated",
      payload: {
        ...queued,
        status: "cancelled",
        completedAt: queued.requestedAt,
      },
    });
    await emitThreadEvent(history.id, {
      type: "message.updated",
      payload: { ...message, runId: active.id },
    });
    await emitThreadEvent(history.id, {
      type: "turn-item.updated",
      payload: {
        ...userItem(history.id, active.id, messageId, 3, text),
        inputIntent: "promoted_queued_to_steer",
      },
    });
  });
  await expect.element(page.getByText(text, { exact: true })).toBeVisible();
  expect(page.getByText(text, { exact: true }).elements()).toHaveLength(1);
  await expect.element(page.getByText("Couldn't send", { exact: true })).not.toBeInTheDocument();
  expect(pendingSends.getSnapshot()).toEqual([]);
  expect(delivery.requests).toHaveLength(1);
});
