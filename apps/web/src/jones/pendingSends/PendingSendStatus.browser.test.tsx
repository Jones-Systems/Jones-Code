import { expect, it } from "vite-plus/test";
import { EnvironmentId, MessageId, ThreadId } from "@t3tools/contracts";
import { act, render } from "../../../test/render";
import { pendingSends } from "./store";
import { PendingSendStatus } from "./PendingSendStatus";
const environmentId = EnvironmentId.make("env");
const threadId = ThreadId.make("thread");
const messageId = MessageId.make("message");

it("keeps uncertain delivery across navigation and dismisses only the selected row", async () => {
  using _pendingCleanup = {
    [Symbol.dispose]: () => {
      for (const entry of pendingSends.getSnapshot()) pendingSends.dismiss(entry.key);
    },
  };
  const key = pendingSends.admit(environmentId, threadId, {
    id: messageId,
    role: "user",
    text: "hello",
    runId: null,
    createdAt: "2026-10-26T20:00:00Z",
    updatedAt: "2026-10-26T20:00:00Z",
    streaming: false,
  });
  pendingSends.issuing(key);
  pendingSends.finish(key, false);
  await using first = await render(
    <PendingSendStatus environmentId={environmentId} threadId={threadId} messageId={messageId} />,
  );
  await expect.element(first.getByText("Waiting for confirmation")).toBeVisible();
  await first.unmount();
  expect(pendingSends.getSnapshot()).toHaveLength(1);
  await using next = await render(
    <PendingSendStatus environmentId={environmentId} threadId={threadId} messageId={messageId} />,
  );
  await expect.element(next.getByText("Kept in this window")).toBeVisible();
  await next.getByRole("button", { name: "Dismiss unsent message" }).click();
  expect(pendingSends.getSnapshot()).toEqual([]);
});
it("shows a static sending indicator immediately without a label timer", async () => {
  using _pendingCleanup = {
    [Symbol.dispose]: () => {
      for (const entry of pendingSends.getSnapshot()) pendingSends.dismiss(entry.key);
    },
  };
  pendingSends.admit(environmentId, threadId, {
    id: messageId,
    role: "user",
    text: "hello",
    runId: null,
    createdAt: "2026-10-26T20:00:00Z",
    updatedAt: "2026-10-26T20:00:00Z",
    streaming: false,
  });
  await using screen = await render(
    <PendingSendStatus environmentId={environmentId} threadId={threadId} messageId={messageId} />,
  );
  await expect.element(screen.getByRole("status")).toHaveTextContent("Sending");
  await act(() => pendingSends.failBeforeDispatch(environmentId, threadId, messageId));
  await expect.element(screen.getByText("Couldn't send")).toBeVisible();
});
