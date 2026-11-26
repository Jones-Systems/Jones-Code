import "~/index.css";
import { RegistryContext } from "@effect/atom-react";
import {
  personalThreadKey,
  type PersonalWorkstreamsController,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { AsyncResult, Atom } from "effect/reactivity";
import { expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";

import { render } from "../../../test/render";
import { MarkdownThreadLink } from "../../components/chat/MarkdownThreadLink";
import { ToastProvider } from "../../components/ui/toast";
import { dismissContextMenu } from "../../contextMenuFallback";
import { appAtomRegistry as registry } from "../../rpc/atomRegistry";

const fixture = vi.hoisted(() => ({
  sourceId: "source",
  targetId: "destination",
  threadId: "same-thread-id",
  canOperate: true,
}));
const source = EnvironmentId.make(fixture.sourceId);
const target = EnvironmentId.make(fixture.targetId);
const threadId = ThreadId.make(fixture.threadId);
const workstream = {
  id: "planning",
  name: "Planning",
  lifecycle: "active" as const,
  color: null,
  orderKey: "n",
  createdAt: "2026-11-12T12:00:00.000Z",
  updatedAt: "2026-11-12T12:00:00.000Z",
};
const state = Atom.make<PersonalWorkstreamsState>({
  workstreams: [workstream],
  environments: [source, target].map((environmentId) => ({
    environmentId,
    status: "ready" as const,
    confirmed: true,
    workstreamCount: 1,
  })),
  assignmentsByThreadKey: new Map(),
  pendingThreadKeys: new Set(),
  canCreate: true,
}).pipe(Atom.keepAlive);
const linkMetadata = Atom.make(
  AsyncResult.success({ [threadId]: { title: "Linked thread", projectId: null } }),
);

vi.mock(import("../../state/workstreamsPersonal"), async (original) => ({
  ...(await original()),
  personalWorkstreamsController: {
    get state() {
      return state;
    },
    canAssign: () => true,
    create: vi.fn(),
    rename: vi.fn(),
    setColor: vi.fn(),
    setLifecycle: vi.fn(),
    reorder: vi.fn(),
    remove: vi.fn(),
    createAndAssign: vi.fn(),
    assign: async (thread, workstreamId) => {
      const current = registry.get(state);
      registry.set(state, {
        ...current,
        assignmentsByThreadKey: new Map(current.assignmentsByThreadKey).set(
          personalThreadKey(thread),
          { threadId: thread.id, workstreamId, revision: 1, updatedAt: null },
        ),
      });
      return { ok: true, value: undefined };
    },
  } satisfies PersonalWorkstreamsController,
}));
vi.mock(import("../../state/session"), async (original) => ({
  ...(await original()),
  ensureEnvironmentScope: async () => ({ status: "granted" as const }),
  readEnvironmentScope: (environmentId: string) =>
    fixture.canOperate || environmentId !== fixture.targetId,
}));
vi.mock(import("../../state/entities"), async (original) => ({
  ...(await original()),
  readThreadShell: () => null,
  useThreadShell: () => null,
  useThreadRefs: () => [],
  useProject: () => null,
}));
vi.mock(import("../../state/environments"), async (original) => ({
  ...(await original()),
  useEnvironmentIdentities: () => [
    { environmentId: EnvironmentId.make(fixture.targetId), label: "Destination" },
  ],
}));
vi.mock("../../state/orchestration", () => ({
  orchestrationEnvironment: {
    threadLinkMetadata: () => linkMetadata,
  },
}));

it("assigns and unassigns the exact linked thread without navigating to it", async () => {
  using _menu = { [Symbol.dispose]: dismissContextMenu };
  await using screen = await render(
    <RegistryContext value={registry}>
      <ToastProvider>
        <MarkdownThreadLink
          environmentId={source}
          link={{ environmentId: target, threadId }}
          label="Linked thread"
        />
      </ToastProvider>
    </RegistryContext>,
  );
  const link = screen.getByRole("link", { name: "Thread, Linked thread" });
  const openAssignment = async () => {
    await link.click({ button: "right" });
    await page.getByRole("menuitem", { name: "Assign thread" }).click();
  };

  await openAssignment();
  await page.getByRole("menuitemradio", { name: "Planning" }).click();
  await vi.waitFor(() =>
    expect(
      registry
        .get(state)
        .assignmentsByThreadKey.get(personalThreadKey({ environmentId: target, id: threadId }))
        ?.workstreamId,
    ).toBe(workstream.id),
  );
  expect(screen.router?.state.location.pathname).toBe("/");
  expect(
    registry
      .get(state)
      .assignmentsByThreadKey.get(personalThreadKey({ environmentId: source, id: threadId })),
  ).toBeUndefined();

  await openAssignment();
  await expect.element(page.getByRole("menuitemradio", { name: "Planning" })).toBeChecked();
  await page.getByRole("menuitem", { name: "Search or create Workstream…" }).click();
  const picker = page.getByRole("dialog", { name: "Move to Workstream" });
  await expect.element(picker.getByRole("textbox", { name: "Search Workstreams" })).toBeVisible();
  await picker.getByRole("button", { name: "Unassigned", exact: true }).click();
  await expect.element(picker).not.toBeInTheDocument();

  await openAssignment();
  await expect
    .element(page.getByRole("menuitemradio", { name: "Unassigned", exact: true }))
    .toBeChecked();
  dismissContextMenu();

  fixture.canOperate = false;
  try {
    await openAssignment();
    await expect.element(page.getByRole("menuitemradio", { name: "Planning" })).toBeDisabled();
  } finally {
    fixture.canOperate = true;
  }
});
