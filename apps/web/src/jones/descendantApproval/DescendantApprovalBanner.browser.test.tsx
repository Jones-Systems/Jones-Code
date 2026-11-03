import { RegistryContext, useAtomValue } from "@effect/atom-react";
import {
  createDescendantApprovalAtoms,
  descendantApprovalChildKey,
  type DescendantApprovalSources,
} from "@t3tools/client-runtime/jones/descendant-approval";
import type { ThreadPendingApproval } from "@t3tools/client-runtime/state/thread-requests";
import {
  EnvironmentId,
  RuntimeRequestId,
  ThreadId,
  type AuthSessionState,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Option from "effect/Option";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import { useState } from "react";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { act, render } from "../../../test/render";
import {
  ComposerBannerStack,
  type ComposerBannerStackItem,
} from "../../components/chat/ComposerBannerStack";
import { childInputBannerItem } from "../../components/chat/ThreadActivityBanners";
import { ComposerPendingUserInputPanel } from "../../components/chat/ComposerPendingUserInputPanel";
import type { PendingUserInputDraftAnswer } from "../../pendingUserInput";
import { makeThreadFixture } from "../../test-fixtures";
import {
  useDescendantApprovalBanner,
  type DescendantApprovalDispatch,
} from "./useDescendantApprovalBanner";
import "~/index.css";

const mocks = vi.hoisted(() => ({ toast: vi.fn() }));
vi.mock(import("./atoms"), () => ({ descendantApproval: null }) as never);
vi.mock(import("../../state/use-orchestration-command"), () => ({
  useOrchestrationCommand: () => vi.fn(),
}));
vi.mock(import("../../components/ui/toast"), async (original) => ({
  ...(await original()),
  toastManager: { add: mocks.toast } as never,
}));
const parent = {
  environmentId: EnvironmentId.make("parent-env"),
  threadId: ThreadId.make("parent"),
};
const first = {
  environmentId: EnvironmentId.make("child-env-a"),
  threadId: ThreadId.make("same-child"),
};
const second = {
  environmentId: EnvironmentId.make("child-env-b"),
  threadId: ThreadId.make("same-child"),
};
const requestId = RuntimeRequestId.make("same-request");
const approval: ThreadPendingApproval = {
  requestId,
  requestKind: "command",
  createdAt: "2026-10-26T12:00:00Z",
  detail: "synthetic-command --child",
  responseCapability: "live",
  options: [
    { decision: "decline", label: "Decline" },
    { decision: "accept", label: "Approve" },
    { decision: "acceptForSession", label: "Allow this session" },
  ],
};
const session: AuthSessionState = {
  authenticated: true,
  scopes: ["orchestration:operate"],
  auth: {
    policy: "remote-reachable",
    bootstrapMethods: [],
    sessionMethods: [],
    sessionCookieName: "test",
  },
};
function fixture() {
  const registry = AtomRegistry.make();
  const children = Atom.make([
    makeThreadFixture({
      environmentId: first.environmentId,
      id: first.threadId,
      title: "First child",
      hasPendingApprovals: true,
    }),
    makeThreadFixture({
      environmentId: second.environmentId,
      id: second.threadId,
      title: "Second child",
      hasPendingApprovals: true,
    }),
  ]);
  const shells = Atom.family((_env: EnvironmentId) =>
    Atom.make<Atom.Type<ReturnType<DescendantApprovalSources["shellStateAtom"]>>>({
      status: "live",
      error: Option.none(),
    }),
  );
  const statuses = Atom.family((_key: string) =>
    Atom.make<Atom.Type<ReturnType<DescendantApprovalSources["threadStatusAtom"]>>>("live"),
  );
  const requests = Atom.family((_key: string) =>
    Atom.make({ approvals: [approval] as ReadonlyArray<ThreadPendingApproval> }),
  );
  const presentation = Atom.make<
    Atom.Type<ReturnType<DescendantApprovalSources["presentationAtom"]>>
  >({ connection: { phase: "connected", error: null, traceId: null } });
  const error = Atom.make<string | null>(null);
  const sessionAtom = Atom.make<AuthSessionState | null>(session);
  const model = createDescendantApprovalAtoms({
    childThreadInputsAtom: () => children,
    presentationAtom: () => presentation,
    shellStateAtom: shells,
    sessionStateAtom: () => sessionAtom,
    threadStatusAtom: (ref) => statuses(descendantApprovalChildKey(ref)),
    threadErrorAtom: () => error,
    pendingRequestsAtom: (ref) => requests(descendantApprovalChildKey(ref)),
  });
  const dispatch = vi.fn<DescendantApprovalDispatch>(async () => AsyncResult.success(undefined));
  const open = vi.fn();
  const answerParent = vi.fn();
  return {
    [Symbol.dispose]() {
      registry.dispose();
      mocks.toast.mockReset();
    },
    registry,
    children,
    shells,
    statuses,
    requests,
    presentation,
    error,
    sessionAtom,
    model,
    dispatch,
    open,
    answerParent,
  };
}
function ParentQuestion({ h }: { h: ReturnType<typeof fixture> }) {
  const [answers, setAnswers] = useState<Record<string, PendingUserInputDraftAnswer>>({});
  return (
    <ComposerPendingUserInputPanel
      pendingUserInputs={[
        {
          requestId: RuntimeRequestId.make("parent-question"),
          createdAt: "2026-10-26T12:00:00Z",
          responseCapability: "live",
          dismissible: false,
          questions: [
            {
              id: "parent-question",
              header: "Parent question",
              question: "What should the parent do?",
              options: [],
              multiSelect: false,
              allowCustomAnswer: true,
            },
          ],
        },
      ]}
      cwd="/synthetic"
      threadRef={parent}
      disabled={false}
      respondingRequestIds={[]}
      answers={answers}
      questionIndex={0}
      onToggleOption={() => {}}
      onChangeCustomAnswer={(questionId, value) =>
        setAnswers((previous) => ({ ...previous, [questionId]: { customAnswer: value } }))
      }
      onPrevious={() => {}}
      onAdvance={() => h.answerParent(answers["parent-question"]?.customAnswer)}
      onDismiss={() => {}}
    />
  );
}
function Harness({ h, coexist = false }: { h: ReturnType<typeof fixture>; coexist?: boolean }) {
  const item = useDescendantApprovalBanner({
    parentRef: parent,
    model: h.model,
    dispatch: h.dispatch,
    onOpenThread: h.open,
  });
  const children = useAtomValue(h.children);
  const questionItem = childInputBannerItem({
    childThreadInputs: item ? children.filter((child) => !child.hasPendingApprovals) : children,
    parentAwaitingUser: false,
    onOpenThread: h.open,
  });
  const warning: ComposerBannerStackItem = {
    id: "other-warning",
    variant: "warning",
    icon: null,
    title: "Other warning",
  };
  return (
    <>
      <ComposerBannerStack
        items={[
          ...(item ? [item] : []),
          ...(coexist ? [warning] : []),
          ...(questionItem ? [questionItem] : []),
        ]}
      />
      {coexist ? <ParentQuestion h={h} /> : null}
    </>
  );
}
function tree(h: ReturnType<typeof fixture>, coexist = false) {
  return (
    <RegistryContext.Provider value={h.registry}>
      <Harness h={h} coexist={coexist} />
    </RegistryContext.Provider>
  );
}

it("dispatches once to the exact child's environment, thread and request, releases failure, and progresses to its sibling", async () => {
  using h = fixture();
  let finish!: (result: Awaited<ReturnType<DescendantApprovalDispatch>>) => void;
  h.dispatch.mockImplementationOnce(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  await using screen = await render(tree(h), { router: false });
  // Two native activations in the same turn also cover a stale pre-render handler.
  await act(() => {
    const approve = [...screen.container.querySelectorAll("button")].find(
      (button) => button.textContent === "Approve",
    );
    expect(approve).toBeDefined();
    approve!.click();
    approve!.click();
  });
  expect(h.dispatch).toHaveBeenCalledTimes(1);
  expect(h.dispatch).toHaveBeenCalledWith({
    environmentId: first.environmentId,
    input: { threadId: first.threadId, requestId, decision: "accept" },
  });
  await expect.element(screen.getByRole("button", { name: "Approve", exact: true })).toBeDisabled();
  await expect
    .element(screen.getByRole("button", { name: "More approval options" }))
    .toBeDisabled();
  await act(() => finish(AsyncResult.failure(Cause.fail(new Error("Synthetic approval failed")))));
  await expect.element(screen.getByRole("button", { name: "Approve", exact: true })).toBeEnabled();
  expect(mocks.toast).toHaveBeenCalledWith(
    expect.objectContaining({ type: "error", title: "Synthetic approval failed" }),
  );
  await screen.getByRole("button", { name: "Approve", exact: true }).click();
  await act(() =>
    h.registry.set(
      h.children,
      h.registry.get(h.children).filter((child) => child.environmentId !== first.environmentId),
    ),
  );
  await expect
    .element(screen.getByText("Second child needs approval", { exact: true }))
    .toBeVisible();
  await screen.getByRole("button", { name: "Approve", exact: true }).click();
  expect(h.dispatch.mock.calls.at(-1)?.[0]).toEqual({
    environmentId: second.environmentId,
    input: { threadId: second.threadId, requestId, decision: "accept" },
  });
});

it("disables every offered decision and More for cached detail, shows its reason, and leaves Open enabled", async () => {
  using h = fixture();
  h.registry.set(h.statuses(descendantApprovalChildKey(first)), "cached");
  await using screen = await render(tree(h), { router: false });
  await expect
    .element(screen.getByText("synthetic-command --child", { exact: true }))
    .toBeVisible();
  await expect
    .element(screen.getByText("Waiting for current subagent detail.", { exact: true }))
    .toBeVisible();
  for (const name of ["Approve", "Decline", "More approval options"])
    await expect.element(screen.getByRole("button", { name, exact: true })).toBeDisabled();
  await userEvent.keyboard("{Tab}");
  await act(() => {
    (
      screen
        .getByRole("group", { name: "Subagent approval decisions", exact: true })
        .element() as HTMLElement
    ).focus();
  });
  await expect
    .poll(() => document.querySelector('[data-slot="tooltip-popup"][data-open]')?.textContent)
    .toBe("Waiting for current subagent detail.");
  await screen.getByRole("button", { name: "Open", exact: true }).click();
  expect(h.open).toHaveBeenCalledWith(first);
  expect(h.dispatch).not.toHaveBeenCalled();
  await act(() => h.registry.set(h.statuses(descendantApprovalChildKey(first)), "live"));
  await expect.element(screen.getByRole("button", { name: "Approve", exact: true })).toBeEnabled();
});

it("uses the selected child's offered session decision from More", async () => {
  using h = fixture();
  await using screen = await render(tree(h), { router: false });
  await screen.getByRole("button", { name: "More approval options" }).click();
  await page.getByRole("menuitem", { name: "Allow this session", exact: true }).click();
  expect(h.dispatch).toHaveBeenCalledExactlyOnceWith({
    environmentId: first.environmentId,
    input: { threadId: first.threadId, requestId, decision: "acceptForSession" },
  });
});

it("keeps the standard decisions for legacy approvals without offered options", async () => {
  using h = fixture();
  const { options: _options, ...legacyApproval } = approval;
  h.registry.set(h.requests(descendantApprovalChildKey(first)), { approvals: [legacyApproval] });
  await using screen = await render(tree(h), { router: false });
  await screen.getByRole("button", { name: "Approve", exact: true }).click();
  expect(h.dispatch).toHaveBeenCalledExactlyOnceWith({
    environmentId: first.environmentId,
    input: { threadId: first.threadId, requestId, decision: "accept" },
  });
});

it("cycles scoped siblings with identical IDs and keeps urgent approval in front while parent questions and other notices remain usable", async () => {
  using h = fixture();
  const question = makeThreadFixture({
    environmentId: EnvironmentId.make("question-env"),
    id: ThreadId.make("question"),
    title: "Question child",
    hasPendingApprovals: false,
    hasPendingUserInput: true,
  });
  h.registry.set(h.children, [...h.registry.get(h.children), question]);
  await using screen = await render(tree(h, true), { router: false });
  const front = screen.container.querySelector('[data-composer-banner-drawer="true"]');
  expect(front?.textContent).toContain("First child needs approval");
  expect(front?.textContent).not.toContain("Other warning");
  await screen.getByRole("textbox", { name: "Your answer" }).fill("Parent answer remains editable");
  await expect
    .element(screen.getByRole("textbox", { name: "Your answer" }))
    .toHaveValue("Parent answer remains editable");
  await screen.getByRole("button", { name: "Next", exact: true }).click();
  await expect
    .element(screen.getByText("Second child needs approval", { exact: true }))
    .toBeVisible();
  await screen.getByRole("button", { name: "Open", exact: true }).click();
  expect(h.open).toHaveBeenLastCalledWith(second);
  await screen.getByRole("button", { name: "Next", exact: true }).click();
  await expect
    .element(screen.getByText("First child needs approval", { exact: true }))
    .toBeVisible();
  // Pointer entry expands the stack and hides its peek before a click can land.
  await screen.getByRole("button", { name: "Show other notices" }).hover();
  await expect.element(screen.getByText("Other warning", { exact: true })).toBeVisible();
  await screen.getByRole("button", { name: "Question child", exact: true }).click();
  expect(h.open).toHaveBeenLastCalledWith({
    environmentId: question.environmentId,
    threadId: question.id,
  });
  await expect
    .element(screen.getByRole("textbox", { name: "Your answer" }))
    .toHaveValue("Parent answer remains editable");
  await screen.getByRole("button", { name: "Send", exact: true }).click();
  expect(h.answerParent).toHaveBeenCalledExactlyOnceWith("Parent answer remains editable");
});

it("refuses a just-resolved request from a pre-render control and explains that it is no longer current", async () => {
  using h = fixture();
  await using screen = await render(tree(h), { router: false });
  const approve = [...screen.container.querySelectorAll("button")].find(
    (button) => button.textContent === "Approve",
  );
  expect(approve).toBeDefined();
  await act(() => {
    h.registry.set(h.requests(descendantApprovalChildKey(first)), { approvals: [] });
    approve!.click();
  });
  expect(h.dispatch).not.toHaveBeenCalled();
  expect(mocks.toast).toHaveBeenCalledWith(
    expect.objectContaining({ type: "info", title: "This approval is no longer current." }),
  );
  await expect
    .element(screen.getByText("This approval is no longer pending.", { exact: true }))
    .toBeVisible();
});
