import { RegistryContext } from "@effect/atom-react";
import {
  createDescendantApprovalAtoms,
  DESCENDANT_APPROVAL_MESSAGES,
  type DescendantApprovalModel,
  type DescendantApprovalSources,
} from "@t3tools/client-runtime/jones/descendant-approval";
import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import {
  EnvironmentId,
  RuntimeRequestId,
  ThreadId,
  type ScopedThreadRef,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Option from "effect/Option";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import { act, useLayoutEffect, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { beforeEach, describe, expect, it, vi } from "vite-plus/test";

const harness = vi.hoisted(() => ({
  model: null as DescendantApprovalModel | null,
  dispatch: vi.fn<(input: unknown) => Promise<AtomCommandResult<unknown, unknown>>>(),
  alert: vi.fn(),
}));
vi.mock("react-native", () => ({
  View: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  ScrollView: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  Pressable: ({ children, onPress }: { children: ReactNode; onPress: () => void }) => (
    <button onClick={onPress}>{children}</button>
  ),
}));
vi.mock("../../components/ui/AppText", () => ({
  AppText: ({ children }: { children: ReactNode }) => <span>{children}</span>,
}));
vi.mock("../../components/ui/Button", () => ({
  Button: (props: {
    label: string;
    disabled?: boolean;
    onPress: () => void;
    accessibilityLabel?: string;
    accessibilityHint?: string;
  }) => (
    <button
      disabled={props.disabled}
      aria-label={props.accessibilityLabel}
      aria-description={props.accessibilityHint}
      onClick={props.onPress}
    >
      {props.label}
    </button>
  ),
}));
vi.mock("../../components/ui/Dialog", () => ({ alert: harness.alert }));
vi.mock("../../state/threads", () => ({ threadEnvironment: { respondToApproval: {} } }));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => harness.dispatch }));
vi.mock("./atoms", () => ({
  get descendantApprovals() {
    if (!harness.model) throw new Error("Missing test model");
    return harness.model;
  },
}));

import { DescendantApprovalCard } from "./DescendantApprovalCard";
import { useDescendantApproval } from "./useDescendantApproval";

type Value<K extends keyof DescendantApprovalSources> = Atom.Type<
  ReturnType<DescendantApprovalSources[K]>
>;
const parent = {
  environmentId: EnvironmentId.make("parent-env"),
  threadId: ThreadId.make("parent"),
};
const child = (environment: string): ScopedThreadRef => ({
  environmentId: EnvironmentId.make(environment),
  threadId: ThreadId.make("same-child-id"),
});
const first = child("env-a");
const second = child("env-b");
const requestId = RuntimeRequestId.make("same-request-id");
const approval = {
  requestId,
  requestKind: "command" as const,
  createdAt: "2026-10-27T00:00:00.000Z",
  detail: "Run the requested build",
  responseCapability: "live" as const,
  options: [
    { decision: "accept" as const, label: "Run once" },
    { decision: "decline" as const, label: "Reject build" },
  ],
};

function setup() {
  const children = Atom.make<Value<"childThreadInputsAtom">>(
    [first, second].map((ref, index) => ({
      environmentId: ref.environmentId,
      id: ref.threadId,
      title: `Builder ${index + 1}`,
      hasPendingApprovals: true,
    })),
  );
  const presentation = Atom.family((_environmentId: EnvironmentId) =>
    Atom.make<Value<"presentationAtom">>({
      connection: { phase: "connected", error: null, traceId: null },
    }),
  );
  const shell = Atom.family((_environmentId: EnvironmentId) =>
    Atom.make<Value<"shellStateAtom">>({ status: "live", error: Option.none() }),
  );
  const session = Atom.family((_environmentId: EnvironmentId) =>
    Atom.make<Value<"sessionStateAtom">>({
      authenticated: true,
      permissions: ["orchestration:operate"],
      auth: {
        policy: "loopback-browser",
        bootstrapMethods: [],
        sessionMethods: [],
        sessionCookieName: "test",
      },
    }),
  );
  const status = Atom.family((_environmentId: EnvironmentId) =>
    Atom.make<Value<"threadStatusAtom">>("live"),
  );
  const error = Atom.family((_environmentId: EnvironmentId) =>
    Atom.make<Value<"threadErrorAtom">>(null),
  );
  const pending = Atom.family((_environmentId: EnvironmentId) =>
    Atom.make<Value<"pendingRequestsAtom">>({ approvals: [approval] }),
  );
  const model = createDescendantApprovalAtoms({
    childThreadInputsAtom: () => children,
    presentationAtom: presentation,
    shellStateAtom: shell,
    sessionStateAtom: session,
    threadStatusAtom: (ref) => status(ref.environmentId),
    threadErrorAtom: (ref) => error(ref.environmentId),
    pendingRequestsAtom: (ref) => pending(ref.environmentId),
  });
  harness.model = model;
  const registry = AtomRegistry.make();
  const container = document.createElement("div");
  const root = createRoot(container);
  const open = vi.fn();
  let controller: ReturnType<typeof useDescendantApproval>;
  function Screen({ parentRef }: { parentRef: ScopedThreadRef | null }) {
    const current = useDescendantApproval(parentRef);
    useLayoutEffect(() => {
      controller = current;
    });
    return current.state ? (
      <DescendantApprovalCard
        state={current.state}
        onRespond={current.onRespond}
        onNext={current.onNext}
        onOpen={open}
      />
    ) : null;
  }
  return {
    registry,
    model,
    children,
    presentation,
    shell,
    session,
    status,
    error,
    pending,
    container,
    open,
    get controller() {
      return controller;
    },
    render(parentRef: ScopedThreadRef | null = parent) {
      act(() =>
        root.render(
          <RegistryContext.Provider value={registry}>
            <Screen parentRef={parentRef} />
          </RegistryContext.Provider>,
        ),
      );
    },
    button(label: string) {
      const button = [...container.querySelectorAll("button")].find(
        (node) => node.textContent === label,
      );
      if (!button) throw new Error(`Missing button: ${label}`);
      return button;
    },
    [Symbol.dispose]() {
      act(() => root.unmount());
      registry.dispose();
      harness.model = null;
    },
  };
}

beforeEach(() => {
  (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  harness.dispatch.mockReset().mockResolvedValue(AsyncResult.success(null));
  harness.alert.mockReset();
});

describe("DescendantApprovalCard", () => {
  it("reuses offered actions, dispatches to the child, and blocks duplicate sends until completion", async () => {
    using screen = setup();
    const response = Promise.withResolvers<AtomCommandResult<unknown, unknown>>();
    harness.dispatch.mockReturnValue(response.promise);
    screen.render();
    expect(screen.container.textContent).toContain("Approval needed");
    expect(screen.container.textContent).toContain(approval.detail);
    expect(screen.container.textContent).not.toContain("Allow session");
    await act(async () => {
      screen.button("Run once").click();
      screen.button("Run once").click();
    });
    expect(harness.dispatch).toHaveBeenCalledExactlyOnceWith({
      environmentId: first.environmentId,
      input: { threadId: first.threadId, requestId, decision: "accept" },
    });
    expect(screen.button("Run once").disabled).toBe(true);
    expect(screen.button("Reject build").disabled).toBe(true);
    expect(screen.button("Open").disabled).toBe(false);
    act(() => screen.button("Next").click());
    expect(screen.button("Reject build").disabled).toBe(false);
    await act(async () => screen.button("Reject build").click());
    expect(harness.dispatch).toHaveBeenCalledTimes(2);
    expect(harness.dispatch).toHaveBeenLastCalledWith({
      environmentId: second.environmentId,
      input: { threadId: second.threadId, requestId, decision: "decline" },
    });
    expect(screen.button("Run once").disabled).toBe(true);
    await act(async () => response.resolve(AsyncResult.success(null)));
    expect(screen.button("Run once").disabled).toBe(false);
  });

  it("selects the ready child after an offline first child and cycles across identical IDs in distinct environments", async () => {
    using screen = setup();
    screen.registry.set(screen.presentation(first.environmentId), {
      connection: { phase: "offline", error: null, traceId: null },
    });
    screen.render();
    expect(screen.container.textContent).toContain("Builder 2 needs approval");
    expect(screen.container.textContent).toContain("2 of 2 subagents waiting");
    act(() => screen.button("Open").click());
    expect(screen.open).toHaveBeenLastCalledWith(second);
    act(() => screen.button("Next").click());
    expect(screen.container.textContent).toContain("Builder 1 needs approval");
    expect(screen.container.textContent).toContain(DESCENDANT_APPROVAL_MESSAGES.environment);
    act(() =>
      screen.registry.set(screen.presentation(first.environmentId), {
        connection: { phase: "connected", error: null, traceId: null },
      }),
    );
    expect(screen.container.textContent).toContain("Builder 1 needs approval");
    await act(async () => screen.button("Reject build").click());
    expect(harness.dispatch).toHaveBeenLastCalledWith({
      environmentId: first.environmentId,
      input: { threadId: first.threadId, requestId, decision: "decline" },
    });
    act(() => screen.button("Next").click());
    await act(async () => screen.button("Run once").click());
    expect(harness.dispatch).toHaveBeenLastCalledWith({
      environmentId: second.environmentId,
      input: { threadId: second.threadId, requestId, decision: "accept" },
    });
  });

  it.each(["detail", "environment", "permission", "resolved", "capability"] as const)(
    "shows the shared %s reason, blocks responses, and retains Open and Next",
    async (reason) => {
      using screen = setup();
      screen.render();
      const selectedKey = screen.controller.state!.selected.key;
      act(() => {
        screen.model.select(screen.registry, parent, selectedKey);
        if (reason === "detail") screen.registry.set(screen.status(first.environmentId), "cached");
        if (reason === "environment")
          screen.registry.set(screen.presentation(first.environmentId), {
            connection: { phase: "offline", error: null, traceId: null },
          });
        if (reason === "permission") screen.registry.set(screen.session(first.environmentId), null);
        if (reason === "resolved")
          screen.registry.set(screen.pending(first.environmentId), { approvals: [] });
        if (reason === "capability")
          screen.registry.set(screen.pending(first.environmentId), {
            approvals: [{ ...approval, responseCapability: "not_resumable" }],
          });
      });
      expect(screen.container.textContent).toContain(DESCENDANT_APPROVAL_MESSAGES[reason]);
      expect(screen.container.textContent).not.toContain(
        "The provider process for this request is no longer available",
      );
      expect(screen.container.textContent).not.toContain("Approval needed");
      if (reason !== "resolved") {
        expect(screen.container.textContent).toContain(approval.detail);
        expect(screen.button("Run once").disabled).toBe(true);
        expect(screen.button("Reject build").disabled).toBe(true);
        act(() => screen.button("Run once").click());
      }
      expect(harness.dispatch).not.toHaveBeenCalled();
      act(() => screen.button("Open").click());
      expect(screen.open).toHaveBeenCalledWith(first);
      act(() => screen.button("Next").click());
      expect(screen.container.textContent).toContain("Builder 2 needs approval");
      expect(screen.button("Run once").disabled).toBe(false);
      await act(async () => screen.button("Run once").click());
      expect(harness.dispatch).toHaveBeenCalledExactlyOnceWith({
        environmentId: second.environmentId,
        input: { threadId: second.threadId, requestId, decision: "accept" },
      });
    },
  );

  it("rejects a stale response and reports a send failure while allowing retry", async () => {
    using screen = setup();
    screen.render();
    const respond = screen.controller.onRespond;
    act(() => screen.registry.set(screen.pending(first.environmentId), { approvals: [] }));
    await act(async () => {
      await respond(requestId, "accept");
    });
    expect(harness.dispatch).not.toHaveBeenCalled();
    expect(harness.alert).toHaveBeenCalledWith("Approval changed", expect.any(String));
    act(() => screen.registry.set(screen.pending(first.environmentId), { approvals: [approval] }));
    harness.dispatch.mockResolvedValueOnce(
      AsyncResult.failure(Cause.fail(new Error("Connection lost"))),
    );
    await act(async () => screen.button("Run once").click());
    expect(harness.alert).toHaveBeenLastCalledWith(
      "Could not respond to approval",
      "Connection lost",
    );
    expect(screen.button("Run once").disabled).toBe(false);
    await act(async () => screen.button("Run once").click());
    expect(harness.dispatch).toHaveBeenCalledTimes(2);
  });

  it("does not show a card for a local draft or after the last approval child leaves", () => {
    using screen = setup();
    screen.render(null);
    expect(screen.container.textContent).toBe("");
    screen.render();
    expect(screen.container.textContent).toContain("Builder 1 needs approval");
    act(() => screen.registry.set(screen.children, []));
    expect(screen.container.textContent).toBe("");
  });
});
