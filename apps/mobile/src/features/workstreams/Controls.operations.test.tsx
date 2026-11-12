import { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, expect, it, vi } from "vite-plus/test";
import type {
  PersonalWorkstreamsController,
  PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { MobileWorkstreamControls } from "./Controls";
vi.mock("react-native", () => {
  const Container = ({ children }: { children?: ReactNode }) => <div>{children}</div>;
  return {
    Alert: { alert: vi.fn() },
    View: Container,
    Text: Container,
    ScrollView: Container,
    Modal: ({ visible, children }: { visible: boolean; children?: ReactNode }) =>
      visible ? <div>{children}</div> : null,
    TextInput: () => <input />,
    Pressable: ({
      accessibilityLabel,
      disabled,
      onPress,
      children,
    }: {
      accessibilityLabel: string;
      disabled: boolean;
      onPress: () => void;
      children?: ReactNode;
    }) => (
      <button aria-label={accessibilityLabel} disabled={disabled} onClick={onPress}>
        {children}
      </button>
    ),
  };
});
vi.mock("../../jones/workstreamAppearance/WorkstreamColorPicker", () => ({
  WorkstreamColorPicker: () => null,
}));
let root: Root | undefined;
let element: HTMLDivElement | undefined;
afterEach(async () => {
  await act(async () => root?.unmount());
  element?.remove();
  vi.unstubAllGlobals();
});
const thread = {
  environmentId: EnvironmentId.make("env:synthetic"),
  id: ThreadId.make("thread:synthetic"),
};
const state: PersonalWorkstreamsState = {
  workstreams: [
    {
      id: "group",
      name: "Synthetic group",
      lifecycle: "active",
      color: null,
      orderKey: "n",
      createdAt: "2026-01-01T00:00:00.000Z",
      updatedAt: "2026-01-01T00:00:00.000Z",
    },
  ],
  environments: [],
  assignmentsByThreadKey: new Map(),
  pendingThreadKeys: new Set(),
  canCreate: true,
};
async function mount(writable: boolean) {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  const assign = vi.fn<PersonalWorkstreamsController["assign"]>(async () => ({
    ok: true,
    value: undefined,
  }));
  const controller = {
    canAssign: () => writable,
    assign,
  } as unknown as PersonalWorkstreamsController;
  element = document.createElement("div");
  document.body.append(element);
  root = createRoot(element);
  await act(async () =>
    root!.render(
      <MobileWorkstreamControls
        controller={controller}
        state={state}
        selection={{ thread }}
        onSelect={() => {}}
        onClose={() => {}}
        environmentNames={new Map()}
        perform={async (action) => (await action()).ok}
      />,
    ),
  );
  return assign;
}
it("assigns and unassigns through the personal controller without a native registry operation", async () => {
  const assign = await mount(true);
  await act(async () =>
    element!.querySelector<HTMLButtonElement>('button[aria-label="Synthetic group"]')!.click(),
  );
  expect(assign).toHaveBeenLastCalledWith(thread, "group");
  await act(async () =>
    element!
      .querySelector<HTMLButtonElement>('button[aria-label="Remove from Workstream"]')!
      .click(),
  );
  expect(assign).toHaveBeenLastCalledWith(thread, null);
});
it("disables assignment for old-server capability fallback and read-only sessions", async () => {
  const assign = await mount(false);
  const button = element!.querySelector<HTMLButtonElement>('button[aria-label="Synthetic group"]')!;
  expect(button.disabled).toBe(true);
  await act(async () => button.click());
  expect(assign).not.toHaveBeenCalled();
});

it("releases pending controls after a failed native action so the user can try again", async () => {
  const assign = await mount(true);
  assign.mockResolvedValueOnce({
    ok: false,
    failure: {
      action: "assign",
      reason: "offline",
      environmentId: thread.environmentId,
      dedupeKey: "assign:synthetic:offline",
    },
  });
  await act(async () =>
    element!.querySelector<HTMLButtonElement>('button[aria-label="Synthetic group"]')!.click(),
  );
  const retry = element!.querySelector<HTMLButtonElement>('button[aria-label="Synthetic group"]')!;
  expect(retry.disabled).toBe(false);
  await act(async () => retry.click());
  expect(assign).toHaveBeenCalledTimes(2);
  expect(retry.disabled).toBe(false);
});
