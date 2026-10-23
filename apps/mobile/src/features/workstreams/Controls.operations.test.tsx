// @vitest-environment jsdom
import { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { MobileWorkstreamControls } from "./Controls";
import type { MobileWorkstreams } from "./types";
import type { MobileOperationActions } from "./actions";
import type { MobileWorkstreamSnapshot } from "./projection";
import { data, thread } from "./actions.fixtures";
vi.mock("react-native", () => {
  const Container = ({ children }: { children?: ReactNode }) => <div>{children}</div>;
  return {
    Modal: ({ visible, children }: { visible?: boolean; children?: ReactNode }) =>
      visible ? <div>{children}</div> : null,
    Pressable: ({
      accessibilityLabel,
      disabled,
      onPress,
      children,
    }: {
      accessibilityLabel?: string;
      disabled?: boolean;
      onPress?: () => void;
      children?: ReactNode;
    }) => (
      <button aria-label={accessibilityLabel} disabled={disabled} onClick={onPress}>
        {children}
      </button>
    ),
    ScrollView: Container,
    Text: Container,
    TextInput: () => <input />,
    View: Container,
  };
});
vi.mock("../../jones/workstreamAppearance/WorkstreamColorPicker", () => ({
  WorkstreamColorPicker: () => null,
}));
vi.mock("../../lib/runtime", () => ({ runtime: { runPromise: vi.fn() } }));
let root: Root | undefined;
let container: HTMLDivElement | undefined;
function mountControls(api: MobileWorkstreams & MobileOperationActions) {
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
  root.render(
    <MobileWorkstreamControls
      api={api}
      selection={{ thread }}
      onSelect={() => {}}
      onClose={() => {}}
    />,
  );
}
function assignmentButton() {
  const button = container?.querySelector<HTMLButtonElement>(
    'button[aria-label="Assign to alpha"]',
  );
  if (!button) throw new Error("Assignment button not rendered");
  return button;
}
afterEach(async () => {
  await act(async () => root?.unmount());
  container?.remove();
  root = undefined;
  container = undefined;
  vi.unstubAllGlobals();
});
const snapshot = {
  prepared: { environmentId: thread.environmentId },
  generation: 1,
  data,
  placements: null,
  identityKeys: new Set(),
  index: null,
  stale: false,
} as unknown as MobileWorkstreamSnapshot;
const makeApi = (capable: boolean): MobileWorkstreams & MobileOperationActions => ({
  snapshots: [snapshot],
  groups: [],
  secondaryLabelsByKey: new Map(),
  assuranceByKey: new Map(),
  confirmedUnassignedKeys: new Set(),
  conflictingThreadKeys: new Set(),
  stale: false,
  enabled: true,
  toggleEnabled: vi.fn(),
  collapsedKeys: new Set(),
  toggleGroup: vi.fn(),
  bindingRevision: "fixture",
  readiness: "unavailable",
  error: null,
  refresh: vi.fn(),
  reorderGroup: vi.fn(),
  submit: vi.fn(),
  read: vi.fn(),
  pages: vi.fn(),
  saveAppearance: vi.fn(),
  supportsOperations: () => capable,
  assignOperation: vi.fn(async () => {}),
});
describe("mobile never-verified primary assignment", () => {
  it("uses one operation with no reference scan on a capable server", async () => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    const api = makeApi(true);
    await act(async () => {
      mountControls(api);
    });
    const assign = assignmentButton();
    expect(assign.disabled).toBe(false);
    await act(async () => assign.click());
    expect(api.assignOperation).toHaveBeenCalledWith(snapshot, thread, "alpha");
    expect(api.pages).not.toHaveBeenCalled();
    expect(api.submit).not.toHaveBeenCalled();
  });
  it("keeps legacy assignment disabled without verified enrollment", async () => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    const api = makeApi(false);
    await act(async () => {
      mountControls(api);
    });
    const assign = assignmentButton();
    expect(assign.disabled).toBe(true);
    await act(async () => assign.click());
    expect(api.assignOperation).not.toHaveBeenCalled();
  });
});
