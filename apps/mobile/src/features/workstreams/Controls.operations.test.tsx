import { act } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { MobileWorkstreamControls } from "./Controls";
import type { MobileWorkstreams } from "./types";
import type { MobileOperationActions } from "./actions";
import type { MobileWorkstreamSnapshot } from "./projection";
import { data, thread } from "./actions.fixtures";
vi.mock("react-native", () => ({
  Modal: "Modal",
  Pressable: "Pressable",
  ScrollView: "ScrollView",
  Text: "Text",
  TextInput: "TextInput",
  View: "View",
}));
vi.mock("../../jones/workstreamAppearance/WorkstreamColorPicker", () => ({
  WorkstreamColorPicker: () => null,
}));
vi.mock("../../lib/runtime", () => ({ runtime: { runPromise: vi.fn() } }));
let renderer: ReactTestRenderer | undefined;
afterEach(async () => {
  await act(async () => renderer?.unmount());
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
      renderer = create(
        <MobileWorkstreamControls
          api={api}
          selection={{ thread }}
          onSelect={() => {}}
          onClose={() => {}}
        />,
      );
    });
    const assign = renderer!.root
      .findAllByProps({ accessibilityLabel: "Assign to alpha" })
      .find((item) => item.type === "Pressable")!;
    expect(assign.props.disabled).toBe(false);
    await act(async () => assign.props.onPress());
    expect(api.assignOperation).toHaveBeenCalledWith(snapshot, thread, "alpha");
    expect(api.pages).not.toHaveBeenCalled();
    expect(api.submit).not.toHaveBeenCalled();
  });
  it("keeps legacy assignment disabled without verified enrollment", async () => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    const api = makeApi(false);
    await act(async () => {
      renderer = create(
        <MobileWorkstreamControls
          api={api}
          selection={{ thread }}
          onSelect={() => {}}
          onClose={() => {}}
        />,
      );
    });
    const assign = renderer!.root
      .findAllByProps({ accessibilityLabel: "Assign to alpha" })
      .find((item) => item.type === "Pressable")!;
    expect(assign.props.disabled).toBe(true);
    expect(api.assignOperation).not.toHaveBeenCalled();
  });
});
