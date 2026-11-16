import { beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId, ThreadId, type DesktopCompanionState } from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";

const state = vi.hoisted(() => ({
  electron: true,
  binding: { status: "unsupported" } as unknown,
  companion: null as unknown,
}));
vi.mock("~/env", () => ({
  get isElectron() {
    return state.electron;
  },
}));
vi.mock("~/previewStateStore", () => ({ isPreviewSupportedInRuntime: () => state.electron }));
vi.mock("~/state/entities", () => ({
  readEnvironmentSupportsServerBrowser: () => true,
  useEnvironmentSupportsServerBrowser: () => true,
}));
vi.mock("~/state/primaryEnvironment", () => ({ primaryEnvironmentIdAtom: "primary" }));
vi.mock("../jones/previewCompanion/state", () => ({
  bindingsQuery: () => "binding",
  companionStateAtom: "companion",
  readBindingResult: () => state.binding,
}));
vi.mock("~/rpc/atomRegistry", () => ({
  appAtomRegistry: { get: () => state.companion },
}));
vi.mock("@effect/atom-react", () => ({
  useAtomValue: (atom: string) =>
    atom === "primary"
      ? "env"
      : atom === "companion"
        ? state.companion
        : AsyncResult.success(state.binding),
}));

import { rendersServerTabNatively, useRendersServerTabNatively } from "./previewRuntime";

const environmentId = EnvironmentId.make("env");
const threadId = ThreadId.make("thread");
const snapshot = { runtime: "server" as const, threadId, tabId: "tab" };
const companion: DesktopCompanionState = {
  config: { enabled: true, environmentId, hostId: "mini", label: "Mini", browserOnly: true },
  status: "online",
  connectionGeneration: 2,
  assignments: [{ threadId, tabId: "tab" }],
};
const bind = (hostId: string | null) => {
  state.binding = {
    status: "ready",
    value: { selection: null, effective: { _tag: "server" }, tabs: [{ tabId: "tab", hostId }] },
  };
};

beforeEach(() => {
  state.electron = true;
  state.binding = { status: "unsupported" };
  state.companion = companion;
});

describe("server tab rendering", () => {
  it.each([undefined, false, true])("respects headless=%s on legacy local tabs", (headless) => {
    const tab = { ...snapshot, ...(headless === undefined ? {} : { headless }) };
    expect(rendersServerTabNatively(environmentId, environmentId, tab)).toBe(headless !== true);
    expect(useRendersServerTabNatively(environmentId, tab)).toBe(headless !== true);
  });

  it("streams a headless fallback even when the local binding would mount it", () => {
    bind(null);
    const tab = { ...snapshot, headless: true };
    expect(rendersServerTabNatively(environmentId, environmentId, tab)).toBe(false);
    expect(useRendersServerTabNatively(environmentId, tab)).toBe(false);
  });

  it("preserves exact companion assignment and offline fences", () => {
    const remote = EnvironmentId.make("remote");
    bind("mini");
    expect(rendersServerTabNatively(environmentId, remote, snapshot)).toBe(true);
    expect(useRendersServerTabNatively(environmentId, snapshot)).toBe(true);
    state.companion = { ...companion, status: "reconnecting" };
    expect(rendersServerTabNatively(environmentId, remote, snapshot)).toBe(false);
    expect(useRendersServerTabNatively(environmentId, snapshot)).toBe(false);
    state.companion = companion;
    bind("other-mini");
    expect(rendersServerTabNatively(environmentId, environmentId, snapshot)).toBe(false);
    expect(useRendersServerTabNatively(environmentId, snapshot)).toBe(false);
  });

  it("keeps web and remote environments on the streamed page", () => {
    expect(rendersServerTabNatively(environmentId, EnvironmentId.make("other"), snapshot)).toBe(
      false,
    );
    state.electron = false;
    expect(rendersServerTabNatively(environmentId, environmentId, snapshot)).toBe(false);
    expect(useRendersServerTabNatively(environmentId, snapshot)).toBe(false);
  });
});
