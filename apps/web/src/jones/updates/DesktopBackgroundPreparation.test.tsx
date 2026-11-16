import { type ReactNode } from "react";
import { act } from "../testing/nodeRenderer";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

// The settings row reads environment scope from live atoms; this leaf only supplies its content.
vi.mock("~/components/settings/settingsLayout", () => ({
  SettingsRow: (props: { title: ReactNode; description: ReactNode; control: ReactNode }) => (
    <div>
      <span>{props.title}</span>
      <p>{props.description}</p>
      {props.control}
    </div>
  ),
}));
vi.mock("~/components/ui/switch", () => ({
  Switch: (props: {
    checked: boolean;
    disabled?: boolean;
    onCheckedChange: (checked: boolean) => void;
  }) => (
    <button
      type="button"
      aria-pressed={props.checked}
      disabled={props.disabled}
      onClick={() => props.onCheckedChange(!props.checked)}
    />
  ),
}));
const toasts = vi.hoisted(() => ({ add: vi.fn() }));
vi.mock("~/components/ui/toast", () => ({
  toastManager: toasts,
  stackedThreadToast: (toast: unknown) => toast,
}));

import {
  DesktopBackgroundPreparationRow,
  describeDesktopBackgroundPreparation,
} from "./DesktopBackgroundPreparation";

let tree: ReactTestRenderer | undefined;
afterEach(async () => {
  await act(() => tree?.unmount());
  tree = undefined;
  toasts.add.mockReset();
  vi.unstubAllGlobals();
});

const target = "0.0.44-preview.20261020.11.1";

describe("describeDesktopBackgroundPreparation", () => {
  it("says what each phase is doing without implying an automatic install", () => {
    expect(describeDesktopBackgroundPreparation({ enabled: false, phase: "off" })).toContain(
      "Installing stays your choice",
    );
    expect(
      describeDesktopBackgroundPreparation({
        enabled: true,
        phase: "ready",
        targetVersion: target,
      }),
    ).toBe(`${target} is downloaded and verified. Install it when you are ready.`);
    expect(
      describeDesktopBackgroundPreparation({
        enabled: true,
        phase: "preparing",
        targetVersion: target,
        replacingVersion: "0.0.44-preview.20261020.10.1",
      }),
    ).toBe(`Replacing the unused 0.0.44-preview.20261020.10.1 download with ${target}.`);
    expect(
      describeDesktopBackgroundPreparation({
        enabled: true,
        phase: "pinned",
        targetVersion: target,
      }),
    ).toBe(`An install is in progress. ${target} waits until it commits or rolls back.`);
    expect(
      describeDesktopBackgroundPreparation({
        enabled: true,
        phase: "blocked",
        reason: "2 replaced downloads could not be removed.",
      }),
    ).toBe("2 replaced downloads could not be removed.");
    expect(
      describeDesktopBackgroundPreparation({
        enabled: true,
        phase: "settling",
        targetVersion: target,
        nextAt: "not-a-time",
      }),
    ).toBe(`${target} is the newest build. It will be prepared later if nothing newer arrives.`);
  });
});

describe("DesktopBackgroundPreparationRow", () => {
  it("renders nothing for a desktop that cannot prepare in the background", async () => {
    vi.stubGlobal("window", { desktopBridge: {} });
    await act(() => {
      tree = create(
        <DesktopBackgroundPreparationRow preparation={{ enabled: false, phase: "off" }} />,
      );
    });
    expect(tree!.toJSON()).toBeNull();
  });

  it("asks the desktop to change the setting and reports a refusal", async () => {
    const setJonesBackgroundPreparation = vi.fn(async () => {
      throw new Error("The settings could not be read.");
    });
    vi.stubGlobal("window", { desktopBridge: { setJonesBackgroundPreparation } });
    await act(() => {
      tree = create(
        <DesktopBackgroundPreparationRow preparation={{ enabled: false, phase: "off" }} />,
      );
    });
    const toggle = tree!.root.findByType("button");
    expect(toggle.props["aria-pressed"]).toBe(false);
    await act(async () => {
      toggle.props.onClick();
    });
    expect(setJonesBackgroundPreparation).toHaveBeenCalledWith(true);
    expect(toasts.add).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "error",
        title: "Could not change background preparation",
        description: "The settings could not be read.",
      }),
    );
    expect(tree!.root.findByType("button").props.disabled).toBe(false);
  });
});
