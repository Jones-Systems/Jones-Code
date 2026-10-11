// @vitest-environment jsdom
import { act, useEffect, useState, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import type { WorkQueueMetadataResult } from "@t3tools/contracts";

const fixture = vi.hoisted(() => ({
  environments: [
    {
      environmentId: "alpha",
      label: "Alpha",
      entry: { enabled: true },
      connection: { phase: "connected" },
      serverConfig: { environment: { capabilities: { workQueueMetadata: true } } },
    },
    {
      environmentId: "beta",
      label: "Beta",
      entry: { enabled: true },
      connection: { phase: "connected" },
      serverConfig: { environment: { capabilities: { workQueueMetadata: true } } },
    },
  ],
  mounts: new Map<string, number>(),
  blockerOptions: null as null | {
    shouldBlockFn: () => boolean;
    enableBeforeUnload: () => boolean;
  },
  blockerStatus: "idle" as "idle" | "blocked",
  reset: vi.fn(),
  proceed: vi.fn(),
  load: vi.fn<() => Promise<WorkQueueMetadataResult>>(),
}));

vi.mock("../../state/environments", () => ({
  useEnvironments: () => ({ environments: fixture.environments }),
}));
vi.mock("@tanstack/react-router", () => ({
  useBlocker: (options: NonNullable<typeof fixture.blockerOptions>) => {
    fixture.blockerOptions = options;
    return { status: fixture.blockerStatus, reset: fixture.reset, proceed: fixture.proceed };
  },
}));
vi.mock("../../env", () => ({ isElectron: false }));
vi.mock("../../components/ui/sidebar", () => ({
  SidebarInset: ({ children }: { children: ReactNode }) => <main>{children}</main>,
}));
vi.mock("./useWorkQueueMetadata", () => ({ useWorkQueueMetadata: () => fixture.load }));
vi.mock("../../components/voiceReview/VoiceReviewPage", () => ({
  EnvironmentVoiceReview: ({
    environmentId,
    pane,
    unavailable,
    onDirtyChange,
  }: {
    environmentId: string;
    pane: string;
    unavailable: boolean;
    onDirtyChange: (dirty: boolean) => void;
  }) => {
    const [text, setText] = useState("");
    const [uncertain, setUncertain] = useState(false);
    useEffect(() => {
      fixture.mounts.set(environmentId, (fixture.mounts.get(environmentId) ?? 0) + 1);
    }, [environmentId]);
    useEffect(() => {
      onDirtyChange(text !== "" || uncertain);
      return () => onDirtyChange(false);
    }, [onDirtyChange, text, uncertain]);
    return (
      <div hidden={pane !== "pending"}>
        <textarea
          aria-label={`Edit ${environmentId}`}
          value={text}
          disabled={unavailable}
          onChange={(event) => setText(event.target.value)}
        />
        <button type="button" onClick={() => setText("")}>
          Clean {environmentId}
        </button>
        <button type="button" onClick={() => setUncertain(true)}>
          Uncertain {environmentId}
        </button>
        {uncertain ? <p>Action outcome unknown for {environmentId}</p> : null}
      </div>
    );
  },
}));

import { UnifiedQueuePage } from "./UnifiedQueuePage";

describe("Prompts environment composition", () => {
  let root: Root;
  let container: HTMLDivElement;
  let originals: typeof fixture.environments;

  beforeEach(() => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    originals = structuredClone(fixture.environments);
    fixture.mounts.clear();
    fixture.blockerStatus = "idle";
    fixture.blockerOptions = null;
    fixture.load
      .mockReset()
      .mockResolvedValue({ status: "unconfigured", reason: "not_configured" });
    fixture.reset.mockReset().mockImplementation(() => {
      fixture.blockerStatus = "idle";
      root.render(<UnifiedQueuePage />);
    });
    fixture.proceed.mockReset().mockImplementation(() => root.render(<p>Destination</p>));
    container = document.createElement("div");
    document.body.append(container);
    root = createRoot(container);
  });

  afterEach(async () => {
    await act(async () => root.unmount());
    container.remove();
    fixture.environments = originals;
    vi.unstubAllGlobals();
  });

  async function render() {
    await act(async () => root.render(<UnifiedQueuePage />));
  }
  function editor(id: string) {
    return container.querySelector<HTMLTextAreaElement>(`[aria-label="Edit ${id}"]`)!;
  }
  async function edit(id: string, value: string) {
    await act(async () => {
      const element = editor(id);
      Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value")!.set!.call(
        element,
        value,
      );
      element.dispatchEvent(new Event("input", { bubbles: true }));
    });
  }
  async function click(text: string) {
    const button = [...container.querySelectorAll("button")].find(
      (item) => item.textContent === text,
    );
    if (!button) throw new Error(`Missing button: ${text}`);
    await act(async () => button.click());
  }
  async function scope(label: string) {
    const trigger = container.querySelector<HTMLButtonElement>(
      '[aria-label="Prompts environment"]',
    )!;
    await act(async () => trigger.click());
    expect(document.querySelector('[role="menu"]')).not.toBeNull();
    const option = [...document.querySelectorAll<HTMLElement>('[role="menuitemcheckbox"]')].find(
      (item) => item.textContent === label,
    );
    if (!option) throw new Error(`Missing environment: ${label}`);
    await act(async () => option.click());
    expect(document.querySelector('[role="menu"]')).toBeNull();
  }
  function groups() {
    return [...container.querySelectorAll('section[aria-label$=" prompts"]')].map((element) =>
      element.getAttribute("aria-label"),
    );
  }
  async function navigate() {
    await act(async () => {
      if (fixture.blockerOptions!.shouldBlockFn()) {
        fixture.blockerStatus = "blocked";
        root.render(<UnifiedQueuePage />);
      } else {
        fixture.proceed();
      }
    });
  }

  it("defaults to All and groups environments across All → single → All without remounting the retained editor", async () => {
    await render();
    expect(groups()).toEqual(["Alpha prompts", "Beta prompts"]);
    expect(container.querySelector("header")!.textContent).toContain(
      "Prompts/All environmentsPendingQueuedSent",
    );
    expect(
      container.querySelector('header [data-slot="toggle-group"]')?.getAttribute("data-size"),
    ).toBe("segmented");
    const alpha = editor("alpha");
    await scope("Alpha");
    expect(groups()).toEqual(["Alpha prompts"]);
    expect(editor("alpha")).toBe(alpha);
    await scope("All environments");
    expect(groups()).toEqual(["Alpha prompts", "Beta prompts"]);
    expect(editor("alpha")).toBe(alpha);
    expect(fixture.mounts.get("alpha")).toBe(1);
    expect(fixture.mounts.get("beta")).toBe(2);
  });

  it("allows narrowing around a dirty retained environment and preserves its guard when All returns", async () => {
    await render();
    await edit("alpha", "Keep Alpha text");
    await scope("Alpha");
    expect(container.textContent).not.toContain("Leave Prompts?");
    expect(editor("alpha").value).toBe("Keep Alpha text");
    expect(fixture.blockerOptions!.enableBeforeUnload()).toBe(true);
    await scope("All environments");
    expect(editor("alpha").value).toBe("Keep Alpha text");
    expect(fixture.blockerOptions!.shouldBlockFn()).toBe(true);
  });

  it("cancels a scope change without losing text, then discards only the removed environment", async () => {
    await render();
    await edit("alpha", "Alpha draft");
    await edit("beta", "Beta draft");
    const alpha = editor("alpha");
    await scope("Alpha");
    expect(container.textContent).toContain("Leave Prompts?");
    expect(groups()).toHaveLength(2);
    await click("Keep reviewing");
    expect(editor("beta").value).toBe("Beta draft");
    await scope("Alpha");
    await click("Leave Prompts");
    expect(groups()).toEqual(["Alpha prompts"]);
    expect(editor("alpha")).toBe(alpha);
    expect(editor("alpha").value).toBe("Alpha draft");
    expect(fixture.blockerOptions!.shouldBlockFn()).toBe(true);
    await scope("All environments");
    expect(editor("beta").value).toBe("");
    expect(editor("alpha").value).toBe("Alpha draft");
  });

  it("keeps the second environment dirty when the first is cleaned", async () => {
    await render();
    await edit("alpha", "Alpha draft");
    await edit("beta", "Beta draft");
    await click("Clean alpha");
    expect(fixture.blockerOptions!.shouldBlockFn()).toBe(true);
    await scope("Alpha");
    expect(container.textContent).toContain("Leave Prompts?");
    await click("Keep reviewing");
    await click("Clean beta");
    expect(fixture.blockerOptions!.shouldBlockFn()).toBe(false);
    expect(fixture.blockerOptions!.enableBeforeUnload()).toBe(false);
    await scope("Alpha");
    expect(groups()).toEqual(["Alpha prompts"]);
  });

  it("protects dirty navigation and preserves uncertain child state across every pane", async () => {
    await render();
    await edit("beta", "Beta navigation draft");
    await click("Uncertain alpha");
    const alpha = editor("alpha");
    const beta = editor("beta");
    for (const label of ["Queued", "Sent", "Pending"]) await click(label);
    expect(editor("alpha")).toBe(alpha);
    expect(editor("beta")).toBe(beta);
    expect(container.textContent).toContain("Action outcome unknown for alpha");
    await navigate();
    expect(container.textContent).toContain("Leave Prompts?");
    await click("Keep reviewing");
    expect(fixture.reset).toHaveBeenCalledOnce();
    expect(editor("beta").value).toBe("Beta navigation draft");
    expect(fixture.blockerOptions!.shouldBlockFn()).toBe(true);
    await navigate();
    await click("Leave Prompts");
    expect(fixture.proceed).toHaveBeenCalledOnce();
    expect(container.textContent).toBe("Destination");
  });

  it("supports arrow, Home and End tab navigation with panel labelling and retained children", async () => {
    await render();
    const alpha = editor("alpha");
    const transitions = [
      ["pending", "ArrowRight", "queued"],
      ["queued", "End", "sent"],
      ["sent", "Home", "pending"],
      ["pending", "ArrowLeft", "sent"],
    ] as const;
    for (const [from, key, to] of transitions) {
      await act(async () =>
        container
          .querySelector(`#queue-${from}-tab`)!
          .dispatchEvent(new KeyboardEvent("keydown", { key, bubbles: true, cancelable: true })),
      );
      const active = container.querySelector(`#queue-${to}-tab`)!;
      expect(active.getAttribute("aria-selected")).toBe("true");
      expect(document.activeElement).toBe(active);
      expect(container.querySelector('[role="tabpanel"]')!.getAttribute("aria-labelledby")).toBe(
        active.id,
      );
      expect(editor("alpha")).toBe(alpha);
    }
    expect(container.textContent).toContain("Sent history is unavailable");
  });

  it("keeps healthy content usable beside disconnected, disabled and unsupported environments", async () => {
    fixture.environments[1]!.connection.phase = "disconnected";
    fixture.environments.push(
      {
        ...structuredClone(fixture.environments[0]!),
        environmentId: "disabled",
        label: "Disabled",
        entry: { enabled: false },
      },
      {
        ...structuredClone(fixture.environments[0]!),
        environmentId: "unsupported",
        label: "Unsupported",
        serverConfig: { environment: { capabilities: { workQueueMetadata: false } } },
      },
    );
    await render();
    expect(editor("alpha").disabled).toBe(false);
    expect(editor("beta").disabled).toBe(true);
    const beta = container.querySelector('[aria-label="Beta prompts"]')!;
    expect(beta.textContent).toContain("This environment is disconnected");
    expect(container.querySelector('[aria-label="Disabled prompts"]')!.textContent).toContain(
      "This environment is disabled",
    );
    await click("Queued");
    expect(container.querySelector('[aria-label="Unsupported prompts"]')!.textContent).toContain(
      "Queue metadata unsupported",
    );
    expect(container.querySelector('[aria-label="Alpha prompts"]')!.textContent).toContain(
      "not configured",
    );
    expect(fixture.load).toHaveBeenCalledTimes(1);
  });

  it("distinguishes an empty environment catalog from an unavailable connection", async () => {
    fixture.environments = [];
    await render();
    expect(container.textContent).toContain("No environments configured.");
    expect(groups()).toEqual([]);
    expect(fixture.load).not.toHaveBeenCalled();
  });
});
