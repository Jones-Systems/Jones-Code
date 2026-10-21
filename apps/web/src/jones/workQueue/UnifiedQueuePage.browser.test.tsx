import { act, render as renderScreen, type Screen } from "../../../test/render";
import { useEffect, useState, type ReactNode } from "react";
import { describe, expect, it, vi } from "vite-plus/test";
import type { WorkQueueMetadataResult, VoiceReviewRecentList } from "@t3tools/contracts";

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
  voiceList: vi.fn(),
  selfIdentity: vi.fn(),
  recent: null as VoiceReviewRecentList | null,
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

vi.mock("../../state/environments", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../state/environments")>()),
  useEnvironments: () => ({ environments: fixture.environments }),
}));
vi.mock("@tanstack/react-router", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@tanstack/react-router")>()),
  useBlocker: (options: NonNullable<typeof fixture.blockerOptions>) => {
    fixture.blockerOptions = options;
    return { status: fixture.blockerStatus, reset: fixture.reset, proceed: fixture.proceed };
  },
}));
vi.mock("../../env", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../env")>()),
  isElectron: false,
}));
vi.mock("../../components/ui/sidebar", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../components/ui/sidebar")>()),
  SidebarInset: ({ children }: { children: ReactNode }) => <main>{children}</main>,
}));
vi.mock("./useWorkQueueMetadata", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./useWorkQueueMetadata")>()),
  useWorkQueueMetadata: () => fixture.load,
}));
vi.mock("../../components/voiceReview/useVoiceReview", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../components/voiceReview/useVoiceReview")>()),
  useVoiceReview: () => ({ fetchList: fixture.voiceList, fetchSelfIdentity: fixture.selfIdentity }),
}));
vi.mock("../../components/voiceReview/VoiceReviewPage", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../components/voiceReview/VoiceReviewPage")>()),
  EnvironmentVoiceReview: ({
    environmentId,
    pane,
    unavailable,
    onDirtyChange,
    onRecentChange,
    queuedEntriesOverride,
  }: {
    environmentId: string;
    pane: string;
    unavailable: boolean;
    onDirtyChange: (dirty: boolean) => void;
    onRecentChange?: (recent: VoiceReviewRecentList | null) => void;
    queuedEntriesOverride?: readonly VoiceReviewRecentList["entries"][number][];
  }) => {
    useEffect(() => {
      onRecentChange?.(fixture.recent);
    }, [onRecentChange]);
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
      <div hidden={pane === "sent"}>
        {pane === "queued"
          ? queuedEntriesOverride?.map((entry) => (
              <p key={entry.draft.id}>Queued voice: {entry.text}</p>
            ))
          : null}
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

import { voiceReviewRecentFixture } from "@t3tools/client-runtime/voice-review/fixtures";
import { UnifiedQueuePage } from "./UnifiedQueuePage";

describe("Prompts environment composition", () => {
  let root: Screen;
  let renderDisposables: AsyncDisposableStack;
  let container: HTMLDivElement;
  let originals: typeof fixture.environments;

  async function setupFixture1() {
    const stack = new AsyncDisposableStack();
    renderDisposables = stack;
    stack.defer(async () => {
      await act(async () => root.unmount());
      container.remove();
      fixture.environments = originals;
      vi.unstubAllGlobals();
    });
    try {
      originals = structuredClone(fixture.environments);
      fixture.mounts.clear();
      fixture.recent = null;
      fixture.selfIdentity.mockReset();
      fixture.voiceList.mockReset().mockResolvedValue({ drafts: [] });
      fixture.blockerStatus = "idle";
      fixture.blockerOptions = null;
      fixture.load
        .mockReset()
        .mockResolvedValue({ status: "unconfigured", reason: "not_configured" });
      fixture.reset.mockReset().mockImplementation(async () => {
        fixture.blockerStatus = "idle";
        await root.rerender(<UnifiedQueuePage />);
      });
      fixture.proceed.mockReset().mockImplementation(() => root.rerender(<p>Destination</p>));
      container = document.createElement("div");
      document.body.append(container);
      root = renderDisposables.use(await renderScreen(null, { container, router: false }));

      return stack;
    } catch (error) {
      await stack.disposeAsync();
      throw error;
    }
  }

  function setConnection(environmentId: string, phase: string) {
    fixture.environments = fixture.environments.map((environment) =>
      environment.environmentId === environmentId
        ? { ...environment, connection: { ...environment.connection, phase } }
        : environment,
    );
  }
  async function render() {
    await act(async () => root.rerender(<UnifiedQueuePage />));
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
        await root.rerender(<UnifiedQueuePage />);
      } else {
        await fixture.proceed();
      }
    });
  }

  it("shows one configured source across all stages without offering unconfigured servers", async () => {
    await using _fixture = await setupFixture1();
    fixture.environments[0]!.label = "VPS";
    fixture.environments[1]!.label = "Mac Mini";
    fixture.voiceList
      .mockResolvedValueOnce({ drafts: [] })
      .mockRejectedValueOnce({ _tag: "VoiceReviewNotConfiguredError" });
    await render();
    expect(groups()).toEqual(["VPS prompts"]);
    expect(container.querySelector('[aria-label="Prompts environment"]')).toBeNull();
    expect(container.querySelector('[aria-label="Mac Mini prompts"]')).toBeNull();
    for (const stage of ["Queued", "Sent", "Pending"]) {
      await click(stage);
      expect(groups()).toEqual(["VPS prompts"]);
      expect(container.querySelector('[aria-label="Mac Mini prompts"]')).toBeNull();
    }
  });

  it("defaults to All and groups environments across All → single → All without remounting the retained editor", async () => {
    await using _fixture = await setupFixture1();
    await render();
    expect(groups()).toEqual(["Alpha prompts", "Beta prompts"]);
    expect(container.querySelector("header")!.textContent).toContain(
      "Prompts/All prompt sourcesPendingQueuedSent",
    );
    expect(
      container.querySelector('header [data-slot="toggle-group"]')?.getAttribute("data-size"),
    ).toBe("segmented");
    const alpha = editor("alpha");
    await scope("Alpha");
    expect(groups()).toEqual(["Alpha prompts"]);
    expect(editor("alpha")).toBe(alpha);
    await scope("All prompt sources");
    expect(groups()).toEqual(["Alpha prompts", "Beta prompts"]);
    expect(editor("alpha")).toBe(alpha);
    expect(fixture.mounts.get("alpha")).toBe(1);
    expect(fixture.mounts.get("beta")).toBe(2);
  });

  it("allows narrowing around a dirty retained environment and preserves its guard when All returns", async () => {
    await using _fixture = await setupFixture1();
    await render();
    await edit("alpha", "Keep Alpha text");
    await scope("Alpha");
    expect(container.textContent).not.toContain("Leave Prompts?");
    expect(editor("alpha").value).toBe("Keep Alpha text");
    expect(fixture.blockerOptions!.enableBeforeUnload()).toBe(true);
    await scope("All prompt sources");
    expect(editor("alpha").value).toBe("Keep Alpha text");
    expect(fixture.blockerOptions!.shouldBlockFn()).toBe(true);
  });

  it("cancels a scope change without losing text, then discards only the removed environment", async () => {
    await using _fixture = await setupFixture1();
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
    await scope("All prompt sources");
    expect(editor("beta").value).toBe("");
    expect(editor("alpha").value).toBe("Alpha draft");
  });

  it("keeps the second environment dirty when the first is cleaned", async () => {
    await using _fixture = await setupFixture1();
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
    await using _fixture = await setupFixture1();
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

  it("moves only a proved linked delivery from Queued to one Sent row", async () => {
    await using _fixture = await setupFixture1();
    fixture.environments = [
      {
        ...fixture.environments[0]!,
        serverConfig: {
          environment: { environmentId: "alpha", capabilities: { workQueueMetadata: true } },
        },
      } as (typeof fixture.environments)[number],
    ];
    fixture.recent = {
      ...voiceReviewRecentFixture,
      entries: [
        {
          ...voiceReviewRecentFixture.entries[0]!,
          text: "Linked prompt text",
          queue_delivery_links: {
            status: "ready",
            reason: null,
            unlinked_delivery_count: 0,
            deliveries: [
              {
                delivery_id: "delivery",
                queue_request_id: "request",
                payload_sha256: "b".repeat(64),
                destination: {
                  backend: "t3",
                  instance: "opaque-instance",
                  thread_id: "thread",
                  project_id: null,
                },
              },
            ],
          },
        },
      ],
    };
    fixture.load.mockResolvedValue({
      status: "partial",
      expires_at_ms: Date.now() + 60000,
      snapshot: {
        schema: "codex.t3-work-queue-metadata/v2",
        source: {
          queue_id: "queue",
          host_id: "host",
          environment_ref: "alpha",
          exporter_instance_id: "exporter",
        },
        observed_at_ms: Date.now(),
        snapshot_token: "a".repeat(64),
        coverage: "partial",
        authority_effect: "none",
        items: [
          {
            request_id: "request",
            workstream_id: "workstream",
            canonical_binding: null,
            entry_kind: "ordinary",
            request_kind: "initial",
            lane: "normal",
            queue_state: "waiting_remote",
            submitted_at_ms: null,
            target: { host_id: "host", environment_ref: "alpha", thread_id: "thread" },
            dispatch_status: "accepted",
            native_command_status: "accepted",
            finish_line: "not_tracked",
            payload_sha256: "b".repeat(64),
            native_handoff: {
              command_id: "command",
              message_id: "message",
              command_status: "accepted",
              correlation_status: "exact",
              receipt_sequence: 0,
              turn_id: "turn",
              turn_state: "pending",
              observed_at_ms: Date.now() - 1000,
            },
          },
        ],
      },
    });
    await render();
    await click("Queued");
    expect(container.textContent).not.toContain("Queued voice: Linked prompt text");
    await click("Sent");
    expect(container.querySelectorAll('[aria-label="Sent prompts"] article')).toHaveLength(1);
    expect(container.textContent).toContain("Linked prompt text");
    expect(container.textContent).toContain("Provider turn: pending");
    expect(container.textContent).toContain("Coverage: partial");
  });

  it("supports arrow, Home and End tab navigation with panel labelling and retained children", async () => {
    await using _fixture = await setupFixture1();
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

  it("keeps a discovered source visible and its editor disabled after disconnect", async () => {
    await using _fixture = await setupFixture1();
    await render();
    setConnection("beta", "disconnected");
    await render();
    expect(editor("alpha").disabled).toBe(false);
    expect(editor("beta").disabled).toBe(true);
    expect(container.querySelector('[aria-label="Beta prompts"]')!.textContent).toContain(
      "This environment is disconnected",
    );
  });

  it("retains read failures as unknown and allows retry to discover a configured source", async () => {
    await using _fixture = await setupFixture1();
    fixture.voiceList.mockRejectedValue({ _tag: "VoiceReviewForbiddenError" });
    fixture.load.mockRejectedValue(new Error("Offline"));
    await render();
    expect(groups()).toEqual([]);
    expect(container.textContent).toContain("Could not check prompt sources on Alpha");
    expect(container.textContent).not.toContain("No prompt sources configured");
    fixture.voiceList.mockResolvedValue({ drafts: [] });
    await click("Retry sources");
    expect(groups()).toEqual(["Alpha prompts", "Beta prompts"]);
  });

  it("shows an empty state only after every source explicitly reports absence", async () => {
    await using _fixture = await setupFixture1();
    fixture.voiceList.mockRejectedValue({ _tag: "VoiceReviewNotConfiguredError" });
    await render();
    expect(groups()).toEqual([]);
    expect(container.textContent).toContain("No prompt sources configured");
    expect(container.querySelector('[aria-label="Alpha prompts"]')).toBeNull();
    expect(container.querySelector('[aria-label="Prompts environment"]')).toBeNull();
  });

  it("keeps a configured queue visible when its sample cannot be read", async () => {
    await using _fixture = await setupFixture1();
    fixture.voiceList.mockRejectedValue({ _tag: "VoiceReviewNotConfiguredError" });
    fixture.load.mockResolvedValue({ status: "unavailable", reason: "source_unavailable" });
    await render();
    expect(groups()).toEqual(["Alpha prompts", "Beta prompts"]);
    await click("Queued");
    expect(container.textContent).toContain("the source cannot be read");
  });

  it("does not mistake an undiscovered disconnected server for an absent source", async () => {
    await using _fixture = await setupFixture1();
    setConnection("beta", "disconnected");
    await render();
    expect(groups()).toEqual(["Alpha prompts"]);
    expect(container.textContent).toContain(
      "Cannot check prompt sources on Beta: server disconnected",
    );
    expect(fixture.voiceList).toHaveBeenCalledTimes(1);
    setConnection("beta", "connected");
    await render();
    expect(groups()).toEqual(["Alpha prompts", "Beta prompts"]);
  });

  it("allows review on a connected source while its queue check waits and another server is offline", async () => {
    await using _fixture = await setupFixture1();
    setConnection("beta", "disconnected");
    fixture.load.mockImplementation(() => new Promise(() => {}));
    await render();
    expect(groups()).toEqual(["Alpha prompts"]);
    expect(editor("alpha").disabled).toBe(false);
    await act(async () => {
      const textarea = editor("alpha");
      Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value")!.set!.call(
        textarea,
        "Review while another source is offline",
      );
      textarea.dispatchEvent(new Event("input", { bubbles: true }));
    });
    expect(editor("alpha").value).toBe("Review while another source is offline");
    expect(container.textContent).toContain("Cannot check prompt sources on Beta");
    expect(fixture.load).toHaveBeenCalledTimes(1);
  });

  it("shows a connected queue before its voice check settles with another server offline", async () => {
    await using _fixture = await setupFixture1();
    setConnection("beta", "disconnected");
    fixture.voiceList.mockImplementation(() => new Promise(() => {}));
    fixture.load.mockResolvedValue({ status: "unavailable", reason: "source_unavailable" });
    await render();
    expect(groups()).toEqual(["Alpha prompts"]);
    await click("Queued");
    expect(container.textContent).toContain("the source cannot be read");
    expect(container.textContent).toContain("Cannot check prompt sources on Beta");
    expect(fixture.voiceList).toHaveBeenCalledTimes(1);
  });

  it("explains that connected servers have no sources without blaming the offline server", async () => {
    await using _fixture = await setupFixture1();
    setConnection("beta", "disconnected");
    fixture.voiceList.mockRejectedValue({ _tag: "VoiceReviewNotConfiguredError" });
    await render();
    expect(container.textContent).toContain("No prompt review or queue is configured on Alpha.");
    expect(container.textContent).toContain(
      "Reconnect unavailable servers to access their prompts.",
    );
    expect(container.textContent).not.toContain("Prompt sources have not all been checked.");
  });

  it.each(["VoiceReviewForbiddenError", "VoiceReviewNotConfiguredError"])(
    "keeps %s discovery honest without device setup or identity reads",
    async (_tag) => {
      await using _fixture = await setupFixture1();
      setConnection("beta", "disconnected");
      fixture.voiceList.mockRejectedValue({ _tag });
      await render();
      expect(container.textContent).not.toContain("Set up this device");
      expect(container.querySelector('[aria-label$="device setup"]')).toBeNull();
      expect(fixture.selfIdentity).not.toHaveBeenCalled();
      expect(container.textContent).toContain("Prompt review is unavailable on Alpha.");
      expect(container.textContent).toContain(
        _tag === "VoiceReviewNotConfiguredError"
          ? "No prompt review or queue is configured on Alpha."
          : "Could not check prompt sources on Alpha",
      );
      expect(container.textContent).toContain(
        "Reconnect unavailable servers to access their prompts.",
      );
    },
  );

  it("ignores a superseded source check after disconnect and retry", async () => {
    await using _fixture = await setupFixture1();
    setConnection("beta", "disconnected");
    let rejectOld!: (error: unknown) => void;
    fixture.voiceList.mockImplementationOnce(
      () =>
        new Promise((_resolve, reject) => {
          rejectOld = reject;
        }),
    );
    await render();
    setConnection("alpha", "disconnected");
    await render();
    setConnection("alpha", "connected");
    await render();
    expect(groups()).toEqual(["Alpha prompts"]);
    await act(async () => rejectOld({ _tag: "VoiceReviewNotConfiguredError" }));
    expect(groups()).toEqual(["Alpha prompts"]);
    expect(editor("alpha").disabled).toBe(false);
  });

  it("distinguishes an empty environment catalog from an unavailable connection", async () => {
    await using _fixture = await setupFixture1();
    fixture.environments = [];
    await render();
    expect(container.textContent).toContain("No prompt sources configured.");
    expect(groups()).toEqual([]);
    expect(fixture.load).not.toHaveBeenCalled();
  });
});
