// @vitest-environment jsdom
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import type { WorkQueueMetadata, WorkQueueMetadataResult } from "@t3tools/contracts";
import { voiceReviewRecentFixture } from "@t3tools/client-runtime/voice-review/fixtures";
import {
  WorkQueueMetadataPanel,
  type WorkQueueMetadataLoader,
} from "../../jones/workQueue/WorkQueueMetadataPanel";
import { WorkQueuePanel } from "./WorkQueuePanel";
import { WorkQueuePreview } from "./WorkQueuePreview";
import {
  createMockWorkQueueSource,
  type WorkQueueItem,
  type WorkQueueSaveResult,
  type WorkQueueSource,
} from "./mockWorkQueue";

const first: WorkQueueItem = {
  id: "one",
  snapshotToken: "v1",
  text: "Original text",
  statusLabel: "Pending",
  submittedAt: null,
  targetLabel: "Example target",
  editability: "editable",
};
const second: WorkQueueItem = { ...first, id: "two", text: "Second text" };
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function makeSource(items: readonly WorkQueueItem[] = [first, second]) {
  return {
    mode: "mock" as const,
    load: vi.fn(async () => items),
    beginEdit: vi.fn((id: string) => items.find((item) => item.id === id)),
    finishEdit: vi.fn((id: string) => items.find((item) => item.id === id)),
    setManualPause: vi.fn((id: string) => items.find((item) => item.id === id)),
    tick: vi.fn(() => items),
    sendNow: vi.fn((): WorkQueueSaveResult => ({
      kind: "error",
      message: "Disabled test send",
      effect: "none",
    })),
    save: vi.fn(async (): Promise<WorkQueueSaveResult> => ({
      kind: "saved",
      item: { ...first, text: "Edited text", snapshotToken: "v2" },
    })),
  };
}

describe("submitted work preview", () => {
  let root: Root;
  let container: HTMLDivElement;
  beforeEach(() => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    container = document.createElement("div");
    document.body.append(container);
    root = createRoot(container);
  });
  afterEach(async () => {
    await act(() => root.unmount());
    container.remove();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });
  async function render(source: WorkQueueSource) {
    await act(() => root.render(<WorkQueuePanel source={source} />));
  }
  function button(text: string) {
    const found = [...container.querySelectorAll("button")].find(
      (item) => item.textContent === text || item.textContent?.includes(text),
    );
    if (!found) throw new Error(`Missing button: ${text}`);
    return found;
  }
  async function click(text: string) {
    await act(() => button(text).click());
  }
  function draft() {
    return container.querySelector("textarea")!;
  }
  async function edit(value = "Edited text") {
    await act(() => {
      Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value")!.set!.call(
        draft(),
        value,
      );
      draft().dispatchEvent(new Event("input", { bubbles: true }));
    });
  }
  async function key(options: KeyboardEventInit = {}) {
    await act(() =>
      draft().dispatchEvent(
        new KeyboardEvent("keydown", { key: "Enter", bubbles: true, cancelable: true, ...options }),
      ),
    );
  }
  it("toggles the row pause icon to resume and back without sending", async () => {
    const source = createMockWorkQueueSource();
    const send = vi.spyOn(source, "sendNow");
    await render(source);
    const pause = container.querySelector<HTMLButtonElement>('[aria-label="Pause pending"]')!;
    expect(pause.querySelector("svg")).not.toBeNull();
    await act(() => pause.click());
    expect((await source.load())[0]!.pause).toBe("manual");
    const resume = container.querySelector<HTMLButtonElement>('[aria-label="Resume pending"]')!;
    expect(resume.querySelector("svg")).not.toBeNull();
    await act(() => resume.click());
    expect((await source.load())[0]!.pause).toBeUndefined();
    expect(container.querySelector('[aria-label="Pause pending"]')).not.toBeNull();
    expect(send).not.toHaveBeenCalled();
  });
  it("guards device navigation and unload while preserving a dirty preview draft", async () => {
    await act(() => root.render(<WorkQueuePreview />));
    const cleanUnload = new Event("beforeunload", { cancelable: true });
    window.dispatchEvent(cleanUnload);
    expect(cleanUnload.defaultPrevented).toBe(false);
    await edit("Preserve this preview draft");
    const dirtyUnload = new Event("beforeunload", { cancelable: true });
    window.dispatchEvent(dirtyUnload);
    expect(dirtyUnload.defaultPrevented).toBe(true);
    await click("Connect a device");
    expect(container.textContent).toContain("Leave this preview?");
    expect(draft().value).toBe("Preserve this preview draft");
    await click("Keep editing");
    expect(container.textContent).not.toContain("Leave this preview?");
    expect(draft().value).toBe("Preserve this preview draft");
    await click("Discard changes");
    const discardedUnload = new Event("beforeunload", { cancelable: true });
    window.dispatchEvent(discardedUnload);
    expect(discardedUnload.defaultPrevented).toBe(false);
  });
  it("updates once with Enter while Shift+Enter, IME and repeated Enter do not save", async () => {
    const source = makeSource();
    await render(source);
    await edit();
    await key({ shiftKey: true });
    await key({ isComposing: true });
    await key({ repeat: true });
    expect(source.save).not.toHaveBeenCalled();
    await key();
    expect(source.save).toHaveBeenCalledTimes(1);
    expect(source.sendNow).not.toHaveBeenCalled();
    await edit("   ");
    await key();
    expect(source.save).toHaveBeenCalledTimes(1);
  });
  it("pauses on first edit, resets grace on updates, and expires without submitting", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-10-04T12:00:00Z"));
    const source = createMockWorkQueueSource();
    await render(source);
    expect((await source.load())[0]!.pause).toBeUndefined();
    await edit();
    expect((await source.load())[0]!.pause).toBe("editing");
    await act(() => vi.advanceTimersByTime(180_000));
    expect((await source.load())[0]!.pause).toBe("editing");
    await key();
    expect((await source.load())[0]!.pause).toBe("grace");
    await act(() => vi.advanceTimersByTime(119_000));
    await edit("Second edit");
    await act(() => vi.advanceTimersByTime(5_000));
    expect((await source.load())[0]!.pause).toBe("editing");
    await key();
    await act(() => vi.advanceTimersByTime(119_000));
    expect((await source.load())[0]!.pause).toBe("grace");
    await act(() => vi.advanceTimersByTime(1_000));
    expect((await source.load())[0]!.pause).toBeUndefined();
    expect((await source.load())[0]!.statusLabel).toBe("Pending");
    expect(container.querySelector('[aria-label="Recently submitted"]')!.textContent).not.toContain(
      "Second edit",
    );
  });
  it("does not lose a pending save acknowledgment when another row's grace expires", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-10-04T12:00:00Z"));
    const source = createMockWorkQueueSource();
    const other = (await source.load())[1]!;
    source.finishEdit(other.id);
    const pending = deferred<WorkQueueSaveResult>();
    const originalSave = source.save.bind(source);
    source.save = vi.fn(() => pending.promise);
    await render(source);
    await edit();
    await key();
    await act(() => vi.advanceTimersByTime(125_000));
    expect(draft().disabled).toBe(true);
    const result = await originalSave({
      id: "pending",
      baseToken: "sample-pending-1",
      text: "Edited text",
    });
    await act(() => pending.resolve(result));
    expect(draft().disabled).toBe(false);
    expect(container.textContent).toContain("No unsaved changes");
    await act(() => vi.advanceTimersByTime(1));
    expect((await source.load()).find((item) => item.id === other.id)!.pause).toBeUndefined();
  });
  it("lets a dirty automatic edit pause become an indefinite manual pause", async () => {
    const source = createMockWorkQueueSource();
    await render(source);
    await edit();
    await act(() =>
      container.querySelector<HTMLButtonElement>('[aria-label="Pause pending"]')!.click(),
    );
    expect((await source.load())[0]!.pause).toBe("manual");
    expect(
      container.querySelector<HTMLButtonElement>('[aria-label="Resume pending"]')!.disabled,
    ).toBe(true);
    await key();
    expect((await source.load())[0]!.pause).toBe("manual");
  });
  it.each(["conflict", "unknown"] as const)(
    "keeps %s edits protected beyond the grace interval",
    async (outcome) => {
      vi.useFakeTimers();
      const source = createMockWorkQueueSource();
      const original = (await source.load())[0]!;
      source.save = vi.fn(async (): Promise<WorkQueueSaveResult> =>
        outcome === "conflict"
          ? {
              kind: "conflict",
              current: { ...original, text: "Concurrent edit", snapshotToken: "concurrent" },
            }
          : { kind: "error", message: "Unknown effect", effect: "unknown" },
      );
      await render(source);
      await edit();
      await key();
      await act(() => vi.advanceTimersByTime(180_000));
      expect(draft().value).toBe("Edited text");
      expect(button("Send now (mock)").disabled).toBe(true);
      expect(source.tick([])[0]!.pause).toBe("editing");
    },
  );
  it("keeps manual pause through save, discard and clock advancement", async () => {
    vi.useFakeTimers();
    const source = createMockWorkQueueSource();
    await render(source);
    await act(() =>
      container.querySelector<HTMLButtonElement>('[aria-label="Pause pending"]')!.click(),
    );
    await edit();
    await key();
    await edit("Discard this");
    await click("Discard changes");
    await act(() => vi.advanceTimersByTime(300_000));
    expect((await source.load())[0]!.pause).toBe("manual");
    expect((await source.load())[0]!.text).toBe("Edited text");
    expect(button("Send now (mock)").disabled).toBe(false);
  });
  it("sends the latest acknowledged text once into visible read-only history", async () => {
    const source = createMockWorkQueueSource();
    await render(source);
    await edit("Latest acknowledged text");
    expect(button("Send now (mock)").disabled).toBe(true);
    await key();
    await click("Send now (mock)");
    const history = container.querySelector('[aria-label="Recently submitted"]')!;
    expect(history.textContent).toContain("Latest acknowledged text");
    expect(history.querySelectorAll("article")).toHaveLength(2);
    expect(history.querySelector("button, textarea")).toBeNull();
    expect(container.querySelector("textarea")).toBeNull();
    expect(
      (await source.load()).filter(
        (item) => item.id === "pending" && item.statusLabel === "Submitted (mock)",
      ),
    ).toHaveLength(1);
  });
  it("clears dirty only after a matching save acknowledgment and freezes selection while saving", async () => {
    const source = makeSource();
    const pending = deferred<WorkQueueSaveResult>();
    source.save.mockImplementation(() => pending.promise);
    await render(source);
    await edit();
    await click("Save mock edit");
    expect(draft().disabled).toBe(true);
    expect(button("Second text").disabled).toBe(true);
    expect(source.save).toHaveBeenCalledWith({ id: "one", baseToken: "v1", text: "Edited text" });
    await act(() =>
      pending.resolve({
        kind: "saved",
        item: { ...first, text: "Edited text", snapshotToken: "v2" },
      }),
    );
    expect(container.textContent).toContain("No unsaved changes");
    expect(button("Save mock edit").disabled).toBe(true);
    expect(button("Edited text").textContent).toContain("Edited text");
  });
  it("requires explicit discard when selecting another submission", async () => {
    await render(makeSource());
    await edit();
    await click("Second text");
    await click("Keep editing");
    expect(draft().value).toBe("Edited text");
    await click("Second text");
    await click("Discard changes and switch");
    expect(draft().value).toBe("Second text");
  });
  it("keeps the draft and base token through refresh and shows current text on conflict", async () => {
    const source = makeSource();
    await render(source);
    await edit();
    source.load.mockResolvedValue([
      { ...first, text: "Current text", snapshotToken: "v2" },
      second,
    ]);
    await click("Refresh sample data");
    expect(draft().value).toBe("Edited text");
    expect(container.textContent).toContain("Current text");
    expect(button("Save mock edit").disabled).toBe(true);
    await click("Reload current text and discard draft");
    expect(draft().value).toBe("Current text");
    await edit();
    source.save.mockResolvedValue({
      kind: "conflict",
      current: { ...first, text: "Newest text", snapshotToken: "v3" },
    });
    await click("Save mock edit");
    expect(source.save).toHaveBeenCalledWith({ id: "one", baseToken: "v2", text: "Edited text" });
    expect(draft().value).toBe("Edited text");
    expect(container.textContent).toContain("Newest text");
  });
  it("never offers saves for reserved, delivered, or UNKNOWN submissions and renders literal text safely", async () => {
    const source = makeSource(
      ["Reserved", "UNKNOWN", "Held"].map((statusLabel, index) => ({
        ...first,
        id: String(index),
        statusLabel,
        text: `<script>${statusLabel}</script>`,
        editability: "readonly" as const,
        reason: `${statusLabel} cannot be edited.`,
      })),
    );
    await render(source);
    for (const status of ["Reserved", "UNKNOWN", "Held"]) {
      await click(`<script>${status}</script>`);
      expect(container.querySelector("textarea")).toBeNull();
      expect(container.textContent).toContain(`${status} cannot be edited.`);
      expect(container.querySelector("script")).toBeNull();
    }
    expect(source.save).not.toHaveBeenCalled();
  });
  it.each(["held", "none", "unknown"] as const)(
    "preserves drafts for %s outcomes without retrying",
    async (outcome) => {
      const source = makeSource();
      source.save.mockResolvedValue(
        outcome === "held"
          ? { kind: "held", current: first, reason: "Target held" }
          : { kind: "error", message: "Save unavailable", effect: outcome },
      );
      await render(source);
      await edit();
      await click("Save mock edit");
      expect(draft().value).toBe("Edited text");
      expect(source.save).toHaveBeenCalledTimes(1);
      if (outcome === "unknown") {
        expect(button("Save mock edit").disabled).toBe(true);
        expect(button("Second text").disabled).toBe(true);
        await click("Read back sample data");
        expect(draft().value).toBe("Edited text");
        expect(button("Save mock edit").disabled).toBe(false);
        expect(source.save).toHaveBeenCalledTimes(1);
      }
    },
  );
  it("keeps a thrown save effect unknown until an explicit successful readback", async () => {
    const source = makeSource();
    source.save.mockRejectedValue(new Error("Lost response"));
    await render(source);
    await edit();
    await click("Save mock edit");
    expect(draft().value).toBe("Edited text");
    expect(button("Save mock edit").disabled).toBe(true);
    source.load.mockRejectedValueOnce(new Error("Readback failed"));
    await click("Read back sample data");
    expect(button("Save mock edit").disabled).toBe(true);
    await click("Read back sample data");
    expect(button("Save mock edit").disabled).toBe(false);
    expect(source.save).toHaveBeenCalledTimes(1);
  });
  it("ignores a read begun before a save so it cannot overwrite the acknowledged text", async () => {
    const source = makeSource();
    await render(source);
    await edit();
    const read = deferred<readonly WorkQueueItem[]>();
    source.load.mockImplementationOnce(() => read.promise);
    await click("Refresh sample data");
    await click("Save mock edit");
    await act(() => read.resolve([first, second]));
    expect(draft().value).toBe("Edited text");
    expect(container.textContent).toContain("No unsaved changes");
  });
  it("ignores a save completion from a replaced source", async () => {
    const source = makeSource();
    const pending = deferred<WorkQueueSaveResult>();
    source.save.mockImplementation(() => pending.promise);
    await render(source);
    await edit();
    await click("Save mock edit");
    await render(makeSource([{ ...first, text: "Replacement text" }]));
    await act(() =>
      pending.resolve({
        kind: "saved",
        item: { ...first, text: "Edited text", snapshotToken: "v2" },
      }),
    );
    expect(draft().value).toBe("Replacement text");
  });
  it("blocks further saves on mismatched acknowledgments", async () => {
    const source = makeSource();
    source.save.mockResolvedValue({ kind: "saved", item: { ...first, text: "Different text" } });
    await render(source);
    await edit();
    await click("Save mock edit");
    expect(draft().value).toBe("Edited text");
    expect(button("Save mock edit").disabled).toBe(true);
    expect(container.textContent).toContain("did not match");
  });
});

describe("mock adapter isolation", () => {
  it("compares opaque tokens and changes only its own in-memory instance", async () => {
    const source = createMockWorkQueueSource();
    const item = (await source.load())[0]!;
    expect((await source.save({ id: item.id, baseToken: "wrong", text: "Edit" })).kind).toBe(
      "conflict",
    );
    expect(
      (await source.save({ id: item.id, baseToken: item.snapshotToken, text: "Edit" })).kind,
    ).toBe("saved");
    expect((await source.load())[0]!.text).toBe("Edit");
    expect((await createMockWorkQueueSource().load())[0]!.text).toBe(item.text);
    expect(
      (await source.save({ id: item.id, baseToken: item.snapshotToken, text: "Stale" })).kind,
    ).toBe("conflict");
  });
  it("rejects edits to read-only sample rows", async () => {
    const source = createMockWorkQueueSource();
    for (const item of (await source.load()).filter((row) => row.editability === "readonly")) {
      expect(
        (await source.save({ id: item.id, baseToken: item.snapshotToken, text: "Changed" })).kind,
      ).toBe("held");
      expect((await source.load()).find((row) => row.id === item.id)!.text).toBe(item.text);
    }
  });
});

describe("mock pause clock", () => {
  it("protects deadlines, resets discarded grace, and never dispatches on expiry", async () => {
    let clock = 0;
    const source = createMockWorkQueueSource(() => clock);
    const item = (await source.load())[0]!;
    source.beginEdit(item.id);
    const saved = await source.save({ id: item.id, baseToken: item.snapshotToken, text: "Latest" });
    expect(saved.kind).toBe("saved");
    clock = 120_000;
    expect(source.tick([item.id])[0]!.pause).toBe("grace");
    source.finishEdit(item.id);
    clock = 239_999;
    expect(source.tick([])[0]!.pause).toBe("grace");
    clock++;
    const ready = source.tick([])[0]!;
    expect(ready.pause).toBeUndefined();
    expect(ready.statusLabel).toBe("Pending");
    expect(source.sendNow({ id: ready.id, baseToken: ready.snapshotToken }).kind).toBe("saved");
    expect(source.sendNow({ id: ready.id, baseToken: ready.snapshotToken }).kind).toBe("held");
  });
  it("manual pauses survive indefinite time and updates until explicitly resumed", async () => {
    let clock = 0;
    const source = createMockWorkQueueSource(() => clock);
    const item = (await source.load())[0]!;
    source.setManualPause(item.id, true);
    source.beginEdit(item.id);
    await source.save({ id: item.id, baseToken: item.snapshotToken, text: "Changed" });
    source.finishEdit(item.id);
    clock = 900_000;
    expect(source.tick([])[0]!.pause).toBe("manual");
    const ready = source.setManualPause(item.id, false)!;
    expect(source.sendNow({ id: ready.id, baseToken: ready.snapshotToken })).toMatchObject({
      kind: "saved",
      item: { text: "Changed" },
    });
  });
});

describe("submitted work metadata", () => {
  let root: Root;
  let container: HTMLDivElement;
  const sample = (): WorkQueueMetadata => ({
    schema: "codex.t3-work-queue-metadata/v1",
    source: {
      queue_id: "queue",
      host_id: "host",
      environment_ref: "environment",
      exporter_instance_id: "worker",
    },
    observed_at_ms: Date.now(),
    snapshot_token: "a".repeat(64),
    coverage: "complete",
    authority_effect: "none",
    items: [
      {
        request_id: "request-legacy",
        workstream_id: "raw-legacy-id",
        canonical_binding: null,
        entry_kind: "ordinary",
        request_kind: "initial",
        lane: "normal",
        queue_state: "unknown",
        submitted_at_ms: null,
        target: null,
        dispatch_status: "unknown",
        native_command_status: null,
        finish_line: "not_tracked",
      },
      {
        request_id: "request-canonical",
        workstream_id: "exact-canonical-id",
        canonical_binding: {
          owner_id: "owner",
          server_generation: 1,
          registry_version: 2,
          membership_id: "membership",
          native_reference_id: "reference",
          source_instance_id: "instance",
          native_thread_id: "thread",
          authority_namespace: "namespace",
          store_generation: 1,
          expires_at: "2099-01-01T00:00:00Z",
        },
        entry_kind: "flexible",
        request_kind: "owner_followup",
        lane: "high",
        queue_state: "observed_terminal",
        submitted_at_ms: null,
        target: { host_id: "host", environment_ref: "environment", thread_id: "thread" },
        dispatch_status: "accepted",
        native_command_status: "accepted",
        finish_line: "not_tracked",
      },
    ],
  });
  beforeEach(() => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    container = document.createElement("div");
    document.body.append(container);
    root = createRoot(container);
  });
  afterEach(async () => {
    await act(() => root.unmount());
    container.remove();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });
  async function render(load: WorkQueueMetadataLoader, key = "environment-a") {
    await act(() => root.render(<WorkQueueMetadataPanel key={key} load={load} />));
  }
  const ready = (): WorkQueueMetadataResult => ({
    status: "ready",
    snapshot: sample(),
    expires_at_ms: Date.now() + 60_000,
  });
  it("shows raw and canonical identities, unknown outcomes and no mutation controls", async () => {
    await render(async () => ready());
    const text = container.textContent;
    for (const value of [
      "raw-legacy-id",
      "exact-canonical-id",
      "Legacy / unverified",
      "Canonical binding verified at sample",
      "membership",
      "observed_terminal",
      "Dispatch: unknown",
      "Native command: Not observed",
      "Not tracked",
      "does not prove a handoff or completed work",
      "Handoff: unconfirmed in this sample",
      "Source: queue",
      "Sampled",
    ]) {
      expect(text).toContain(value);
    }
    expect(container.querySelector("textarea,input")).toBeNull();
    expect([...container.querySelectorAll("button")].map((button) => button.textContent)).toEqual([
      "Refresh metadata",
    ]);
  });
  it.each(["partial", "stale"] as const)(
    "keeps %s rows visible with their sample status",
    async (status) => {
      await render(async () => ({
        status,
        snapshot: { ...sample(), coverage: "partial" },
        expires_at_ms: Date.now() + 60_000,
      }));
      expect(container.textContent).toContain(
        status === "stale" ? "Stale sample" : "Partial sample",
      );
      expect(container.textContent).toContain("Coverage: partial");
      expect(container.textContent).toContain("request-legacy");
    },
  );
  it.each([
    { status: "unconfigured", reason: "not_configured" },
    { status: "unavailable", reason: "future_sample" },
    { status: "unavailable", reason: "source_unavailable" },
  ] satisfies WorkQueueMetadataResult[])(
    "does not turn $reason into an empty queue",
    async (result) => {
      await render(async () => result);
      expect(container.textContent).toContain(
        result.status === "unconfigured"
          ? "no source is configured"
          : result.reason === "future_sample"
            ? "the sample timestamp is in the future"
            : "the source cannot be read",
      );
      expect(container.textContent).not.toContain("No submitted work");
      expect(container.querySelector("table")).toBeNull();
    },
  );
  it("marks a cached sample stale when its validity expires without polling", async () => {
    vi.useFakeTimers();
    const load = vi.fn(async () => ready());
    await render(load);
    expect(container.textContent).toContain("Ready sample");
    await act(() => vi.advanceTimersByTime(60_000));
    expect(container.textContent).toContain("Stale sample");
    expect(load).toHaveBeenCalledTimes(1);
  });
  it("clears old data, aborts the old environment read and ignores its late result", async () => {
    const oldRead = deferred<WorkQueueMetadataResult>();
    let oldSignal: AbortSignal | undefined;
    await render((signal) => {
      oldSignal = signal;
      return oldRead.promise;
    });
    await render(
      async () => ({ status: "unconfigured", reason: "not_configured" }),
      "environment-b",
    );
    expect(oldSignal?.aborted).toBe(true);
    await act(() => oldRead.resolve(ready()));
    expect(container.textContent).toContain("no source is configured");
    expect(container.textContent).not.toContain("request-legacy");
  });
  it("refreshes explicitly and reports a failed read without displaying old rows as current", async () => {
    const load = vi
      .fn<WorkQueueMetadataLoader>()
      .mockResolvedValueOnce(ready())
      .mockRejectedValueOnce(new Error("offline"));
    await render(load);
    expect(container.textContent).toContain("request-legacy");
    await act(() => container.querySelector<HTMLButtonElement>("button")!.click());
    expect(container.textContent).toContain("Queue metadata unavailable");
    expect(container.textContent).not.toContain("request-legacy");
    expect(container.textContent).not.toContain("No submitted work");
  });
});

describe("submitted work page integration", () => {
  const mockedModules = [
    "../../state/environments",
    "../../hooks/useSettings",
    "../../jones/workQueue/useWorkQueueMetadata",
    "../voiceReview/useVoiceReview",
    "@tanstack/react-router",
    "../WorkspacePageHeader",
    "../WorkspacePageContainer",
    "../ui/sidebar",
  ];
  let root: Root;
  let container: HTMLDivElement;
  beforeEach(() => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    container = document.createElement("div");
    document.body.append(container);
    root = createRoot(container);
  });
  afterEach(async () => {
    await act(() => root.unmount());
    container.remove();
    for (const path of mockedModules) vi.doUnmock(path);
    vi.unstubAllGlobals();
  });
  it.each([
    [false, "pending"],
    [true, "pending"],
    [true, "queued"],
    [true, "sent"],
  ] as const)(
    "negotiates metadata capability=%s and hydrates initial stage=%s without synthetic controls",
    async (supported, initialStage) => {
      vi.resetModules();
      const load = vi.fn<WorkQueueMetadataLoader>(async () => ({
        status: "unconfigured",
        reason: "not_configured",
      }));
      const environment = {
        environmentId: "fixture-environment",
        label: "Fixture environment",
        entry: { enabled: true },
        connection: { phase: "connected" },
        serverConfig: {
          environment: { capabilities: supported ? { workQueueMetadata: true } : {} },
        },
      };
      let hydrationStatus = "pending";
      const retryPreferences = vi.fn(async () => undefined);
      vi.doMock("../../hooks/useSettings", () => ({
        useClientSettings: () => ({ promptsDefaultStage: initialStage }),
        useClientSettingsHydrationStatus: () => hydrationStatus,
        ensureClientSettingsHydrated: retryPreferences,
      }));
      vi.doMock("../../state/environments", () => ({
        useEnvironments: () => ({ environments: [environment] }),
        usePrimaryEnvironmentId: () => environment.environmentId,
      }));
      vi.doMock("../../jones/workQueue/useWorkQueueMetadata", () => ({
        useWorkQueueMetadata: () => load,
      }));
      const draft = voiceReviewRecentFixture.entries[1]!.draft;
      const mutate = vi.fn(async () => ({
        draft: { ...draft, state: "editing", revision: 2 },
        edit_handle: "fixture-edit-handle",
      }));
      const voiceReview = {
        fetchList: async () => ({ drafts: [draft] }),
        transport: { get: vi.fn(), mutate },
        review: {
          recent: async () => ({
            ...voiceReviewRecentFixture,
            entries: [voiceReviewRecentFixture.entries[0]!],
          }),
          registry: async () => ({ threads: [], partial: false, unavailable: [] }),
          workstreams: async () => ({ workstreams: [] }),
          diagnostics: vi.fn(),
          correctAssociation: vi.fn(),
        },
      };
      vi.doMock("../voiceReview/useVoiceReview", () => ({ useVoiceReview: () => voiceReview }));
      vi.doMock("@tanstack/react-router", () => ({
        useBlocker: () => ({ status: "idle" }),
      }));
      const wrapper = ({ children }: { children: import("react").ReactNode }) => (
        <div>{children}</div>
      );
      vi.doMock("../WorkspacePageHeader", () => ({ WorkspacePageHeader: wrapper }));
      vi.doMock("../WorkspacePageContainer", () => ({ WorkspacePageContainer: wrapper }));
      vi.doMock("../ui/sidebar", () => ({ SidebarInset: wrapper }));
      const { WorkQueuePage } = await import("./WorkQueuePage");
      await act(() => root.render(<WorkQueuePage />));
      expect(container.textContent).toContain("Loading Prompts preferences");
      expect(container.querySelector('[role="tablist"]')).toBeNull();
      expect(load).not.toHaveBeenCalled();
      hydrationStatus = "failed";
      await act(() => root.render(<WorkQueuePage />));
      expect(container.textContent).toContain("Prompts preferences could not be loaded");
      await act(() => container.querySelector<HTMLButtonElement>("button")!.click());
      expect(retryPreferences).toHaveBeenCalledTimes(1);
      hydrationStatus = "ready";
      await act(() => root.render(<WorkQueuePage />));
      expect(container.querySelector('[aria-label="Mock queue preview"]')).toBeNull();
      expect(container.textContent).not.toContain("sample data only");
      const pending = container.querySelector<HTMLButtonElement>("#queue-pending-tab")!;
      const queued = container.querySelector<HTMLButtonElement>("#queue-queued-tab")!;
      const sent = container.querySelector<HTMLButtonElement>("#queue-sent-tab")!;
      expect(
        container.querySelector(`#queue-${initialStage}-tab`)?.getAttribute("aria-selected"),
      ).toBe("true");
      await act(() => pending.click());
      expect(
        container.querySelector('[aria-label="Recent voice prompts"]')?.closest("[hidden]"),
      ).not.toBeNull();
      if (supported)
        expect(
          container.querySelector('[aria-label="Queue metadata"]')?.closest("[hidden]"),
        ).not.toBeNull();
      expect(pending.getAttribute("aria-selected")).toBe("true");
      const edit = [...container.querySelectorAll("button")].find(
        (button) => button.textContent === "Edit",
      )!;
      await act(() => edit.click());
      const editor = container.querySelector<HTMLTextAreaElement>(
        '[aria-label="Edit voice prompt"]',
      )!;
      expect(editor.value).toBe(draft.text);
      expect(mutate.mock.calls[0]).toEqual([
        draft.id,
        "edit-begin",
        { expected_revision: draft.revision },
      ]);
      await act(() => {
        pending.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowRight", bubbles: true }));
      });
      expect(queued.getAttribute("aria-selected")).toBe("true");
      expect(document.activeElement).toBe(queued);
      expect(
        container.querySelector('[aria-label="Recent voice prompts"]')?.closest("[hidden]"),
      ).toBeNull();
      expect(
        container.querySelector('[aria-label="Pending voice prompts"]')?.closest("[hidden]"),
      ).not.toBeNull();
      expect(container.querySelector('[aria-label="Edit voice prompt"]')).toBe(editor);
      await act(() =>
        queued.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowRight", bubbles: true })),
      );
      expect(sent.getAttribute("aria-selected")).toBe("true");
      expect(document.activeElement).toBe(sent);
      expect(
        container.querySelector('[aria-label="Pending voice prompts"]')?.closest("[hidden]"),
      ).not.toBeNull();
      expect(
        container.querySelector('[aria-label="Recent voice prompts"]')?.closest("[hidden]"),
      ).not.toBeNull();
      expect(container.querySelector('[aria-label="Edit voice prompt"]')).toBe(editor);
      expect(container.textContent).toContain(
        "Sent history is not available from this connection yet",
      );
      await act(() =>
        sent.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowRight", bubbles: true })),
      );
      expect(pending.getAttribute("aria-selected")).toBe("true");
      expect(document.activeElement).toBe(pending);
      await act(() =>
        pending.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowLeft", bubbles: true })),
      );
      expect(sent.getAttribute("aria-selected")).toBe("true");
      await act(() =>
        sent.dispatchEvent(new KeyboardEvent("keydown", { key: "Home", bubbles: true })),
      );
      expect(pending.getAttribute("aria-selected")).toBe("true");
      await act(() =>
        pending.dispatchEvent(new KeyboardEvent("keydown", { key: "End", bubbles: true })),
      );
      expect(sent.getAttribute("aria-selected")).toBe("true");
      await act(() => pending.click());
      expect(container.querySelector('[aria-label="Edit voice prompt"]')).toBe(editor);
      expect(mutate).toHaveBeenCalledTimes(1);
      expect(load).toHaveBeenCalledTimes(supported ? 1 : 0);
      expect(container.querySelector('[aria-label="Queue metadata"]') !== null).toBe(supported);
      expect(container.textContent).toContain(
        supported ? "no source is configured" : "Queue metadata unsupported by this environment",
      );
    },
  );
});
