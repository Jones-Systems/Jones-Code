// @vitest-environment jsdom
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { WorkQueuePanel } from "./WorkQueuePage";
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
      ["Reserved", "Delivered", "UNKNOWN", "Held"].map((statusLabel, index) => ({
        ...first,
        id: String(index),
        statusLabel,
        text: `<script>${statusLabel}</script>`,
        editability: "readonly" as const,
        reason: `${statusLabel} cannot be edited.`,
      })),
    );
    await render(source);
    for (const status of ["Reserved", "Delivered", "UNKNOWN", "Held"]) {
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
