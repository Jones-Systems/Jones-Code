import { act, render as renderScreen, type Screen } from "../../../test/render";
import { useState } from "react";
import { describe, expect, it, vi } from "vite-plus/test";
import type { EnvironmentId } from "@t3tools/contracts";
import { PromptMasterDetail, type PromptListGroup } from "./PromptList";
import { displayText, threadHref, toneVariant, type PromptRowView } from "./PromptRow";
import { PromptThreadStatus } from "./PromptThreadStatus";

const environmentId = "environment" as EnvironmentId;
const row = (key: string, overrides: Partial<PromptRowView> = {}): PromptRowView => ({
  key,
  environmentId,
  threadId: null,
  fallbackTitle: `Destination ${key}`,
  destinationCount: 1,
  status: { label: "Queued", tone: "neutral" },
  preview: `Preview ${key}`,
  previewMuted: false,
  source: "Voice",
  workstreams: ["Docs"],
  time: null,
  dirty: false,
  gone: false,
  ...overrides,
});

function Harness({ groups, headings = false }: { groups: PromptListGroup[]; headings?: boolean }) {
  const [selected, setSelected] = useState<string | null>(null);
  return (
    <PromptMasterDetail
      label="Queued prompts"
      groups={groups}
      showGroupHeadings={headings}
      selectedKey={selected}
      onSelect={setSelected}
      empty={<p>Nothing in the queue.</p>}
      detail={
        <section>
          <h3 tabIndex={-1} data-prompt-detail-heading>
            Detail for {selected}
          </h3>
        </section>
      }
    />
  );
}

describe("prompt master-detail list", () => {
  let root: Screen;
  let renderDisposables: AsyncDisposableStack;
  let container: HTMLDivElement;
  async function setupFixture() {
    const stack = new AsyncDisposableStack();
    renderDisposables = stack;
    stack.defer(async () => {
      await act(() => root.unmount());
      container.remove();
      vi.unstubAllGlobals();
    });
    try {
      container = document.createElement("div");
      document.body.append(container);
      root = renderDisposables.use(await renderScreen(null, { container, router: false }));
      return stack;
    } catch (error) {
      await stack.disposeAsync();
      throw error;
    }
  }
  const buttons = () => [
    ...container.querySelectorAll<HTMLButtonElement>(
      '[aria-label="Queued prompts"] li > div > button',
    ),
  ];

  it("shows compact rows and opens the detail inline under the selected row on narrow screens", async () => {
    await using _fixture = await setupFixture();
    await act(() =>
      root.rerender(
        <Harness groups={[{ key: "all", label: "All", rows: [row("a"), row("b")] }]} />,
      ),
    );
    expect(container.querySelector('ul[aria-label="Queued prompts: All"]')).not.toBeNull();
    expect(container.querySelector("h3")).toBeNull();
    const [first, second] = buttons();
    expect(first!.textContent).toContain("Destination a");
    expect(first!.textContent).toContain("Queued");
    expect(first!.textContent).toContain("Preview a");
    expect(first!.textContent).toContain("Docs");
    expect(first!.getAttribute("aria-current")).toBeNull();
    await act(() => second!.click());
    expect(second!.getAttribute("aria-current")).toBe("true");
    const items = container.querySelectorAll("li");
    expect(items[1]!.textContent).toContain("Detail for b");
    expect(items[0]!.textContent).not.toContain("Detail for");
    expect(document.activeElement?.textContent).toBe("Detail for b");
  });

  it("labels groups with counts, marks unsaved work and holds a moved row still", async () => {
    await using _fixture = await setupFixture();
    await act(() =>
      root.rerender(
        <Harness
          headings
          groups={[
            { key: "a", label: "Alpha", rows: [row("a", { dirty: true })] },
            {
              key: "b",
              label: "No workstream",
              rows: [row("b", { gone: true }), row("c", { destinationCount: 2 })],
            },
          ]}
        />,
      ),
    );
    expect([...container.querySelectorAll("h3")].map((heading) => heading.textContent)).toEqual([
      "Alpha · 1",
      "No workstream · 2",
    ]);
    expect(container.querySelector('[role="img"]')!.getAttribute("aria-label")).toBe(
      "Unsaved or unconfirmed work",
    );
    const [, gone, multi] = buttons();
    expect(gone!.getAttribute("aria-disabled")).toBe("true");
    expect(gone!.textContent).toContain("Moved · refresh for current state");
    await act(() => gone!.click());
    expect(gone!.getAttribute("aria-current")).toBeNull();
    expect(multi!.textContent).toContain("2 destinations");
  });

  it("shows the supplied empty state when no group has rows", async () => {
    await using _fixture = await setupFixture();
    await act(() => root.rerender(<Harness groups={[]} />));
    expect(container.textContent).toBe("Nothing in the queue.");
  });

  it("hides thread status and links when the thread is not loaded here", async () => {
    await using _fixture = await setupFixture();
    await act(() =>
      root.rerender(
        <div>
          <Harness
            groups={[{ key: "all", label: "All", rows: [row("a", { threadId: "thread" })] }]}
          />
          <PromptThreadStatus environmentId={environmentId} threadId="thread" />
        </div>,
      ),
    );
    expect(container.querySelector("a")).toBeNull();
    expect(buttons()[0]!.textContent).toContain("Destination a");
  });

  it("formats links, display text and tones", () => {
    expect(threadHref(environmentId, "thread/1")).toBe("/environment/thread%2F1");
    expect(displayText("line\u0000one", "fallback")).not.toContain("\u0000");
    expect(displayText(null, "fallback")).toBe("fallback");
    expect(toneVariant.neutral).toBe("outline");
    expect(toneVariant.success).toBe("success");
  });
});
