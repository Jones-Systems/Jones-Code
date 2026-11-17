import { act, render as renderScreen, type Screen } from "../../../test/render";
import { useState } from "react";
import { describe, expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import type { EnvironmentId } from "@t3tools/contracts";
import { PromptMasterDetail, type PromptListGroup } from "./PromptList";
import { displayText, threadHref, toneVariant, type PromptRowView } from "./PromptRow";
import { PromptThreadStatus } from "./PromptThreadStatus";
import "../../index.css";

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
  workstreams: [
    {
      key: "docs",
      filterKey: "environment:docs",
      id: "docs",
      name: "Docs",
      color: null,
      provenance: "suggested",
      summary: null,
      threadId: null,
      assignedAutomatically: false,
    },
  ],
  time: null,
  dirty: false,
  gone: false,
  ...overrides,
});

function Harness({ groups }: { groups: PromptListGroup[] }) {
  const [selected, setSelected] = useState<string | null>(null);
  return (
    <PromptMasterDetail
      label="Queued prompts"
      groups={groups}
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

  it("keeps the focused rows in place and disables a prompt that leaves the stage", async () => {
    await using _fixture = await setupFixture();
    await act(() =>
      root.rerender(
        <Harness groups={[{ key: "all", label: "All", rows: [row("a"), row("b")] }]} />,
      ),
    );
    const first = buttons()[0]!;
    await act(() => first.focus());
    await act(() =>
      root.rerender(
        <Harness
          groups={[
            { key: "all", label: "All", rows: [row("c"), row("b", { preview: "New preview" })] },
          ]}
        />,
      ),
    );
    expect(buttons()[0]).toBe(first);
    expect(first.getAttribute("aria-disabled")).toBe("true");
    expect(first.textContent).toContain("Moved · refresh for current state");
    expect(buttons()[1]!.textContent).toContain("New preview");
    await act(() => first.click());
    expect(first.getAttribute("aria-current")).toBeNull();
    await act(() => first.blur());
    expect(buttons()[0]!.textContent).toContain("Destination c");
  });

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
    expect(first!.parentElement?.textContent).toContain("Docs");
    expect(first!.getAttribute("aria-current")).toBeNull();
    await act(() => second!.click());
    expect(second!.getAttribute("aria-current")).toBe("true");
    const items = container.querySelectorAll("li");
    expect(items[1]!.textContent).toContain("Detail for b");
    expect(items[0]!.textContent).not.toContain("Detail for");
    expect(document.activeElement?.textContent).toBe("Detail for b");
  });

  it("wraps a long status below the destination in a narrow prompt row", async () => {
    await using _fixture = await setupFixture();
    const { innerWidth, innerHeight } = window;
    try {
      await page.viewport(320, 740);
      container.style.width = "280px";
      await act(() =>
        root.rerender(
          <Harness
            groups={[
              {
                key: "all",
                label: "All",
                rows: [
                  row("blocked", {
                    fallbackTitle: "Destination not observed yet",
                    status: { label: "Blocked · Needs a destination", tone: "warning" },
                    promptHref: "/voice-review?stage=pending",
                  }),
                ],
              },
            ]}
          />,
        ),
      );
      const button = buttons()[0]!;
      const title = button.querySelector("bdi")!.getBoundingClientRect();
      const status = button.querySelector('[data-slot="badge"]')!.getBoundingClientRect();
      expect(title.bottom).toBeLessThanOrEqual(status.top);
      expect(status.right).toBeLessThanOrEqual(button.getBoundingClientRect().right);
    } finally {
      await page.viewport(innerWidth, innerHeight);
    }
  });

  it("labels groups with counts, marks unsaved work and holds a moved row still", async () => {
    await using _fixture = await setupFixture();
    await act(() =>
      root.rerender(
        <Harness
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

  it("retains group headings while focus pins the last row from a disappearing group", async () => {
    await using _fixture = await setupFixture();
    const first = { key: "a", label: "Alpha", rows: [row("a")] };
    const second = { key: "b", label: "Beta", rows: [row("b")] };
    await act(() => root.rerender(<Harness groups={[first, second]} />));
    await act(() => buttons()[0]!.focus());
    await act(() => root.rerender(<Harness groups={[first]} />));
    expect([...container.querySelectorAll("h3")].map((heading) => heading.textContent)).toEqual([
      "Alpha · 1",
      "Beta · 1",
    ]);
    await act(() => buttons()[0]!.blur());
    expect(container.querySelectorAll("h3")).toHaveLength(0);
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
