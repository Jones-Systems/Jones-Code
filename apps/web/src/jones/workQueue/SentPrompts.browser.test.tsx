import { act, render as renderScreen, type Screen } from "../../../test/render";
import { describe, expect, it, vi } from "vite-plus/test";
import { SentPrompts, type SentPromptsState } from "./SentPrompts";

// Row content, receipts and invalid-confirmation filtering moved to the Prompts item model
// (promptItems.test.ts) and the shared row and detail (PromptList and PromptDetail tests).
describe("confirmed sent prompts frame", () => {
  let root: Screen;
  let renderDisposables: AsyncDisposableStack;
  let container: HTMLDivElement;
  async function setupFixture1() {
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

  async function render(state: SentPromptsState, rows: string | null = null) {
    await act(() =>
      root.rerender(
        <SentPrompts state={state}>{rows === null ? null : <p>{rows}</p>}</SentPrompts>,
      ),
    );
  }
  const sampledAt = Date.parse("2026-10-11T00:30:00Z");

  it("states what Sent proves and renders supplied rows without mutation controls", async () => {
    await using _fixture = await setupFixture1();
    await render({ status: "ready", count: 1, omitted: false }, "Confirmed row");
    const section = container.querySelector('section[aria-label="Sent prompts"]')!;
    expect(section.textContent).toContain("Confirmed row");
    expect(section.textContent).not.toContain("No sent prompts observed.");
    expect(container.querySelector("button, input, textarea, select")).toBeNull();
  });
  it("qualifies stale partial handoffs and never claims complete empty history", async () => {
    await using _fixture = await setupFixture1();
    const observation = { sampledAt, coverage: "partial" as const, stale: true };
    await render({ status: "ready", count: 1, omitted: false, observation }, "Row");
    expect(container.textContent).toContain("Previously observed confirmed handoffs");
    expect(container.textContent).toContain("Coverage: partial");
    expect(container.textContent).toContain("Sampled");
    expect(container.textContent).toContain("This sample is not a complete sent history.");
    await render({
      status: "ready",
      count: 1,
      omitted: false,
      observation: { ...observation, stale: false },
    });
    expect(container.textContent).toContain("Confirmed handoffs in this sample");
    expect(container.textContent).not.toContain("Previously observed");
    await render({ status: "ready", count: 0, omitted: false, observation });
    expect(container.textContent).toContain(
      "No confirmed handoffs in this sample; other sent prompts may be unavailable.",
    );
    expect(container.textContent).not.toContain("No sent prompts observed.");
  });
  it("keeps observed-empty history distinct from unavailable and loading", async () => {
    await using _fixture = await setupFixture1();
    await render({ status: "unavailable" });
    expect(container.textContent).toBe(
      "Sent history is unavailable from this connection. Unconfirmed handoffs remain under Queued.",
    );
    await render({ status: "loading" });
    expect(container.querySelector('[role="status"]')!.textContent).toBe("Loading sent prompts…");
    expect(container.textContent).not.toContain("No sent prompts observed");
    await render({ status: "ready", count: 0, omitted: false });
    expect(container.textContent).toContain("No sent prompts observed.");
    expect(container.textContent).not.toContain("unavailable");
  });
  it("reports omitted invalid confirmations instead of an empty history", async () => {
    await using _fixture = await setupFixture1();
    await render({ status: "ready", count: 0, omitted: true });
    expect(container.textContent).toContain("delivery confirmation is missing or invalid");
    expect(container.textContent).not.toContain("No sent prompts observed.");
  });
});
