import { act, render as renderScreen, type Screen } from "../../../test/render";
import { describe, expect, it, vi } from "vite-plus/test";
import { SentPrompts, type ConfirmedSentPrompt, type SentPromptsState } from "./SentPrompts";

const prompt: ConfirmedSentPrompt = {
  id: "sample-sent-1",
  text: "Keep the prompt readable.\nPreserve its full text after handoff.",
  target: { threadId: "sample-thread-1", label: "Prompt review" },
  sentAt: Date.parse("2026-10-11T00:30:00Z"),
  confirmation: {
    status: "confirmed",
    kind: "handoff-receipt",
    receiptId: "sample-receipt-1",
    source: "sample-handoff-source",
  },
  sourceLabel: "Voice prompt",
  workstreams: ["Prompt history"],
};

describe("confirmed sent prompts", () => {
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

  async function render(state: SentPromptsState) {
    await act(() => root.rerender(<SentPrompts state={state} />));
  }
  it("labels sample history and reveals the handoff receipt without mutation controls", async () => {
    await using _fixture = await setupFixture1();
    await render({ status: "ready", provenance: "sample", entries: [prompt] });
    expect(container.textContent).toContain("SAMPLE DATA");
    expect(container.textContent).toContain("Example sent prompts · not live history");
    expect(container.textContent).toContain("Sent confirms handoff, not completed work.");
    const article = container.querySelector("article")!;
    expect(article.textContent).toContain(prompt.text);
    expect(article.textContent).toContain("Prompt review");
    expect(article.textContent).toContain("Voice prompt");
    expect(article.textContent).toContain("Prompt history");
    expect(article.querySelector("time")!.textContent).toContain("Handoff observed");
    expect(article.querySelector("time")!.textContent).toContain("EDT");
    expect(article.querySelector("time")!.dateTime).toBe(new Date(prompt.sentAt).toISOString());
    const details = article.querySelector("details")!;
    expect(details.open).toBe(false);
    await act(() => details.querySelector("summary")!.click());
    expect(details.open).toBe(true);
    expect(details.textContent).toContain("sample-receipt-1");
    expect(details.textContent).toContain("sample-handoff-source");
    expect(details.textContent).toContain("sample-thread-1");
    await act(() => details.querySelector("summary")!.click());
    expect(details.open).toBe(false);
    expect(container.querySelector("button, input, textarea, select")).toBeNull();
  });
  it("qualifies stale partial handoffs and never claims complete empty history", async () => {
    await using _fixture = await setupFixture1();
    const observation = { sampledAt: prompt.sentAt, coverage: "partial" as const, stale: true };
    await render({
      status: "ready",
      provenance: "live",
      entries: [{ ...prompt, providerState: "pending" }],
      observation,
    });
    expect(container.textContent).toContain("Previously observed confirmed handoffs");
    expect(container.textContent).toContain("Coverage: partial");
    expect(container.textContent).toContain("Sampled");
    expect(container.textContent).toContain("Provider turn: pending");
    expect(container.textContent).not.toContain("SAMPLE DATA");
    await render({ status: "ready", provenance: "live", entries: [], observation });
    expect(container.textContent).toContain(
      "No confirmed handoffs in this sample; other sent prompts may be unavailable.",
    );
    expect(container.textContent).not.toContain("No sent prompts observed.");
  });
  it("keeps live observed-empty history distinct from unavailable and loading", async () => {
    await using _fixture = await setupFixture1();
    await render({ status: "unavailable" });
    expect(container.textContent).toBe(
      "Sent history is unavailable from this connection. Unconfirmed handoffs remain under Queued.",
    );
    expect(container.textContent).not.toContain("No sent prompts observed");
    await render({ status: "loading" });
    expect(container.querySelector('[role="status"]')!.textContent).toBe("Loading sent prompts…");
    expect(container.textContent).not.toContain("No sent prompts observed");
    await render({ status: "ready", provenance: "live", entries: [] });
    expect(container.textContent).toContain("No sent prompts observed.");
    expect(container.textContent).not.toContain("SAMPLE DATA");
    expect(container.textContent).not.toContain("unavailable");
    expect(container.querySelector("article")).toBeNull();
  });
  it("preserves full literal long text and reports unknown text without substituting content", async () => {
    await using _fixture = await setupFixture1();
    const text = `<script>literal</script>\n${"long-unbroken-prompt".repeat(100)}`;
    await render({
      status: "ready",
      provenance: "live",
      entries: [
        { ...prompt, text },
        {
          ...prompt,
          id: "unknown-text",
          text: null,
          target: { threadId: "known-thread", label: null },
          sourceLabel: null,
          workstreams: [],
        },
      ],
    });
    const articles = container.querySelectorAll("article");
    expect(articles).toHaveLength(2);
    expect(articles[0]!.textContent).toContain(text);
    expect(container.querySelector("script")).toBeNull();
    expect(articles[1]!.textContent).toContain("Prompt text is unavailable.");
    expect(articles[1]!.textContent).toContain("known-thread");
    expect(container.textContent).not.toContain("SAMPLE DATA");
    expect(container.querySelector("button, input, textarea, select")).toBeNull();
  });
  it.each([
    { status: "released", kind: "handoff-receipt", receiptId: "receipt", source: "source" },
    { status: "confirmed", kind: "command-accepted", receiptId: "receipt", source: "source" },
    { status: "confirmed", kind: "handoff-receipt", receiptId: "", source: "source" },
    { status: "confirmed", kind: "handoff-receipt", receiptId: "receipt", source: "" },
    null,
  ])("does not present invalid confirmation %j as a sent prompt", async (confirmation) => {
    await using _fixture = await setupFixture1();
    const invalid = { ...prompt, confirmation } as unknown as ConfirmedSentPrompt;
    await render({ status: "ready", provenance: "live", entries: [invalid] });
    expect(container.querySelector("article")).toBeNull();
    expect(container.textContent).toContain("delivery confirmation is missing or invalid");
    expect(container.textContent).not.toContain("No sent prompts observed.");
    expect(container.textContent).not.toContain(prompt.text);
  });
});
