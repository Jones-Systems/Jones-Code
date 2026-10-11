// @vitest-environment jsdom
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
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
  async function render(state: SentPromptsState) {
    await act(() => root.render(<SentPrompts state={state} />));
  }
  it("labels sample history and reveals the handoff receipt without mutation controls", async () => {
    await render({ status: "ready", provenance: "sample", entries: [prompt] });
    expect(container.textContent).toContain("SAMPLE DATA");
    expect(container.textContent).toContain("Example sent prompts · not live history");
    expect(container.textContent).toContain("Sent confirms handoff, not completed work.");
    const article = container.querySelector("article")!;
    expect(article.textContent).toContain(prompt.text);
    expect(article.textContent).toContain("Prompt review");
    expect(article.textContent).toContain("Voice prompt");
    expect(article.textContent).toContain("Prompt history");
    expect(article.querySelector("time")!.textContent).toContain("EDT");
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
  it("keeps live observed-empty history distinct from unavailable and loading", async () => {
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
    const invalid = { ...prompt, confirmation } as unknown as ConfirmedSentPrompt;
    await render({ status: "ready", provenance: "live", entries: [invalid] });
    expect(container.querySelector("article")).toBeNull();
    expect(container.textContent).toContain("delivery confirmation is missing or invalid");
    expect(container.textContent).not.toContain("No sent prompts observed.");
    expect(container.textContent).not.toContain(prompt.text);
  });
});
