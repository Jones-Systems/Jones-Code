// @vitest-environment jsdom
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import {
  voiceReviewRecentFixture,
  threadRegistrySnapshotFixture,
  threadRegistryWorkstreamsFixture,
} from "@t3tools/client-runtime/voice-review/fixtures";
import { RecentVoicePrompts, recentThread } from "./RecentVoicePrompts";

describe("recent voice prompt interactions", () => {
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
  async function render(entries = voiceReviewRecentFixture.entries, correctAssociation = vi.fn()) {
    await act(() =>
      root.render(
        <RecentVoicePrompts
          entries={entries}
          registry={threadRegistrySnapshotFixture}
          workstreams={threadRegistryWorkstreamsFixture}
          transport={{ correctAssociation }}
          onRefresh={async () => undefined}
          unavailable={false}
        />,
      ),
    );
    return correctAssociation;
  }
  function button(text: string, article: Element = container) {
    const found = [...article.querySelectorAll("button")].find((item) => item.textContent === text);
    if (!found) throw new Error(`Missing button: ${text}`);
    return found;
  }
  it("joins only the verified composite key and never a bare routing target", () => {
    const entry = voiceReviewRecentFixture.entries[0]!;
    const thread = threadRegistrySnapshotFixture.threads[0]!;
    expect(
      recentThread(
        {
          ...entry,
          thread_key: null,
          draft: { ...entry.draft, routing_target: thread.thread_key },
        },
        [thread],
      ),
    ).toBeNull();
    expect(recentThread({ ...entry, thread_key: thread.thread_key }, [thread])).toBe(thread);
    expect(recentThread({ ...entry, thread_key: "unmapped" }, [thread])).toBeNull();
  });
  it("shows literal prompt text immediately and discloses provenance without fabricating missing text", async () => {
    const retained = voiceReviewRecentFixture.entries[0]!;
    const deleted = voiceReviewRecentFixture.entries[2]!;
    await render([
      { ...retained, text: "<script>literal retained text</script>" },
      deleted,
      {
        ...deleted,
        draft: { ...deleted.draft, id: "expired-1", state: "expired" },
        text_state: "expired",
      },
    ]);
    const articles = container.querySelectorAll("article");
    expect(articles[0]!.textContent).toContain("<script>literal retained text</script>");
    expect(container.querySelector("script")).toBeNull();
    expect(articles[1]!.textContent).toContain("Prompt text was deleted.");
    expect(articles[2]!.textContent).toContain("Prompt text has expired.");
    const details = articles[0]!.querySelector("details")!;
    expect(details.open).toBe(false);
    await act(() => details.querySelector("summary")!.click());
    expect(details.open).toBe(true);
    expect(details.textContent).toContain("Retained command text");
    expect(details.textContent).toContain("command-1");
    expect(details.textContent).toContain("Original transcript");
    await act(() => details.querySelector("summary")!.click());
    expect(details.open).toBe(false);
    expect(articles[0]!.textContent).toContain("literal retained text");
  });
  it("removes only acknowledged metadata and re-adds the same workstream at the suppression revision", async () => {
    const getRandomValues = globalThis.crypto.getRandomValues.bind(globalThis.crypto);
    vi.stubGlobal("crypto", { getRandomValues });
    expect(globalThis.crypto.randomUUID).toBeUndefined();
    const entry = voiceReviewRecentFixture.entries[0]!;
    const record = entry.associations![0]!;
    const correctAssociation = vi.fn().mockImplementation(async (payload) => ({
      schema: "voice.registry-receipt/v1",
      request_id: payload.request_id,
      revision: payload.expected_revision + 1,
      event_sequence: 10,
      record: { ...record, ...payload, revision: payload.expected_revision + 1, origin: "owner" },
    }));
    await render([entry], correctAssociation);
    const remove = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Remove Voice review workstream"]',
    )!;
    remove.focus();
    expect(document.activeElement).toBe(remove);
    await act(() => remove.click());
    expect(correctAssociation.mock.calls[0]![0]).toMatchObject({
      subject: "prompt:command-1",
      state: "suppressed",
      expected_revision: 1,
      command_id: "command-1",
    });
    expect(
      container.querySelector('button[aria-label="Remove Voice review workstream"]'),
    ).toBeNull();
    const select = container.querySelector("select")!;
    await act(() => {
      select.value = record.workstream_ref;
      select.dispatchEvent(new Event("change", { bubbles: true }));
    });
    await act(() => button("Add").click());
    expect(correctAssociation.mock.calls[1]![0]).toMatchObject({
      state: "active",
      expected_revision: 2,
    });
    const requestIds = correctAssociation.mock.calls.map(([payload]) => payload.request_id);
    for (const requestId of requestIds) {
      expect(requestId).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
      );
    }
    expect(new Set(requestIds).size).toBe(2);
    expect(
      container.querySelector('button[aria-label="Remove Voice review workstream"]'),
    ).not.toBeNull();
  });
  it("keeps native membership read-only and accepts updated workstream lists", async () => {
    const entry = { ...voiceReviewRecentFixture.entries[0]!, workstream_refs: ["native:board"] };
    const correctAssociation = await render([entry]);
    expect(container.textContent).toContain("native:board");
    expect(
      container.querySelector('button[aria-label="Remove native:board workstream"]'),
    ).toBeNull();
    await act(() =>
      root.render(
        <RecentVoicePrompts
          entries={[entry]}
          registry={threadRegistrySnapshotFixture}
          workstreams={{
            ...threadRegistryWorkstreamsFixture,
            workstreams: [
              ...threadRegistryWorkstreamsFixture.workstreams,
              {
                ...threadRegistryWorkstreamsFixture.workstreams[0]!,
                label_id: "inferred:new",
                name: "New workstream",
              },
            ],
          }}
          transport={{ correctAssociation }}
          onRefresh={async () => undefined}
          unavailable={false}
        />,
      ),
    );
    expect([...container.querySelectorAll("option")].map((option) => option.textContent)).toContain(
      "New workstream",
    );
    expect(correctAssociation).not.toHaveBeenCalled();
  });
  it("does not make a missing revision actionable", async () => {
    const correctAssociation = await render([
      { ...voiceReviewRecentFixture.entries[0]!, associations: undefined },
    ]);
    const remove = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Remove Voice review workstream"]',
    )!;
    await act(() => remove.click());
    expect(correctAssociation).not.toHaveBeenCalled();
    expect(container.textContent).toContain("association revisions are observed");
  });
  it("shows an empty observation without offering metadata corrections", async () => {
    await render([]);
    expect(container.textContent).toContain("No recent prompts observed.");
    expect(container.querySelector("article, select")).toBeNull();
  });
  it("keeps stale prompt text readable but disables corrections when context is unavailable", async () => {
    const correctAssociation = vi.fn();
    await act(() =>
      root.render(
        <RecentVoicePrompts
          entries={[voiceReviewRecentFixture.entries[0]!]}
          registry={{
            ...threadRegistrySnapshotFixture,
            threads: threadRegistrySnapshotFixture.threads.map((thread) => ({
              ...thread,
              freshness: { ...thread.freshness, stale: true },
            })),
          }}
          workstreams={threadRegistryWorkstreamsFixture}
          transport={{ correctAssociation }}
          onRefresh={async () => undefined}
          unavailable
        />,
      ),
    );
    expect(container.textContent).toContain("Keep the recent prompts readable.");
    expect(container.textContent).toContain("Thread context is stale.");
    const remove = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Remove Voice review workstream"]',
    )!;
    expect(remove.disabled).toBe(true);
    expect(container.querySelector("select")!.disabled).toBe(true);
    await act(() => remove.click());
    expect(correctAssociation).not.toHaveBeenCalled();
  });
  it("requires successful metadata refresh after an uncertain correction without retrying it", async () => {
    let rejectCorrection!: (error: Error) => void;
    const correction = new Promise<never>((_, reject) => {
      rejectCorrection = reject;
    });
    const correctAssociation = vi.fn(() => correction);
    const onRefresh = vi
      .fn()
      .mockRejectedValueOnce(new Error("Offline"))
      .mockResolvedValueOnce(undefined);
    await act(() =>
      root.render(
        <RecentVoicePrompts
          entries={[voiceReviewRecentFixture.entries[0]!]}
          registry={threadRegistrySnapshotFixture}
          workstreams={threadRegistryWorkstreamsFixture}
          transport={{ correctAssociation }}
          onRefresh={onRefresh}
          unavailable={false}
        />,
      ),
    );
    const remove = () => {
      const current = container.querySelector<HTMLButtonElement>(
        'button[aria-label="Remove Voice review workstream"]',
      )!;
      expect(current.isConnected).toBe(true);
      return current;
    };
    await act(() => remove().click());
    expect(remove().disabled).toBe(true);
    expect(container.querySelector("select")!.disabled).toBe(true);
    await act(() => remove().click());
    expect(correctAssociation).toHaveBeenCalledTimes(1);
    await act(() => rejectCorrection(new Error("Lost receipt")));
    expect(remove().disabled).toBe(true);
    expect(container.textContent).toContain("Correction is unconfirmed");
    await act(() => remove().click());
    expect(correctAssociation).toHaveBeenCalledTimes(1);
    await act(() => button("Check current workstreams").click());
    expect(container.textContent).toContain("Correction remains unconfirmed");
    expect(remove().disabled).toBe(true);
    await act(() => remove().click());
    expect(correctAssociation).toHaveBeenCalledTimes(1);
    await act(() => button("Check current workstreams").click());
    expect(remove().disabled).toBe(false);
    expect(container.querySelector('[role="alert"]')).toBeNull();
    expect(correctAssociation).toHaveBeenCalledTimes(1);
    expect(onRefresh).toHaveBeenCalledTimes(2);
  });
});
