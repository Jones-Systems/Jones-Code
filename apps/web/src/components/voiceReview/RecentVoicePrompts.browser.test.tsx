import { act, render as renderTest, type Screen } from "../../../test/render";

import { describe, expect, it, vi } from "vite-plus/test";
import {
  voiceReviewRecentFixture,
  threadRegistrySnapshotFixture,
  threadRegistryWorkstreamsFixture,
} from "@t3tools/client-runtime/voice-review/fixtures";
import type { VoiceReviewRecentEntry } from "@t3tools/contracts";
import type { ThreadRegistryComposedSnapshot, ThreadRegistryWorkstreams } from "@t3tools/contracts";
import {
  recentThread,
  recentThreadPurpose,
  VoiceWorkstreamCorrections,
} from "./RecentVoicePrompts";

// Prompt text, provenance and delivery identities moved to the Prompts detail evidence
// (PromptDetail.browser.test.tsx). This file covers the Workstream corrections block.
function Corrections({
  entry,
  registry = threadRegistrySnapshotFixture,
  workstreams = threadRegistryWorkstreamsFixture,
  correctAssociation,
  onRefresh = async () => undefined,
  unavailable = false,
}: {
  entry: VoiceReviewRecentEntry;
  registry?: ThreadRegistryComposedSnapshot;
  workstreams?: ThreadRegistryWorkstreams;
  correctAssociation: (...args: never[]) => unknown;
  onRefresh?: () => Promise<void>;
  unavailable?: boolean;
}) {
  return (
    <VoiceWorkstreamCorrections
      entry={entry}
      thread={recentThread(entry, registry.threads)}
      registry={registry}
      workstreams={workstreams}
      transport={{ correctAssociation } as never}
      onRefresh={onRefresh}
      unavailable={unavailable}
    />
  );
}

describe("recent voice prompt interactions", () => {
  let root: Screen;
  let container: HTMLDivElement;
  async function setupTest(): Promise<AsyncDisposable> {
    let initialized = false;
    let ownedContainer: HTMLDivElement | undefined;
    async function cleanup() {
      const screen = initialized ? root : undefined;
      try {
        await act(() => screen?.unmount());
      } finally {
        try {
          await screen?.[Symbol.asyncDispose]();
        } finally {
          ownedContainer?.remove();
          vi.unstubAllGlobals();
        }
      }
    }
    try {
      ownedContainer = container = document.createElement("div");
      document.body.append(container);
      root = await renderTest(null, { container: container, router: false });
      initialized = true;

      return { [Symbol.asyncDispose]: cleanup };
    } catch (error) {
      await cleanup();
      throw error;
    }
  }

  async function render(
    entry: VoiceReviewRecentEntry = voiceReviewRecentFixture.entries[0]!,
    correctAssociation = vi.fn(),
  ) {
    await act(
      async () =>
        await root.rerender(<Corrections entry={entry} correctAssociation={correctAssociation} />),
    );
    return correctAssociation;
  }
  function button(text: string, article: Element = container) {
    const found = [...article.querySelectorAll("button")].find((item) => item.textContent === text);
    if (!found) throw new Error(`Missing button: ${text}`);
    return found;
  }
  it("joins only the verified composite key and never a bare routing target", async () => {
    await using _fixture = await setupTest();
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
    expect(recentThreadPurpose(thread)).toBe("Voice review bridge");
    expect(recentThreadPurpose(null)).toBeNull();
  });
  it("removes only acknowledged metadata and re-adds the same workstream at the suppression revision", async () => {
    await using _fixture = await setupTest();
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
    await render(entry, correctAssociation);
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
    await using _fixture = await setupTest();
    const entry = { ...voiceReviewRecentFixture.entries[0]!, workstream_refs: ["native:board"] };
    const correctAssociation = await render(entry);
    expect(container.textContent).toContain("native:board");
    expect(
      container.querySelector('button[aria-label="Remove native:board workstream"]'),
    ).toBeNull();
    await act(
      async () =>
        await root.rerender(
          <Corrections
            entry={entry}
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
            correctAssociation={correctAssociation}
          />,
        ),
    );
    expect([...container.querySelectorAll("option")].map((option) => option.textContent)).toContain(
      "New workstream",
    );
    expect(correctAssociation).not.toHaveBeenCalled();
  });
  it("does not make a missing revision actionable", async () => {
    await using _fixture = await setupTest();
    const correctAssociation = await render({
      ...voiceReviewRecentFixture.entries[0]!,
      associations: undefined,
    });
    const remove = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Remove Voice review workstream"]',
    )!;
    await act(() => remove.click());
    expect(correctAssociation).not.toHaveBeenCalled();
    expect(container.textContent).toContain("association revisions are observed");
  });
  it("disables corrections when context is unavailable and says the thread is stale", async () => {
    await using _fixture = await setupTest();
    const correctAssociation = vi.fn();
    await act(
      async () =>
        await root.rerender(
          <Corrections
            entry={voiceReviewRecentFixture.entries[0]!}
            registry={{
              ...threadRegistrySnapshotFixture,
              threads: threadRegistrySnapshotFixture.threads.map((thread) => ({
                ...thread,
                freshness: { ...thread.freshness, stale: true },
              })),
            }}
            correctAssociation={correctAssociation}
            unavailable
          />,
        ),
    );
    expect(container.textContent).toContain("Thread context is stale.");
    expect(container.textContent).toContain("Workstream corrections never resend prompts.");
    const remove = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Remove Voice review workstream"]',
    )!;
    expect(remove.disabled).toBe(true);
    expect(container.querySelector("select")!.disabled).toBe(true);
    await act(() => remove.click());
    expect(correctAssociation).not.toHaveBeenCalled();
  });
  it("requires successful metadata refresh after an uncertain correction without retrying it", async () => {
    await using _fixture = await setupTest();
    let rejectCorrection!: (error: Error) => void;
    const correction = new Promise<never>((_, reject) => {
      rejectCorrection = reject;
    });
    const correctAssociation = vi.fn(() => correction);
    const onRefresh = vi
      .fn()
      .mockRejectedValueOnce(new Error("Offline"))
      .mockResolvedValueOnce(undefined);
    await act(
      async () =>
        await root.rerender(
          <Corrections
            entry={voiceReviewRecentFixture.entries[0]!}
            correctAssociation={correctAssociation}
            onRefresh={onRefresh}
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
