import { EnvironmentId, type VoiceReviewDraft } from "@t3tools/contracts";
import { voiceReviewRecentFixture } from "@t3tools/client-runtime/voice-review/fixtures";
import { describe, expect, it, vi } from "vite-plus/test";
import { act, fakeTimers, render } from "../../../test/render";
import { EnvironmentVoiceReview } from "./VoiceReviewPage";
import type { VoiceReviewTransport } from "./voiceReviewActions";

const voice = vi.hoisted(() => ({
  list: vi.fn(),
  mutate: vi.fn<VoiceReviewTransport["mutate"]>(),
  get: vi.fn<VoiceReviewTransport["get"]>(),
}));

vi.mock("./useVoiceReview", () => ({
  useVoiceReview: () => ({
    fetchList: voice.list,
    transport: { get: voice.get, mutate: voice.mutate },
    review: { diagnostics: vi.fn() },
  }),
}));

const draft: VoiceReviewDraft = {
  ...voiceReviewRecentFixture.entries[1]!.draft,
  state: "held",
  remaining_ms: 180_000,
  routing_target: "Review fixture",
};
const second = { ...draft, id: "second-prompt", routing_target: "Other fixture" } as const;

function input(textarea: HTMLTextAreaElement, text: string) {
  Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value")!.set!.call(
    textarea,
    text,
  );
  textarea.dispatchEvent(new Event("input", { bubbles: true }));
}

describe("Pending inline composition", () => {
  it("shows both fields, keeps the countdown on focus, and retains fields through a capped refresh and selection", async () => {
    const { id, revision } = draft;
    const editing = { ...draft, revision: revision + 1, state: "editing" } as const;
    const text = "Updated prompt\n\nAdditional instructions:\nInclude examples.";
    voice.list.mockReset().mockResolvedValue({ drafts: [draft, second] });
    voice.mutate
      .mockReset()
      .mockResolvedValueOnce({ draft: editing, edit_handle: "fixture-lease" })
      .mockResolvedValueOnce({
        draft: { ...editing, revision: revision + 2, state: "paused", text },
        edit_handle: null,
      })
      .mockResolvedValueOnce({
        draft: { ...editing, revision: revision + 3, state: "released", text: null },
        edit_handle: null,
      });
    const onDirtyChange = vi.fn();
    using _timers = fakeTimers();
    const show = (selection: string) => (
      <EnvironmentVoiceReview
        environmentId={EnvironmentId.make("fixture")}
        linkedKey={`voice:${selection}`}
        onDirtyChange={onDirtyChange}
      />
    );
    await using screen = await render(show(id), { router: false });
    const promptField = () => screen.container.querySelectorAll("textarea")[0]!;
    const instructionsField = () => screen.container.querySelectorAll("textarea")[1]!;
    expect(promptField().value).toBe(draft.text);
    expect(instructionsField().value).toBe("");
    expect(voice.mutate).not.toHaveBeenCalled();
    await act(() => promptField().focus());
    await act(() => vi.advanceTimersByTimeAsync(1_000));
    expect(screen.container.textContent).toContain("Releasing in 179s");
    expect(voice.mutate).not.toHaveBeenCalled();

    const originalField = promptField();
    await act(() => input(originalField, "Updated prompt"));
    expect(document.activeElement).toBe(originalField);
    expect(promptField()).toBe(originalField);
    await act(() => input(instructionsField(), "Include examples."));
    expect(onDirtyChange).toHaveBeenLastCalledWith(true);
    expect(voice.mutate).toHaveBeenCalledExactlyOnceWith(id, "edit-begin", {
      expected_revision: revision,
    });

    const capped = Array.from({ length: 49 }, (_, i) => ({ ...second, id: `fixture-${i}` }));
    voice.list.mockResolvedValue({ drafts: [second, ...capped] });
    await act(() => vi.advanceTimersByTimeAsync(2_000));
    expect(screen.container.textContent).toContain("Outside the latest Pending sample");
    expect(promptField().value).toBe("Updated prompt");
    expect(instructionsField().value).toBe("Include examples.");

    await act(() => screen.rerender(show(second.id)));
    expect(promptField().value).toBe(second.text);
    expect(instructionsField().value).toBe("");
    voice.list.mockResolvedValue({ drafts: [editing, second] });
    await act(() => vi.advanceTimersByTimeAsync(3_000));
    await act(() => screen.rerender(show(id)));
    expect(promptField().value).toBe("Updated prompt");
    expect(instructionsField().value).toBe("Include examples.");
    const send = screen.container.querySelector<HTMLButtonElement>(
      'button[aria-label="Send prompt to Review fixture now"]',
    )!;
    await act(() => send.click());
    expect(voice.mutate.mock.calls.slice(1)).toEqual([
      [id, "edit-save", { expected_revision: revision + 1, text, edit_handle: "fixture-lease" }],
      [id, "send-now", { expected_revision: revision + 2 }],
    ]);
    expect(onDirtyChange).toHaveBeenLastCalledWith(false);
  });
});
