import { EnvironmentId, type VoiceReviewDraft } from "@t3tools/contracts";
import { voiceReviewRecentFixture } from "@t3tools/client-runtime/voice-review/fixtures";
import type { PersonalEnvironmentState } from "@t3tools/client-runtime/jones/workstreams/personal";
import { describe, expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import { act, fakeTimers, render } from "../../../test/render";
import { EnvironmentVoiceReview } from "./VoiceReviewPage";
import type { VoiceReviewTransport } from "./voiceReviewActions";

const voice = vi.hoisted(() => ({
  list: vi.fn(),
  mutate: vi.fn<VoiceReviewTransport["mutate"]>(),
  get: vi.fn<VoiceReviewTransport["get"]>(),
  sendOptions: vi.fn<VoiceReviewTransport["sendOptions"]>(),
  sendReviewed: vi.fn<VoiceReviewTransport["sendReviewed"]>(),
  environments: [] as readonly PersonalEnvironmentState[],
}));

vi.mock("./useVoiceReview", () => ({
  useVoiceReview: () => ({
    fetchList: voice.list,
    transport: voice,
    review: { diagnostics: vi.fn() },
  }),
}));

vi.mock("../../state/workstreamsPersonal", () => ({
  usePersonalWorkstreams: () => ({ state: { environments: voice.environments } }),
}));

const draft: VoiceReviewDraft = {
  ...voiceReviewRecentFixture.entries[1]!.draft,
  state: "held",
  remaining_ms: 180_000,
  routing_target: "Review fixture",
};
const second = { ...draft, id: "second-prompt", routing_target: "Other fixture" } as const;
const environmentId = EnvironmentId.make("fixture");
const options = {
  schema: "voice.reviewed-send/v1",
  workstream_ids: ["local", "remote", "closed"],
  can_send_without_workstream: true,
} as const;

function workstream(id: string, name: string, lifecycle: "active" | "completed" = "active") {
  return {
    id,
    name,
    lifecycle,
    color: null,
    orderKey: "b",
    createdAt: "2026-10-02T12:00:00.000Z",
    updatedAt: "2026-10-02T12:00:00.000Z",
  };
}

function pending(id = draft.id, unavailable = false, onDirtyChange?: (dirty: boolean) => void) {
  return (
    <EnvironmentVoiceReview
      environmentId={environmentId}
      linkedKey={`voice:${id}`}
      unavailable={unavailable}
      {...(onDirtyChange ? { onDirtyChange } : {})}
    />
  );
}

function setupVoiceFixture(): Disposable {
  voice.list.mockResolvedValue({ drafts: [draft, second] });
  voice.sendOptions.mockResolvedValue(null);
  return {
    [Symbol.dispose]() {
      voice.list.mockReset();
      voice.mutate.mockReset();
      voice.get.mockReset();
      voice.sendOptions.mockReset();
      voice.sendReviewed.mockReset();
      voice.environments = [];
    },
  };
}

function input(textarea: HTMLTextAreaElement, text: string) {
  Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value")!.set!.call(
    textarea,
    text,
  );
  textarea.dispatchEvent(new Event("input", { bubbles: true }));
}

describe("Pending inline composition", () => {
  it("shows both fields, keeps the countdown on focus, and retains fields through a capped refresh and selection", async () => {
    using _fixture = setupVoiceFixture();
    const { id, revision } = draft;
    const editing = { ...draft, revision: revision + 1, state: "editing" } as const;
    const text = "Updated prompt\n\nAdditional instructions:\nInclude examples.";
    voice.mutate
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
      'button[aria-label="Send now to Review fixture"]',
    )!;
    await act(() => send.click());
    expect(voice.mutate.mock.calls.slice(1)).toEqual([
      [id, "edit-save", { expected_revision: revision + 1, text, edit_handle: "fixture-lease" }],
      [id, "send-now", { expected_revision: revision + 2 }],
    ]);
    expect(onDirtyChange).toHaveBeenLastCalledWith(false);
  });

  it("announces the selected Workstream and sends to its reviewed route", async () => {
    using _fixture = setupVoiceFixture();
    voice.environments = [
      {
        environmentId,
        status: "ready",
        confirmed: true,
        workstreamCount: 1,
        workstreams: [workstream("local", "Design")],
      },
    ];
    voice.sendOptions.mockResolvedValue(options);
    const editing = { ...draft, state: "editing", revision: draft.revision + 1 } as const;
    voice.mutate.mockResolvedValueOnce({ draft: editing, edit_handle: "fixture-lease" });
    voice.sendReviewed.mockResolvedValue({
      draft: {
        ...draft,
        state: "released",
        revision: editing.revision + 1,
        command_id: "fixture-command",
      },
      edit_handle: null,
    });
    await using screen = await render(pending(), { router: false });
    await screen.getByRole("combobox", { name: "Workstream (optional)" }).click();
    await page.getByRole("option", { name: "Design", exact: true }).click();
    const send = screen.getByRole("button", { name: "Send now to Design", exact: true });
    await expect.element(send).toHaveTextContent("Send now");
    await send.click();
    expect(voice.sendReviewed).toHaveBeenCalledExactlyOnceWith(draft.id, {
      workstream_id: "local",
      expected_revision: editing.revision,
      edit_handle: "fixture-lease",
    });
  });

  it("offers only open broker-approved Workstreams from this environment and retains a stale choice", async () => {
    using _fixture = setupVoiceFixture();
    const local = workstream("local", "Design");
    const source = {
      environmentId,
      status: "ready",
      confirmed: true,
      workstreamCount: 3,
      workstreams: [
        local,
        workstream("closed", "Completed", "completed"),
        workstream("other", "Unapproved"),
      ],
    } as const;
    const foreign = {
      ...source,
      environmentId: EnvironmentId.make("another-environment"),
      workstreamCount: 2,
      workstreams: [workstream("local", "Foreign name"), workstream("remote", "Foreign inbox")],
    };
    voice.environments = [source, foreign];
    voice.sendOptions.mockResolvedValue(options);
    const editing = { ...draft, state: "editing", revision: draft.revision + 1 } as const;
    voice.mutate.mockResolvedValueOnce({ draft: editing, edit_handle: "fixture-lease" });
    voice.sendReviewed.mockResolvedValue({
      draft: {
        ...draft,
        state: "released",
        revision: editing.revision + 1,
        command_id: "fixture-command",
      },
      edit_handle: null,
    });
    const dirty = vi.fn();
    await using screen = await render(pending(draft.id, false, dirty), { router: false });
    const picker = screen.getByRole("combobox", { name: "Workstream (optional)" });
    const send = screen.getByRole("button", { name: "Send now", exact: true });
    await expect.element(picker).toHaveTextContent("No Workstream");
    await expect.element(send).toBeVisible();
    await picker.click();
    await expect.element(page.getByRole("option", { name: "No Workstream" })).toBeVisible();
    await expect.element(page.getByRole("option", { name: "Design", exact: true })).toBeVisible();
    for (const name of ["Foreign name", "Foreign inbox", "Unapproved", "Completed"]) {
      await expect.element(page.getByRole("option", { name, exact: true })).not.toBeInTheDocument();
    }
    expect(voice.mutate).not.toHaveBeenCalled();
    await page.getByRole("option", { name: "Design", exact: true }).click();
    await expect.element(picker).toHaveTextContent("Design");
    expect(dirty).toHaveBeenLastCalledWith(true);
    expect(voice.mutate).toHaveBeenCalledExactlyOnceWith(draft.id, "edit-begin", {
      expected_revision: draft.revision,
    });

    await screen.rerender(pending(second.id, false, dirty));
    await expect.element(picker).toHaveTextContent("No Workstream");
    await screen.rerender(pending(draft.id, true, dirty));
    voice.environments = [{ ...source, confirmed: false }, foreign];
    await screen.rerender(pending(draft.id, false, dirty));
    await expect.element(picker).toHaveTextContent("Selected Workstream unavailable");
    await expect.element(send).toHaveAttribute("aria-disabled", "true");
    await act(() => (send.element() as HTMLButtonElement).click());
    expect(voice.sendReviewed).not.toHaveBeenCalled();

    voice.sendOptions.mockResolvedValueOnce(null);
    const retry = screen.getByRole("button", { name: "Check send options again" });
    await retry.click();
    await expect.element(send).toHaveAttribute("aria-disabled", "true");
    await act(() => (send.element() as HTMLButtonElement).click());
    expect(voice.sendReviewed).not.toHaveBeenCalled();
    expect(voice.mutate).toHaveBeenCalledExactlyOnceWith(draft.id, "edit-begin", {
      expected_revision: draft.revision,
    });

    await retry.click();
    await picker.click();
    await page.getByRole("option", { name: "No Workstream" }).click();
    await expect.element(picker).toHaveTextContent("No Workstream");
    await send.click();
    expect(voice.sendReviewed).toHaveBeenCalledExactlyOnceWith(draft.id, {
      workstream_id: null,
      expected_revision: editing.revision,
      edit_handle: "fixture-lease",
    });
  });

  it("holds Send now while checking or on failure and gives an actionable route issue for zero options", async () => {
    using _fixture = setupVoiceFixture();
    let resolveCheck!: (value: Awaited<ReturnType<VoiceReviewTransport["sendOptions"]>>) => void;
    const check = new Promise<Awaited<ReturnType<VoiceReviewTransport["sendOptions"]>>>(
      (resolve) => {
        resolveCheck = resolve;
      },
    );
    voice.sendOptions
      .mockReturnValueOnce(check)
      .mockRejectedValueOnce(new Error("Probe failed"))
      .mockResolvedValueOnce(null);
    await using screen = await render(pending(), { router: false });
    const send = screen.getByRole("button", { name: "Send now", exact: true });
    const retry = screen.getByRole("button", { name: "Check send options again" });
    await expect.element(send).toHaveAttribute("aria-disabled", "true");
    await expect.element(screen.getByText("Checking where this prompt can be sent…")).toBeVisible();
    await act(() =>
      resolveCheck({ ...options, workstream_ids: [], can_send_without_workstream: false }),
    );
    await expect.element(send).toHaveAttribute("aria-disabled", "true");
    await expect.element(screen.getByText(/No receiving thread is available/)).toBeVisible();
    await retry.click();
    await expect.element(send).toHaveAttribute("aria-disabled", "true");
    await expect
      .element(screen.getByText("Could not check this prompt’s destinations."))
      .toBeVisible();
    expect(voice.mutate).not.toHaveBeenCalled();
    expect(voice.sendReviewed).not.toHaveBeenCalled();
    await retry.click();
    await expect
      .element(screen.getByRole("button", { name: "Send now to Review fixture" }))
      .not.toHaveAttribute("aria-disabled");
  });

  it("identifies a known earlier-prompt hold without describing it as a missing inbox", async () => {
    using _fixture = setupVoiceFixture();
    voice.list.mockResolvedValue({
      drafts: [
        { ...draft, routing_state: "awaiting_dependency", dependencies: [second.id] },
        second,
      ],
    });
    voice.sendOptions.mockResolvedValue({
      ...options,
      workstream_ids: [],
      can_send_without_workstream: false,
    });
    await using screen = await render(pending(), { router: false });
    await expect
      .element(
        screen.getByText(
          "Waiting for an earlier prompt. Check send options again after it clears.",
        ),
      )
      .toBeVisible();
    await expect
      .element(screen.getByRole("button", { name: "Send now", exact: true }))
      .toHaveAttribute("aria-disabled", "true");
    expect(voice.mutate).not.toHaveBeenCalled();
    expect(voice.sendReviewed).not.toHaveBeenCalled();
  });

  it("reconciles a lost reviewed send for the retained draft after a capped poll and row switch", async () => {
    using _fixture = setupVoiceFixture();
    using _timers = fakeTimers();
    voice.list.mockResolvedValue({
      drafts: [
        { ...draft, state: "blocked", routing_state: "owner_required", reason: "owner_required" },
        second,
      ],
    });
    voice.sendOptions.mockResolvedValue(options);
    const editing = { ...draft, revision: draft.revision + 1, state: "editing" } as const;
    voice.mutate.mockResolvedValueOnce({ draft: editing, edit_handle: "fixture-lease" });
    voice.sendReviewed.mockRejectedValue(new Error("Lost reviewed send acknowledgement"));
    voice.get.mockResolvedValue(editing);
    await using screen = await render(pending(), { router: false });
    const send = screen.getByRole("button", { name: "Send now", exact: true });
    const textarea = screen.getByRole("textbox", { name: "Prompt text to Review fixture" });
    expect(screen.container.textContent).toContain(
      "Automatic release is waiting for a destination.",
    );
    expect(send.element().hasAttribute("aria-disabled")).toBe(false);
    await act(() => input(textarea.element() as HTMLTextAreaElement, "Local follow-up"));
    await act(() => (send.element() as HTMLButtonElement).click());
    expect(voice.sendReviewed).toHaveBeenCalledExactlyOnceWith(draft.id, {
      expected_revision: editing.revision,
      workstream_id: null,
      text: "Local follow-up",
      edit_handle: "fixture-lease",
    });
    expect(voice.get).toHaveBeenCalledExactlyOnceWith(draft.id);

    const capped = Array.from({ length: 49 }, (_, i) => ({ ...second, id: `capped-${i}` }));
    voice.list.mockResolvedValue({ drafts: [second, ...capped] });
    await act(() => vi.advanceTimersByTimeAsync(3_000));
    await screen.rerender(pending(second.id));
    await screen.rerender(pending());
    expect((textarea.element() as HTMLTextAreaElement).value).toBe("Local follow-up");
    expect(send.element().getAttribute("aria-disabled")).toBe("true");
    expect(screen.container.textContent).toContain("Outside the latest Pending sample");
    await act(() => (send.element() as HTMLButtonElement).click());
    expect(voice.sendReviewed).toHaveBeenCalledTimes(1);

    voice.get.mockResolvedValue({
      ...editing,
      revision: editing.revision + 1,
      state: "released",
      command_id: "fixture-command",
    });
    const check = screen.getByRole("button", { name: "Check current state" });
    await act(() => (check.element() as HTMLButtonElement).click());
    expect(voice.get.mock.calls).toEqual([[draft.id], [draft.id]]);
    expect(voice.sendReviewed).toHaveBeenCalledTimes(1);
    expect(voice.mutate).toHaveBeenCalledTimes(1);
    expect(screen.container.textContent).toContain("This prompt has ended");
  });
});
