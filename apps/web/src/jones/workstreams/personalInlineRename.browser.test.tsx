import "~/index.css";
import { useState } from "react";
import { describe, expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { act, render } from "../../../test/render";
import { PersonalSidebarGroupHeader } from "./PersonalSidebarGroupHeader";
import {
  PersonalWorkstreamTitleEditor,
  type PersonalWorkstreamTitleEditorProps,
} from "./PersonalWorkstreamTitleEditor";

const summary = { total: 3, running: 1, attention: 1, done: 1, failed: 0 };
const title = () => page.getByRole("textbox", { name: "Workstream name", exact: true });

function HeaderFixture({ initiallyCollapsed = false }: { initiallyCollapsed?: boolean }) {
  const [collapsed, setCollapsed] = useState(initiallyCollapsed);
  const [editing, setEditing] = useState(false);
  const [name, setName] = useState("Alpha");
  return (
    <div style={{ width: 360 }}>
      <PersonalSidebarGroupHeader
        name={name}
        summary={summary}
        collapsed={collapsed}
        onToggle={() => {
          setCollapsed((value) => !value);
          return () => setCollapsed(collapsed);
        }}
        onRenameRequested={() => setEditing(true)}
        nameEditor={
          editing ? (
            <PersonalWorkstreamTitleEditor
              name={name}
              pending={false}
              onSave={(value) => {
                setName(value);
                setEditing(false);
              }}
              onCancel={() => setEditing(false)}
            />
          ) : undefined
        }
      />
      {!collapsed ? <p>Members of {name}</p> : null}
    </div>
  );
}

describe("inline Workstream title editing", () => {
  it("preserves title-click collapse when rename is not enabled", async () => {
    function Header() {
      const [collapsed, setCollapsed] = useState(false);
      return (
        <PersonalSidebarGroupHeader
          name="Alpha"
          summary={summary}
          collapsed={collapsed}
          onToggle={() => setCollapsed((value) => !value)}
        />
      );
    }
    await using _screen = await render(<Header />);
    await userEvent.click(page.getByText("Alpha", { exact: true }));
    await expect
      .element(page.getByRole("button", { name: "Expand Alpha", exact: true }))
      .toHaveAttribute("aria-expanded", "false");
  });

  it.each([false, true])(
    "double-clicks the title without changing collapsed=%s, selects it, and saves inline",
    async (initiallyCollapsed) => {
      await using screen = await render(<HeaderFixture initiallyCollapsed={initiallyCollapsed} />);
      await userEvent.dblClick(page.getByText("Alpha", { exact: true }));
      await expect.element(title()).toHaveFocus();
      const input = screen.container.querySelector<HTMLInputElement>("input")!;
      expect(input.selectionStart).toBe(0);
      expect(input.selectionEnd).toBe("Alpha".length);
      expect(input.closest("button")).toBeNull();
      await userEvent.keyboard("  Beta  {Enter}");
      await expect.element(title()).not.toBeInTheDocument();
      await expect
        .element(
          page.getByRole("button", {
            name: `${initiallyCollapsed ? "Expand" : "Collapse"} Beta`,
            exact: true,
          }),
        )
        .toHaveAttribute("aria-expanded", String(!initiallyCollapsed));
      if (!initiallyCollapsed) {
        await expect.element(page.getByText("Members of Beta", { exact: true })).toBeVisible();
      }
    },
  );

  it("preserves single title-click and keyboard collapse, and Escape restores the unchanged title", async () => {
    await using screen = await render(<HeaderFixture />);
    await userEvent.click(page.getByText("Alpha", { exact: true }));
    await expect
      .element(page.getByRole("button", { name: "Expand Alpha", exact: true }))
      .toBeVisible();
    await act(() => screen.container.querySelector<HTMLButtonElement>("button")!.focus());
    await userEvent.keyboard("{Enter}");
    await expect
      .element(page.getByRole("button", { name: "Collapse Alpha", exact: true }))
      .toBeVisible();
    await userEvent.dblClick(page.getByText("Alpha", { exact: true }));
    await userEvent.fill(title(), "Unsaved title");
    await userEvent.keyboard("{Escape}");
    await expect.element(title()).not.toBeInTheDocument();
    await expect
      .element(page.getByRole("button", { name: "Collapse Alpha", exact: true }))
      .toBeVisible();
    await expect.element(page.getByText("Unsaved title", { exact: true })).not.toBeInTheDocument();
  });

  it("retains the native title-click reversal across a slow double-click sequence", async () => {
    await using screen = await render(<HeaderFixture />);
    const target = () => screen.container.querySelector(".line-clamp-2")!;
    await act(() => target().dispatchEvent(new MouseEvent("click", { bubbles: true, detail: 1 })));
    await expect
      .element(page.getByRole("button", { name: "Expand Alpha", exact: true }))
      .toBeVisible();
    await act(() => {
      target().dispatchEvent(new MouseEvent("click", { bubbles: true, detail: 2 }));
      target().dispatchEvent(new MouseEvent("dblclick", { bubbles: true, detail: 2 }));
    });
    await expect.element(title()).toHaveFocus();
    expect(screen.container.querySelector("button input")).toBeNull();
    await userEvent.keyboard("{Escape}");
    await expect
      .element(page.getByRole("button", { name: "Collapse Alpha", exact: true }))
      .toBeVisible();
    await expect.element(page.getByText("Members of Alpha", { exact: true })).toBeVisible();
  });

  it("rejects blank names on Enter and blur, then saves a trimmed 160-character name", async () => {
    const save = vi.fn();
    await using _screen = await render(
      <>
        <PersonalWorkstreamTitleEditor
          name="Alpha"
          pending={false}
          onSave={save}
          onCancel={vi.fn()}
        />
        <button type="button">Outside</button>
      </>,
    );
    await userEvent.fill(title(), "   ");
    await userEvent.keyboard("{Enter}");
    await userEvent.click(page.getByRole("button", { name: "Outside", exact: true }));
    expect(save).not.toHaveBeenCalled();
    await expect.element(title()).toHaveValue("   ");
    await expect.element(title()).toHaveFocus();
    await expect
      .element(page.getByRole("alert"))
      .toHaveTextContent("Workstream name cannot be empty.");
    await userEvent.fill(title(), "x".repeat(160));
    await userEvent.keyboard("{Enter}");
    await expect.poll(() => save.mock.calls).toEqual([["x".repeat(160)]]);
    await expect.element(title()).toHaveAttribute("maxlength", "160");
  });

  it("locks external pending edits, Enter, Escape, and blur", async () => {
    const save = vi.fn();
    const cancel = vi.fn();
    await using _screen = await render(
      <>
        <PersonalWorkstreamTitleEditor name="Alpha" pending onSave={save} onCancel={cancel} />
        <button type="button">Outside</button>
      </>,
    );
    await userEvent.keyboard("Changed{Enter}{Escape}");
    await userEvent.click(page.getByRole("button", { name: "Outside", exact: true }));
    await expect.element(title()).toHaveValue("Alpha");
    expect(save).not.toHaveBeenCalled();
    expect(cancel).not.toHaveBeenCalled();
  });

  it("commits blur once, locks duplicate saves, and preserves focus and draft for rejected-save retry", async () => {
    let reject!: (error: Error) => void;
    const request = new Promise<void>((_resolve, fail) => {
      reject = fail;
    });
    const save = vi
      .fn<PersonalWorkstreamTitleEditorProps["onSave"]>()
      .mockReturnValueOnce(request)
      .mockResolvedValueOnce(undefined);
    const cancel = vi.fn();
    await using _screen = await render(
      <>
        <PersonalWorkstreamTitleEditor
          name="Alpha"
          pending={false}
          onSave={save}
          onCancel={cancel}
        />
        <button type="button">Outside</button>
      </>,
    );
    await userEvent.fill(title(), "  Retry this title  ");
    await userEvent.click(page.getByRole("button", { name: "Outside", exact: true }));
    await userEvent.click(title());
    await userEvent.keyboard("{Enter}{Enter}{Escape}");
    expect(save).toHaveBeenCalledExactlyOnceWith("Retry this title");
    expect(cancel).not.toHaveBeenCalled();
    await userEvent.click(page.getByRole("button", { name: "Outside", exact: true }));
    await act(() => reject(new Error("Transport failed")));
    await expect.element(title()).toHaveFocus();
    await expect.element(title()).toHaveValue("  Retry this title  ");
    await expect
      .element(page.getByRole("alert"))
      .toHaveTextContent("Couldn't rename the Workstream. Try again.");
    await userEvent.keyboard("{Enter}");
    await expect.poll(() => save.mock.calls.length).toBe(2);
    expect(save).toHaveBeenLastCalledWith("Retry this title");
  });

  it("retains an unsaved draft across parent rollback and exposes the supplied failure", async () => {
    const save = vi.fn();
    const cancel = vi.fn();
    await using screen = await render(
      <PersonalWorkstreamTitleEditor
        name="Alpha"
        pending={false}
        onSave={save}
        onCancel={cancel}
      />,
    );
    await userEvent.fill(title(), "Unsaved retry");
    await screen.rerender(
      <PersonalWorkstreamTitleEditor
        name="Server rollback"
        pending={false}
        error="Rename failed remotely"
        onSave={save}
        onCancel={cancel}
      />,
    );
    await expect.element(title()).toHaveValue("Unsaved retry");
    await expect.element(title()).toHaveAccessibleDescription("Rename failed remotely");
    await userEvent.keyboard("{Escape}");
    expect(cancel).toHaveBeenCalledOnce();
    expect(save).not.toHaveBeenCalled();
  });
});
