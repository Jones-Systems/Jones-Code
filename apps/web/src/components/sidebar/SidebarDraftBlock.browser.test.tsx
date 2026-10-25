import { EnvironmentId, ProjectId } from "@t3tools/contracts";
import { scopeProjectRef } from "@t3tools/client-runtime/environment";
import { expect, it, vi } from "vite-plus/test";
import { act, render } from "../../../test/render";
import { DraftId, useComposerDraftStore } from "../../composerDraftStore";
import { SidebarDraftBlock } from "./SidebarDraftBlock";

it("hides excluded drafts, including the frozen active row, and restores them without losing text", async () => {
  const previous = useComposerDraftStore.getState();
  const first = EnvironmentId.make("draft-first");
  const second = EnvironmentId.make("draft-second");
  const project = ProjectId.make("draft-project");
  const firstDraft = DraftId.make("draft-first-id");
  const secondDraft = DraftId.make("draft-second-id");
  const navigate = vi.fn();
  const view = (
    excludedEnvironmentIds: ReadonlySet<string>,
    scopedProjectKeys: ReadonlySet<string> | null = null,
  ) => (
    <ul>
      <SidebarDraftBlock
        projectByKey={new Map()}
        projectDisplayNameByKey={new Map()}
        scopedProjectKeys={scopedProjectKeys}
        excludedEnvironmentIds={excludedEnvironmentIds}
        routeDraftId={firstDraft}
        onNavigateToDraft={navigate}
        onDraftContextMenu={() => undefined}
      />
    </ul>
  );
  try {
    useComposerDraftStore.setState({ draftThreadsByThreadKey: {}, draftsByThreadKey: {} });
    const store = useComposerDraftStore.getState();
    store.setLogicalProjectDraftThreadId(
      "first-project",
      scopeProjectRef(first, project),
      firstDraft,
    );
    store.setLogicalProjectDraftThreadId(
      "second-project",
      scopeProjectRef(second, project),
      secondDraft,
    );
    store.setPrompt(firstDraft, "Active draft text");
    store.setPrompt(secondDraft, "Other draft text");
    await using screen = await render(view(new Set()));
    const rows = () =>
      screen.container.querySelectorAll<HTMLElement>('[data-testid="sidebar-draft-row"]');
    expect(rows()).toHaveLength(2);

    await screen.rerender(view(new Set([first])));
    expect(rows()).toHaveLength(1);
    expect(screen.container.textContent).not.toContain("Active draft text");
    await act(() => rows()[0]!.click());
    expect(navigate).toHaveBeenCalledExactlyOnceWith(secondDraft);

    await screen.rerender(view(new Set([first, second])));
    expect(rows()).toHaveLength(0);
    expect(screen.container.querySelector('[data-testid="sidebar-draft-divider"]')).toBeNull();
    await screen.rerender(view(new Set(), new Set([`${first}:${project}`])));
    expect(rows()).toHaveLength(1);
    expect(screen.container.textContent).toContain("Active draft text");
    expect(useComposerDraftStore.getState().getComposerDraft(secondDraft)?.prompt).toBe(
      "Other draft text",
    );
  } finally {
    useComposerDraftStore.setState(previous);
  }
});
