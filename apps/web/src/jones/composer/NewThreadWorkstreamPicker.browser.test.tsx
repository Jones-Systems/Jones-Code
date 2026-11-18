import "~/index.css";
import { scopeProjectRef } from "@t3tools/client-runtime/environment";
import type {
  PersonalWorkstreamView,
  PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import { render } from "../../../test/render";
import {
  DraftId,
  partializeComposerDraftStoreState,
  useComposerDraftStore,
} from "../../composerDraftStore";
import { NewThreadWorkstreamPicker } from "./NewThreadWorkstreamPicker";

vi.mock("../../state/workstreamsPersonal", () => ({
  usePersonalWorkstreams: () => ({ state: personalState }),
  personalWorkstreamsController: {},
}));

const environmentId = EnvironmentId.make("picker-test-environment");
const project = scopeProjectRef(environmentId, ProjectId.make("picker-test-project"));
const draftId = DraftId.make("picker-test-draft");
const timestamp = "2026-11-17T12:00:00.000Z";
const workstream: PersonalWorkstreamView = {
  id: "alpha",
  name: "Alpha",
  lifecycle: "active",
  color: null,
  orderKey: "a",
  createdAt: timestamp,
  updatedAt: timestamp,
};
const personalState: PersonalWorkstreamsState = {
  workstreams: [
    workstream,
    { ...workstream, id: "paused", name: "Paused", lifecycle: "paused" },
    { ...workstream, id: "snoozed", name: "Snoozed", snoozedUntil: "2099-01-01T00:00:00.000Z" },
  ],
  environments: [{ environmentId, status: "ready", confirmed: true, workstreamCount: 3 }],
  assignmentsByThreadKey: new Map(),
  pendingThreadKeys: new Set(),
  canCreate: true,
};

function DraftPicker() {
  const selected = useComposerDraftStore(
    (store) => store.getDraftSession(draftId)?.newThreadWorkstreamId,
  );
  return (
    <NewThreadWorkstreamPicker
      environmentId={environmentId}
      value={selected ?? null}
      disabled={false}
      onChange={(id) => useComposerDraftStore.getState().setNewThreadWorkstream(draftId, id)}
    />
  );
}

it("selects, persists, and clears an eligible Workstream on the current draft", async () => {
  const drafts = useComposerDraftStore.getState();
  drafts.setProjectDraftThreadId(project, draftId, {
    threadId: ThreadId.make("picker-test-thread"),
  });
  try {
    await using _screen = await render(<DraftPicker />, { router: false });
    await page.getByRole("button", { name: "Workstream for new thread" }).click();
    await expect.element(page.getByRole("menuitemradio", { name: "Paused" })).toBeVisible();
    await expect
      .element(page.getByRole("menuitemradio", { name: "Snoozed" }))
      .not.toBeInTheDocument();
    await page.getByRole("menuitemradio", { name: "Alpha" }).click();
    expect(
      partializeComposerDraftStoreState(useComposerDraftStore.getState()).draftThreadsByThreadKey[
        draftId
      ]?.newThreadWorkstreamId,
    ).toBe("alpha");

    await page.getByRole("button", { name: "Workstream: Alpha" }).click();
    await page.getByRole("menuitemradio", { name: "No Workstream" }).click();
    await expect
      .element(page.getByRole("button", { name: "Workstream for new thread" }))
      .toBeVisible();
    expect(drafts.getDraftSession(draftId)?.newThreadWorkstreamId).toBeUndefined();
  } finally {
    drafts.clearDraftThread(draftId);
  }
});
