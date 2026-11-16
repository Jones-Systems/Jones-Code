import "~/index.css";
import { type ReactNode, useState } from "react";
import { EnvironmentId, RuntimeRequestId, ThreadId } from "@t3tools/contracts";
import { AsyncResult, Atom } from "effect/reactivity";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { render } from "../../../test/render";
import { ComposerBanner } from "../../components/chat/ComposerBanner";
import { ComposerBannerStack } from "../../components/chat/ComposerBannerStack";
import { useComposerCardViewport } from "./useComposerCardViewport";

const childState = vi.hoisted(() => ({ approvals: null as unknown, respond: vi.fn() }));
vi.mock("../../state/session", () => ({ useEnvironmentScope: () => true }));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => childState.respond }));
vi.mock("../../state/threads", () => ({
  childThreadApprovalsAtom: () => childState.approvals,
  threadEnvironment: { respondToApproval: {} },
}));
import { ChildThreadApprovals } from "../../components/chat/ChildThreadApprovals";

async function browserViewport() {
  const original = { width: innerWidth, height: innerHeight };
  await page.viewport(900, 900);
  return {
    async [Symbol.asyncDispose]() {
      await page.viewport(original.width, original.height);
    },
  };
}

function Fixture({
  initialHeight = 480,
  independent = false,
  attached = true,
  independentNotices,
}: {
  initialHeight?: number;
  independent?: boolean;
  attached?: boolean;
  independentNotices?: ReactNode;
}) {
  const [workspace, setWorkspace] = useState<HTMLDivElement | null>(null);
  const [height, setHeight] = useState(initialHeight);
  const [warning, setWarning] = useState(false);
  const [editorHeight, setEditorHeight] = useState(110);
  const [showIndependent, setShowIndependent] = useState(independent);
  const viewport = useComposerCardViewport(workspace);
  return (
    <div style={{ width: 840, height, position: "relative" }}>
      <header style={{ height: 52 }} data-testid="header">
        Conversation
        <button onClick={() => setHeight(height === 480 ? 800 : 480)}>Resize conversation</button>
        <button onClick={() => setWarning(true)}>Add environment notice</button>
        <button onClick={() => setEditorHeight(180)}>Expand draft</button>
        <button onClick={() => setShowIndependent(false)}>Clear independent notice</button>
      </header>
      <div ref={setWorkspace} style={{ position: "absolute", inset: "52px 0 0" }}>
        <div
          data-chat-composer-overlay
          className="[--chat-composer-drawer-inset:1.375rem]"
          style={{ position: "absolute", bottom: 0, width: "100%", paddingTop: 8 }}
        >
          <ComposerBanner.Dock className="not-has-data-[composer-banner-surface=attached]:has-data-[composer-independent-notices]:flex">
            <ComposerBanner.Column {...viewport} role="region" aria-label="Conversation cards">
              {showIndependent
                ? (independentNotices ?? (
                    <div
                      data-composer-independent-notices="true"
                      className="mb-2 flex max-h-80 shrink-0 flex-col gap-2 overflow-auto"
                    >
                      <section style={{ minHeight: 114 }} aria-label="Independent child approval">
                        Independent child approval
                        <button type="button">Independent approval action</button>
                      </section>
                    </div>
                  ))
                : null}
              {attached ? (
                <>
                  <ComposerBannerStack
                    items={[
                      {
                        id: "approval",
                        icon: null,
                        title: "Child needs approval",
                        children: <div style={{ minHeight: 100 }}>Original approval details</div>,
                        actions: <button type="button">Open child</button>,
                        variant: "warning",
                      },
                      ...(warning
                        ? [
                            {
                              id: "environment",
                              icon: null,
                              title: "Environment notice",
                              group: "environment" as const,
                              children: (
                                <div style={{ minHeight: 80 }}>
                                  Environment details remain available
                                </div>
                              ),
                              variant: "warning" as const,
                            },
                          ]
                        : []),
                    ]}
                  />
                  <ComposerBanner.Attachment>
                    <ComposerBanner.Root>
                      <label style={{ display: "block", padding: 20, minHeight: 180 }}>
                        Question answer
                        <textarea
                          aria-label="Question answer"
                          style={{ display: "block", marginTop: 80 }}
                        />
                      </label>
                    </ComposerBanner.Root>
                  </ComposerBanner.Attachment>
                </>
              ) : null}
            </ComposerBanner.Column>
          </ComposerBanner.Dock>
          <div style={{ height: editorHeight }} data-testid="editor">
            <textarea aria-label="Ordinary draft" defaultValue="Independent normal draft" />
          </div>
          <div role="group" aria-label="External shortcuts" style={{ height: 52 }}>
            <button>Accounts</button>
            <button>Models</button>
            <button>Effort</button>
          </div>
        </div>
      </div>
    </div>
  );
}

it("keeps approval above the question within a short conversation and scrolls the answer without moving the editor or rails", async () => {
  await using _viewport = await browserViewport();
  await using screen = await render(<Fixture />);
  const cards = screen.getByRole("region", { name: "Conversation cards" }).element();
  const header = screen.getByTestId("header").element();
  const editor = screen.getByTestId("editor").element();
  const rails = screen.getByRole("group", { name: "External shortcuts" }).element();
  const answer = screen.getByRole("textbox", { name: "Question answer" }).element();
  const draft = screen.getByRole("textbox", { name: "Ordinary draft" }).element();
  await expect
    .poll(() => cards.getBoundingClientRect().top)
    .toBeGreaterThanOrEqual(header.getBoundingClientRect().bottom);
  expect(cards.scrollHeight).toBeGreaterThan(cards.clientHeight);
  expect(cards.textContent!.indexOf("Child needs approval")).toBeLessThan(
    cards.textContent!.indexOf("Question answer"),
  );
  const editorBefore = editor.getBoundingClientRect();
  const railsBefore = rails.getBoundingClientRect();
  await userEvent.click(answer);
  await userEvent.keyboard("Independent question answer");
  await expect.element(answer).toHaveValue("Independent question answer");
  await expect.poll(() => cards.scrollTop).toBeGreaterThan(0);
  expect(answer.getBoundingClientRect().top).toBeGreaterThanOrEqual(
    cards.getBoundingClientRect().top,
  );
  expect(answer.getBoundingClientRect().bottom).toBeLessThanOrEqual(
    cards.getBoundingClientRect().bottom,
  );
  await expect.element(draft).toHaveValue("Independent normal draft");
  expect(editor.getBoundingClientRect().top).toBe(editorBefore.top);
  expect(rails.getBoundingClientRect().top).toBe(railsBefore.top);
  cards.scrollTo({ top: 0 });
  const open = screen.getByRole("button", { name: "Open child" }).element();
  await expect
    .poll(() => open.getBoundingClientRect().top)
    .toBeGreaterThanOrEqual(header.getBoundingClientRect().bottom);
  const bounds = open.getBoundingClientRect();
  expect(document.elementFromPoint(bounds.x + bounds.width / 2, bounds.y + bounds.height / 2)).toBe(
    open,
  );
  open.focus();
  await userEvent.keyboard("{Tab}");
  expect(document.activeElement).toBe(answer);
  const answerBounds = answer.getBoundingClientRect();
  expect(
    document.elementFromPoint(
      answerBounds.x + answerBounds.width / 2,
      answerBounds.y + answerBounds.height / 2,
    ),
  ).toBe(answer);
});

it("does not shrink the actual independent approval component inside the constrained column", async () => {
  await using _viewport = await browserViewport();
  const thread = {
    environmentId: EnvironmentId.make("fixture-child-env"),
    threadId: ThreadId.make("fixture-child"),
  };
  childState.respond.mockReset();
  childState.respond.mockResolvedValue(AsyncResult.success(undefined));
  childState.approvals = Atom.make([
    {
      thread,
      title: "Actual child",
      approval: {
        requestId: RuntimeRequestId.make("fixture-request"),
        requestKind: "command",
        detail: "Run fixture compiler",
        createdAt: "2026-11-04T16:00:00.000Z",
        responseCapability: "live" as const,
      },
    },
  ]);
  await using screen = await render(
    <Fixture independent independentNotices={<ChildThreadApprovals threadRef={thread} />} />,
  );
  const cards = screen.getByRole("region", { name: "Conversation cards" }).element();
  const approval = screen.getByRole("region", { name: "Approval from Actual child" }).element();
  await expect
    .poll(() => cards.getBoundingClientRect().top)
    .toBeGreaterThanOrEqual(screen.getByTestId("header").element().getBoundingClientRect().bottom);
  const section = approval.getBoundingClientRect();
  expect(section.height).toBeGreaterThan(0);
  expect(approval.parentElement!.getBoundingClientRect().height).toBeGreaterThanOrEqual(
    section.height,
  );
  expect(section.top).toBeGreaterThanOrEqual(cards.getBoundingClientRect().top);
  expect(section.bottom).toBeLessThanOrEqual(cards.getBoundingClientRect().bottom);
  const button = screen.getByRole("button", { name: "Approve", exact: true }).element();
  const point = button.getBoundingClientRect();
  expect(
    button.contains(
      document.elementFromPoint(point.x + point.width / 2, point.y + point.height / 2),
    ),
  ).toBe(true);
  await userEvent.click(screen.getByRole("textbox", { name: "Question answer" }));
  await userEvent.keyboard("Answer below actual approval");
  await expect.poll(() => cards.scrollTop).toBeGreaterThan(0);
  await expect
    .element(screen.getByRole("textbox", { name: "Ordinary draft" }))
    .toHaveValue("Independent normal draft");
  expect(childState.respond).not.toHaveBeenCalled();
});

it("keeps the independent child approval before attached cards in the same bounded scroll region", async () => {
  await using _viewport = await browserViewport();
  await using screen = await render(<Fixture independent />);
  const cards = screen.getByRole("region", { name: "Conversation cards" }).element();
  const header = screen.getByTestId("header").element();
  await expect
    .poll(() => cards.getBoundingClientRect().top)
    .toBeGreaterThanOrEqual(header.getBoundingClientRect().bottom);
  expect(cards.textContent!.indexOf("Independent child approval")).toBeLessThan(
    cards.textContent!.indexOf("Child needs approval"),
  );
  const action = screen.getByRole("button", { name: "Independent approval action" }).element();
  const independent = screen.getByRole("region", { name: "Independent child approval" }).element();
  expect(independent.parentElement!.getBoundingClientRect().height).toBe(114);
  const actionBounds = action.getBoundingClientRect();
  expect(
    document.elementFromPoint(
      actionBounds.x + actionBounds.width / 2,
      actionBounds.y + actionBounds.height / 2,
    ),
  ).toBe(action);
  const editorTop = screen.getByTestId("editor").element().getBoundingClientRect().top;
  const railsTop = screen
    .getByRole("group", { name: "External shortcuts" })
    .element()
    .getBoundingClientRect().top;
  await userEvent.click(screen.getByRole("textbox", { name: "Question answer" }));
  await userEvent.keyboard("Independent answer with both approval surfaces");
  await expect.poll(() => cards.scrollTop).toBeGreaterThan(0);
  await expect
    .element(screen.getByRole("textbox", { name: "Ordinary draft" }))
    .toHaveValue("Independent normal draft");
  expect(screen.getByTestId("editor").element().getBoundingClientRect().top).toBe(editorTop);
  expect(
    screen.getByRole("group", { name: "External shortcuts" }).element().getBoundingClientRect().top,
  ).toBe(railsTop);
  const point = header.getBoundingClientRect();
  expect(cards.contains(document.elementFromPoint(point.x + 10, point.y + 10))).toBe(false);
});

it("shows an independent notice without attached cards and leaves no empty dock after it clears", async () => {
  await using _viewport = await browserViewport();
  await using screen = await render(<Fixture independent attached={false} />);
  const cards = screen.getByRole("region", { name: "Conversation cards" }).element();
  await expect
    .element(screen.getByRole("button", { name: "Independent approval action" }))
    .toBeVisible();
  await userEvent.click(screen.getByRole("button", { name: "Clear independent notice" }));
  await expect
    .element(screen.getByRole("button", { name: "Independent approval action" }))
    .not.toBeInTheDocument();
  expect(cards.getBoundingClientRect().height).toBe(0);
});

it("recalculates for new cards and resizing while preserving group order and restoring unconstrained tall layout", async () => {
  await using _viewport = await browserViewport();
  await using screen = await render(<Fixture initialHeight={800} />);
  const cards = screen.getByRole("region", { name: "Conversation cards" }).element();
  await expect.poll(() => cards.style.maxHeight).toBe("");
  await userEvent.click(screen.getByRole("button", { name: "Resize conversation" }));
  await expect.poll(() => getComputedStyle(cards).overflowY).toBe("auto");
  await userEvent.click(screen.getByRole("button", { name: "Add environment notice" }));
  await expect.element(screen.getByText("Environment notice", { exact: true })).toBeVisible();
  expect(cards.textContent!.indexOf("Environment notice")).toBeLessThan(
    cards.textContent!.indexOf("Child needs approval"),
  );
  expect(cards.textContent!.indexOf("Child needs approval")).toBeLessThan(
    cards.textContent!.indexOf("Question answer"),
  );
  expect(cards.getBoundingClientRect().top).toBeGreaterThanOrEqual(
    screen.getByTestId("header").element().getBoundingClientRect().bottom,
  );
  await userEvent.click(screen.getByRole("button", { name: "Expand draft" }));
  await expect
    .poll(() => cards.getBoundingClientRect().top)
    .toBeGreaterThanOrEqual(screen.getByTestId("header").element().getBoundingClientRect().bottom);
  await userEvent.click(screen.getByRole("button", { name: "Resize conversation" }));
  await expect.poll(() => cards.style.maxHeight).toBe("");
  expect(getComputedStyle(cards).overflowY).toBe("visible");
  await expect.element(screen.getByRole("textbox", { name: "Question answer" })).toBeVisible();
  await expect.element(screen.getByRole("group", { name: "External shortcuts" })).toBeVisible();
});
