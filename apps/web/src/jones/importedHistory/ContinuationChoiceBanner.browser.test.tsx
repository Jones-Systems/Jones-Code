import "../../index.css";
import { render } from "../../../test/render";
import { expect, it, vi } from "vite-plus/test";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type { ImportedHistoryCorrelation } from "@t3tools/client-runtime/jones/imported-history/continuation";
const h = vi.hoisted(() => ({
  saved: null as ImportedHistoryCorrelation | null,
  start: vi.fn(),
  observe: vi.fn(),
  identity: vi.fn(),
}));
vi.mock("@t3tools/client-runtime/jones/imported-history/commands", () => ({
  createImportedHistoryCommands: () => ({
    start: "start",
    observe: "observe",
    identity: "identity",
  }),
}));
vi.mock("../../connection/runtime", () => ({ connectionAtomRuntime: {} }));
vi.mock("../../state/use-atom-command", () => ({
  useAtomCommand: (command: string) =>
    command === "start" ? h.start : command === "observe" ? h.observe : h.identity,
}));
vi.mock("../../composerDraftStore", () => ({
  importedHistoryCorrelationStorage: () => ({
    withLock: async <A,>(operation: () => Promise<A>): Promise<A> => operation(),
    read: () => h.saved,
    reserve: (value: ImportedHistoryCorrelation) => {
      h.saved = value;
    },
    remove: () => {
      h.saved = null;
    },
  }),
}));
import { ContinuationChoiceBanner } from "./ContinuationChoiceBanner";
it.each([320, 768])(
  "keeps the real attached banner readable and actions inside a %spx composer",
  async (width) => {
    h.saved = null;
    const sizedContainer = document.createElement("div");
    sizedContainer.style.cssText = `width:${width}px; --chat-composer-drawer-inset:8px; --glass-opacity:100%; --glass-blur:0px; --glass-saturation:100%;`;
    await using screen = await render(
      <>
        <ContinuationChoiceBanner
          environmentId={EnvironmentId.make("environment:test")}
          threadId={ThreadId.make("thread:test")}
          prepared={{
            delivery: {
              type: "queued_run",
              runId: "run:test" as never,
              messageId: "message:test" as never,
            },
            reviewedBasis: "a".repeat(64),
            draftIdentity: "draft:test",
            unchanged: () => true,
          }}
          onDismiss={() => undefined}
        />
        <div
          data-testid="composer"
          style={{ position: "relative", height: 80, background: "white" }}
        />
      </>,
      { container: sizedContainer, router: false },
    );
    const container = screen.container;
    const attachment = container.querySelector('[data-slot="composer-banner-attachment"]')!;
    const attachmentRect = attachment.getBoundingClientRect();
    const composerRect = container
      .querySelector('[data-testid="composer"]')!
      .getBoundingClientRect();
    const statusRect = container.querySelector('[role="status"]')!.getBoundingClientRect();
    expect(attachmentRect.width).toBeGreaterThan(width - 32);
    expect(composerRect.top).toBeLessThan(attachmentRect.bottom);
    expect(statusRect.width).toBeGreaterThan(150);
    for (const button of container.querySelectorAll("button")) {
      const rect = button.getBoundingClientRect();
      expect(rect.left).toBeGreaterThanOrEqual(attachmentRect.left);
      expect(rect.right).toBeLessThanOrEqual(attachmentRect.right);
      expect(rect.bottom).toBeLessThanOrEqual(composerRect.top);
    }
    expect(container.scrollWidth).toBeLessThanOrEqual(width);
  },
);
