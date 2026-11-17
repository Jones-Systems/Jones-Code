import producerV4Raw from "../../../../../packages/contracts/src/jones/fixtures/work_queue_metadata_v4.json.fixture?raw";
import {
  publishWorkQueueLocalNames,
  readWorkQueueLocalNames,
  workQueueDestinationLabel,
} from "@t3tools/client-runtime/work-queue-metadata";
import { voiceReviewRecentFixture } from "@t3tools/client-runtime/voice-review/fixtures";
import * as Schema from "effect/Schema";
import {
  WorkQueueMetadata as MetadataSchema,
  type VoiceReviewRecentEntry,
} from "@t3tools/contracts";
import { act, render as renderScreen, type Screen } from "../../../test/render";
import { describe, expect, it, vi } from "vite-plus/test";
import { buildPromptItems, type PromptItem } from "./promptItems";
import {
  PromptAction,
  PromptActions,
  PromptDetail,
  PromptEvidence,
  PromptFacts,
} from "./PromptDetail";
import { PromptProgress } from "./PromptProgress";
import { promptProgressSteps } from "./promptPresentation";

const decodeMetadata = Schema.decodeUnknownSync(MetadataSchema);
const formatTime = (ms: number) => `at ${ms}`;

describe("prompt detail", () => {
  let root: Screen;
  let renderDisposables: AsyncDisposableStack;
  let container: HTMLDivElement;
  async function setupFixture() {
    const stack = new AsyncDisposableStack();
    renderDisposables = stack;
    stack.defer(async () => {
      await act(() => root.unmount());
      container.remove();
      vi.unstubAllGlobals();
    });
    try {
      container = document.createElement("div");
      document.body.append(container);
      root = renderDisposables.use(await renderScreen(null, { container, router: false }));
      return stack;
    } catch (error) {
      await stack.disposeAsync();
      throw error;
    }
  }
  function fact(scope: Element, label: string) {
    return [...scope.querySelectorAll("dt")]
      .filter((term) => term.textContent === label)
      .map((term) => term.nextElementSibling?.textContent ?? "");
  }
  const voiceItem = (entry: VoiceReviewRecentEntry): PromptItem =>
    buildPromptItems({ drafts: [], recent: [entry], snapshot: null, nativeEnvironmentId: null })
      .queued[0]!;

  it("shows all ten assignment states and same-environment names without write controls", async () => {
    await using _fixture = await setupFixture();
    const snapshot = decodeMetadata(JSON.parse(producerV4Raw));
    const built = buildPromptItems({
      drafts: [],
      recent: [],
      snapshot,
      nativeEnvironmentId: null,
    });
    const items = [...built.queued, ...built.sent];
    const release = publishWorkQueueLocalNames(snapshot.source.environment_ref, [
      { workstreamId: "assigned-work", name: "Desired project" },
      { workstreamId: "other-work", name: "<img src=x onerror=alert(1)>" },
    ]);
    try {
      const names = readWorkQueueLocalNames();
      const render = async (environmentRef: string) =>
        await act(() =>
          root.rerender(
            <div>
              {items.map((item) => (
                <PromptEvidence
                  key={item.key}
                  item={item}
                  formatTime={formatTime}
                  workstreamName={(id) => workQueueDestinationLabel(id, environmentRef, names)}
                />
              ))}
            </div>,
          ),
        );
      await render(snapshot.source.environment_ref);
      const states = fact(container, "Assignment");
      for (const label of [
        "Issued",
        "Pending",
        "Satisfied",
        "Conflict",
        "Rejected",
        "Unavailable",
        "Unsupported",
        "Permanent hold",
        "Excluded",
        "Cancelled",
      ]) {
        expect(states).toContain(label);
      }
      expect(states).toContain("Assignment information unavailable");
      expect(fact(container, "Desired workstream")).toContain("Desired project (assigned-work)");
      expect(fact(container, "Observed workstream")).toContain("Not observed");
      expect(fact(container, "Observed workstream")).toContain(
        "<img src=x onerror=alert(1)> (other-work)",
      );
      expect(fact(container, "Assignment reason")).toContain(
        "Thread belongs to another workstream",
      );
      expect(container.textContent).not.toContain("Unassigned");
      expect(fact(container, "Completion").every((value) => value === "Not tracked")).toBe(true);
      expect(container.querySelector("img,textarea,input,select,button")).toBeNull();
      await render("another-environment");
      expect(fact(container, "Desired workstream")).toContain("assigned-work");
      expect(container.textContent).not.toContain("Desired project");
    } finally {
      release();
    }
  });

  it("shows opaque delivery identities without claiming native handoff", async () => {
    await using _fixture = await setupFixture();
    const item = voiceItem({
      ...voiceReviewRecentFixture.entries[0]!,
      queue_delivery_links: {
        status: "ready",
        reason: null,
        deliveries: [
          {
            delivery_id: "unconfirmed-sibling",
            queue_request_id: "queue-request",
            payload_sha256: "b".repeat(64),
            destination: {
              backend: "t3",
              instance: "opaque-instance",
              thread_id: "thread",
              project_id: null,
            },
          },
        ],
      },
    });
    await act(() =>
      root.rerender(
        <PromptEvidence item={item} formatTime={formatTime} workstreamName={(id) => id} />,
      ),
    );
    const details = container.querySelector("details")!;
    expect(details.open).toBe(false);
    await act(() => details.querySelector("summary")!.click());
    expect(details.open).toBe(true);
    expect(fact(details, "Delivery")).toEqual(["unconfirmed-sibling"]);
    expect(fact(details, "Backend")).toEqual(["t3 · opaque-instance"]);
    expect(fact(details, "Queue request")).toEqual(["queue-request"]);
    expect(details.textContent).toContain("do not prove native handoff");
    expect(details.textContent).toContain("Additional delivery links may be unavailable");
  });

  it("explains retained unlinked siblings and missing link data", async () => {
    await using _fixture = await setupFixture();
    const entry = voiceReviewRecentFixture.entries[0]!;
    const unlinked = voiceItem({
      ...entry,
      queue_delivery_links: {
        status: "ready",
        reason: null,
        deliveries: [],
        unlinked_delivery_count: 2,
      },
    });
    await act(() =>
      root.rerender(
        <PromptEvidence item={unlinked} formatTime={formatTime} workstreamName={(id) => id} />,
      ),
    );
    expect(container.textContent).toContain("Deliveries without queue links: 2.");
    expect(container.textContent).toContain(
      "Native handoff remains unconfirmed for these deliveries.",
    );
    expect(fact(container, "Queue request")).toEqual([]);
    await act(() =>
      root.rerender(
        <PromptEvidence
          item={voiceItem(entry)}
          formatTime={formatTime}
          workstreamName={(id) => id}
        />,
      ),
    );
    expect(container.textContent).toContain("Queue delivery links unavailable");
  });

  it("keeps literal text, provenance and the original transcript", async () => {
    await using _fixture = await setupFixture();
    const item = voiceItem({
      ...voiceReviewRecentFixture.entries[0]!,
      text: "<script>literal retained text</script>",
    });
    await act(() =>
      root.rerender(
        <PromptDetail
          title="fixture-thread"
          status={{ label: "Queued", tone: "neutral", explanation: "Read-only here." }}
          meta="Voice"
          progress={null}
          evidence={
            <PromptEvidence item={item} formatTime={formatTime} workstreamName={(id) => id} />
          }
        >
          <p>{item.text.kind === "available" ? item.text.text : ""}</p>
        </PromptDetail>,
      ),
    );
    expect(container.textContent).toContain("<script>literal retained text</script>");
    expect(container.querySelector("script")).toBeNull();
    expect(fact(container, "Text source")).toEqual(["Retained command text"]);
    expect(fact(container, "Command")[0]).toContain("command-1");
    expect(container.textContent).toContain("Original transcript");
    expect(container.textContent).toContain("Read-only here.");
    const heading = container.querySelector("h3")!;
    expect(heading.textContent).toBe("fixture-thread");
    expect(container.querySelector("section")!.getAttribute("aria-labelledby")).toBe(heading.id);
  });

  it("guards unavailable actions and describes why", async () => {
    await using _fixture = await setupFixture();
    const onClick = vi.fn();
    await act(() =>
      root.rerender(
        <PromptActions
          destructive={
            <PromptAction label="Delete prompt" reason="Working…" reasonId="why" onClick={onClick}>
              Delete
            </PromptAction>
          }
        >
          <PromptAction label="Send prompt" reason={null} reasonId="why" onClick={onClick}>
            Send
          </PromptAction>
        </PromptActions>,
      ),
    );
    const remove = container.querySelector<HTMLButtonElement>('[aria-label="Delete prompt"]')!;
    expect(remove.getAttribute("aria-disabled")).toBe("true");
    expect(remove.getAttribute("aria-describedby")).toBe("why");
    await act(() => remove.click());
    expect(onClick).not.toHaveBeenCalled();
    await act(() =>
      container.querySelector<HTMLButtonElement>('[aria-label="Send prompt"]')!.click(),
    );
    expect(onClick).toHaveBeenCalledTimes(1);
  });

  it("renders the progress rail with the current step and a fixed last line", async () => {
    await using _fixture = await setupFixture();
    const item = voiceItem(voiceReviewRecentFixture.entries[0]!);
    await act(() =>
      root.rerender(
        <div>
          <PromptProgress
            label="Progress for fixture"
            steps={promptProgressSteps(item, null)}
            formatTime={formatTime}
          />
          <PromptFacts facts={[["Example", "value"]]} />
        </div>,
      ),
    );
    const rail = container.querySelector('ol[aria-label="Progress for fixture"]')!;
    const steps = [...rail.querySelectorAll("li")];
    expect(steps).toHaveLength(7);
    expect(rail.querySelector('[aria-current="step"]')!.textContent).toContain("Released");
    expect(steps.at(-1)!.textContent).toContain("Finished");
    expect(steps.at(-1)!.textContent).toContain("Not tracked");
    expect(fact(container, "Example")).toEqual(["value"]);
  });
});
