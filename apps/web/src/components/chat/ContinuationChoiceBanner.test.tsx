// @vitest-environment jsdom
import { act } from "react";
import type { ButtonHTMLAttributes } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import * as DateTime from "effect/DateTime";
import type {
  OrchestrationV2ImportedHistoryReviewResult,
  OrchestrationV2ImportedHistoryStartReceipt,
} from "@t3tools/contracts";

const commands = vi.hoisted(() => ({
  review: vi.fn(),
  start: vi.fn(),
  observe: vi.fn(),
  reserve: vi.fn(),
  stopObserve: vi.fn(),
  stopClear: vi.fn(),
}));
vi.mock("../../state/threads", () => ({
  threadEnvironment: {
    reviewImportedHistoryStart: "review",
    deliverImportedContinuation: "start",
    observeImportedHistoryStart: "observe",
    observeCurrentThreadRuntimeStop: "stopObserve",
  },
}));
vi.mock("../../state/use-atom-command", () => ({
  useAtomCommand: (command: "review" | "start" | "observe" | "stopObserve") => commands[command],
}));
vi.mock("../../composerDraftStore", () => ({
  reserveImportedContinuationPointer: commands.reserve,
  clearImportedContinuationPointer: vi.fn(),
  clearCurrentRuntimeStopPointer: commands.stopClear,
  useComposerDraftStore: () => undefined,
}));
vi.mock("../ui/button", () => ({
  Button: ({
    size: _size,
    variant: _variant,
    ...props
  }: ButtonHTMLAttributes<HTMLButtonElement> & { size?: string; variant?: string }) => (
    <button {...props} />
  ),
}));

import {
  ContinuationChoiceBanner,
  QueuedContinuationChoice,
  CurrentRuntimeStopRecoveryBanner,
  type ContinuationChoiceBannerProps,
} from "./ContinuationChoiceBanner";
import type { CurrentRuntimeStopPointer } from "../../composerDraftStore";

const target = { type: "queued_run", runId: "run:held", messageId: "message:original" } as const;
const review = (patch = {}): OrchestrationV2ImportedHistoryReviewResult =>
  ({
    version: 2,
    threadId: "thread:imported",
    target,
    capability: { startWithImportedHistory: true },
    applicability: "imported",
    qualification: { type: "unknown", reason: "Imported provider version is unknown." },
    restoredBinding: { type: "missing", reason: "No attached native conversation." },
    nativeEffects: { type: "clear" },
    transcriptEligibility: { type: "eligible" },
    reviewedBasis: "basis:reviewed",
    ...patch,
  }) as never;

function receipt(
  input: Parameters<ContinuationChoiceBannerProps["onStart"]>[0],
  patch = {},
): OrchestrationV2ImportedHistoryStartReceipt {
  return {
    version: 2,
    commandId: input.commandId,
    threadId: input.threadId,
    target,
    reviewedBasis: input.reviewedBasis,
    intentStatus: "accepted",
    rejectionReason: null,
    receipt: {
      commandId: input.commandId,
      threadId: input.threadId,
      commandType: "thread.imported-history.start",
      acceptedAt: "2026-10-03T00:00:00Z",
      resultSequence: 1,
      status: "accepted",
      error: null,
    },
    execution: {
      status: "pending",
      runId: "run:held",
      providerThreadId: null,
      providerSessionId: null,
      nativeThreadId: null,
      effectOutcome: null,
      error: null,
    },
    ...patch,
  } as never;
}

describe("ContinuationChoiceBanner explicit unified delivery", () => {
  let root: Root | null = null;
  let container: HTMLDivElement | null = null;
  beforeEach(() => {
    vi.clearAllMocks();
  });
  afterEach(async () => {
    await act(async () => {
      root?.unmount();
    });
    container?.remove();
    root = null;
    container = null;
  });
  const mount = async (overrides: Partial<ContinuationChoiceBannerProps> = {}) => {
    const props: ContinuationChoiceBannerProps = {
      environmentId: "environment:test" as never,
      threadId: "thread:imported" as never,
      delivery: target as never,
      snapshot: {},
      review: review(),
      onStart: vi.fn(async (input) => receipt(input)),
      onObserve: vi.fn(),
      onReserve: vi.fn(),
      ...overrides,
    };
    container = document.createElement("div");
    document.body.append(container);
    root = createRoot(container);
    await act(async () => {
      root!.render(<ContinuationChoiceBanner {...props} />);
    });
    return props;
  };
  const button = (text: string) =>
    [...container!.querySelectorAll("button")].find((element) => element.textContent === text)!;

  it("requires the explicit action and admits the same held run once while its response is pending", async () => {
    let resolve!: (value: OrchestrationV2ImportedHistoryStartReceipt) => void;
    const onStart = vi.fn(
      (_input: Parameters<ContinuationChoiceBannerProps["onStart"]>[0]) =>
        new Promise<OrchestrationV2ImportedHistoryStartReceipt>((done) => {
          resolve = done;
        }),
    );
    const onIntentAccepted = vi.fn();
    await mount({ onStart, onIntentAccepted });
    expect(onStart).not.toHaveBeenCalled();
    await act(async () => {
      button("Start with imported history").click();
      button("Start with imported history").click();
    });
    expect(onStart).toHaveBeenCalledTimes(1);
    expect(onStart.mock.calls[0]![0]).toEqual({
      commandId: expect.any(String),
      threadId: "thread:imported",
      reviewedBasis: "basis:reviewed",
      delivery: target,
    });
    await act(async () => {
      resolve(receipt(onStart.mock.calls[0]![0]));
    });
    expect(onIntentAccepted).toHaveBeenCalledTimes(1);
    expect(container!.textContent).toContain("has not been confirmed started");
    expect(container!.textContent).not.toContain("Started a new agent conversation");
  });

  it.each([
    null,
    review({ capability: { startWithImportedHistory: false }, reviewedBasis: null }),
    review({ applicability: "not_imported", reviewedBasis: null }),
    review({
      qualification: { type: "qualified" },
      restoredBinding: { type: "ready" },
      reviewedBasis: null,
    }),
    review({ qualification: { type: "qualified" }, reviewedBasis: null }),
    review({
      nativeEffects: { type: "unknown", reason: "An earlier native effect needs reconciliation." },
      reviewedBasis: null,
    }),
    review({
      transcriptEligibility: { type: "ineligible", reason: "Transcript is unavailable." },
      reviewedBasis: null,
    }),
    review({ target: { ...target, runId: "run:other" } }),
  ])("does not offer start without a positive matching eligible review %#", async (value) => {
    const props = await mount({ review: value });
    expect(button("Start with imported history")).toBeUndefined();
    expect(props.onStart).not.toHaveBeenCalled();
  });

  it("reconciles a lost response with the original command without starting another operation", async () => {
    const onStart = vi.fn(
      async (_input: Parameters<ContinuationChoiceBannerProps["onStart"]>[0]) => {
        throw new Error("Lost response");
      },
    );
    const onObserve = vi.fn(
      async (input: Parameters<ContinuationChoiceBannerProps["onObserve"]>[0]) =>
        receipt({ ...input, delivery: target, reviewedBasis: "basis:reviewed" } as never, {
          execution: {
            status: "started",
            runId: "run:held",
            providerThreadId: "provider:thread",
            providerSessionId: "provider:session",
            nativeThreadId: "native:thread",
            effectOutcome: "confirmed_success",
            error: null,
          },
        }),
    );
    await mount({ onStart, onObserve });
    await act(async () => {
      button("Start with imported history").click();
    });
    expect(container!.textContent).toContain("Do not send it again");
    expect(button("Start with imported history")).toBeUndefined();
    await act(async () => {
      button("Check status").click();
    });
    expect(onObserve).toHaveBeenCalledWith({
      threadId: "thread:imported",
      commandId: onStart.mock.calls[0]![0].commandId,
    });
    expect(onStart).toHaveBeenCalledTimes(1);
    expect(container!.textContent).toContain("Started a new agent conversation");
  });

  it("does not retire an edited draft or accept success after the environment changes", async () => {
    let resolve!: (value: OrchestrationV2ImportedHistoryStartReceipt) => void;
    const onStart = vi.fn(
      (_input: Parameters<ContinuationChoiceBannerProps["onStart"]>[0]) =>
        new Promise<OrchestrationV2ImportedHistoryStartReceipt>((done) => {
          resolve = done;
        }),
    );
    const onIntentAccepted = vi.fn();
    const props = await mount({ onStart, onIntentAccepted });
    await act(async () => {
      button("Start with imported history").click();
    });
    await act(async () => {
      root!.render(
        <ContinuationChoiceBanner
          {...props}
          snapshot={{}}
          environmentId={"environment:other" as never}
        />,
      );
    });
    await act(async () => {
      resolve(receipt(onStart.mock.calls[0]![0]));
    });
    expect(onIntentAccepted).not.toHaveBeenCalled();
    expect(container!.textContent).toBe("");
  });

  it("preserves a newer draft in the same environment when the captured intent is accepted", async () => {
    let resolve!: (value: OrchestrationV2ImportedHistoryStartReceipt) => void;
    const onStart = vi.fn(
      (_input: Parameters<ContinuationChoiceBannerProps["onStart"]>[0]) =>
        new Promise<OrchestrationV2ImportedHistoryStartReceipt>((done) => {
          resolve = done;
        }),
    );
    const onIntentAccepted = vi.fn();
    const props = await mount({ onStart, onIntentAccepted });
    await act(async () => {
      button("Start with imported history").click();
    });
    await act(async () => {
      root!.render(<ContinuationChoiceBanner {...props} snapshot={{}} review={null} />);
    });
    await act(async () => {
      resolve(receipt(onStart.mock.calls[0]![0]));
    });
    expect(onIntentAccepted).not.toHaveBeenCalled();
    expect(container!.textContent).toContain("pending");
  });

  it("does not claim execution for a receipt correlated to a different queued run", async () => {
    await mount({
      onStart: async (input) =>
        receipt(input, {
          execution: {
            status: "started",
            runId: "run:other",
            providerThreadId: "provider:thread",
            providerSessionId: "provider:session",
            nativeThreadId: "native:thread",
            effectOutcome: "confirmed_success",
            error: null,
          },
        }),
    });
    await act(async () => {
      button("Start with imported history").click();
    });
    expect(container!.textContent).toContain("Do not send it again");
    expect(container!.textContent).not.toContain("Started a new agent conversation");
  });

  it.each(["not_found", "unknown"] as const)(
    "retains reconciliation when a %s receipt has no target",
    async (intentStatus) => {
      const onIntentAccepted = vi.fn();
      const onTerminal = vi.fn();
      const props = await mount({
        onIntentAccepted,
        onTerminal,
        onStart: vi.fn(async (input) =>
          receipt(input, {
            target: null,
            reviewedBasis: null,
            intentStatus,
            receipt: null,
            execution: {
              status: "not_started",
              runId: null,
              providerThreadId: null,
              providerSessionId: null,
              nativeThreadId: null,
              effectOutcome: null,
              error: null,
            },
          }),
        ),
      });
      await act(async () => {
        button("Start with imported history").click();
      });
      expect(container!.textContent).toContain("Do not send it again");
      expect(button("Start with imported history")).toBeUndefined();
      expect(button("Check status")).toBeDefined();
      expect(onIntentAccepted).not.toHaveBeenCalled();
      expect(onTerminal).not.toHaveBeenCalled();
      expect(props.onStart).toHaveBeenCalledTimes(1);
    },
  );

  it("preserves an immediate draft on a known pre-admission rejection", async () => {
    const delivery = {
      type: "message",
      messageId: "message:draft",
      text: "Continue",
      attachments: [],
      runtimeMode: "full-access",
      interactionMode: "default",
      dispatchMode: { type: "start_immediately" },
    } as never;
    const onIntentAccepted = vi.fn();
    await mount({
      delivery,
      review: review({ target: { type: "message", messageId: "message:draft" } }),
      onIntentAccepted,
      onStart: async (input) =>
        receipt(input, {
          target: { type: "message", messageId: "message:draft" },
          intentStatus: "rejected",
          receipt: null,
          rejectionReason: "Reviewed draft is stale.",
          execution: {
            status: "not_started",
            runId: null,
            providerThreadId: null,
            providerSessionId: null,
            nativeThreadId: null,
            effectOutcome: "known_no_effect",
            error: null,
          },
        }),
    });
    await act(async () => {
      button("Start with imported history").click();
    });
    expect(onIntentAccepted).not.toHaveBeenCalled();
    expect(container!.textContent).toContain("preserved");
  });

  it("sends no RPC and preserves the draft when saving the pointer fails", async () => {
    const props = await mount({
      onReserve: () => {
        throw new Error("Storage is full");
      },
      onIntentAccepted: vi.fn(),
    });
    await act(async () => {
      button("Start with imported history").click();
    });
    expect(props.onStart).not.toHaveBeenCalled();
    expect(props.onIntentAccepted).not.toHaveBeenCalled();
    expect(container!.textContent).toContain("No start request was sent");
  });

  it("keeps a confirmed start and retires its captured draft if terminal metadata cleanup fails", async () => {
    const onIntentAccepted = vi.fn();
    await mount({
      onIntentAccepted,
      onTerminal: () => {
        throw new Error("Storage is full");
      },
      onStart: async (input) =>
        receipt(input, {
          execution: {
            status: "started",
            runId: "run:held",
            providerThreadId: "provider:thread",
            providerSessionId: "provider:session",
            nativeThreadId: "native:thread",
            effectOutcome: "confirmed_success",
            error: null,
          },
        }),
    });
    await act(async () => {
      button("Start with imported history").click();
    });
    expect(container!.textContent).toContain("Started a new agent conversation");
    expect(container!.textContent).toContain("saved correlation could not be cleared");
    expect(onIntentAccepted).toHaveBeenCalledTimes(1);
    expect(button("Start with imported history")).toBeUndefined();
  });

  it("reviews and delivers the real queue row with IDs only, reserving its pointer before the unified RPC", async () => {
    commands.review.mockResolvedValue({ _tag: "Success", value: review() });
    commands.start.mockImplementation(
      async ({ input }: { input: Parameters<ContinuationChoiceBannerProps["onStart"]>[0] }) => ({
        _tag: "Success",
        value: receipt(input),
      }),
    );
    container = document.createElement("div");
    document.body.append(container);
    root = createRoot(container);
    await act(async () => {
      root!.render(
        <QueuedContinuationChoice
          environmentId={"environment:test" as never}
          threadId={"thread:imported" as never}
          runId={"run:held" as never}
          messageId={"message:original" as never}
          snapshot={{}}
          disabled={false}
        />,
      );
    });
    expect(commands.review).toHaveBeenCalledWith({
      environmentId: "environment:test",
      input: { threadId: "thread:imported", delivery: target },
    });
    expect(commands.start).not.toHaveBeenCalled();
    await act(async () => {
      button("Start with imported history").click();
    });
    expect(commands.start).toHaveBeenCalledTimes(1);
    const call = commands.start.mock.calls[0]![0];
    expect(call.input.delivery).toEqual(target);
    expect(commands.reserve).toHaveBeenCalledWith({
      environmentId: "environment:test",
      threadId: "thread:imported",
      commandId: call.input.commandId,
      target,
    });
    expect(commands.reserve.mock.invocationCallOrder[0]).toBeLessThan(
      commands.start.mock.invocationCallOrder[0]!,
    );
  });

  it("recovers a saved stop by observing only its original ID and preserves pending correlation", async () => {
    const pointer = {
      environmentId: "environment:test",
      threadId: "thread:stopping",
      commandId: "stop:original",
      target: {
        binding: {
          threadId: "thread:stopping",
          providerThreadId: "provider:original",
          providerSessionId: "session:original",
          instanceId: "codex",
          runtimeGeneration: "generation:original",
        },
        driver: "codex",
        evidenceRevision: 3,
      },
    } as CurrentRuntimeStopPointer;
    commands.stopObserve.mockResolvedValue({
      _tag: "Success",
      value: {
        version: 2,
        threadId: pointer.threadId,
        commandId: pointer.commandId,
        target: pointer.target,
        commandStatus: "accepted",
        receipt: {
          threadId: pointer.threadId,
          commandId: pointer.commandId,
          commandType: "provider-session.detach",
          status: "accepted",
          resultSequence: 1,
          acceptedAt: DateTime.makeUnsafe("2026-10-03T00:00:00Z"),
          error: null,
        },
        queueFence: { status: "installed", affectedRunIds: [] },
        runtimeStop: { status: "pending" },
        reason: null,
      },
    });
    container = document.createElement("div");
    document.body.append(container);
    root = createRoot(container);
    await act(async () => {
      root!.render(<CurrentRuntimeStopRecoveryBanner pointer={pointer} />);
    });
    expect(commands.stopObserve).toHaveBeenCalledWith({
      environmentId: pointer.environmentId,
      input: { threadId: pointer.threadId, commandId: pointer.commandId },
    });
    expect(container!.textContent).toContain("does not confirm that the runtime stopped");
    expect(commands.stopClear).not.toHaveBeenCalled();
    expect(commands.start).not.toHaveBeenCalled();
    await act(async () => {
      button("Check saved stop").click();
    });
    expect(commands.stopObserve).toHaveBeenCalledTimes(2);
    expect(commands.stopClear).not.toHaveBeenCalled();
  });
});
