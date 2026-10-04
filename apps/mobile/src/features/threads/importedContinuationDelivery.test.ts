import {
  CommandId,
  EnvironmentId,
  MessageId,
  OrchestrationV2ImportedHistoryReviewBasis,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunId,
  ThreadId,
  type OrchestrationV2ImportedHistoryReviewResult,
  type OrchestrationV2ImportedHistoryStartReceipt,
  type OrchestrationV2ReviewImportedHistoryStartInput,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it, vi } from "vite-plus/test";
import {
  createMobileImportedContinuationDelivery,
  canUseOrdinaryImportedContinuationDelivery,
  importedContinuationTarget,
  presentMobileImportedContinuation,
  type ImportedContinuationStartInput,
  type MobileImportedContinuationPorts,
} from "./importedContinuationDelivery";

const threadId = ThreadId.make("imported-thread");
const commandId = CommandId.make("explicit-start");
const reviewedBasis = OrchestrationV2ImportedHistoryReviewBasis.make("reviewed-draft");
const immediate: OrchestrationV2ReviewImportedHistoryStartInput = {
  threadId,
  delivery: {
    type: "message",
    messageId: MessageId.make("reviewed-message"),
    text: "Continue the investigation",
    attachments: [{ type: "file", id: "file-1", name: "notes.txt", mimeType: "text/plain", sizeBytes: 4 }],
    context: { version: 1, records: [] },
    modelSelection: { instanceId: ProviderInstanceId.make("codex_work"), model: "gpt-test" },
    runtimeMode: "full-access",
    interactionMode: "default",
    dispatchMode: { type: "start_immediately" },
  },
};
const queued: OrchestrationV2ReviewImportedHistoryStartInput = {
  threadId,
  delivery: { type: "queued_run", runId: RunId.make("held-run"), messageId: MessageId.make("original-message") },
};
function review(input = immediate): OrchestrationV2ImportedHistoryReviewResult {
  return {
    version: 2, threadId, target: importedContinuationTarget(input.delivery),
    capability: { startWithImportedHistory: true }, applicability: "imported",
    qualification: { type: "unknown", reason: "Resume has not been qualified." },
    restoredBinding: { type: "missing", reason: "No restored binding." },
    nativeEffects: { type: "clear" }, transcriptEligibility: { type: "eligible" }, reviewedBasis,
  };
}
function receipt(command: ImportedContinuationStartInput, started = false): OrchestrationV2ImportedHistoryStartReceipt {
  return {
    version: 2, commandId: command.commandId, threadId: command.threadId,
    target: importedContinuationTarget(command.delivery), reviewedBasis: command.reviewedBasis,
    intentStatus: "accepted", rejectionReason: null,
    receipt: {
      commandId: command.commandId, threadId, commandType: "thread.imported-history.start",
      acceptedAt: DateTime.makeUnsafe("2026-10-03T00:00:00Z"), resultSequence: 1, status: "accepted", error: null,
    },
    execution: {
      status: started ? "started" : "pending",
      runId: started ? (command.delivery.type === "queued_run" ? command.delivery.runId : RunId.make("new-run")) : null,
      providerThreadId: started ? ProviderThreadId.make("provider-thread") : null,
      providerSessionId: started ? ProviderSessionId.make("provider-session") : null,
      nativeThreadId: started ? "native-thread" : null,
      effectOutcome: started ? "confirmed_success" : null, error: null,
    },
  };
}
function fixture(input = immediate) {
  let delivered: ImportedContinuationStartInput | undefined;
  const ports: { -readonly [K in keyof MobileImportedContinuationPorts]: MobileImportedContinuationPorts[K] } = {
    environmentId: EnvironmentId.make("environment-1"), threadId,
    savePointer: vi.fn(async () => {}),
    review: vi.fn(async () => review(input)),
    deliver: vi.fn(async (command) => { delivered = command; return receipt(command); }),
    observe: vi.fn(async () => receipt(delivered!, true)),
    isCurrent: () => true,
  };
  return { ports, controller: createMobileImportedContinuationDelivery(() => ports) };
}

describe("mobile imported continuation delivery", () => {
  it("allows missing-read fallback only without an unresolved operation or known unknown native effect", async () => {
    const { ports, controller } = fixture();
    const ordinarySend = vi.fn();
    ports.review = vi.fn(async () => { throw new Error("RPC unavailable"); });
    await controller.review(immediate);
    if (canUseOrdinaryImportedContinuationDelivery(controller.getSnapshot(), undefined)) ordinarySend();
    expect(ordinarySend).toHaveBeenCalledTimes(1);
    controller.restorePointer({ environmentId: ports.environmentId, threadId, commandId,
      target: importedContinuationTarget(immediate.delivery) });
    controller.invalidateReview();
    await controller.review(immediate);
    expect(canUseOrdinaryImportedContinuationDelivery(controller.getSnapshot(), undefined)).toBe(false);
    const held = fixture();
    held.ports.review = vi.fn(async (): Promise<OrchestrationV2ImportedHistoryReviewResult> => ({ ...review(), reviewedBasis: null,
      nativeEffects: { type: "unknown", reason: "Reconcile the native effect." } }));
    await held.controller.review(immediate);
    held.controller.invalidateReview();
    held.ports.review = ports.review;
    await held.controller.review({ ...immediate, delivery: { ...immediate.delivery, messageId: MessageId.make("edited-target") } });
    expect(canUseOrdinaryImportedContinuationDelivery(held.controller.getSnapshot(), undefined)).toBe(false);
    expect(held.controller.getSnapshot().nativeEffectsUnknown).toBe(true);
  });
  it("flushes correlation before delivery and sends nothing after a known write failure", async () => {
    const { ports, controller } = fixture();
    let release!: () => void;
    ports.savePointer = vi.fn(() => new Promise<void>((resolve) => { release = resolve; }));
    await controller.review(immediate);
    const start = controller.start(commandId);
    expect(ports.deliver).not.toHaveBeenCalled();
    release();
    await start;
    expect(ports.deliver).toHaveBeenCalledTimes(1);
    const failed = fixture();
    failed.ports.savePointer = vi.fn(async () => { throw new Error("disk full"); });
    await failed.controller.review(immediate);
    await failed.controller.start(commandId);
    expect(failed.ports.deliver).not.toHaveBeenCalled();
    expect(failed.controller.getSnapshot().pointer?.commandId).toBe(commandId);
    expect(failed.controller.getSnapshot().notice).toContain("not sent");
    expect(presentMobileImportedContinuation(failed.controller.getSnapshot()).isSaveRetry).toBe(true);
    const remounted = fixture();
    remounted.controller.restorePointer(failed.controller.getSnapshot().pointer!);
    await remounted.controller.start(commandId);
    expect(remounted.ports.deliver).not.toHaveBeenCalled();
    expect(presentMobileImportedContinuation(remounted.controller.getSnapshot()).isSaveRetry).toBe(false);
    await failed.controller.start(CommandId.make("different-retry-id"));
    expect(failed.ports.deliver).not.toHaveBeenCalled();
    let saveAgain!: () => void;
    failed.ports.savePointer = vi.fn(() => new Promise<void>((resolve) => { saveAgain = resolve; }));
    const retry = failed.controller.start(commandId);
    expect(failed.ports.deliver).not.toHaveBeenCalled();
    saveAgain();
    await retry;
    expect(failed.ports.deliver).toHaveBeenCalledExactlyOnceWith({ commandId, threadId, reviewedBasis,
      delivery: immediate.delivery });
    expect(failed.controller.getSnapshot().saveFailedBeforeDelivery).toBe(false);
  });

  it("keeps an old controller bound to its original environment when current ports change", async () => {
    const { ports, controller } = fixture();
    await controller.review(immediate);
    await controller.start(commandId);
    ports.environmentId = EnvironmentId.make("another-environment");
    await controller.observe();
    expect(ports.observe).not.toHaveBeenCalled();
    expect(controller.getSnapshot().receipt?.status).toBe("pending");
    expect(controller.getSnapshot().pointer?.environmentId).toBe(EnvironmentId.make("environment-1"));
  });

  it("rehydrates IDs without a payload and observes the same request without resending", async () => {
    const initial = fixture(queued);
    await initial.controller.review(queued);
    await initial.controller.start(commandId);
    const pointer = initial.controller.getSnapshot().pointer!;
    const recovered = fixture(queued);
    recovered.ports.observe = vi.fn(async () => receipt(initial.controller.getSnapshot().command!, true));
    recovered.controller.restorePointer(pointer);
    expect(recovered.controller.getSnapshot().command).toBeNull();
    await recovered.controller.observe();
    expect(recovered.ports.observe).toHaveBeenCalledExactlyOnceWith({ threadId, commandId });
    expect(recovered.ports.deliver).not.toHaveBeenCalled();
    expect(recovered.controller.getSnapshot().receipt?.status).toBe("started");
    recovered.controller.retirePointer(pointer);
    expect(recovered.controller.getSnapshot().pointer).toBeNull();
    const wrong = fixture();
    wrong.controller.restorePointer({ ...pointer, threadId: ThreadId.make("another-thread") });
    expect(wrong.controller.getSnapshot().pointer).toBeNull();
  });
  it("reviews without consent, then delivers the exact draft once and distinguishes admission from start", async () => {
    const { ports, controller } = fixture();
    await controller.review(immediate);
    expect(ports.deliver).not.toHaveBeenCalled();
    expect(presentMobileImportedContinuation(controller.getSnapshot()).canStart).toBe(true);
    await controller.start(commandId);
    await controller.start(CommandId.make("second-request"));
    expect(ports.deliver).toHaveBeenCalledExactlyOnceWith({ commandId, threadId, reviewedBasis, delivery: immediate.delivery });
    expect(controller.getSnapshot().receipt?.status).toBe("pending");
    expect(presentMobileImportedContinuation(controller.getSnapshot()).notice).toContain("has not started");
    await controller.observe();
    expect(ports.observe).toHaveBeenCalledExactlyOnceWith({ threadId, commandId });
    expect(controller.getSnapshot().receipt?.status).toBe("started");
  });

  it("addresses the same held run and original message without a replacement payload", async () => {
    const { controller, ports } = fixture(queued);
    await controller.review(queued);
    await controller.start(commandId);
    expect(ports.deliver).toHaveBeenCalledExactlyOnceWith({ commandId, threadId, reviewedBasis, delivery: queued.delivery });
  });

  it("retains the same command on a lost response and only observes it", async () => {
    const { controller, ports } = fixture();
    ports.deliver = vi.fn(async () => { throw new Error("response lost"); });
    await controller.review(immediate);
    await controller.start(commandId);
    await controller.start(CommandId.make("replacement"));
    expect(ports.deliver).toHaveBeenCalledTimes(1);
    expect(controller.getSnapshot().command?.commandId).toBe(commandId);
    expect(presentMobileImportedContinuation(controller.getSnapshot()).isSaveRetry).toBe(false);
    ports.observe = vi.fn(async () => receipt(controller.getSnapshot().command!, true));
    await controller.observe();
    expect(controller.getSnapshot().receipt?.status).toBe("started");
  });

  it("keeps a nullable not-found observation unknown without clearing its pointer or allowing resend", async () => {
    const { controller, ports } = fixture();
    await controller.review(immediate);
    ports.deliver = vi.fn(async () => { throw new Error("response lost"); });
    await controller.start(commandId);
    const pointer = controller.getSnapshot().pointer!;
    ports.observe = vi.fn(async (): Promise<OrchestrationV2ImportedHistoryStartReceipt> => ({ ...receipt(controller.getSnapshot().command!),
      target: null, reviewedBasis: null, intentStatus: "not_found", receipt: null,
      execution: { status: "not_started", runId: null, providerThreadId: null,
        providerSessionId: null, nativeThreadId: null, effectOutcome: null, error: null } }));
    await controller.observe();
    controller.retirePointer(pointer);
    await controller.start(CommandId.make("replacement"));
    expect(controller.getSnapshot().receipt?.status).toBe("unknown");
    expect(controller.getSnapshot().pointer).toBe(pointer);
    expect(canUseOrdinaryImportedContinuationDelivery(controller.getSnapshot(), pointer)).toBe(false);
    expect(ports.deliver).toHaveBeenCalledTimes(1);
  });

  it.each(["effect-unknown", "capability-missing", "qualified-binding-missing", "target-changed"])(
    "does not create a new conversation for %s", async (reason) => {
      const { controller, ports } = fixture();
      const result = review();
      ports.review = vi.fn(async (): Promise<OrchestrationV2ImportedHistoryReviewResult> => reason === "effect-unknown" ? { ...result, nativeEffects: { type: "unknown", reason: "Reconcile native effect." }, reviewedBasis: null } :
        reason === "capability-missing" ? { ...result, capability: { startWithImportedHistory: false }, reviewedBasis: null } :
        reason === "qualified-binding-missing" ? { ...result, qualification: { type: "qualified" }, reviewedBasis: null } :
        { ...result, target: { type: "message", messageId: MessageId.make("another-message") } });
      await controller.review(immediate);
      await controller.start(commandId);
      expect(ports.deliver).not.toHaveBeenCalled();
      expect(presentMobileImportedContinuation(controller.getSnapshot()).canStart).toBe(false);
    },
  );

  it("rejects late cross-environment replies and correlates observation to the original command", async () => {
    const { controller, ports } = fixture();
    await controller.review(immediate);
    let current = true;
    ports.isCurrent = () => current;
    ports.deliver = vi.fn(async (command) => { current = false; return receipt(command, true); });
    await controller.start(commandId);
    expect(controller.getSnapshot().receipt).toBeNull();
    expect(presentMobileImportedContinuation(controller.getSnapshot()).blocksOrdinarySend).toBe(true);
    current = true;
    ports.observe = vi.fn(async () => ({ ...receipt(controller.getSnapshot().command!, true), commandId: CommandId.make("another-command") }));
    await controller.observe();
    expect(controller.getSnapshot().receipt?.status).toBe("unknown");
  });

  it("invalidates an in-flight review when the draft changes", async () => {
    const { controller, ports } = fixture();
    let resolve!: (value: OrchestrationV2ImportedHistoryReviewResult) => void;
    ports.review = vi.fn(() => new Promise<OrchestrationV2ImportedHistoryReviewResult>((done) => { resolve = done; }));
    const pending = controller.review(immediate);
    controller.invalidateReview();
    resolve(review());
    await pending;
    await controller.start(commandId);
    expect(ports.deliver).not.toHaveBeenCalled();
  });
});
