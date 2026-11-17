import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";
import { ThreadId, type WorkspaceRetirementOutcome } from "@t3tools/contracts";
import type * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import type * as Registry from "../workspaceLifecycle/RegistryCli.ts";
import { inside } from "./Filesystem.ts";
import { Receipts, layer, RetirementError, type RetirementReceipt } from "./Receipts.ts";
import { finishInterrupted, interruptedCheckoutAbsent } from "./RetirementExecutor.ts";

/** Reason shown while an interrupted retirement whose checkout is gone waits for the owner's Retire. */
const INTERRUPTED_CHECKOUT_ABSENT = "interrupted_retirement_checkout_absent";

const isRetirementError = Schema.is(RetirementError);

export interface InterruptedRetirementRequest {
  readonly worktree: string;
  readonly projectRoot: string;
  readonly threadId: string | null;
  readonly cause: RetirementReceipt["cause"];
  readonly execute: boolean;
  readonly ownerCoverage?: { readonly fingerprint: string };
  readonly admit: Effect.Effect<void, RetirementError>;
}

const detail = (
  request: InterruptedRetirementRequest,
  reason: string,
  fact: string,
  nextAction: string,
) => ({
  reason,
  target: request.worktree,
  root: request.worktree,
  fact,
  missingFact: reason,
  lineage: request.threadId ? [{ threadId: request.threadId }] : [],
  nextAction,
});

/**
 * A checkout removed before a restart cannot be bound, so the ordinary evaluation fails before the
 * executor can reconcile its interrupted receipt. This settles only that exact receipt, and only on
 * the owner's Retire; every other request reads it and holds without changing state.
 * Returns null when no interrupted intent receipt holds an absent checkout at this path.
 */
export const interruptedRetirement = Effect.fn("WorkspaceRetirement.interruptedRetirement")(
  function* (
    request: InterruptedRetirementRequest,
    store: Receipts["Service"],
    registry: Registry.LifecycleRegistry["Service"] | null,
    projections: Projections.ProjectionStoreV2["Service"],
  ) {
    const stale = yield* interruptedCheckoutAbsent(request.worktree).pipe(
      Effect.provideService(Receipts, store),
    );
    if (!stale) return null;
    // The receipt id is the proposal token, so Retire settles only the receipt the owner was shown.
    const owner =
      request.execute &&
      request.cause === "owner_action" &&
      request.ownerCoverage?.fingerprint === stale.receipt.id;
    if (!owner) {
      const reason =
        request.execute && request.cause === "owner_action"
          ? "proposal_changed"
          : INTERRUPTED_CHECKOUT_ABSENT;
      const outcome: WorkspaceRetirementOutcome = {
        state: "held",
        reasonCodes: [reason],
        receiptId: stale.receipt.id,
        fingerprint: request.execute ? null : stale.receipt.id,
        paths: [],
        details: [
          detail(
            request,
            reason,
            "A retirement was interrupted by a restart, and this checkout's directory is already gone. Its receipt still marks the checkout as retiring.",
            reason === "proposal_changed"
              ? "Propose again, then run Retire on the new proposal."
              : "Run Retire to finish. Retire deletes nothing: it reads back the branch and refs and records the retirement as retired, or as partial when they do not read back.",
          ),
        ],
        outputObligations: [],
      };
      return { state: "held" as const, reasons: [reason], target: request.worktree, outcome };
    }
    const receipt = yield* finishInterrupted({
      worktree: request.worktree,
      projectRoot: request.projectRoot,
      admit: request.admit,
      finalizeOutputs: Effect.fn("WorkspaceRetirement.finalizeInterruptedOutputs")(
        function* (settled: RetirementReceipt) {
          if (!registry?.outputs) return;
          const internal = (yield* registry.outputs(request.worktree)).filter(
            (r) =>
              inside(r.path, request.worktree) && r.state === "active" && r.class !== "evidence",
          );
          if (!internal.length) return;
          if (!registry.forgetOutputs)
            return yield* new RetirementError({
              target: settled.binding.path,
              reason: "output_finalization_unavailable",
            });
          yield* registry.forgetOutputs(internal);
        },
        Effect.mapError((cause) =>
          isRetirementError(cause)
            ? cause
            : new RetirementError({
                target: request.worktree,
                reason: "observation_failed",
                cause,
              }),
        ),
      ),
    }).pipe(Effect.provideService(Receipts, store));
    // The path reappeared and read back untouched; its receipt is released, so a fresh proposal can run.
    const result = receipt ?? {
      state: "not_retired" as const,
      reasons: ["interrupted_before_effect"],
    };
    const covered = receipt?.qualification.details ?? [];
    const outcome: WorkspaceRetirementOutcome = {
      state: result.state,
      reasonCodes: [...result.reasons],
      receiptId: receipt?.id ?? stale.receipt.id,
      fingerprint: null,
      paths: [],
      details: [
        ...covered,
        ...result.reasons
          .filter((reason) => !covered.some((entry) => entry.reason === reason))
          .map((reason) =>
            detail(
              request,
              reason,
              reason === "interrupted_after_removal"
                ? "The checkout was removed before the restart. Its branch and refs read back, so the interrupted retirement is recorded as retired."
                : reason,
              reason === "interrupted_after_removal"
                ? "No further action is needed."
                : "Propose again before any further retirement.",
            ),
          ),
      ],
      outputObligations: [],
    };
    if (request.threadId) {
      const thread = yield* projections.getThread(ThreadId.make(request.threadId));
      if (thread.settledAt) {
        const intent = yield* store.settlement(
          request.threadId,
          request.worktree,
          DateTime.formatIso(thread.settledAt),
        );
        if (intent) yield* store.outcome(intent.event_id, outcome);
      }
    }
    return { ...result, outcome };
  },
);

/** Whether an interrupted intent receipt holds this absent checkout; unreadable storage reads as no. */
export const holdsAbsentCheckout = (worktree: string) =>
  Receipts.pipe(
    Effect.provide(layer),
    Effect.flatMap((store) =>
      interruptedCheckoutAbsent(worktree).pipe(Effect.provideService(Receipts, store)),
    ),
    Effect.map((stale) => stale !== null),
    Effect.orElseSucceed(() => false),
  );
