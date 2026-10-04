import {
  CommandId,
  type EnvironmentId,
  type OrchestrationV2ImportedHistoryReviewResult,
  type OrchestrationV2ImportedHistoryStartReceipt,
  type OrchestrationV2StartWithImportedHistoryCommand,
} from "@t3tools/contracts";
import {
  resolveImportedContinuationReceipt,
  resolveImportedContinuationReview,
  resolveCurrentThreadRuntimeStop,
} from "@t3tools/client-runtime/state/thread-continuation";
import { useEffect, useMemo, useRef, useState } from "react";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import {
  clearImportedContinuationPointer,
  reserveImportedContinuationPointer,
  useComposerDraftStore,
  type ImportedContinuationPointer,
  type CurrentRuntimeStopPointer,
  clearCurrentRuntimeStopPointer,
} from "../../composerDraftStore";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { Button } from "../ui/button";
import { randomUUID } from "../../lib/utils";

type StartInput = Omit<OrchestrationV2StartWithImportedHistoryCommand, "type">;
type Target = OrchestrationV2ImportedHistoryReviewResult["target"];

export interface ContinuationChoiceBannerProps {
  readonly environmentId: EnvironmentId;
  readonly threadId: StartInput["threadId"];
  readonly review: OrchestrationV2ImportedHistoryReviewResult | null;
  readonly delivery: StartInput["delivery"];
  /** Identity of the exact prepared draft or immutable queue projection reviewed. */
  readonly snapshot: unknown;
  readonly disabled?: boolean;
  readonly onStart: (input: StartInput) => Promise<OrchestrationV2ImportedHistoryStartReceipt>;
  readonly onObserve: (
    input: Pick<StartInput, "threadId" | "commandId">,
  ) => Promise<OrchestrationV2ImportedHistoryStartReceipt>;
  readonly onReserve: (pointer: ImportedContinuationPointer) => void;
  readonly onTerminal?: (pointer: ImportedContinuationPointer) => void;
  readonly onIntentAccepted?: () => void;
}

export function ContinuationChoiceBanner(props: ContinuationChoiceBannerProps) {
  const latest = useRef(props);
  latest.current = props;
  const inFlight = useRef(false);
  const accepted = useRef(false);
  const operation = useRef<{
    readonly environmentId: EnvironmentId;
    readonly snapshot: unknown;
    readonly input: StartInput;
    readonly target: Target;
    readonly onIntentAccepted: (() => void) | undefined;
  } | null>(null);
  const [outcome, setOutcome] = useState<ReturnType<
    typeof resolveImportedContinuationReceipt
  > | null>(null);
  const [busy, setBusy] = useState(false);
  const [storageError, setStorageError] = useState<string | null>(null);
  const target: Target =
    props.delivery.type === "queued_run"
      ? { type: "queued_run", runId: props.delivery.runId, messageId: props.delivery.messageId }
      : { type: "message", messageId: props.delivery.messageId };
  const resolution =
    props.review === null
      ? null
      : resolveImportedContinuationReview(props.review, {
          threadId: props.threadId,
          target,
        });

  const reconcile = async (observe: boolean) => {
    if (inFlight.current || (!observe && props.disabled)) return;
    if (!observe) {
      if (operation.current !== null || resolution?.status !== "available") return;
      const captured = {
        environmentId: props.environmentId,
        snapshot: props.snapshot,
        target,
        onIntentAccepted: props.onIntentAccepted,
        input: {
          commandId: CommandId.make(randomUUID()),
          threadId: props.threadId,
          reviewedBasis: resolution.reviewedBasis,
          delivery: props.delivery,
        },
      };
      try {
        props.onReserve({
          environmentId: props.environmentId,
          threadId: props.threadId,
          commandId: captured.input.commandId,
          target,
        });
      } catch (error) {
        setStorageError(
          error instanceof Error
            ? error.message
            : "The operation could not be saved. The request was not sent.",
        );
        return;
      }
      operation.current = captured;
    }
    const captured = operation.current;
    if (
      captured === null ||
      captured.environmentId !== props.environmentId ||
      captured.input.threadId !== props.threadId
    )
      return;
    inFlight.current = true;
    setBusy(true);
    setOutcome({ status: "pending", intentAccepted: accepted.current, reason: null });
    try {
      const receipt = await (observe
        ? props.onObserve({
            threadId: captured.input.threadId,
            commandId: captured.input.commandId,
          })
        : props.onStart(captured.input));
      if (
        latest.current.environmentId !== captured.environmentId ||
        latest.current.threadId !== captured.input.threadId
      )
        return;
      const next = resolveImportedContinuationReceipt(receipt, {
        threadId: captured.input.threadId,
        commandId: captured.input.commandId,
        target: captured.target,
        reviewedBasis: captured.input.reviewedBasis,
      });
      setOutcome(next);
      if (next.status === "started" || next.status === "rejected") {
        try {
          props.onTerminal?.({
            environmentId: captured.environmentId,
            threadId: captured.input.threadId,
            commandId: captured.input.commandId,
            target: captured.target,
          });
        } catch {
          setOutcome({
            ...next,
            reason:
              "The operation result is confirmed, but its saved correlation could not be cleared.",
          });
        }
      }
      if (next.intentAccepted && !accepted.current) {
        accepted.current = true;
        // Retire only the captured draft, never text entered while the request was pending.
        if (latest.current.snapshot === captured.snapshot) captured.onIntentAccepted?.();
      }
    } catch {
      if (
        latest.current.environmentId === captured.environmentId &&
        latest.current.threadId === captured.input.threadId
      ) {
        setOutcome({
          status: "unknown",
          intentAccepted: accepted.current,
          reason: "The response was lost. Check this operation's status before continuing.",
        });
      }
    } finally {
      inFlight.current = false;
      setBusy(false);
    }
  };

  if (
    operation.current !== null &&
    (operation.current.environmentId !== props.environmentId ||
      operation.current.input.threadId !== props.threadId)
  )
    return null;
  if (storageError !== null)
    return (
      <span role="status" className="px-3 py-2 text-xs">
        {storageError} No start request was sent.
      </span>
    );
  if (outcome !== null) {
    return (
      <span role="status" className="flex w-full flex-wrap items-center gap-2 px-3 py-2 text-xs">
        <span>
          {outcome.status === "started"
            ? "Started a new agent conversation with imported history."
            : outcome.status === "rejected"
              ? "The imported-history request was rejected. Your draft or queued message is preserved."
              : outcome.status === "unknown"
                ? "The operation's outcome is unknown. Do not send it again."
                : "The imported-history request is pending. The agent conversation has not been confirmed started."}
        </span>
        {outcome.reason ? <span>{outcome.reason}</span> : null}
        {outcome.status === "pending" || outcome.status === "unknown" ? (
          <Button size="xs" variant="ghost" disabled={busy} onClick={() => void reconcile(true)}>
            Check status
          </Button>
        ) : null}
      </span>
    );
  }
  if (
    resolution === null ||
    resolution.status === "ordinary" ||
    resolution.status === "native" ||
    resolution.status === "unavailable"
  )
    return null;
  if (resolution.status !== "available") {
    return (
      <span role="status" className="px-3 py-2 text-xs">
        {resolution.reason}
      </span>
    );
  }
  return (
    <span
      role="region"
      aria-label="Imported history continuation"
      className="flex w-full flex-wrap items-center gap-2 px-3 py-2 text-xs"
    >
      <span>
        Start a new agent conversation using the imported history. Your{" "}
        {target.type === "queued_run" ? "existing queued message" : "message"} may remain pending.
      </span>
      <Button
        size="xs"
        variant="outline"
        disabled={busy || props.disabled}
        onClick={() => void reconcile(false)}
      >
        Start with imported history
      </Button>
    </span>
  );
}

export function QueuedContinuationChoice(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: StartInput["threadId"];
  readonly runId: Extract<StartInput["delivery"], { type: "queued_run" }>["runId"];
  readonly messageId: Target["messageId"];
  readonly snapshot: unknown;
  readonly disabled: boolean;
}) {
  const reviewStart = useAtomCommand(threadEnvironment.reviewImportedHistoryStart, {
    reportFailure: false,
  });
  const start = useAtomCommand(threadEnvironment.deliverImportedContinuation, {
    reportFailure: false,
  });
  const observe = useAtomCommand(threadEnvironment.observeImportedHistoryStart, {
    reportFailure: false,
  });
  const delivery = useMemo(
    () => ({ type: "queued_run" as const, runId: props.runId, messageId: props.messageId }),
    [props.runId, props.messageId],
  );
  const [review, setReview] = useState<{
    snapshot: unknown;
    value: OrchestrationV2ImportedHistoryReviewResult;
  } | null>(null);
  const pointer = useComposerDraftStore(
    (state) =>
      state.getComposerDraft(scopeThreadRef(props.environmentId, props.threadId))
        ?.importedContinuation,
  );
  const stopPointer = useComposerDraftStore(
    (state) =>
      state.getComposerDraft(scopeThreadRef(props.environmentId, props.threadId))
        ?.currentRuntimeStop,
  );
  useEffect(() => {
    let current = true;
    setReview(null);
    if (!props.disabled) {
      void reviewStart({
        environmentId: props.environmentId,
        input: { threadId: props.threadId, delivery },
      })
        .then((result) => {
          if (current && result._tag === "Success")
            setReview({ snapshot: props.snapshot, value: result.value });
        })
        .catch(() => undefined);
    }
    return () => {
      current = false;
    };
  }, [props.environmentId, props.threadId, props.snapshot, props.disabled, delivery, reviewStart]);
  return (
    <ContinuationChoiceBanner
      environmentId={props.environmentId}
      threadId={props.threadId}
      delivery={delivery}
      snapshot={props.snapshot}
      review={review !== null && review.snapshot === props.snapshot ? review.value : null}
      disabled={props.disabled || pointer !== undefined || stopPointer !== undefined}
      onReserve={reserveImportedContinuationPointer}
      onTerminal={clearImportedContinuationPointer}
      onStart={async (input) => {
        const result = await start({ environmentId: props.environmentId, input });
        if (result._tag === "Failure") throw squashAtomCommandFailure(result);
        return result.value;
      }}
      onObserve={async (input) => {
        const result = await observe({ environmentId: props.environmentId, input });
        if (result._tag === "Failure") throw squashAtomCommandFailure(result);
        return result.value;
      }}
    />
  );
}

export function ContinuationRecoveryBanner({
  pointer,
}: {
  readonly pointer: ImportedContinuationPointer;
}) {
  const observe = useAtomCommand(threadEnvironment.observeImportedHistoryStart, {
    reportFailure: false,
  });
  const latest = useRef(pointer);
  latest.current = pointer;
  const inFlight = useRef(false);
  const [status, setStatus] = useState(
    "An imported-history operation is unresolved. Check its status before sending again.",
  );
  const [busy, setBusy] = useState(false);
  const check = async () => {
    if (inFlight.current) return;
    inFlight.current = true;
    setBusy(true);
    const captured = pointer;
    try {
      const result = await observe({
        environmentId: captured.environmentId,
        input: { threadId: captured.threadId, commandId: captured.commandId },
      });
      if (latest.current !== captured) return;
      if (result._tag === "Failure") throw squashAtomCommandFailure(result);
      const receipt = result.value;
      const outcome =
        receipt.reviewedBasis === null
          ? null
          : resolveImportedContinuationReceipt(receipt, {
              threadId: captured.threadId,
              commandId: captured.commandId,
              target: captured.target,
              reviewedBasis: receipt.reviewedBasis,
            });
      if (outcome?.status === "started" || outcome?.status === "rejected") {
        clearImportedContinuationPointer(captured);
      } else {
        setStatus(
          outcome?.status === "pending"
            ? "The imported-history request remains pending. Execution has not been confirmed."
            : "The operation's outcome is unknown. Do not send it again.",
        );
      }
    } catch {
      if (latest.current === captured)
        setStatus("Status is unavailable. The saved operation is retained; do not send it again.");
    } finally {
      inFlight.current = false;
      setBusy(false);
    }
  };
  useEffect(() => {
    void check();
  }, [pointer, observe]);
  return (
    <div role="status" className="flex w-full flex-wrap items-center gap-2 px-3 py-2 text-xs">
      <span>{status}</span>
      <Button size="xs" variant="ghost" disabled={busy} onClick={() => void check()}>
        Check saved operation
      </Button>
    </div>
  );
}

export function CurrentRuntimeStopRecoveryBanner({
  pointer,
}: {
  readonly pointer: CurrentRuntimeStopPointer;
}) {
  const observe = useAtomCommand(threadEnvironment.observeCurrentThreadRuntimeStop, {
    reportFailure: false,
  });
  const latest = useRef(pointer);
  latest.current = pointer;
  const inFlight = useRef(false);
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState(
    "The saved runtime stop is unresolved. Check its status before continuing.",
  );
  const check = async () => {
    if (inFlight.current) return;
    inFlight.current = true;
    setBusy(true);
    const captured = pointer;
    try {
      const result = await observe({
        environmentId: captured.environmentId,
        input: { threadId: captured.threadId, commandId: captured.commandId },
      });
      if (latest.current !== captured) return;
      if (result._tag === "Failure") throw squashAtomCommandFailure(result);
      const outcome = resolveCurrentThreadRuntimeStop(result.value, captured);
      if (outcome.status === "stopped" || outcome.status === "rejected") {
        try {
          clearCurrentRuntimeStopPointer(captured);
        } catch {
          setStatus(
            "The stop result is confirmed, but its saved correlation could not be cleared.",
          );
          return;
        }
        setStatus(
          outcome.status === "stopped"
            ? "The captured agent runtime is stopped."
            : "The captured runtime stop was rejected.",
        );
      } else {
        setStatus(
          outcome.status === "pending"
            ? "Runtime stop remains pending. An installed queue fence does not confirm that the runtime stopped."
            : "The runtime stop outcome is unknown. Check the original operation; do not submit another stop.",
        );
      }
    } catch {
      if (latest.current === captured)
        setStatus("Runtime stop status is unavailable. The original operation is retained.");
    } finally {
      inFlight.current = false;
      setBusy(false);
    }
  };
  useEffect(() => {
    void check();
  }, [pointer, observe]);
  return (
    <div role="status" className="flex w-full flex-wrap items-center gap-2 px-3 py-2 text-xs">
      <span>{status}</span>
      <Button size="xs" variant="ghost" disabled={busy} onClick={() => void check()}>
        Check saved stop
      </Button>
    </div>
  );
}
