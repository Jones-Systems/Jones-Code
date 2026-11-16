import type { ServerSelfUpdateError } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type { ServiceLauncherClientError } from "../../cloud/serviceLauncherClient.ts";
import type { OperationReconciliation } from "./launcherOperation.ts";

type HandoffOutcome = "accepted" | "rejected";

/** What the launcher's durable receipts prove about one operation, if anything. */
const receiptOutcome = (
  operationId: string,
  receipt: OperationReconciliation,
): HandoffOutcome | undefined =>
  receipt.operationId !== operationId
    ? undefined
    : receipt.state === "rejected"
      ? "rejected"
      : receipt.state === "pending" ||
          receipt.state === "committed" ||
          receipt.state === "rolled-back" ||
          receipt.updateId === operationId
        ? "accepted"
        : undefined;

/**
 * A handoff refusal proves rejection unless durable receipts show acceptance, which always wins.
 * Otherwise await durable receipts or a later launcher query; timeout, missing answers, reservations
 * and unreadable receipts prove nothing. Back off to 30s while the launcher can answer;
 * otherwise wait until this process exits.
 */
export const awaitHandoffOutcome = Effect.fn("HandoffOutcome.awaitHandoffOutcome")(
  function* (input: {
    readonly operationId: string | undefined;
    readonly cause: Cause.Cause<ServerSelfUpdateError>;
    readonly reconcile: (operationId: string) => Effect.Effect<OperationReconciliation>;
    /** Absent when the launcher cannot report outcomes. */
    readonly query?: (
      operationId: string,
    ) => Effect.Effect<
      { readonly outcome: "accepted" | "rejected" | "unknown" },
      ServiceLauncherClientError
    >;
  }): Effect.fn.Return<HandoffOutcome> {
    const error = Cause.findErrorOption(input.cause);
    const refused =
      Option.isSome(error) &&
      (error.value.cause as { readonly _tag?: unknown } | undefined)?._tag ===
        "ServiceLauncherRejectedError";
    const operationId = input.operationId;
    if (operationId === undefined) return refused ? "rejected" : yield* Effect.never;
    for (let delay = 1_000; ; delay = Math.min(delay * 2, 30_000)) {
      const proven = receiptOutcome(operationId, yield* input.reconcile(operationId));
      if (proven !== undefined) return proven;
      if (refused) return "rejected";
      if (input.query === undefined) return yield* Effect.never;
      const answer = yield* input
        .query(operationId)
        .pipe(Effect.orElseSucceed(() => ({ outcome: "unknown" as const })));
      if (answer.outcome !== "unknown") return answer.outcome;
      yield* Effect.sleep(Duration.millis(delay));
    }
  },
);
