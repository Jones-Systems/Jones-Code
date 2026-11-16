import type { CommandId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { IncomingLinkedMessage } from "../peerTransport/LinkedMessageOrigin.ts";

// Structural view of the core owner's actual tagged error and private port.
// No second Context service or receipt owner is created in this lane.
export interface RemoteSendReceiptError {
  readonly _tag: "RemoteSendReceiptError";
  readonly reason: string;
  readonly cause?: unknown;
}
export interface SendPreparationPorts {
  readonly remoteSendReceipts?: {
    readonly guardPreparation: <A, E, R>(
      commandId: CommandId,
      preparation: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | RemoteSendReceiptError, R>;
  };
}
export const isReceiptError = (error: unknown): error is RemoteSendReceiptError =>
  typeof error === "object" && error !== null && "_tag" in error && error._tag === "RemoteSendReceiptError";

/** Authenticated ingress must provide the core's outer withSendCommit capture.
 * guardPreparation owns the target lock and birth check for ONLY this callback.
 * It releases before ordinary dispatch reacquires and repeats the commit fence.
 */
export const prepare = <A, E, R>(
  owner: SendPreparationPorts,
  commandId: CommandId,
  preparation: Effect.Effect<A, E, R>,
): Effect.Effect<A, E | RemoteSendReceiptError, R> => Effect.gen(function* () {
  if (!(yield* IncomingLinkedMessage)) return yield* preparation;
  if (owner.remoteSendReceipts === undefined)
    return yield* Effect.fail({_tag: "RemoteSendReceiptError" as const, reason: "preparation_port_missing"});
  return yield* owner.remoteSendReceipts.guardPreparation(commandId, preparation);
});
