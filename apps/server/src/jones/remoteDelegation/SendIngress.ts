import { CommandId, MessageId, OrchestratorMcpFailure, OrchestratorMcpThreadSendInput, OrchestratorMcpThreadSendReceiptLookupInput,
  OrchestratorMcpThreadSendReceiptLookupResult, REMOTE_FOLLOW_UP_RECEIPT_VERSION,
  type OrchestratorMcpThreadSendResult, type ThreadId } from "@t3tools/contracts";
import * as Digest from "effect/crypto/Digest";
import * as Hex from "effect/encoding/Hex";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import { isReceiptError, type RemoteSendReceiptError, type SendPreparationPorts } from "./SendPreparation.ts";
import { canonicalJson } from "./shared.ts";
export const receiptVersion = REMOTE_FOLLOW_UP_RECEIPT_VERSION;

export type SendInput = OrchestratorMcpThreadSendInput;
export type ReceiptLookupInput = OrchestratorMcpThreadSendReceiptLookupInput;
export interface SendCapture {
  readonly originNamespace: string;
  readonly clientRequestId: string;
  readonly requestDigest: string;
  readonly threadId: ThreadId;
  readonly messageId: MessageId;
  readonly expectedThreadCreatedAt?: string;
  readonly mode?: NonNullable<SendInput["mode"]>;
  readonly freshContext?: boolean;
  readonly link?: string;
}
type ReceiptOutcome = {readonly outcome: "accepted"; readonly sent: OrchestratorMcpThreadSendResult} |
  {readonly outcome: "unknown"; readonly sent: null};
export interface SendIngressPorts {
  readonly remoteSendReceipts?: NonNullable<SendPreparationPorts["remoteSendReceipts"]> & {
    readonly withSendCommit: <A, E, R>(capture: SendCapture, operation: Effect.Effect<A, E, R>) =>
      Effect.Effect<OrchestratorMcpThreadSendResult, E | RemoteSendReceiptError, R>;
    readonly lookup: (input: ReceiptLookupInput & {readonly originNamespace: string}) =>
      Effect.Effect<ReceiptOutcome, RemoteSendReceiptError>;
  };
}
const unavailable = () => new OrchestratorMcpFailure({code: "capability_denied",
  message: "Authenticated durable remote send receipts are unavailable; nothing was sent."});
const failed = () => new OrchestratorMcpFailure({code: "orchestration_error",
  message: "Remote send receipt reconciliation failed. Retain the original command key; do not resend a replacement."});
const namespace = (scope: McpInvocationScope) => {
  const client = scope.client;
  return client?.linked === true && client.remoteOrigin?.linkSessionId === client.sessionId &&
    scope.thread === undefined && scope.requestNamespace.length > 0 ? scope.requestNamespace : undefined;
};

export const make = (owner: SendIngressPorts) => Effect.gen(function* () {
  const digest = yield* Digest.Digest;
  return {
    commit: Effect.fn("RemoteSendIngress.commit")(function* <E, R>(scope: McpInvocationScope, input: SendInput,
      identity: {readonly key: string; readonly commandId: CommandId; readonly messageId: MessageId; readonly link: string},
      operation: Effect.Effect<OrchestratorMcpThreadSendResult, E, R>) {
      if (scope.client?.linked !== true) return yield* operation;
      const originNamespace = namespace(scope);
      if (originNamespace === undefined || owner.remoteSendReceipts === undefined) return yield* unavailable();
      const expectedCommandId = CommandId.make(`command:mcp:${encodeURIComponent(originNamespace)}:thread-send:${encodeURIComponent(identity.key)}`);
      if (identity.commandId !== expectedCommandId || (input.clientRequestId !== undefined && input.clientRequestId !== identity.key))
        return yield* unavailable();
      // Match the forwarding sender's final wire arguments, with routing removed.
      // No caller-supplied namespace is accepted or included as authority.
      const encoded = yield* Schema.encodeUnknownEffect(OrchestratorMcpThreadSendInput)(input).pipe(Effect.mapError(failed));
      const {environmentId: _routing, ...arguments_} = encoded;
      const requestDigest = Hex.encode(yield* digest.digest(Digest.sha256,
        new TextEncoder().encode(canonicalJson(arguments_))).pipe(Effect.mapError(failed)));
      return yield* owner.remoteSendReceipts.withSendCommit({originNamespace, clientRequestId: identity.key,
        requestDigest, threadId: input.threadId, messageId: identity.messageId, link: identity.link,
        ...(input.expectedThreadCreatedAt === undefined ? {} : {expectedThreadCreatedAt: input.expectedThreadCreatedAt}),
        ...(input.mode === undefined ? {} : {mode: input.mode}),
        ...(input.freshContext === undefined ? {} : {freshContext: input.freshContext})}, operation).pipe(
          Effect.mapError((error) => isReceiptError(error) ? failed() : error));
    }),
    lookup: Effect.fn("RemoteSendIngress.lookup")(function* (scope: McpInvocationScope, input: ReceiptLookupInput) {
      yield* Schema.decodeUnknownEffect(OrchestratorMcpThreadSendReceiptLookupInput)(input).pipe(Effect.mapError(failed));
      const originNamespace = namespace(scope);
      if (originNamespace === undefined || owner.remoteSendReceipts === undefined) return yield* unavailable();
      if (input.clientRequestId.length === 0 || !/^[0-9a-f]{64}$/.test(input.requestDigest))
        return yield* new OrchestratorMcpFailure({code: "invalid_request", message: "Exact command key and SHA-256 digest are required."});
      const receipt = yield* owner.remoteSendReceipts.lookup({...input, originNamespace}).pipe(Effect.mapError(failed));
      return yield* Schema.decodeUnknownEffect(OrchestratorMcpThreadSendReceiptLookupResult)({
        version: receiptVersion, environmentId: scope.environmentId, threadId: input.threadId,
        clientRequestId: input.clientRequestId, requestDigest: input.requestDigest, ...receipt,
      }).pipe(Effect.mapError(failed));
    }),
  };
});
