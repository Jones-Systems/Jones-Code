import type { EnvironmentId, OrchestratorMcpFailure, ProviderInteractionMode,
  RuntimeMode, OrchestrationV2LinkedSender, LinkedRequestContext } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as PeerMcpTransport from "../jones/peerTransport/PeerMcpTransport.ts";
import type { LinkedMessageOrigin } from "../jones/peerTransport/LinkedMessageOrigin.ts";
export { lookupSendReceipt, type SendReceiptLookupInput } from "../jones/peerTransport/PeerMcpTransport.ts";
export interface PeerCallLimits {
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
}

export class PeerMcpClient extends Context.Service<
  PeerMcpClient,
  {
    /** Calls `tool` on the peer and decodes its structured result with `success`. */
    readonly call: <S extends Schema.Decoder<unknown>>(input: {
      readonly environmentId: EnvironmentId;
      readonly expectedGeneration?: string;
      readonly origin?: LinkedMessageOrigin;
      readonly context?: LinkedRequestContext;
      readonly tool: string;
      readonly arguments: Record<string, unknown>;
      readonly limits: PeerCallLimits;
      /** The thread here the call is made for, sent as `T3-Sender-Thread`. */
      readonly sender?: OrchestrationV2LinkedSender;
      readonly success: S;
    }) => Effect.Effect<S["Type"], OrchestratorMcpFailure>;
  }
>()("t3/peer/PeerMcpClient") {}

export const layer = Layer.effect(PeerMcpClient, PeerMcpTransport.make);
