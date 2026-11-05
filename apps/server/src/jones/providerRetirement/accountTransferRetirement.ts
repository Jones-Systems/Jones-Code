import type { OrchestrationV2ThreadProjection, ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import type * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import type * as Registry from "../../orchestration-v2/ProviderAdapterRegistry.ts";

export const SELECTION_CHANGED = "Provider or model selection changed.";
export const ACCOUNT_CONTINUED = "Provider account changed; continuing the native thread.";

export const isAccountTransferDetach = Effect.fn("jones.isAccountTransferDetach")(function* (
  effect: Outbox.OrchestrationEffectV2,
  receipts: Pick<Receipts.CommandReceiptStoreV2Shape, "getByCommandId">,
) {
  const request = effect.request;
  if (
    request.type !== "provider-session.detach" ||
    request.runtimeStopCommandId !== undefined ||
    request.revokeMcpCredential === true ||
    (request.detail !== SELECTION_CHANGED && request.detail !== ACCOUNT_CONTINUED)
  )
    return false;
  return yield* receipts.getByCommandId(effect.commandId).pipe(
    Effect.map(
      Option.exists(
        (receipt) =>
          receipt.status === "accepted" &&
          receipt.threadId === effect.threadId &&
          receipt.commandType !== "provider-session.detach",
      ),
    ),
    Effect.orElseSucceed(() => false),
  );
});

type Metadata = Effect.Success<
  ReturnType<NonNullable<Registry.ProviderAdapterRegistryV2Shape["getMetadata"]>>
>;
type TransferProjection = Pick<OrchestrationV2ThreadProjection, "thread" | "runs">;

export function accountTransferDestination(projection: TransferProjection): ProviderInstanceId {
  return (
    projection.runs.find((run) => run.status === "starting" && run.startedAt === null)
      ?.modelSelection.instanceId ?? projection.thread.modelSelection.instanceId
  );
}

// Include the driver in the key so a known unrelated driver never becomes an unknown destination.
export function retirementConversationKey(metadata: Metadata | undefined): string | undefined {
  if (metadata === undefined) return undefined;
  if (metadata.driver !== "codex") return JSON.stringify([metadata.driver]);
  return metadata.continuationKey.trim().length === 0
    ? undefined
    : JSON.stringify([metadata.driver, metadata.continuationKey]);
}

export function decideAccountTransfer(facts: {
  readonly optedIn: boolean;
  readonly revokeMcpCredential: boolean;
  readonly projection: TransferProjection | undefined;
  readonly sourceInstanceId: ProviderInstanceId;
  readonly sourceDriver: string;
  readonly supportsRetirement: boolean;
  readonly attached: boolean;
  readonly loadedKey: string | undefined;
  readonly complete: boolean;
  readonly sourceMetadata: Metadata | undefined;
  readonly destinationMetadata: Metadata | undefined;
}):
  | { readonly type: "ordinary" }
  | { readonly type: "prove"; readonly loadedKey: string; readonly conversationKey: string } {
  const { projection } = facts;
  if (
    !facts.optedIn ||
    facts.revokeMcpCredential ||
    projection === undefined ||
    projection.thread.archivedAt !== null ||
    projection.thread.deletedAt !== null
  )
    return { type: "ordinary" };
  const conversationKey = retirementConversationKey(facts.sourceMetadata);
  if (
    facts.sourceDriver !== "codex" ||
    !facts.supportsRetirement ||
    !facts.attached ||
    facts.loadedKey === undefined ||
    !facts.complete ||
    accountTransferDestination(projection) === facts.sourceInstanceId ||
    facts.sourceMetadata?.driver !== "codex" ||
    facts.destinationMetadata?.driver !== "codex" ||
    !facts.destinationMetadata.enabled ||
    conversationKey === undefined ||
    retirementConversationKey(facts.destinationMetadata) !== conversationKey
  )
    return { type: "ordinary" };
  return { type: "prove", loadedKey: facts.loadedKey, conversationKey };
}
