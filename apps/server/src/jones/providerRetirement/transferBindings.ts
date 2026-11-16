import type {
  OrchestrationV2ProviderThread,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
} from "@t3tools/contracts";
import { runtimeBinding } from "@t3tools/provider-core/server/ProviderAdapter";

export function selectTransferBindings(
  input: {
    readonly providerSessionId: ProviderSessionId;
    readonly threadId: ThreadId;
    readonly providerThreadId?: ProviderThreadId;
    readonly replacementProviderSessionId?: ProviderSessionId;
  },
  source: { readonly driver: ProviderDriverKind; readonly instanceId: ProviderInstanceId },
  threads: ReadonlyArray<OrchestrationV2ProviderThread>,
) {
  const selected = threads.filter(
    (thread) =>
      // Parent projections include native children owned by other app threads.
      (thread.appThreadId === null || thread.appThreadId === input.threadId) &&
      (input.providerThreadId === undefined || thread.id === input.providerThreadId) &&
      (thread.providerSessionId === input.providerSessionId ||
        (input.providerThreadId !== undefined &&
          input.replacementProviderSessionId !== undefined &&
          thread.providerSessionId === input.replacementProviderSessionId)),
  );
  const providerThreads = selected.flatMap((thread) => {
    if (
      thread.appThreadId !== input.threadId ||
      thread.driver !== source.driver ||
      thread.providerInstanceId !== source.instanceId ||
      thread.nativeThreadRef?.driver !== source.driver ||
      thread.nativeThreadRef?.nativeId == null
    )
      return [];
    // Restart commits the replacement first; only its recorded source may retire the old writer.
    return [{ ...thread, providerSessionId: input.providerSessionId }];
  });
  const bindings = providerThreads
    .flatMap((thread) => {
      const generation = thread.runtimeIdentity?.runtimeGeneration;
      const binding = generation === undefined ? undefined : runtimeBinding(thread, generation);
      return binding === undefined ? [] : [binding];
    })
    .sort((left, right) => left.providerThreadId.localeCompare(right.providerThreadId));
  return {
    providerThreads,
    bindings,
    complete: selected.length > 0 && selected.length === bindings.length,
  };
}
