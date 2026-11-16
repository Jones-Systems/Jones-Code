import * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import { RemoteEnvironmentAuthorization } from "../authorization/service.ts";
import type { PreparedConnection } from "../connection/model.ts";
import type { ManagedRelayDpopSigner } from "../relay/managedRelay.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "../state/environmentHttpAuth.ts";

export const fetchWorkQueueMetadata = Effect.fn("clientRuntime.environment.workQueueMetadata")(
  function* (input: {
    readonly prepared: PreparedConnection;
    readonly signer: Option.Option<ManagedRelayDpopSigner["Service"]>;
    readonly remoteAuthorization?: Option.Option<RemoteEnvironmentAuthorization["Service"]>;
    readonly timeoutMs?: number;
  }) {
    return yield* executeAuthenticatedEnvironmentHttpRequest({
      ...input,
      remoteAuthorization:
        input.remoteAuthorization ?? (yield* Effect.serviceOption(RemoteEnvironmentAuthorization)),
      group: "workQueueMetadata",
      method: "GET",
      timeoutMs: input.timeoutMs ?? 10_000,
      url: (urls) => urls.snapshot(),
      request: ({ client, headers }) => client.snapshot({ headers }),
    });
  },
);

export interface WorkQueueLocalNames {
  readonly environmentId: string;
  readonly names: ReadonlyMap<string, string>;
}
let localNames: WorkQueueLocalNames | null = null;
const nameListeners = new Set<() => void>();
export const readWorkQueueLocalNames = () => localNames;
export const subscribeWorkQueueLocalNames = (listener: () => void) => {
  nameListeners.add(listener);
  return () => {
    nameListeners.delete(listener);
  };
};

/** Shares already loaded names only for the environment that supplied them. */
export function publishWorkQueueLocalNames(
  environmentId: string,
  items: readonly { readonly workstreamId: string; readonly name: string }[],
): () => void {
  const published = {
    environmentId,
    names: new Map(items.map((item) => [item.workstreamId, item.name])),
  };
  localNames = published;
  for (const listener of nameListeners) listener();
  return () => {
    if (localNames !== published) return;
    localNames = null;
    for (const listener of nameListeners) listener();
  };
}

export function workQueueDestinationLabel(
  id: string,
  sourceEnvironmentId: string,
  names: WorkQueueLocalNames | null,
): string {
  const name = names?.environmentId === sourceEnvironmentId ? names.names.get(id) : undefined;
  if (
    !name ||
    name.trim().length === 0 ||
    name.length > 256 ||
    // Avoid invisible controls and bidi spoofing in local labels.
    /[\u0000-\u001f\u007f-\u009f\u200b-\u200f\u202a-\u202e\u2060-\u206f]/.test(name)
  )
    return id;
  return `${name} (${id})`;
}
