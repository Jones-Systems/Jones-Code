import { RegistryContext, useAtomValue } from "@effect/atom-react";
import { EnvironmentNativeWorkstreamsLifetime } from "@t3tools/client-runtime/jones/workstreams/native-lifetime";
import {
  nativeWorkstreamsCapability,
  isNativeWorkstreams,
} from "@t3tools/client-runtime/state/workstreams";
import { EnvironmentId, type T3WorkstreamBinding } from "@t3tools/contracts";
import * as Option from "effect/Option";
import { AsyncResult, Atom, type AtomRegistry } from "effect/reactivity";
import { useContext, useLayoutEffect, useState, useSyncExternalStore } from "react";
import { environmentCatalog } from "../../connection/catalog";
import { environmentSession } from "../../state/session";
import { runEnvironmentWorkstreamsEffect } from "./environmentWorkstreamsRequest";

const nativeConnection = Atom.family((id: EnvironmentId | null) =>
  Atom.make((get) => {
    if (id === null) return null;
    const prepared = get(environmentSession.preparedConnectionValueAtom(id));
    const connection = get(environmentCatalog.stateAtom(id));
    const session = get(environmentSession.sessionStateAtom(id));
    const config = get(environmentSession.initialConfigValueAtom(id));
    if (
      Option.isNone(prepared) ||
      prepared.value.environmentId !== id ||
      config?.environment.environmentId !== id ||
      !AsyncResult.isSuccess(connection) ||
      connection.value.phase !== "connected" ||
      !AsyncResult.isSuccess(session) ||
      !session.value.authenticated
    )
      return null;
    return {
      prepared: prepared.value,
      generation: connection.value.generation,
      session: session.value,
      capability: nativeWorkstreamsCapability(config?.environment.capabilities),
    };
  }),
);

const sha256 = async (bytes: Uint8Array) =>
  Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", new Uint8Array(bytes))), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");

type NativeReadScope = {
  readonly registry: AtomRegistry.AtomRegistry;
  readonly atom: ReturnType<typeof nativeConnection>;
  readonly connection: Atom.Type<ReturnType<typeof nativeConnection>>;
  readonly binding: T3WorkstreamBinding | null;
  readonly key: string | null;
};
class NativeReadStore {
  #scope: NativeReadScope | null = null;
  #reader: EnvironmentNativeWorkstreamsLifetime | null = null;
  readonly #subscribers = new Set<() => void>();

  readonly getSnapshot = () => this.#reader;
  readonly subscribe = (notify: () => void) => {
    this.#subscribers.add(notify);
    if (this.#subscribers.size === 1) this.#activate();
    notify();
    return () => {
      this.#subscribers.delete(notify);
      if (this.#subscribers.size === 0) this.#close();
    };
  };

  setScope(scope: NativeReadScope) {
    const previous = this.#scope;
    if (
      previous?.registry === scope.registry &&
      previous.atom === scope.atom &&
      previous.connection === scope.connection &&
      previous.key === scope.key
    )
      return;
    this.#close();
    this.#scope = scope;
    if (this.#subscribers.size > 0) this.#activate();
    for (const notify of this.#subscribers) notify();
  }

  #close() {
    this.#reader?.dispose();
    this.#reader = null;
  }

  #activate() {
    const scope = this.#scope;
    const connection = scope?.connection;
    const capability = connection?.capability;
    if (
      !scope?.binding ||
      !connection ||
      !isNativeWorkstreams(capability) ||
      !capability?.reads.includes("workstreams-read/1.0.0")
    )
      return;
    try {
      this.#reader = new EnvironmentNativeWorkstreamsLifetime({
        prepared: connection.prepared,
        scope: { capability, binding: scope.binding },
        sha256,
        run: (effect, signal) =>
          runEnvironmentWorkstreamsEffect(connection.prepared, effect, signal),
        placementConnection: (environmentId) => {
          const id = EnvironmentId.make(environmentId);
          const prepared = scope.registry.get(environmentSession.preparedConnectionValueAtom(id));
          const state = scope.registry.get(environmentCatalog.stateAtom(id));
          const session = scope.registry.get(environmentSession.sessionStateAtom(id));
          const config = scope.registry.get(environmentSession.initialConfigValueAtom(id));
          if (
            Option.isNone(prepared) ||
            !config ||
            !AsyncResult.isSuccess(state) ||
            state.value.phase !== "connected" ||
            !AsyncResult.isSuccess(session) ||
            !session.value.authenticated
          )
            return null;
          return {
            prepared: prepared.value,
            generation: state.value.generation,
            session: session.value,
            config,
          };
        },
        isCurrent: () => this.#scope === scope && scope.registry.get(scope.atom) === connection,
      });
    } catch {
      this.#reader = null;
    }
  }
}

/** The binding is the application's independently authenticated public-list binding. */
export function useNativeWorkstreamReads(
  environmentId: EnvironmentId | null,
  binding: T3WorkstreamBinding | null,
) {
  const registry = useContext(RegistryContext);
  const atom = nativeConnection(environmentId);
  const connection = useAtomValue(atom);
  const key = binding === null ? null : JSON.stringify(binding);
  const [store] = useState(() => new NativeReadStore());
  // oxlint-disable-next-line t3code/react-effect-budget -- pushes the current read scope into an external store before useSyncExternalStore reads it.
  useLayoutEffect(() => {
    store.setScope({ registry, atom, connection, binding, key });
  }, [store, registry, atom, connection, binding, key]);
  return useSyncExternalStore(store.subscribe, store.getSnapshot, () => null);
}
