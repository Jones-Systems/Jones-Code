import { useAtomValue } from "@effect/atom-react";
import * as Option from "effect/Option";
import { AsyncResult, Atom } from "effect/reactivity";
import { useEffect, useRef } from "react";

import { environmentCatalog } from "../../connection/catalog";
import { primaryEnvironmentIdAtom } from "../../state/primaryEnvironment";

const connectedPrimaryGeneration = Atom.make((get) => {
  const environmentId = get(primaryEnvironmentIdAtom);
  if (environmentId === null) return null;
  const state = Option.getOrNull(
    AsyncResult.value(get(environmentCatalog.stateAtom(environmentId))),
  );
  return state?.phase === "connected" ? JSON.stringify([environmentId, state.generation]) : null;
});

export function useWorkstreamsReconnectRefresh(refresh: () => void): void {
  const generation = useAtomValue(connectedPrimaryGeneration);
  // Mount already starts a read; only a newly connected session needs another one.
  const lastConnectedGeneration = useRef(generation);
  useEffect(() => {
    if (generation === null || generation === lastConnectedGeneration.current) return;
    lastConnectedGeneration.current = generation;
    refresh();
  }, [generation, refresh]);
}
