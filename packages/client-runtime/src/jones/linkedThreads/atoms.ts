import type { EnvironmentRegistry } from "../../connection/registry.ts";
import type { ThreadEndpointRef } from "@t3tools/contracts/jones/linkedThreads";
import * as Effect from "effect/Effect";
import { Atom, type AtomRegistry } from "effect/reactivity";
import {
  createEnvironmentRpcCommand,
  createEnvironmentRpcQueryAtomFamily,
} from "../../state/runtime.ts";
import { endpointKey } from "./model.ts";

export function createLinkedThreadsQueryAtomFamily<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
) {
  const refreshTrigger = Atom.family((_key: string) => Atom.make(0).pipe(Atom.setIdleTTL(300_000)));
  const list = createEnvironmentRpcQueryAtomFamily(runtime, {
    label: "jones:linked-threads:list",
    tag: "orchestration.listLinkedThreads",
    staleTimeMs: 30_000,
    idleTtlMs: 300_000,
    refreshTrigger: ({ input }) => refreshTrigger(endpointKey(input.thread)),
  });
  const refreshAtom = (thread: ThreadEndpointRef) => refreshTrigger(endpointKey(thread));
  const changed = (registry: AtomRegistry.AtomRegistry, thread: ThreadEndpointRef) => {
    const atom = refreshAtom(thread);
    registry.set(atom, registry.get(atom) + 1);
  };
  const onSuccess = (
    _target: unknown,
    registry: Parameters<typeof changed>[0],
    result: { readonly link: { readonly from: ThreadEndpointRef; readonly to: ThreadEndpointRef } },
  ) =>
    Effect.sync(() => {
      changed(registry, result.link.from);
      changed(registry, result.link.to);
    });
  return {
    list,
    refreshAtom,
    link: createEnvironmentRpcCommand(runtime, {
      label: "jones:linked-threads:link",
      tag: "orchestration.linkThreads",
      onSuccess,
    }),
    unlink: createEnvironmentRpcCommand(runtime, {
      label: "jones:linked-threads:unlink",
      tag: "orchestration.unlinkThreads",
      onSuccess,
    }),
    accept: createEnvironmentRpcCommand(runtime, {
      label: "jones:linked-threads:accept",
      tag: "orchestration.acceptThreadHandoff",
      onSuccess,
    }),
  };
}
