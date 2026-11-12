import {
  enabledEnvironmentIds,
  type EnvironmentCatalogState,
} from "@t3tools/client-runtime/state/connections";
import {
  groupNativeThreadsByWorkstream,
  nativeWorkstreamThreadKey,
  type ConnectedEnvironmentTrust,
  type ConnectedEnvironmentTrustInput,
  type NativeWorkstreamThreadGrouping,
  type WorkstreamThreadLike,
} from "@t3tools/client-runtime/state/workstreams";
import type { EnvironmentId, ServerConfig } from "@t3tools/contracts";
import { Atom } from "effect/reactivity";

/**
 * One trust input per enabled environment. Only the bootstrap config of the current transport
 * session counts as live, so a disconnected environment contributes no descriptor.
 */
export function createConnectedEnvironmentConfigsAtom(input: {
  readonly catalogValueAtom: Atom.Atom<EnvironmentCatalogState>;
  readonly initialConfigValueAtom: (environmentId: EnvironmentId) => Atom.Atom<ServerConfig | null>;
}): Atom.Atom<readonly ConnectedEnvironmentTrustInput[]> {
  return Atom.make((get): readonly ConnectedEnvironmentTrustInput[] =>
    [...enabledEnvironmentIds(get(input.catalogValueAtom))].map((environmentId) => {
      const config = get(input.initialConfigValueAtom(environmentId));
      return { environmentId, config, live: config !== null };
    }),
  ).pipe(Atom.withLabel("web-workstream-connected-environment-configs"));
}

type GroupingInput<Thread extends WorkstreamThreadLike> = Parameters<
  typeof groupNativeThreadsByWorkstream<Thread>
>[0];

/**
 * Display grouping with trust from every connected environment. Threads of an environment whose
 * descriptors disagree are shown as conflicts, outside any group, so changes to them stay blocked.
 * Assignment changes never use this trust; they keep the primary's placement trust.
 */
export function groupNativeThreadsWithConnectedTrust<Thread extends WorkstreamThreadLike>(
  input: Omit<GroupingInput<Thread>, "trustedEnvironments"> & {
    readonly trust: ConnectedEnvironmentTrust;
  },
): NativeWorkstreamThreadGrouping<Thread> {
  const { trust, ...rest } = input;
  const result = groupNativeThreadsByWorkstream({ ...rest, trustedEnvironments: trust.trusted });
  const blocked = (thread: Thread) => trust.conflicts.has(thread.environmentId);
  if (!input.threads.some(blocked)) return result;

  const key = (thread: Thread) => nativeWorkstreamThreadKey(thread.environmentId, thread.id);
  const groupedKeys = new Set(result.groupedKeys);
  const conflictingKeys = new Set(result.conflictingKeys);
  const assuranceByKey = result.assuranceByKey && new Map(result.assuranceByKey);
  for (const thread of input.threads) {
    if (!blocked(thread)) continue;
    groupedKeys.delete(key(thread));
    conflictingKeys.add(key(thread));
    assuranceByKey?.set(key(thread), "conflict");
  }
  const groups = result.groups.flatMap((group) => {
    const threads = group.threads.filter((thread) => !blocked(thread));
    return threads.length === 0 ? [] : [{ workstream: group.workstream, threads }];
  });
  const ungrouped = input.threads.filter((thread) => !groupedKeys.has(key(thread)));
  return {
    ...result,
    groups,
    ungrouped,
    ordered: [...groups.flatMap((group) => group.threads), ...ungrouped],
    groupedKeys,
    conflictingKeys,
    ...(assuranceByKey ? { assuranceByKey } : {}),
  };
}
