import type { EnvironmentId } from "@t3tools/contracts";
import type {
  LinkedThreadLink,
  LinkedThreadsListResult,
  ThreadEndpointRef,
} from "@t3tools/contracts/jones/linkedThreads";

export const endpointKey = (ref: ThreadEndpointRef) =>
  JSON.stringify([ref.environmentId, ref.threadId]);
export const sameEndpoint = (a: ThreadEndpointRef, b: ThreadEndpointRef) =>
  endpointKey(a) === endpointKey(b);
export type LinkedGroup = "continuations" | "parents" | "delegated" | "children" | "siblings";
export type LinkedRelation = {
  readonly endpoint: ThreadEndpointRef;
  readonly group: LinkedGroup;
  readonly label: string;
  readonly link: LinkedThreadLink;
};
export type LinkedSnapshot = {
  readonly title: string | null;
  readonly environmentLabel: string | null;
};

export function supportsLinkedThreads(capabilities: unknown): boolean {
  if (!capabilities || typeof capabilities !== "object" || !("linkedThreads" in capabilities))
    return false;
  const value = capabilities.linkedThreads;
  return (
    value !== null &&
    typeof value === "object" &&
    "schemaVersion" in value &&
    value.schemaVersion === "t3.linked-threads/v1"
  );
}

export function unionLinkedThreads(
  subject: ThreadEndpointRef,
  results: readonly LinkedThreadsListResult[],
) {
  const copies = new Map<string, LinkedThreadLink[]>();
  const siblings = new Set<string>();
  const snapshots = new Map<string, LinkedSnapshot>();
  const snapshotTimes = new Map<string, string>();
  for (const result of results) {
    for (const { link } of result.siblings) siblings.add(link.linkId);
    for (const link of [...result.links, ...result.siblings.map((s) => s.link)]) {
      const previous = copies.get(link.linkId) ?? [];
      previous.push(link);
      copies.set(link.linkId, previous);
      const foreign = link.from.environmentId === result.environmentId ? link.to : link.from;
      if (foreign.environmentId !== result.environmentId && link.remote) {
        const key = endpointKey(foreign);
        if (link.updatedAt >= (snapshotTimes.get(key) ?? "")) {
          snapshots.set(key, link.remote);
          snapshotTimes.set(key, link.updatedAt);
        }
      }
    }
  }
  const links = [...copies.values()].map((rows): LinkedThreadLink => {
    const sorted = [...rows].sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
    const newest = sorted[0]!;
    const accepted = sorted.find((row) => row.acceptance === "accepted");
    return {
      ...newest,
      handoff:
        sorted
          .filter((row) => row.handoff !== null)
          .sort((a, b) =>
            (b.handoff?.offeredAt ?? b.updatedAt).localeCompare(
              a.handoff?.offeredAt ?? a.updatedAt,
            ),
          )[0]?.handoff ?? null,
      acceptance: accepted ? "accepted" : newest.acceptance,
      acceptedAt: accepted?.acceptedAt ?? newest.acceptedAt,
      reverse: sorted.some((row) => row.reverse === "recorded") ? "recorded" : newest.reverse,
    };
  });
  const relations: LinkedRelation[] = [];
  for (const link of links) {
    const outgoing = sameEndpoint(link.from, subject);
    const incoming = sameEndpoint(link.to, subject);
    if (!outgoing && !incoming) {
      if (siblings.has(link.linkId))
        relations.push({ endpoint: link.to, group: "siblings", label: "Sibling", link });
      continue;
    }
    const endpoint = outgoing ? link.to : link.from;
    relations.push({
      endpoint,
      link,
      group:
        link.kind === "continuation"
          ? "continuations"
          : link.kind === "peer"
            ? "siblings"
            : outgoing
              ? "children"
              : "parents",
      label:
        link.kind === "continuation"
          ? outgoing
            ? "Continued in"
            : "Continues from"
          : link.kind === "peer"
            ? "Peer"
            : outgoing
              ? "Launched"
              : "Launched by",
    });
  }
  return {
    links,
    relations,
    snapshots,
    truncated: results.some((r) => r.truncated),
    hasSuccessor: relations.some((r) => r.label === "Continued in"),
  };
}

export type MirrorState = "recorded" | "unavailable" | "unknown";
export type MutationStep = {
  readonly environmentId: EnvironmentId;
  readonly state: "recorded" | "unavailable" | "unknown";
  readonly detail?: string;
};
export function classifyMirrorFailure(error: unknown): Exclude<MirrorState, "recorded"> {
  const message = error instanceof Error ? error.message : String(error);
  return /unknown.*(method|rpc)|not supported|forbidden|capability|version mismatch|not authorized|authorization|permission|only.*receiv|not_receiver|not_found|not found|endpoint_not/i.test(
    message,
  )
    ? "unavailable"
    : "unknown";
}

export async function mirrorLinkedMutation<A>(options: {
  readonly local: EnvironmentId;
  readonly remote: EnvironmentId;
  readonly remoteCapable: boolean;
  readonly execute: (environmentId: EnvironmentId, reverse?: MirrorState) => Promise<A>;
}) {
  const result = await options.execute(options.local);
  const steps: MutationStep[] = [{ environmentId: options.local, state: "recorded" }];
  if (options.local === options.remote) return { result, steps };
  let reverse: MirrorState = "unavailable";
  if (options.remoteCapable) {
    try {
      await options.execute(options.remote, "recorded");
      reverse = "recorded";
    } catch (error) {
      reverse = classifyMirrorFailure(error);
    }
  }
  steps.push({ environmentId: options.remote, state: reverse });
  try {
    await options.execute(options.local, reverse);
  } catch {
    steps[0] = {
      environmentId: options.local,
      state: "recorded",
      detail: "Reverse status could not be updated",
    };
  }
  return { result, steps };
}
