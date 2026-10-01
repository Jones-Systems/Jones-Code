import type {
  MembershipEpisode,
  NativeReference,
  T3WorkstreamListResult,
  WorkstreamCommand,
  WorkstreamReceipt,
} from "@t3tools/contracts";
import { sortActiveThreadsByOrderKey } from "@t3tools/client-runtime/state/thread-sort";
import type { WorkstreamListView } from "../../state/workstreams";
import {
  currentT3Placement,
  type LiveT3Placements,
} from "@t3tools/client-runtime/state/workstreams";
import {
  attestedNativeThreadKey,
  nativeWorkstreamThreadKey,
  type WorkstreamThreadLike,
  type NativeWorkstreamThreadGrouping,
} from "./nativeThreadGrouping";

export const canEditWorkstreams = (data: T3WorkstreamListResult | null): boolean =>
  data !== null &&
  data.source === "live" &&
  !data.stale &&
  data.nextCursor === null &&
  data.binding.permissions.includes("workstreams:write");

export { workstreamTint } from "@t3tools/client-runtime/state/workstreams";

export type NativeMembershipAction = Extract<
  WorkstreamCommand["action"],
  {
    readonly operation:
      | "move_primary"
      | "attach_primary"
      | "reattach_primary"
      | "remove_membership";
  }
>;

export function planNativeMembership(input: {
  readonly data: T3WorkstreamListResult;
  readonly placements: LiveT3Placements;
  readonly references: readonly NativeReference[];
  readonly destinationMemberships?: readonly MembershipEpisode[];
  readonly thread: WorkstreamThreadLike;
  readonly destination: string | null;
  readonly now: number;
}): NativeMembershipAction | null {
  if (!canEditWorkstreams(input.data))
    throw new Error("Refresh Workstreams with write access before changing membership.");
  const context = input.placements.context;
  const binding = input.data.binding;
  if (
    context.owner_id !== binding.ownerId ||
    context.principal_id !== binding.principalId ||
    context.authorization_revision !== binding.authorizationRevision ||
    context.server_generation !== binding.serverGeneration ||
    context.registry_version !== binding.registryVersion ||
    input.placements.readiness !== "ready"
  )
    throw new Error("Thread placement binding changed. Refresh before changing membership.");
  const key = nativeWorkstreamThreadKey(input.thread.environmentId, input.thread.id);
  const trust = new Map(
    input.placements.trustedEnvironments.map((entry) => [entry.environmentId, entry]),
  );
  const references = input.references.filter(
    (reference) =>
      reference.owner_id === input.data.binding.ownerId &&
      attestedNativeThreadKey(reference, input.now, trust) === key,
  );
  if (references.length !== 1)
    throw new Error(
      "This thread needs one current, verified reference in the owner registry before it can be assigned.",
    );
  const reference = references[0]!;
  const placements = input.placements.items.filter(
    (entry) =>
      entry.source_instance_id === input.thread.environmentId &&
      entry.native_thread_id === input.thread.id &&
      entry.kind === "primary",
  );
  if (placements.some((entry) => !currentT3Placement(entry, input.now)) || placements.length > 1)
    throw new Error(
      "Thread placement is stale or conflicting. Refresh before changing membership.",
    );
  const current = placements[0];
  if (
    current &&
    (current.native_reference_id !== reference.native_reference_id ||
      current.attestation_version !== reference.registration.attestation_version ||
      current.evidence_sha256 !== reference.registration.evidence?.evidence_sha256 ||
      current.authority_namespace !== reference.registration.evidence?.authority_namespace ||
      current.store_generation !== reference.registration.evidence?.store_generation)
  )
    throw new Error("Thread reference changed. Refresh before changing membership.");
  if ((current?.workstream_id ?? null) === input.destination) return null;
  const source = input.data.items.find((item) => item.workstreamId === current?.workstream_id);
  const destination = input.data.items.find((item) => item.workstreamId === input.destination);
  if (current && !source)
    throw new Error("Current Workstream is unavailable. Refresh before moving the thread.");
  if (input.destination !== null && !destination)
    throw new Error("Destination Workstream is unavailable.");
  if (source && current) {
    if (destination)
      return {
        operation: "move_primary",
        source_workstream_id: source.workstreamId,
        expected_source_version: source.version,
        source_membership_id: current.membership_id,
        destination_workstream_id: destination.workstreamId,
        expected_destination_version: destination.version,
      };
    return {
      operation: "remove_membership",
      workstream_id: source.workstreamId,
      expected_version: source.version,
      membership_id: current.membership_id,
    };
  }
  if (!destination) return null;
  const reattaching = input.destinationMemberships?.some(
    (episode) =>
      episode.closed !== null &&
      episode.kind === "primary" &&
      episode.workstream_id === destination.workstreamId &&
      episode.native_reference_id === reference.native_reference_id,
  );
  return {
    operation: reattaching ? "reattach_primary" : "attach_primary",
    workstream_id: destination.workstreamId,
    expected_version: destination.version,
    native_reference_id: reference.native_reference_id,
  };
}

export function moveNativeThreadOrder(
  order: readonly string[],
  movedId: string,
  neighborId: string,
  after: boolean,
): readonly string[] {
  if (movedId === neighborId || !order.includes(movedId) || !order.includes(neighborId))
    return order;
  const next = order.filter((key) => key !== movedId);
  next.splice(next.indexOf(neighborId) + (after ? 1 : 0), 0, movedId);
  return next;
}

export function captureDraggedThreadKeys(
  initiator: string,
  selected: ReadonlySet<string>,
  rendered: readonly string[],
): readonly string[] {
  return selected.has(initiator) ? rendered.filter((key) => selected.has(key)) : [initiator];
}

export function moveNativeThreadBlock(
  order: readonly string[],
  moved: readonly string[],
  neighbor: string | null,
  after: boolean,
): readonly string[] {
  const selected = new Set(moved);
  if (neighbor !== null && selected.has(neighbor)) return order;
  const next = order.filter((key) => !selected.has(key));
  const index = neighbor === null ? next.length : next.indexOf(neighbor);
  if (index < 0) return order;
  next.splice(index + (neighbor !== null && after ? 1 : 0), 0, ...moved);
  return next;
}

export class ThreadMovementError extends Error {
  constructor(
    reason: string,
    readonly completedKeys: readonly string[],
    readonly stoppedKey: string,
    readonly unprocessedKeys: readonly string[],
    readonly commandId: string | null,
  ) {
    super(
      `${reason}. Completed: ${completedKeys.join(", ") || "none"}. Stopped: ${stoppedKey} (${commandId ?? "not submitted"}). Unprocessed: ${unprocessedKeys.join(", ") || "none"}. Refresh before retrying.`,
    );
    this.name = "ThreadMovementError";
  }
}

export async function submitNativeMembershipBatch(input: {
  readonly data: T3WorkstreamListResult;
  readonly steps: readonly { readonly key: string; readonly action: NativeMembershipAction }[];
  readonly commandId: () => Promise<WorkstreamCommand["command_id"]>;
  readonly submit: (command: WorkstreamCommand) => Promise<WorkstreamReceipt>;
}): Promise<readonly string[]> {
  let registryVersion = input.data.binding.registryVersion;
  const versions = new Map(input.data.items.map((item) => [item.workstreamId, item.version]));
  const completed: string[] = [];
  for (const [index, step] of input.steps.entries()) {
    let commandId: WorkstreamCommand["command_id"] | null = null;
    try {
      const action = step.action;
      const versioned =
        action.operation === "move_primary"
          ? {
              ...action,
              expected_source_version:
                versions.get(action.source_workstream_id) ?? action.expected_source_version,
              expected_destination_version:
                versions.get(action.destination_workstream_id) ??
                action.expected_destination_version,
            }
          : action.operation === "attach_primary" ||
              action.operation === "reattach_primary" ||
              action.operation === "remove_membership"
            ? {
                ...action,
                expected_version: versions.get(action.workstream_id) ?? action.expected_version,
              }
            : action;
      commandId = await input.commandId();
      const receipt = await input.submit({
        command_id: commandId,
        expected_server_generation: input.data.binding.serverGeneration,
        expected_registry_version: registryVersion,
        action: versioned,
      });
      if (receipt.state !== "committed") throw new Error(`Membership change ${receipt.state}`);
      registryVersion = receipt.registry_version;
      for (const version of receipt.effects.workstream_versions)
        versions.set(version.workstream_id, version.version);
      completed.push(step.key);
    } catch (cause) {
      throw new ThreadMovementError(
        cause instanceof Error ? cause.message : "Membership effect unknown",
        completed,
        step.key,
        input.steps.slice(index + 1).map((item) => item.key),
        commandId,
      );
    }
  }
  return completed;
}

export async function moveNativeMembershipThreads(input: {
  readonly controller: WorkstreamListView;
  readonly threads: readonly WorkstreamThreadLike[];
  readonly destination: string | null;
  readonly commandId: () => Promise<WorkstreamCommand["command_id"]>;
  readonly now: number;
  readonly signal?: AbortSignal;
}): Promise<readonly string[]> {
  input.signal?.throwIfAborted();
  const { controller, threads, destination } = input;
  const data = controller.data;
  const placements = controller.placements;
  if (!data || !placements || !canEditWorkstreams(data) || controller.loading)
    throw new Error("Refresh thread placements with write access before changing membership.");
  const inventory = new Set(
    controller.placementInventory.identities.map((item) =>
      nativeWorkstreamThreadKey(item.source_instance_id, item.native_thread_id),
    ),
  );
  if (
    threads.some(
      (thread) => !inventory.has(nativeWorkstreamThreadKey(thread.environmentId, thread.id)),
    )
  )
    throw new Error(
      "A selected environment has no verified placement inventory. No threads were moved.",
    );
  const details = new Map<string, Awaited<ReturnType<typeof controller.loadDetail>>>();
  const steps: { key: string; action: NativeMembershipAction }[] = [];
  for (const thread of threads) {
    const current =
      placements.items.find(
        (item) =>
          item.kind === "primary" &&
          item.source_instance_id === thread.environmentId &&
          item.native_thread_id === thread.id,
      )?.workstream_id ?? null;
    const detailId = destination ?? current;
    if (detailId === null) continue;
    let detail = details.get(detailId);
    if (!detail) {
      detail = await controller.loadDetail(
        detailId,
        input.signal === undefined ? {} : { signal: input.signal },
      );
      input.signal?.throwIfAborted();
      details.set(detailId, detail);
    }
    if (
      detail.detail.context.registry_version !== data.binding.registryVersion ||
      detail.detail.context.server_generation !== data.binding.serverGeneration ||
      detail.detail.context.owner_id !== data.binding.ownerId
    )
      throw new Error("Workstreams changed. Refresh before moving the selection.");
    const action = planNativeMembership({
      data,
      placements,
      references: detail.references.items,
      destinationMemberships: detail.memberships.items,
      thread,
      destination,
      now: input.now,
    });
    if (action)
      steps.push({ key: nativeWorkstreamThreadKey(thread.environmentId, thread.id), action });
  }
  input.signal?.throwIfAborted();
  if (steps.length)
    await controller.runBindingOperation((submit) =>
      submitNativeMembershipBatch({
        data,
        steps,
        commandId: input.commandId,
        submit,
      }),
    );
  return threads.map((thread) => nativeWorkstreamThreadKey(thread.environmentId, thread.id));
}

export function projectWorkstreamShelves<
  T extends WorkstreamThreadLike & {
    readonly createdAt: string;
    readonly pinnedAt?: string | null | undefined;
    readonly activeOrderKey?: string | null | undefined;
  },
>(
  grouping: NativeWorkstreamThreadGrouping<T>,
  pinnedThreads: readonly T[],
): { readonly grouping: NativeWorkstreamThreadGrouping<T>; readonly pinnedThreads: readonly T[] } {
  const groups = grouping.groups.map((group) => ({
    ...group,
    threads: sortActiveThreadsByOrderKey(group.threads),
  }));
  const ungrouped = grouping.ungrouped.filter((thread) => thread.pinnedAt == null);
  return {
    grouping: {
      ...grouping,
      groups,
      ungrouped,
      ordered: [...groups.flatMap((group) => group.threads), ...ungrouped],
    },
    pinnedThreads: pinnedThreads.filter(
      (thread) =>
        !grouping.groupedKeys.has(nativeWorkstreamThreadKey(thread.environmentId, thread.id)),
    ),
  };
}
