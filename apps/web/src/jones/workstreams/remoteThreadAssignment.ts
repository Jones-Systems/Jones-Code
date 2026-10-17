import {
  T3WorkstreamPageQuery,
  type T3PlacementIdentity,
  type T3PlacementResult,
  type T3WorkstreamBinding,
  type T3WorkstreamListResult,
  type WorkstreamCommand,
  type WorkstreamMembershipPage,
  type WorkstreamReceipt,
  type WorkstreamReferenceDetail,
  type WorkstreamsRegistrationContextAcceptedResponse,
} from "@t3tools/contracts";
import {
  appendWorkstreamDtoPage,
  loadLiveT3Placements,
  type WorkstreamAssignmentIndexEntry,
  type WorkstreamThreadLike,
} from "@t3tools/client-runtime/state/workstreams";
import * as Schema from "effect/Schema";

import { canEditWorkstreams } from "../../components/workstreams/nativeWorkstreamActions";
import type { EnvironmentWorkstreamsRequest } from "./environmentWorkstreamsRequest";
import {
  assignNativeThread,
  type NativeThreadAssignmentOptions,
  type NativeThreadAssignmentSnapshot,
} from "./nativeThreadAssignment";
import { assertWorkstreamReadContext, WorkstreamActionError } from "./workstreamActionSnapshot";
import { classifyWorkstreamReadFailure, isWorkstreamsNotConfigured } from "./workstreamReadErrors";

/** The Workstreams routes of one environment's own server. */
export interface EnvironmentWorkstreamsRoutes {
  readonly environmentId: string;
  readonly list: (cursor?: string, signal?: AbortSignal) => Promise<T3WorkstreamListResult>;
  readonly registrationContext: (
    signal?: AbortSignal,
  ) => Promise<WorkstreamsRegistrationContextAcceptedResponse>;
  readonly threadPlacements: (
    identities: readonly T3PlacementIdentity[],
    signal?: AbortSignal,
  ) => Promise<T3PlacementResult>;
  readonly reference: (
    nativeReferenceId: string,
    signal?: AbortSignal,
  ) => Promise<WorkstreamReferenceDetail>;
  readonly memberships: (
    workstreamId: string,
    cursor?: string,
    signal?: AbortSignal,
  ) => Promise<WorkstreamMembershipPage>;
  readonly submit: (command: WorkstreamCommand, signal?: AbortSignal) => Promise<WorkstreamReceipt>;
  readonly command: (commandId: string, signal?: AbortSignal) => Promise<WorkstreamReceipt>;
}

const encodePageQuery = Schema.encodeSync(Schema.Struct(T3WorkstreamPageQuery));

/** GET pagination is a contract payload, which the URL builder does not model as a query. */
const pageUrl = (
  endpointUrl: string,
  query: { readonly limit: number; readonly cursor?: string },
) => {
  const url = new URL(endpointUrl);
  for (const [key, value] of Object.entries(encodePageQuery(query))) {
    if (value !== undefined) url.searchParams.append(key, value);
  }
  return url.toString();
};

const page = (cursor: string | undefined) => ({
  limit: 50,
  ...(cursor === undefined ? {} : { cursor }),
});

export function environmentWorkstreamsRoutes(
  environmentId: string,
  request: EnvironmentWorkstreamsRequest,
): EnvironmentWorkstreamsRoutes {
  return {
    environmentId,
    list: (cursor, signal) =>
      request(
        environmentId,
        "GET",
        (urls) => pageUrl(urls.list(), page(cursor)),
        (client, headers) => client.list({ headers, payload: page(cursor) }),
        signal,
      ),
    registrationContext: (signal) =>
      request(
        environmentId,
        "GET",
        (urls) => urls.registrationContext(),
        (client, headers) => client.registrationContext({ headers }),
        signal,
      ),
    threadPlacements: (identities, signal) =>
      request(
        environmentId,
        "POST",
        (urls) => urls.threadPlacements(),
        (client, headers) => client.threadPlacements({ headers, payload: { identities } }),
        signal,
      ),
    reference: (nativeReferenceId, signal) =>
      request(
        environmentId,
        "GET",
        (urls) => urls.reference({ params: { nativeReferenceId } }),
        (client, headers) => client.reference({ headers, params: { nativeReferenceId } }),
        signal,
      ),
    memberships: (workstreamId, cursor, signal) =>
      request(
        environmentId,
        "GET",
        (urls) => pageUrl(urls.memberships({ params: { workstreamId } }), page(cursor)),
        (client, headers) =>
          client.memberships({ headers, params: { workstreamId }, payload: page(cursor) }),
        signal,
      ),
    submit: (command, signal) =>
      request(
        environmentId,
        "POST",
        (urls) => urls.submit(),
        (client, headers) => client.submit({ headers, payload: { command } }),
        signal,
      ),
    command: (commandId, signal) =>
      request(
        environmentId,
        "GET",
        (urls) => urls.command({ params: { commandId } }),
        (client, headers) => client.command({ headers, params: { commandId } }),
        signal,
      ),
  };
}

/**
 * The owner registry a binding writes to. A remote environment acts under its own principal and
 * authorization revision, so those may differ from the primary's; the registry origin, owner,
 * control-plane generation and contract must match, or the remote write would land in a
 * different registry than the one the sidebar displays.
 */
export const ownerRegistryKey = (binding: T3WorkstreamBinding): string =>
  [
    binding.registryId,
    binding.ownerId,
    binding.serverGeneration,
    binding.contractVersion,
    binding.contractManifest,
  ].join("\u0000");

export type RemoteAssignmentRefusal =
  | "unavailable"
  | "unauthenticated"
  | "not-configured"
  | "owner-mismatch"
  | "read-only"
  | "source-unavailable";

const REFUSAL_MESSAGES: Record<RemoteAssignmentRefusal, string> = {
  unavailable:
    "The thread's environment is unavailable. Reconnect it and try again; nothing was changed.",
  unauthenticated:
    "Sign in to the thread's environment again before changing its Workstream; nothing was changed.",
  "not-configured": "Workstreams are not set up on the thread's environment; nothing was changed.",
  "owner-mismatch":
    "The thread's environment uses a different Workstreams owner registry; nothing was changed.",
  "read-only": "The thread's environment has read-only Workstreams access; nothing was changed.",
  "source-unavailable":
    "The thread's environment cannot verify this thread yet; nothing was changed.",
};

/** A remote assignment refused before any command was submitted. */
class RemoteWorkstreamAssignmentError extends WorkstreamActionError {
  constructor(readonly refusal: RemoteAssignmentRefusal) {
    super("activation");
    this.name = "RemoteWorkstreamAssignmentError";
    this.message = REFUSAL_MESSAGES[refusal];
  }
}

function remoteAssignmentRefusal(cause: unknown): RemoteAssignmentRefusal {
  if (isWorkstreamsNotConfigured(cause)) return "not-configured";
  switch (classifyWorkstreamReadFailure(cause)) {
    case "unauthenticated":
      return "unauthenticated";
    case "unsupported":
      return "not-configured";
    case "error":
      return "unavailable";
  }
}

/** Commands sent to another environment whose outcome is not yet known, by command ID. */
export type RemoteWorkstreamCommandLedger = Map<
  string,
  { readonly environmentId: string; readonly ownerRegistry: string }
>;

const waitForReceipt = (delayMs: number, signal?: AbortSignal): Promise<void> =>
  new Promise<void>((resolve, reject) => {
    signal?.throwIfAborted();
    const abort = () => {
      globalThis.clearTimeout(timer);
      reject(signal?.reason ?? new DOMException("The operation was aborted.", "AbortError"));
    };
    const timer = globalThis.setTimeout(() => {
      signal?.removeEventListener("abort", abort);
      resolve();
    }, delayMs);
    signal?.addEventListener("abort", abort, { once: true });
  });

async function loadMembershipPages(
  load: (cursor?: string) => Promise<WorkstreamMembershipPage>,
): Promise<WorkstreamMembershipPage> {
  let result = await load();
  const cursors = new Set<string>();
  while (result.next_cursor !== null) {
    if (cursors.has(result.next_cursor)) throw new Error("Workstream pagination cursor repeated.");
    cursors.add(result.next_cursor);
    result = appendWorkstreamDtoPage(result, await load(result.next_cursor));
  }
  return result;
}

const sourceReady = (
  context: WorkstreamsRegistrationContextAcceptedResponse,
  binding: T3WorkstreamBinding,
  environmentId: string,
): boolean =>
  context.state === "ready" &&
  context.owner_id === binding.ownerId &&
  context.principal_id === binding.principalId &&
  context.authorization_revision === binding.authorizationRevision &&
  context.server_generation === binding.serverGeneration &&
  context.sources.filter(
    (source) => source.provider === "t3" && source.source_instance_id === environmentId,
  ).length === 1;

export interface RemoteThreadAssignmentInput extends NativeThreadAssignmentOptions {
  /** The primary environment's loaded list; its owner registry is the one displayed. */
  readonly primary: T3WorkstreamListResult;
  readonly thread: WorkstreamThreadLike;
  readonly destination: string | null;
  /** The thread's own environment; every read, submit and poll goes to it. */
  readonly routes: EnvironmentWorkstreamsRoutes;
  /** The primary-loaded stored assignment for the thread. */
  readonly stored: WorkstreamAssignmentIndexEntry | undefined;
  /** Throws when the primary snapshot changed, became stale, or the action was superseded. */
  readonly check: () => void;
  readonly commands: RemoteWorkstreamCommandLedger;
  readonly loadCompleteList: (
    load: (cursor?: string) => Promise<T3WorkstreamListResult>,
  ) => Promise<T3WorkstreamListResult>;
  /** Reloads the primary index; the remote receipt is never placement proof. */
  readonly refreshPrimary: () => void;
  readonly now?: () => number;
}

/**
 * Assigns a thread through the server of the environment that owns it. That server alone can
 * register and verify its own native threads, so its list, registration context and placement
 * proof are compared with each other, and its list must name the primary's owner registry.
 * Every refusal happens before a command is submitted.
 */
export async function assignRemoteNativeThread(input: RemoteThreadAssignmentInput): Promise<void> {
  const { routes, thread, signal } = input;
  if (routes.environmentId !== thread.environmentId) throw new WorkstreamActionError("stale");
  const ownerRegistry = ownerRegistryKey(input.primary.binding);
  // An unreconciled command may already have changed this registry; Retry checks it first.
  for (const entry of input.commands.values())
    if (entry.ownerRegistry === ownerRegistry) throw new WorkstreamActionError("unknown");
  let commandAttempted = false;
  const refuse = (refusal: RemoteAssignmentRefusal) =>
    commandAttempted
      ? new WorkstreamActionError("stale")
      : new RemoteWorkstreamAssignmentError(refusal);
  const read = async <A>(load: () => Promise<A>): Promise<A> => {
    input.check();
    let value: A;
    try {
      value = await load();
    } catch (cause) {
      signal?.throwIfAborted();
      if (commandAttempted || cause instanceof WorkstreamActionError) throw cause;
      throw new RemoteWorkstreamAssignmentError(remoteAssignmentRefusal(cause));
    }
    signal?.throwIfAborted();
    input.check();
    return value;
  };
  const loadList = async () => {
    const list = await read(() => input.loadCompleteList((cursor) => routes.list(cursor, signal)));
    if (ownerRegistryKey(list.binding) !== ownerRegistry) throw refuse("owner-mismatch");
    if (!canEditWorkstreams(list)) throw refuse("read-only");
    return list;
  };
  const loadContext = async (list: T3WorkstreamListResult) => {
    const context = await read(() => routes.registrationContext(signal));
    if (!sourceReady(context, list.binding, thread.environmentId))
      throw refuse("source-unavailable");
    return context;
  };

  const initial = await loadList();
  await loadContext(initial);

  const loadSnapshot = async (referenceId?: string): Promise<NativeThreadAssignmentSnapshot> => {
    const live = await loadList();
    const context = await loadContext(live);
    const selected = [{ source_instance_id: thread.environmentId, native_thread_id: thread.id }];
    const projection = await read(() =>
      loadLiveT3Placements(live, selected, () => routes.threadPlacements(selected, signal)),
    );
    const referenceIds = new Set(projection.items.map((item) => item.native_reference_id));
    if (
      referenceIds.size > 1 ||
      (referenceId !== undefined && [...referenceIds].some((id) => id !== referenceId))
    )
      throw new WorkstreamActionError("ambiguous");
    const selectedReference = referenceId ?? [...referenceIds][0];
    const reference =
      selectedReference === undefined
        ? null
        : await read(() => routes.reference(selectedReference, signal));
    const destination = input.destination;
    const destinationMemberships =
      destination === null
        ? null
        : await read(() =>
            loadMembershipPages((cursor) => routes.memberships(destination, cursor, signal)),
          );
    assertWorkstreamReadContext(live, context);
    // As on the primary: once the reference is current, its proof must agree with the stored
    // primary the sidebar shows, so a missing proof never plans a first attach.
    const stored = input.stored;
    const registration = reference?.reference.registration;
    const proven = projection.items.filter((item) => item.kind === "primary");
    if (
      stored?.primary &&
      (proven.length > 0 ||
        (registration?.state === "attested" &&
          Date.parse(registration.expires_at ?? "") > Date.now())) &&
      (proven.length !== 1 ||
        proven[0]!.membership_id !== stored.primary.membershipId ||
        proven[0]!.workstream_id !== stored.primary.workstreamId)
    )
      throw new WorkstreamActionError("stale");
    return {
      data: live,
      registrationContext: context,
      placements: projection,
      reference,
      destinationMemberships,
    };
  };

  const runBindingOperation = async <A>(
    operation: (submit: (command: WorkstreamCommand) => Promise<WorkstreamReceipt>) => Promise<A>,
  ): Promise<A> => {
    const submit = async (command: WorkstreamCommand) => {
      input.check();
      commandAttempted = true;
      input.commands.set(command.command_id, {
        environmentId: routes.environmentId,
        ownerRegistry,
      });
      const settle = (receipt: WorkstreamReceipt) => {
        if (receipt.state === "committed" || receipt.state === "rejected")
          input.commands.delete(command.command_id);
        input.check();
        return receipt;
      };
      let receipt = settle(await routes.submit(command, signal));
      // Pending effects reconcile on the environment that accepted the command; never resend.
      while (receipt.state === "pending" || receipt.state === "unresolved") {
        await waitForReceipt(
          Math.min(30_000, Math.max(250, receipt.retry_after_seconds * 1_000)),
          signal,
        );
        input.check();
        receipt = settle(await routes.command(command.command_id, signal));
      }
      return receipt;
    };
    try {
      return await operation(submit);
    } finally {
      if (commandAttempted) input.refreshPrimary();
    }
  };

  await assignNativeThread({
    data: initial,
    thread,
    destination: input.destination,
    loadSnapshot,
    runBindingOperation,
    commandId: input.commandId,
    ...(signal === undefined ? {} : { signal }),
    ...(input.now === undefined ? {} : { now: input.now }),
  });
}

/**
 * Observes each unresolved remote command on the environment that accepted it. Commands are
 * never resubmitted; an environment that still reports a pending effect keeps it unknown.
 */
export async function reconcileRemoteWorkstreamCommands(input: {
  readonly commands: RemoteWorkstreamCommandLedger;
  readonly ownerRegistry: string;
  readonly routesFor: (environmentId: string) => EnvironmentWorkstreamsRoutes;
}): Promise<void> {
  for (const [commandId, entry] of input.commands) {
    if (entry.ownerRegistry !== input.ownerRegistry) continue;
    const receipt = await input.routesFor(entry.environmentId).command(commandId);
    if (
      receipt.command_id !== commandId ||
      (receipt.state !== "committed" && receipt.state !== "rejected")
    )
      throw new WorkstreamActionError("unknown");
    input.commands.delete(commandId);
  }
}
