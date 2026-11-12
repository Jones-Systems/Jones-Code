import {
  EnvironmentHttpConflictError,
  type MembershipEpisode,
  type NativeReference,
  type T3WorkstreamBinding,
  type T3WorkstreamListResult,
  type WorkstreamMembershipPage,
  type WorkstreamReadContext,
  type WorkstreamReferencePage,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import { appendWorkstreamListResult, workstreamBindingKey } from "./live.ts";
import { nativeWorkstreamThreadKey } from "./nativeThreadGrouping.ts";

/**
 * A stored assignment read from the control plane. The reference registration is display
 * evidence only: it never proves placement and never authorizes a change.
 */
export interface WorkstreamAssignment {
  readonly workstreamId: string;
  readonly membershipId: string;
  readonly referenceId: string;
  readonly kind: "primary" | "secondary";
  readonly reference: NativeReference;
}

export type WorkstreamAssignmentConflict = "multiple-primaries" | "evidence-mismatch";

export interface WorkstreamAssignmentIndexEntry {
  /** `nativeWorkstreamThreadKey(sourceInstanceId, nativeThreadId)`. */
  readonly key: string;
  readonly sourceInstanceId: string;
  readonly nativeThreadId: string;
  readonly state: "assigned" | "secondary-only" | "conflict";
  readonly conflict: WorkstreamAssignmentConflict | null;
  /** The sole open primary; null when there is none or open primaries conflict. */
  readonly primary: WorkstreamAssignment | null;
  readonly primaries: readonly WorkstreamAssignment[];
  readonly secondaries: readonly WorkstreamAssignment[];
}

export interface WorkstreamAssignmentIndex {
  readonly ownerId: string;
  readonly registryVersion: number;
  /** Only a complete index can confirm that a native thread is unassigned. */
  readonly complete: boolean;
  readonly entries: ReadonlyMap<string, WorkstreamAssignmentIndexEntry>;
}

// The T3 thread convention accepted by `attestedNativeThreadKey`, without its freshness checks:
// a reference attested at some point keeps its stored assignment after the proof expires.
const recordedT3ThreadReference = (reference: NativeReference, ownerId: string): boolean =>
  reference.owner_id === ownerId &&
  reference.pr_locator === null &&
  reference.identity.provider === "t3" &&
  reference.identity.resource_kind === "thread" &&
  reference.identity.id_kind === "internal" &&
  reference.identity.account_provenance.kind === "not_account_scoped" &&
  Number.isSafeInteger(reference.registration.attestation_version) &&
  reference.registration.attestation_version >= 1;

const evidenceMatchesIdentity = ({ identity, registration }: NativeReference): boolean =>
  registration.evidence === null ||
  (registration.evidence.provider === identity.provider &&
    registration.evidence.source_instance_id === identity.source_instance_id &&
    registration.evidence.native_id === identity.native_id);

const byMembershipId = (left: WorkstreamAssignment, right: WorkstreamAssignment) =>
  left.membershipId < right.membershipId ? -1 : left.membershipId > right.membershipId ? 1 : 0;

export function buildWorkstreamAssignmentIndex(input: {
  readonly ownerId: string;
  readonly registryVersion: number;
  readonly references: readonly NativeReference[];
  readonly memberships: readonly MembershipEpisode[];
  readonly complete: boolean;
}): WorkstreamAssignmentIndex {
  const references = new Map<string, NativeReference>();
  for (const reference of input.references)
    if (recordedT3ThreadReference(reference, input.ownerId))
      references.set(reference.native_reference_id, reference);
  const open = new Map<string, MembershipEpisode>();
  for (const membership of input.memberships)
    if (membership.closed === null && references.has(membership.native_reference_id))
      open.set(membership.membership_id, membership);

  const byKey = new Map<string, WorkstreamAssignment[]>();
  for (const membership of open.values()) {
    const reference = references.get(membership.native_reference_id)!;
    const key = nativeWorkstreamThreadKey(
      reference.identity.source_instance_id,
      reference.identity.native_id,
    );
    const assignments = byKey.get(key) ?? [];
    assignments.push({
      workstreamId: membership.workstream_id,
      membershipId: membership.membership_id,
      referenceId: reference.native_reference_id,
      kind: membership.kind,
      reference,
    });
    byKey.set(key, assignments);
  }

  const entries = new Map<string, WorkstreamAssignmentIndexEntry>();
  for (const [key, assignments] of byKey) {
    const { identity } = assignments[0]!.reference;
    const primaries = assignments.filter((item) => item.kind === "primary").sort(byMembershipId);
    const secondaries = assignments
      .filter((item) => item.kind === "secondary")
      .sort(byMembershipId);
    const conflict: WorkstreamAssignmentConflict | null =
      primaries.length > 1
        ? "multiple-primaries"
        : assignments.some((item) => !evidenceMatchesIdentity(item.reference))
          ? "evidence-mismatch"
          : null;
    entries.set(key, {
      key,
      sourceInstanceId: identity.source_instance_id,
      nativeThreadId: identity.native_id,
      state:
        conflict !== null ? "conflict" : primaries.length === 1 ? "assigned" : "secondary-only",
      conflict,
      primary: conflict === null ? (primaries[0] ?? null) : null,
      primaries,
      secondaries,
    });
  }
  return {
    ownerId: input.ownerId,
    registryVersion: input.registryVersion,
    complete: input.complete,
    entries,
  };
}

/** Pages advanced past the registry version the index started from; reload from the start. */
export class WorkstreamAssignmentIndexVersionChanged extends Error {
  constructor() {
    super("Workstream registry changed while the assignment index was loading.");
    this.name = "WorkstreamAssignmentIndexVersionChanged";
  }
}

const isHttpConflict = Schema.is(EnvironmentHttpConflictError);

const isWorkstreamCursorStale = (cause: unknown): boolean =>
  isHttpConflict(cause) && cause.message === "workstream_cursor_stale";

const isRestartable = (cause: unknown): boolean =>
  cause instanceof WorkstreamAssignmentIndexVersionChanged || isWorkstreamCursorStale(cause);

export interface WorkstreamAssignmentIndexFetchers {
  readonly listWorkstreams: (request: {
    readonly cursor: string | undefined;
    readonly signal: AbortSignal;
  }) => Promise<T3WorkstreamListResult>;
  readonly references: (request: {
    readonly cursor: string | undefined;
    readonly signal: AbortSignal;
  }) => Promise<WorkstreamReferencePage>;
  readonly memberships: (request: {
    readonly workstreamId: string;
    readonly cursor: string | undefined;
    readonly signal: AbortSignal;
  }) => Promise<WorkstreamMembershipPage>;
}

export interface LoadWorkstreamAssignmentIndexOptions {
  readonly signal?: AbortSignal;
  readonly wait?: (delayMs: number, signal?: AbortSignal) => Promise<void>;
  /** Page fetches allowed per attempt across all reads; reaching it yields a partial index. */
  readonly maxPages?: number;
  readonly membershipConcurrency?: number;
}

export interface LoadedWorkstreamAssignmentIndex {
  /** Workstream metadata read at the index's registry version. */
  readonly list: T3WorkstreamListResult;
  readonly index: WorkstreamAssignmentIndex;
  readonly incompleteReason: "ceiling" | "stale-list" | null;
}

// 1,000 pages of at most 100 items each: far above current registries, still bounded.
const DEFAULT_MAX_PAGES = 1_000;
const RESTART_ATTEMPTS = 3;
const DEFAULT_MEMBERSHIP_CONCURRENCY = 4;

const waitForRetry = (delayMs: number, signal?: AbortSignal): Promise<void> =>
  new Promise<void>((resolve, reject) => {
    signal?.throwIfAborted();
    const abort = () => {
      globalThis.clearTimeout(timer);
      reject(signal?.reason ?? new DOMException("The operation was aborted.", "AbortError"));
    };
    // @effect-diagnostics-next-line globalTimers:off - Promise loaders run outside an Effect runtime.
    const timer = globalThis.setTimeout(() => {
      signal?.removeEventListener("abort", abort);
      resolve();
    }, delayMs);
    signal?.addEventListener("abort", abort, { once: true });
  });

/**
 * Reads every workstream, reference, and membership page at one registry version. A stale
 * cursor or a version change restarts the whole read, at most three attempts in total; other
 * failures propagate so callers can keep their last good index.
 */
export async function loadWorkstreamAssignmentIndex(
  fetchers: WorkstreamAssignmentIndexFetchers,
  options: LoadWorkstreamAssignmentIndexOptions = {},
): Promise<LoadedWorkstreamAssignmentIndex> {
  const maxPages = options.maxPages ?? DEFAULT_MAX_PAGES;
  const concurrency = options.membershipConcurrency ?? DEFAULT_MEMBERSHIP_CONCURRENCY;
  if (!Number.isSafeInteger(maxPages) || maxPages < 1)
    throw new RangeError("Assignment index page ceiling must be a positive integer.");
  if (!Number.isSafeInteger(concurrency) || concurrency < 1 || concurrency > 16)
    throw new RangeError("Assignment index membership concurrency must be between 1 and 16.");
  const wait = options.wait ?? waitForRetry;
  for (let attempt = 0; ; attempt += 1) {
    options.signal?.throwIfAborted();
    try {
      const result = await loadAttempt(fetchers, maxPages, concurrency, options.signal);
      options.signal?.throwIfAborted();
      return result;
    } catch (cause) {
      options.signal?.throwIfAborted();
      if (!isRestartable(cause) || attempt + 1 >= RESTART_ATTEMPTS) throw cause;
      await wait(50 * 2 ** attempt, options.signal);
    }
  }
}

function assertListBinding(first: T3WorkstreamBinding, next: T3WorkstreamBinding): void {
  if (
    next.registryVersion !== first.registryVersion ||
    next.serverGeneration !== first.serverGeneration
  )
    throw new WorkstreamAssignmentIndexVersionChanged();
  if (workstreamBindingKey(next) !== workstreamBindingKey(first))
    throw new Error("Workstream list binding changed during pagination.");
}

function assertPageContext(binding: T3WorkstreamBinding, context: WorkstreamReadContext): void {
  if (context.owner_id !== binding.ownerId)
    throw new Error("Workstream owner changed while the assignment index was loading.");
  if (
    context.server_generation !== binding.serverGeneration ||
    context.registry_version !== binding.registryVersion
  )
    throw new WorkstreamAssignmentIndexVersionChanged();
}

async function loadAttempt(
  fetchers: WorkstreamAssignmentIndexFetchers,
  maxPages: number,
  concurrency: number,
  outer: AbortSignal | undefined,
): Promise<LoadedWorkstreamAssignmentIndex> {
  outer?.throwIfAborted();
  // Attempt-scoped so one failed read stops its concurrent siblings.
  const controller = new AbortController();
  const forward = () => controller.abort(outer?.reason);
  outer?.addEventListener("abort", forward, { once: true });
  const { signal } = controller;
  let pagesLeft = maxPages;
  let truncated = false;
  const takePage = (): boolean => {
    if (pagesLeft <= 0) {
      truncated = true;
      return false;
    }
    pagesLeft -= 1;
    return true;
  };
  try {
    takePage(); // maxPages >= 1, so the first list page always fits.
    let list = await fetchers.listWorkstreams({ cursor: undefined, signal });
    signal.throwIfAborted();
    const binding = list.binding;
    const listCursors = new Set<string>();
    while (list.nextCursor !== null && takePage()) {
      if (listCursors.has(list.nextCursor)) throw new Error("Workstream list cursor repeated.");
      listCursors.add(list.nextCursor);
      const next = await fetchers.listWorkstreams({ cursor: list.nextCursor, signal });
      signal.throwIfAborted();
      assertListBinding(binding, next.binding);
      list = appendWorkstreamListResult(list, next);
    }

    const readPages = async <Item>(
      fetch: (cursor: string | undefined) => Promise<{
        readonly context: WorkstreamReadContext;
        readonly items: readonly Item[];
        readonly next_cursor: string | null;
      }>,
      accept: (item: Item) => void,
    ): Promise<void> => {
      const cursors = new Set<string>();
      let cursor: string | undefined;
      while (takePage()) {
        const page = await fetch(cursor);
        signal.throwIfAborted();
        assertPageContext(binding, page.context);
        for (const item of page.items) accept(item);
        if (page.next_cursor === null) return;
        if (cursors.has(page.next_cursor))
          throw new Error("Workstream pagination cursor repeated.");
        cursors.add(page.next_cursor);
        cursor = page.next_cursor;
      }
    };

    const references: NativeReference[] = [];
    const memberships: MembershipEpisode[] = [];
    const pending = [...new Set(list.items.map((item) => item.workstreamId))];
    const readMemberships = async () => {
      for (let id = pending.shift(); id !== undefined; id = pending.shift()) {
        const workstreamId = id;
        await readPages(
          (cursor) => fetchers.memberships({ workstreamId, cursor, signal }),
          (membership: MembershipEpisode) => {
            if (membership.workstream_id !== workstreamId)
              throw new Error("Workstream membership page returned an unrelated membership.");
            memberships.push(membership);
          },
        );
      }
    };
    const failures: unknown[] = [];
    // The first failure wins; siblings then fail only because they were aborted.
    const guard = (task: Promise<void>) =>
      task.catch((cause: unknown) => {
        if (signal.aborted) return;
        failures.push(cause);
        controller.abort(cause);
      });
    await Promise.all([
      guard(
        readPages(
          (cursor) => fetchers.references({ cursor, signal }),
          (reference: NativeReference) => references.push(reference),
        ),
      ),
      ...Array.from({ length: Math.min(concurrency, pending.length) }, () =>
        guard(readMemberships()),
      ),
    ]);
    outer?.throwIfAborted();
    if (failures.length > 0) throw failures[0];

    const live = list.source === "live" && !list.stale;
    const complete = !truncated && live && list.nextCursor === null;
    return {
      list,
      index: buildWorkstreamAssignmentIndex({
        ownerId: binding.ownerId,
        registryVersion: binding.registryVersion,
        references,
        memberships,
        complete,
      }),
      incompleteReason: complete ? null : truncated ? "ceiling" : "stale-list",
    };
  } finally {
    outer?.removeEventListener("abort", forward);
  }
}
