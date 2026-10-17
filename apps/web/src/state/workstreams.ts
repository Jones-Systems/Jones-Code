import type {
  T3PlacementResult,
  T3WorkstreamListResult,
  T3PlacementIdentity,
  WorkstreamDeclarationPage,
  WorkstreamDetail,
  WorkstreamEdgePage,
  WorkstreamHistoryPage,
  WorkstreamMembershipPage,
  WorkstreamReadContext,
  WorkstreamReferenceDetail,
  WorkstreamReferencePage,
  WorkstreamCommand,
  WorkstreamReceipt,
  WorkstreamsRegistrationContextAcceptedResponse,
} from "@t3tools/contracts";
import {
  EnvironmentHttpConflictError,
  EnvironmentInternalError,
  T3_PLACEMENT_MAX_IDENTITIES,
  T3_PLACEMENT_MAX_REQUEST_BYTES,
} from "@t3tools/contracts";
import {
  appendWorkstreamDtoPage,
  appendWorkstreamListResult,
  LiveWorkstreamMetadataCache,
  loadWorkstreamAssignmentIndex,
  nativeWorkstreamThreadKey,
  orderWorkstreamMetadata,
  type WorkstreamDtoPage,
  loadLiveT3Placements,
  type LiveT3Placements,
  type WorkstreamAssignmentIndex,
  type WorkstreamAssignmentIndexFetchers,
  workstreamBindingKey,
} from "@t3tools/client-runtime/state/workstreams";
import { useAtomValue } from "@effect/atom-react";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { useCallback, useEffect, useRef, useState } from "react";

import {
  assertWorkstreamActionSnapshot,
  assertWorkstreamReadContext,
  WorkstreamActionError,
  type WorkstreamActionSnapshot,
} from "../jones/workstreams/workstreamActionSnapshot";
import {
  assignNativeThread,
  type NativeThreadAssignmentOptions,
  type NativeThreadAssignmentSnapshot,
} from "../jones/workstreams/nativeThreadAssignment";

import { requestEnvironmentWorkstreams } from "../jones/workstreams/environmentWorkstreamsRequest";
import {
  assignRemoteNativeThread,
  environmentWorkstreamsRoutes,
  ownerRegistryKey,
  reconcileRemoteWorkstreamCommands,
  type RemoteWorkstreamCommandLedger,
} from "../jones/workstreams/remoteThreadAssignment";
import { useWorkstreamsReconnectRefresh } from "../jones/workstreams/useWorkstreamsReconnectRefresh";
import {
  classifyWorkstreamReadFailure,
  isWorkstreamsNotConfigured,
} from "../jones/workstreams/workstreamReadErrors";

import { PrimaryEnvironmentHttpClient } from "../environments/primary/httpClient";
import { runPrimaryHttp } from "../lib/runtime";
import { primaryEnvironmentIdAtom } from "./primaryEnvironment";

type PrimaryClient = Effect.Success<typeof PrimaryEnvironmentHttpClient>;

const request = <A, E>(run: (client: PrimaryClient) => Effect.Effect<A, E>, signal?: AbortSignal) =>
  runPrimaryHttp(PrimaryEnvironmentHttpClient.pipe(Effect.flatMap(run)), { signal });

const metadataCache = new LiveWorkstreamMetadataCache();
const isHttpConflict = Schema.is(EnvironmentHttpConflictError);
const isInternalReadError = Schema.is(EnvironmentInternalError);
const CURSOR_RESTART_ATTEMPTS = 3;
const INDEX_PAGE_LIMIT = 100;
const EMPTY_NATIVE_THREADS: readonly { readonly environmentId: string; readonly id: string }[] = [];

const isRestartableCursorStale = (cause: unknown): boolean =>
  isHttpConflict(cause) && cause.message === "workstream_cursor_stale";

export { isWorkstreamsNotConfigured };

/**
 * Error text for a failed list read, or null when Workstreams are simply not configured on the
 * server: retrying cannot succeed, so the sidebar shows ungrouped threads without an error.
 */
export const workstreamListErrorMessage = (cause: unknown): string | null =>
  isWorkstreamsNotConfigured(cause)
    ? null
    : cause instanceof Error
      ? cause.message
      : "Workstreams are unavailable.";

export interface CursorRestartOptions {
  readonly signal?: AbortSignal;
  readonly wait?: (delayMs: number, signal?: AbortSignal) => Promise<void>;
}

const throwIfAborted = (signal?: AbortSignal): void => signal?.throwIfAborted();

const waitForRetry = (delayMs: number, signal?: AbortSignal): Promise<void> =>
  new Promise<void>((resolve, reject) => {
    throwIfAborted(signal);
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

class WorkstreamRegistryVersionChanged extends Error {}

async function withTransientReadRecovery<A>(
  load: () => Promise<A>,
  signal: AbortSignal,
): Promise<A> {
  for (let retry = 0; ; retry += 1) {
    throwIfAborted(signal);
    try {
      const value = await load();
      throwIfAborted(signal);
      return value;
    } catch (cause) {
      throwIfAborted(signal);
      // CP outages share the opaque internal-error response; never retry reads indefinitely.
      if (
        retry === 5 ||
        (!(cause instanceof WorkstreamRegistryVersionChanged) &&
          (!isInternalReadError(cause) || cause.reason !== "internal_error"))
      )
        throw cause;
      await waitForRetry(1_000 * 2 ** retry, signal);
    }
  }
}

async function withCursorRestart<A>(
  load: () => Promise<A>,
  options: CursorRestartOptions = {},
): Promise<A> {
  const wait = options.wait ?? waitForRetry;
  for (let attempt = 0; attempt < CURSOR_RESTART_ATTEMPTS; attempt += 1) {
    throwIfAborted(options.signal);
    try {
      const result = await load();
      throwIfAborted(options.signal);
      return result;
    } catch (cause) {
      throwIfAborted(options.signal);
      if (!isRestartableCursorStale(cause) || attempt + 1 === CURSOR_RESTART_ATTEMPTS) throw cause;
      const delayMs = 50 * 2 ** attempt;
      if (options.signal === undefined) await wait(delayMs);
      else await wait(delayMs, options.signal);
    }
  }
  throw new Error("Workstream cursor restart policy is invalid.");
}

async function loadAllPages<Item>(
  load: (cursor?: string) => Promise<WorkstreamDtoPage<Item>>,
  signal?: AbortSignal,
): Promise<WorkstreamDtoPage<Item>> {
  throwIfAborted(signal);
  let result = await load();
  throwIfAborted(signal);
  const cursors = new Set<string>();
  while (result.next_cursor !== null) {
    throwIfAborted(signal);
    if (cursors.has(result.next_cursor)) throw new Error("Workstream pagination cursor repeated.");
    cursors.add(result.next_cursor);
    result = appendWorkstreamDtoPage(result, await load(result.next_cursor));
    throwIfAborted(signal);
  }
  return result;
}

export async function loadCompleteWorkstreamList(
  load: (cursor?: string) => Promise<T3WorkstreamListResult>,
  options: CursorRestartOptions = {},
): Promise<T3WorkstreamListResult> {
  return withCursorRestart(async () => {
    throwIfAborted(options.signal);
    let result = await load();
    throwIfAborted(options.signal);
    const cursors = new Set<string>();
    while (result.nextCursor !== null) {
      throwIfAborted(options.signal);
      if (cursors.has(result.nextCursor)) throw new Error("Workstream list cursor repeated.");
      cursors.add(result.nextCursor);
      result = appendWorkstreamListResult(result, await load(result.nextCursor));
      throwIfAborted(options.signal);
    }
    return result;
  }, options);
}

export interface WorkstreamDetailView {
  readonly detail: WorkstreamDetail;
  readonly memberships: WorkstreamMembershipPage;
  readonly declarations: WorkstreamDeclarationPage;
  readonly edges: WorkstreamEdgePage;
  readonly history: WorkstreamHistoryPage & {
    readonly coverage: "complete";
    readonly next_cursor: null;
  };
  readonly references: WorkstreamReferencePage;
}

export interface WorkstreamDetailLoaders {
  readonly detail: () => Promise<WorkstreamDetail>;
  readonly memberships: (cursor?: string) => Promise<WorkstreamMembershipPage>;
  readonly declarations: (cursor?: string) => Promise<WorkstreamDeclarationPage>;
  readonly edges: (cursor?: string) => Promise<WorkstreamEdgePage>;
  readonly history: (cursor?: string) => Promise<WorkstreamHistoryPage>;
  readonly references: (cursor?: string) => Promise<WorkstreamReferencePage>;
}

export async function loadCompleteWorkstreamDetail(
  loaders: WorkstreamDetailLoaders,
  options: CursorRestartOptions = {},
): Promise<WorkstreamDetailView> {
  return withCursorRestart(async () => {
    const [
      detailResult,
      membershipsResult,
      declarationsResult,
      edgesResult,
      historyResult,
      referencesResult,
    ] = await Promise.allSettled([
      loaders.detail(),
      loadAllPages(loaders.memberships, options.signal),
      loadAllPages(loaders.declarations, options.signal),
      loadAllPages(loaders.edges, options.signal),
      loadAllPages(loaders.history, options.signal),
      loadAllPages(loaders.references, options.signal),
    ] as const);
    const failures = [
      detailResult,
      membershipsResult,
      declarationsResult,
      edgesResult,
      historyResult,
      referencesResult,
    ].filter((result) => result.status === "rejected");
    const nonRestartableFailure = failures.find(
      (result) => result.status === "rejected" && !isRestartableCursorStale(result.reason),
    );
    if (nonRestartableFailure?.status === "rejected") throw nonRestartableFailure.reason;
    const staleFailure = failures[0];
    if (staleFailure?.status === "rejected") throw staleFailure.reason;
    if (
      detailResult.status !== "fulfilled" ||
      membershipsResult.status !== "fulfilled" ||
      declarationsResult.status !== "fulfilled" ||
      edgesResult.status !== "fulfilled" ||
      historyResult.status !== "fulfilled" ||
      referencesResult.status !== "fulfilled"
    )
      throw new Error("Workstream detail load did not settle.");
    const detail = detailResult.value;
    const memberships = membershipsResult.value;
    const declarations = declarationsResult.value;
    const edges = edgesResult.value;
    const history = historyResult.value;
    const references = referencesResult.value;
    const contexts: readonly WorkstreamReadContext[] = [
      memberships.context,
      declarations.context,
      edges.context,
      history.context,
      references.context,
    ];
    if (
      contexts.some(
        (context) =>
          context.owner_id !== detail.context.owner_id ||
          context.server_generation !== detail.context.server_generation ||
          context.registry_version !== detail.context.registry_version,
      )
    ) {
      throw new Error("Workstream detail changed while it was loading; reload it.");
    }
    if (history.next_cursor !== null)
      throw new Error("Workstream history load did not prove complete coverage.");
    return {
      detail,
      memberships,
      declarations,
      edges,
      history: { ...history, next_cursor: null, coverage: "complete" },
      references,
    };
  }, options);
}

export interface WorkstreamListView {
  /**
   * True for a thread whose own environment server, rather than the primary's, registers and
   * assigns it. That server's fences still decide whether the change is possible.
   */
  readonly assignsThroughOwnEnvironment?: (environmentId: string) => boolean;
  readonly assignNativeThread?: (
    thread: { readonly environmentId: string; readonly id: string },
    destination: string | null,
    options: NativeThreadAssignmentOptions,
  ) => Promise<void>;
  readonly registrationContext?: WorkstreamsRegistrationContextAcceptedResponse | null;
  readonly loadActionSnapshot?: (options?: {
    readonly signal?: AbortSignal;
  }) => Promise<WorkstreamActionSnapshot>;
  readonly retry?: () => Promise<void>;
  /** Identities whose proof requests succeeded for the current placements. */
  readonly placementInventory: NativePlacementInventory;
  readonly placements: LiveT3Placements | null;
  /**
   * Stored assignments read with `data` at one registry version. Durable display evidence
   * only: proofs in `placements` still gate every change.
   */
  readonly assignmentIndex?: WorkstreamAssignmentIndex | null;
  /** The last good `data` and `assignmentIndex` are shown because a later read failed. */
  readonly stale?: boolean;
  /** Clock for proof assurance; advances when a loaded proof or attestation expires. */
  readonly verificationNow?: string;
  readonly data: T3WorkstreamListResult | null;
  readonly error: string | null;
  readonly loading: boolean;
  readonly refresh: () => void;
  readonly submit: (command: WorkstreamCommand) => Promise<WorkstreamReceipt>;
  readonly runBindingOperation: <A>(
    operation: (submit: (command: WorkstreamCommand) => Promise<WorkstreamReceipt>) => Promise<A>,
  ) => Promise<A>;
  readonly loadDetail: (
    workstreamId: string,
    options?: { readonly signal?: AbortSignal },
  ) => Promise<WorkstreamDetailView>;
  readonly loadReference: (
    nativeReferenceId: string,
    options?: { readonly signal?: AbortSignal },
  ) => Promise<WorkstreamReferenceDetail>;
}

export async function loadCompleteWorkstreamReferences(
  load: (cursor?: string) => Promise<WorkstreamReferencePage>,
  options: CursorRestartOptions = {},
): Promise<WorkstreamReferencePage> {
  return withCursorRestart(() => loadAllPages(load, options.signal), options);
}

export async function reconcileWorkstreamCommands(input: {
  readonly commandIds: readonly string[];
  readonly observe: (id: string) => Promise<WorkstreamReceipt>;
  readonly resolved: (id: string) => void;
}): Promise<void> {
  for (const id of input.commandIds) {
    const receipt = await input.observe(id);
    if (receipt.state !== "committed" && receipt.state !== "rejected")
      throw new WorkstreamActionError("unknown");
    input.resolved(id);
  }
}

const actionAuthorityKey = (binding: T3WorkstreamListResult["binding"]) =>
  workstreamBindingKey({ ...binding, registryVersion: 0 });

export interface NativePlacementInventory {
  readonly coverage: "complete" | "partial";
  readonly identities: readonly T3PlacementIdentity[];
  readonly json: string;
  readonly totalIdentities: number;
}

export interface NativePlacementThread {
  readonly environmentId: string;
  readonly id: string;
  readonly settledOverride?: "settled" | "active" | null;
  readonly settledAt?: string | null;
}

interface PlacementCandidate {
  readonly key: string;
  readonly settled: boolean;
  readonly settledAt: number;
  readonly identity: T3PlacementIdentity;
}

interface BindingOperationRequest {
  readonly controller: AbortController;
  readonly acceptedBindingKeys: Set<string>;
}

const EMPTY_PLACEMENT_INVENTORY: NativePlacementInventory = {
  coverage: "complete",
  identities: [],
  json: "[]",
  totalIdentities: 0,
};

const comparePlacementCandidates = (a: PlacementCandidate, b: PlacementCandidate): number =>
  Number(a.settled) - Number(b.settled) ||
  (a.settled ? b.settledAt - a.settledAt : 0) ||
  (a.key < b.key ? -1 : a.key > b.key ? 1 : 0);

const compareIdentities = (a: T3PlacementIdentity, b: T3PlacementIdentity): number => {
  const left = JSON.stringify([a.source_instance_id, a.native_thread_id]);
  const right = JSON.stringify([b.source_instance_id, b.native_thread_id]);
  return left < right ? -1 : left > right ? 1 : 0;
};

/**
 * Splits proof requests for the native threads accepted by `include` (and the preferred
 * thread) so every request stays within the identity and encoded-byte ceilings. The preferred
 * thread is requested first, then unsettled and recently settled conversations; identities
 * within a request keep canonical order.
 */
export function nativePlacementBatches(
  nativeThreads: readonly NativePlacementThread[],
  options: {
    readonly include?: (key: string) => boolean;
    readonly preferred?: { readonly environmentId: string; readonly id: string } | null;
  } = {},
): readonly (readonly T3PlacementIdentity[])[] {
  const preferredKey = options.preferred
    ? nativeWorkstreamThreadKey(options.preferred.environmentId, options.preferred.id)
    : null;
  const candidates = new Map<string, PlacementCandidate>();
  for (const thread of nativeThreads) {
    const key = nativeWorkstreamThreadKey(thread.environmentId, thread.id);
    if (key !== preferredKey && options.include && !options.include(key)) continue;
    const candidate: PlacementCandidate = {
      key,
      settled: thread.settledOverride === "settled",
      settledAt: Date.parse(thread.settledAt ?? "") || 0,
      identity: { source_instance_id: thread.environmentId, native_thread_id: thread.id },
    };
    const previous = candidates.get(key);
    if (!previous || comparePlacementCandidates(candidate, previous) < 0)
      candidates.set(key, candidate);
  }
  const ordered = [...candidates.values()].sort(
    (a, b) =>
      Number(b.key === preferredKey) - Number(a.key === preferredKey) ||
      comparePlacementCandidates(a, b),
  );
  const encoder = new TextEncoder();
  const emptyRequestBytes = encoder.encode(JSON.stringify({ identities: [] })).byteLength;
  const batches: T3PlacementIdentity[][] = [];
  let batch: T3PlacementIdentity[] = [];
  let requestBytes = emptyRequestBytes;
  for (const { identity } of ordered) {
    const identityBytes = encoder.encode(JSON.stringify(identity)).byteLength;
    // An identity too large for any request stays unproven rather than failing the others.
    if (emptyRequestBytes + identityBytes > T3_PLACEMENT_MAX_REQUEST_BYTES) continue;
    if (
      batch.length === T3_PLACEMENT_MAX_IDENTITIES ||
      requestBytes + identityBytes + (batch.length > 0 ? 1 : 0) > T3_PLACEMENT_MAX_REQUEST_BYTES
    ) {
      batches.push(batch);
      batch = [];
      requestBytes = emptyRequestBytes;
    }
    requestBytes += identityBytes + (batch.length > 0 ? 1 : 0);
    batch.push(identity);
  }
  if (batch.length > 0) batches.push(batch);
  return batches.map((identities) => identities.sort(compareIdentities));
}

const sameTrustedEnvironments = (left: LiveT3Placements, right: LiveT3Placements): boolean => {
  const canonical = (value: LiveT3Placements) =>
    JSON.stringify(
      value.trustedEnvironments
        .map((entry) => [entry.environmentId, entry.authorityNamespace, entry.storeGeneration])
        .sort(),
    );
  return canonical(left) === canonical(right);
};

/**
 * Combines independently validated proof batches. Batches answered under different contexts
 * or trust snapshots cannot be combined, so none of them verifies anything.
 */
export function mergeLivePlacements(batches: readonly LiveT3Placements[]): LiveT3Placements | null {
  const [first, ...rest] = batches;
  if (first === undefined) return null;
  for (const batch of rest) {
    if (
      batch.context.owner_id !== first.context.owner_id ||
      batch.context.principal_id !== first.context.principal_id ||
      batch.context.authorization_revision !== first.context.authorization_revision ||
      batch.context.server_generation !== first.context.server_generation ||
      batch.context.registry_version !== first.context.registry_version ||
      batch.readiness !== first.readiness ||
      !sameTrustedEnvironments(first, batch)
    )
      return null;
  }
  return { ...first, items: batches.flatMap((batch) => batch.items) };
}

interface LoadedPlacementProofs {
  readonly key: string;
  readonly placements: LiveT3Placements | null;
  readonly inventory: NativePlacementInventory;
}

/**
 * Requests proof for each batch with its own inventory hash. A failed batch leaves its
 * identities without proof (recorded) and does not discard the other batches.
 */
async function loadPlacementProofs(
  list: T3WorkstreamListResult,
  batches: readonly (readonly T3PlacementIdentity[])[],
  load: (identities: readonly T3PlacementIdentity[]) => Promise<T3PlacementResult>,
  signal: AbortSignal,
): Promise<LoadedPlacementProofs> {
  const key = JSON.stringify(batches);
  const proven: {
    readonly identities: readonly T3PlacementIdentity[];
    result: LiveT3Placements;
  }[] = [];
  let failed = false;
  for (const identities of batches) {
    throwIfAborted(signal);
    try {
      proven.push({
        identities,
        result: await loadLiveT3Placements(list, identities, () => load(identities)),
      });
    } catch (cause) {
      throwIfAborted(signal);
      if (classifyWorkstreamReadFailure(cause) === "unauthenticated") throw cause;
      failed = true;
    }
  }
  const placements = mergeLivePlacements(proven.map((entry) => entry.result));
  const identities =
    placements === null ? [] : proven.flatMap((entry) => entry.identities).sort(compareIdentities);
  return {
    key,
    placements,
    inventory: {
      coverage: failed || (placements === null && proven.length > 0) ? "partial" : "complete",
      identities,
      json: JSON.stringify(identities),
      totalIdentities: batches.reduce((total, batch) => total + batch.length, 0),
    },
  };
}

const normalizeWorkstreamList = (value: T3WorkstreamListResult): T3WorkstreamListResult => ({
  ...value,
  items: [...orderWorkstreamMetadata(value.items)],
});

const sameContextAuthority = (
  context: WorkstreamsRegistrationContextAcceptedResponse,
  binding: T3WorkstreamListResult["binding"],
): boolean =>
  context.owner_id === binding.ownerId &&
  context.principal_id === binding.principalId &&
  context.authorization_revision === binding.authorizationRevision &&
  context.server_generation === binding.serverGeneration;

export function reuseNativePlacementIdentitySnapshot<Thread extends NativePlacementThread>(
  previous: readonly Thread[],
  next: readonly Thread[],
): readonly Thread[] {
  if (previous === next || previous.length !== next.length) return next;
  for (let index = 0; index < previous.length; index += 1) {
    const previousThread = previous[index]!;
    const nextThread = next[index]!;
    if (
      previousThread.environmentId !== nextThread.environmentId ||
      previousThread.id !== nextThread.id ||
      previousThread.settledOverride !== nextThread.settledOverride ||
      previousThread.settledAt !== nextThread.settledAt
    )
      return next;
  }
  return previous;
}

export function useWorkstreams(
  placementsEnabled = true,
  nativeThreads: readonly NativePlacementThread[] = EMPTY_NATIVE_THREADS,
): WorkstreamListView {
  const nativeThreadsSnapshot = useRef(nativeThreads);
  nativeThreadsSnapshot.current = reuseNativePlacementIdentitySnapshot(
    nativeThreadsSnapshot.current,
    nativeThreads,
  );
  const stableNativeThreads = nativeThreadsSnapshot.current;
  const [preferredThread, setPreferredThread] = useState<{
    readonly environmentId: string;
    readonly id: string;
  } | null>(null);
  const preferredThreadRef = useRef(preferredThread);
  preferredThreadRef.current = preferredThread;
  const [registrationContext, setRegistrationContext] =
    useState<WorkstreamsRegistrationContextAcceptedResponse | null>(null);
  const unresolvedCommands = useRef(new Map<string, string>());
  const remoteCommands = useRef<RemoteWorkstreamCommandLedger>(new Map());
  const primaryEnvironmentId = useAtomValue(primaryEnvironmentIdAtom);
  const dataRef = useRef<T3WorkstreamListResult | null>(null);
  const [placements, setPlacements] = useState<LiveT3Placements | null>(null);
  const [placementInventory, setPlacementInventory] =
    useState<NativePlacementInventory>(EMPTY_PLACEMENT_INVENTORY);
  const [assignmentIndex, setAssignmentIndex] = useState<WorkstreamAssignmentIndex | null>(null);
  const assignmentIndexRef = useRef(assignmentIndex);
  assignmentIndexRef.current = assignmentIndex;
  const [stale, setStale] = useState(false);
  const staleRef = useRef(stale);
  staleRef.current = stale;
  const [verificationNow, setVerificationNow] = useState(() => new Date().toISOString());
  const [data, setData] = useState<T3WorkstreamListResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [revision, setRevision] = useState(0);
  const generation = useRef(0);
  const listRequest = useRef<AbortController | null>(null);
  const proofRequest = useRef<AbortController | null>(null);
  const requestedProofKey = useRef<string | null>(null);
  const commandRequests = useRef(new Set<BindingOperationRequest>());
  const currentBindingKey = data ? workstreamBindingKey(data.binding) : null;
  const currentBindingKeyRef = useRef(currentBindingKey);
  currentBindingKeyRef.current = currentBindingKey;
  dataRef.current = data;
  const refresh = useCallback(() => {
    listRequest.current?.abort();
    for (const operation of commandRequests.current) operation.controller.abort();
    generation.current += 1;
    // The last good snapshot stays visible until the reload replaces it as a whole.
    setRevision((value) => value + 1);
  }, []);
  useWorkstreamsReconnectRefresh(refresh);

  /** Authorization, session or availability loss hides every previously authorized read. */
  const purgeAuthorizedState = useCallback(() => {
    metadataCache.purgeAuthorization();
    setData(null);
    setAssignmentIndex(null);
    setPlacements(null);
    setPlacementInventory(EMPTY_PLACEMENT_INVENTORY);
    setRegistrationContext(null);
    setStale(false);
  }, []);

  useEffect(() => {
    for (const operation of commandRequests.current) {
      if (
        (currentBindingKey === null || !operation.acceptedBindingKeys.has(currentBindingKey)) &&
        !operation.controller.signal.aborted
      )
        operation.controller.abort();
    }
  }, [currentBindingKey]);

  useEffect(() => {
    const requests = commandRequests.current;
    return () => {
      for (const operation of requests) operation.controller.abort();
      requests.clear();
    };
  }, []);

  const proofBatches = useCallback(
    (index: WorkstreamAssignmentIndex) =>
      nativePlacementBatches(nativeThreadsSnapshot.current, {
        include: (key) => index.entries.has(key),
        preferred: preferredThreadRef.current,
      }),
    [],
  );
  const requestPlacements = useCallback(
    (identities: readonly T3PlacementIdentity[], signal: AbortSignal) =>
      request(
        (client) => client.workstreams.threadPlacements({ headers: {}, payload: { identities } }),
        signal,
      ),
    [],
  );

  useEffect(() => {
    listRequest.current?.abort();
    proofRequest.current?.abort();
    const controller = new AbortController();
    listRequest.current = controller;
    const current = ++generation.current;
    setLoading(true);
    const loadList = () =>
      loadCompleteWorkstreamList(
        (cursor) =>
          request(
            (client) =>
              client.workstreams.list({
                headers: {},
                payload: { limit: 50, ...(cursor === undefined ? {} : { cursor }) },
              }),
            controller.signal,
          ),
        { signal: controller.signal },
      );
    const indexFetchers: WorkstreamAssignmentIndexFetchers = {
      listWorkstreams: ({ cursor, signal }) =>
        request(
          (client) =>
            client.workstreams.list({
              headers: {},
              payload: { limit: 50, ...(cursor === undefined ? {} : { cursor }) },
            }),
          signal,
        ),
      references: ({ cursor, signal }) =>
        request(
          (client) =>
            client.workstreams.references({
              headers: {},
              payload: { limit: INDEX_PAGE_LIMIT, ...(cursor === undefined ? {} : { cursor }) },
            }),
          signal,
        ),
      memberships: ({ workstreamId, cursor, signal }) =>
        request(
          (client) =>
            client.workstreams.memberships({
              headers: {},
              params: { workstreamId },
              payload: { limit: INDEX_PAGE_LIMIT, ...(cursor === undefined ? {} : { cursor }) },
            }),
          signal,
        ),
    };
    // Grouped display reads the list together with its stored assignments at one version.
    const loadCore = async (): Promise<{
      readonly list: T3WorkstreamListResult;
      readonly index: WorkstreamAssignmentIndex | null;
    }> => {
      if (!placementsEnabled)
        return { list: normalizeWorkstreamList(await loadList()), index: null };
      const loaded = await loadWorkstreamAssignmentIndex(indexFetchers, {
        signal: controller.signal,
      });
      return { list: normalizeWorkstreamList(loaded.list), index: loaded.index };
    };
    const superseded = () => generation.current !== current || controller.signal.aborted;
    void (async () => {
      let core = await withTransientReadRecovery(loadCore, controller.signal);
      let reloadList = false;
      const context = await withTransientReadRecovery(async () => {
        if (reloadList) core = await loadCore();
        const loaded = await request(
          (client) => client.workstreams.registrationContext({ headers: {} }),
          controller.signal,
        );
        if (
          sameContextAuthority(loaded, core.list.binding) &&
          loaded.registry_version !== core.list.binding.registryVersion
        ) {
          // Refresh both reads within this budget; authority mismatches never trigger recovery.
          reloadList = true;
          throw new WorkstreamRegistryVersionChanged();
        }
        return loaded;
      }, controller.signal).then(
        (loaded) =>
          sameContextAuthority(loaded, core.list.binding) &&
          loaded.registry_version === core.list.binding.registryVersion
            ? loaded
            : null,
        (cause: unknown) => {
          if (reloadList && !(cause instanceof WorkstreamRegistryVersionChanged)) throw cause;
          return null;
        },
      );
      if (superseded()) return;
      const batches = core.index === null ? [] : proofBatches(core.index);
      const proofs =
        core.index === null
          ? null
          : await loadPlacementProofs(
              core.list,
              batches,
              (identities) => requestPlacements(identities, controller.signal),
              controller.signal,
            );
      if (superseded()) return;
      // Commit the whole snapshot at once so no render mixes versions or clears groups.
      if (listRequest.current === controller) listRequest.current = null;
      const previous = dataRef.current?.binding;
      if (previous && actionAuthorityKey(previous) !== actionAuthorityKey(core.list.binding))
        metadataCache.purgeAuthorization();
      metadataCache.write(core.list);
      requestedProofKey.current = proofs?.key ?? null;
      setData(metadataCache.read(core.list.binding));
      setRegistrationContext(context);
      setAssignmentIndex(core.index);
      setPlacements(proofs?.placements ?? null);
      setPlacementInventory(proofs?.inventory ?? EMPTY_PLACEMENT_INVENTORY);
      setVerificationNow(new Date().toISOString());
      setStale(false);
      setError(null);
    })()
      .catch((cause: unknown) => {
        if (superseded()) return;
        if (classifyWorkstreamReadFailure(cause) !== "unauthenticated") {
          // Only lost access hides groups; outages, opaque control-plane failures and an
          // unconfigured or missing API keep the last good snapshot as stale.
          if (dataRef.current !== null) setStale(true);
        } else purgeAuthorizedState();
        setError(workstreamListErrorMessage(cause));
      })
      .finally(() => {
        if (listRequest.current === controller) listRequest.current = null;
        if (generation.current === current) setLoading(false);
      });
    return () => {
      controller.abort();
      if (listRequest.current === controller) listRequest.current = null;
      generation.current += 1;
    };
  }, [revision, placementsEnabled, proofBatches, purgeAuthorizedState, requestPlacements]);

  // Native identity changes only refresh proofs for the committed snapshot; they never clear it.
  useEffect(() => {
    if (!placementsEnabled || data === null || assignmentIndex === null) return;
    if (listRequest.current !== null) return;
    const batches = proofBatches(assignmentIndex);
    if (JSON.stringify(batches) === requestedProofKey.current) return;
    proofRequest.current?.abort();
    const controller = new AbortController();
    proofRequest.current = controller;
    const startedGeneration = generation.current;
    const current = () =>
      !controller.signal.aborted &&
      generation.current === startedGeneration &&
      assignmentIndexRef.current === assignmentIndex;
    const previousKey = requestedProofKey.current;
    const key = JSON.stringify(batches);
    let settled = false;
    requestedProofKey.current = key;
    void loadPlacementProofs(
      data,
      batches,
      (identities) => requestPlacements(identities, controller.signal),
      controller.signal,
    ).then(
      (proofs) => {
        if (!current()) return;
        settled = true;
        setPlacements(proofs.placements);
        setPlacementInventory(proofs.inventory);
        setVerificationNow(new Date().toISOString());
      },
      () => {
        // An authorization failure surfaces through the coherent reload, which classifies it.
        if (!current()) return;
        settled = true;
        refresh();
      },
    );
    return () => {
      controller.abort();
      if (proofRequest.current === controller) proofRequest.current = null;
      // An interrupted request did not prove anything; let the next run request it again.
      if (!settled && requestedProofKey.current === key) requestedProofKey.current = previousKey;
    };
  }, [
    placementsEnabled,
    data,
    assignmentIndex,
    stableNativeThreads,
    preferredThread,
    proofBatches,
    requestPlacements,
    refresh,
  ]);

  // Expiry downgrades verified proof to recorded; it never removes a stored assignment.
  useEffect(() => {
    const nowMs = Date.parse(verificationNow);
    let next = Number.POSITIVE_INFINITY;
    const consider = (value: string | null | undefined) => {
      const expiresAt = Date.parse(value ?? "");
      if (expiresAt > nowMs && expiresAt < next) next = expiresAt;
    };
    for (const item of placements?.items ?? []) consider(item.expires_at);
    for (const entry of assignmentIndex?.entries.values() ?? [])
      consider(entry.primary?.reference.registration.expires_at);
    if (!Number.isFinite(next)) return;
    const timer = window.setTimeout(
      () => setVerificationNow(new Date(Math.max(Date.now(), next)).toISOString()),
      Math.max(0, Math.min(2_147_483_647, next - Date.now())),
    );
    return () => window.clearTimeout(timer);
  }, [placements, assignmentIndex, verificationNow]);

  const runBindingOperation = useCallback(
    async <A>(
      operation: (submit: (command: WorkstreamCommand) => Promise<WorkstreamReceipt>) => Promise<A>,
    ) => {
      const startedBinding = data?.binding;
      if (startedBinding === undefined) throw new Error("Workstream binding is unavailable.");
      const authority = actionAuthorityKey(startedBinding);
      const startedGeneration = generation.current;
      if ([...unresolvedCommands.current.values()].includes(authority))
        throw new WorkstreamActionError("unknown");
      const controller = new AbortController();
      const operationRequest: BindingOperationRequest = {
        controller,
        acceptedBindingKeys: new Set([workstreamBindingKey(startedBinding)]),
      };
      commandRequests.current.add(operationRequest);
      let commandAttempted = false;
      let refreshAfterOperation = false;
      const currentBindingIsAccepted = () => {
        const key = currentBindingKeyRef.current;
        return key !== null && operationRequest.acceptedBindingKeys.has(key);
      };
      const assertCurrentBinding = () => {
        if (
          (!currentBindingIsAccepted() || generation.current !== startedGeneration) &&
          !controller.signal.aborted
        )
          controller.abort();
        controller.signal.throwIfAborted();
      };
      const acceptReceiptBinding = (receipt: WorkstreamReceipt) => {
        if (receipt.state !== "committed") return;
        operationRequest.acceptedBindingKeys.add(
          workstreamBindingKey({ ...startedBinding, registryVersion: receipt.registry_version }),
        );
      };
      const submitCommand = async (command: WorkstreamCommand) => {
        assertCurrentBinding();
        commandAttempted = true;
        unresolvedCommands.current.set(command.command_id, authority);
        let receipt = await request(
          (client) => client.workstreams.submit({ headers: {}, payload: { command } }),
          controller.signal,
        );
        acceptReceiptBinding(receipt);
        assertCurrentBinding();
        // Pending effects reconcile through the exact GET route; commands are never resubmitted.
        while (receipt.state === "pending" || receipt.state === "unresolved") {
          const retryAfterSeconds = receipt.retry_after_seconds;
          await waitForRetry(
            Math.min(30_000, Math.max(250, retryAfterSeconds * 1_000)),
            controller.signal,
          );
          assertCurrentBinding();
          receipt = await request(
            (client) =>
              client.workstreams.command({
                headers: {},
                params: { commandId: command.command_id },
              }),
            controller.signal,
          );
          acceptReceiptBinding(receipt);
          assertCurrentBinding();
        }
        if (receipt.state === "committed" || receipt.state === "rejected")
          unresolvedCommands.current.delete(command.command_id);
        return receipt;
      };
      try {
        const value = await operation(submitCommand);
        assertCurrentBinding();
        refreshAfterOperation = true;
        return value;
      } catch (cause) {
        if (!controller.signal.aborted && currentBindingIsAccepted()) {
          generation.current += 1;
          purgeAuthorizedState();
          setError("Workstreams changed or are unavailable. Retry to reload current access.");
          refreshAfterOperation = commandAttempted;
        }
        throw cause;
      } finally {
        commandRequests.current.delete(operationRequest);
        if (refreshAfterOperation && !controller.signal.aborted && currentBindingIsAccepted())
          refresh();
      }
    },
    [data?.binding, purgeAuthorizedState, refresh],
  );
  const submit = useCallback(
    (command: WorkstreamCommand) => runBindingOperation((submitCommand) => submitCommand(command)),
    [runBindingOperation],
  );

  const assignThread = useCallback(
    async (
      thread: { readonly environmentId: string; readonly id: string },
      destination: string | null,
      options: NativeThreadAssignmentOptions,
    ) => {
      const started = dataRef.current;
      // Stale stored assignments are display evidence only and never plan a change.
      if (!started || !placementsEnabled || staleRef.current)
        throw new WorkstreamActionError("stale");
      const authority = actionAuthorityKey(started.binding);
      const startedGeneration = generation.current;
      const threadKey = nativeWorkstreamThreadKey(thread.environmentId, thread.id);
      const stored = assignmentIndexRef.current?.entries.get(threadKey);
      if (stored?.state === "conflict") throw new WorkstreamActionError("ambiguous");
      const check = () => {
        options.signal?.throwIfAborted();
        if (
          generation.current !== startedGeneration ||
          !dataRef.current ||
          actionAuthorityKey(dataRef.current.binding) !== authority
        )
          throw new WorkstreamActionError("stale");
      };
      if (primaryEnvironmentId !== null && thread.environmentId !== primaryEnvironmentId) {
        if ([...unresolvedCommands.current.values()].includes(authority))
          throw new WorkstreamActionError("unknown");
        // Only the thread's own server can register its native threads; the primary snapshot
        // still has to stay current, live and unconflicted for the whole operation.
        await assignRemoteNativeThread({
          ...options,
          primary: started,
          thread,
          destination,
          stored,
          routes: environmentWorkstreamsRoutes(thread.environmentId, requestEnvironmentWorkstreams),
          check: () => {
            check();
            if (staleRef.current) throw new WorkstreamActionError("stale");
          },
          commands: remoteCommands.current,
          loadCompleteList: (load) => loadCompleteWorkstreamList(load, options),
          refreshPrimary: refresh,
        });
        options.signal?.throwIfAborted();
        return;
      }
      const loadSnapshot = async (
        referenceId?: string,
      ): Promise<NativeThreadAssignmentSnapshot> => {
        check();
        const load = <A, E>(run: (client: PrimaryClient) => Effect.Effect<A, E>) =>
          request(run, options.signal);
        const live = await loadCompleteWorkstreamList(
          (cursor) =>
            load((client) =>
              client.workstreams.list({
                headers: {},
                payload: { limit: 50, ...(cursor === undefined ? {} : { cursor }) },
              }),
            ),
          options,
        );
        check();
        if (actionAuthorityKey(live.binding) !== authority)
          throw new WorkstreamActionError("stale");
        const context = await load((client) =>
          client.workstreams.registrationContext({ headers: {} }),
        );
        check();
        const selected = [
          { source_instance_id: thread.environmentId, native_thread_id: thread.id },
        ];
        const projection = await loadLiveT3Placements(live, selected, () =>
          load((client) =>
            client.workstreams.threadPlacements({
              headers: {},
              payload: { identities: selected },
            }),
          ),
        );
        check();
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
            : await load((client) =>
                client.workstreams.reference({
                  headers: {},
                  params: { nativeReferenceId: selectedReference },
                }),
              );
        check();
        const destinationMemberships =
          destination === null
            ? null
            : await loadAllPages(
                (cursor) =>
                  load((client) =>
                    client.workstreams.memberships({
                      headers: {},
                      params: { workstreamId: destination },
                      payload: { limit: 50, ...(cursor === undefined ? {} : { cursor }) },
                    }),
                  ),
                options.signal,
              );
        check();
        assertWorkstreamReadContext(live, context);
        // Once the reference is current, its proof must agree with the stored primary, so a
        // missing proof can never plan a first attach for an already assigned thread.
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
      await assignNativeThread({
        data: started,
        thread,
        destination,
        loadSnapshot,
        runBindingOperation,
        ...options,
      });
      options.signal?.throwIfAborted();
      setPreferredThread({ environmentId: thread.environmentId, id: thread.id });
    },
    [placementsEnabled, primaryEnvironmentId, runBindingOperation, refresh],
  );

  const readFailed = useCallback(
    (cause: unknown) => {
      if (classifyWorkstreamReadFailure(cause) !== "unauthenticated") {
        if (dataRef.current !== null) setStale(true);
      } else {
        generation.current += 1;
        purgeAuthorizedState();
      }
      setError(cause instanceof Error ? cause.message : "Workstreams are unavailable.");
    },
    [purgeAuthorizedState],
  );

  const loadDetail = useCallback(
    async (workstreamId: string, options: { readonly signal?: AbortSignal } = {}) => {
      try {
        const load = <A, E>(run: (client: PrimaryClient) => Effect.Effect<A, E>) =>
          request(run, options.signal);
        return await loadCompleteWorkstreamDetail(
          {
            detail: () =>
              load((client) =>
                client.workstreams.detail({ headers: {}, params: { workstreamId } }),
              ),
            memberships: (cursor) =>
              load((client) =>
                client.workstreams.memberships({
                  headers: {},
                  params: { workstreamId },
                  payload: { limit: 50, ...(cursor === undefined ? {} : { cursor }) },
                }),
              ),
            declarations: (cursor) =>
              load((client) =>
                client.workstreams.declarations({
                  headers: {},
                  params: { workstreamId },
                  payload: { limit: 50, ...(cursor === undefined ? {} : { cursor }) },
                }),
              ),
            edges: (cursor) =>
              load((client) =>
                client.workstreams.edges({
                  headers: {},
                  params: { workstreamId },
                  payload: { limit: 50, ...(cursor === undefined ? {} : { cursor }) },
                }),
              ),
            history: (cursor) =>
              load((client) =>
                client.workstreams.history({
                  headers: {},
                  params: { workstreamId },
                  payload: { limit: 50, ...(cursor === undefined ? {} : { cursor }) },
                }),
              ),
            references: (cursor) =>
              load((client) =>
                client.workstreams.references({
                  headers: {},
                  payload: { limit: 50, ...(cursor === undefined ? {} : { cursor }) },
                }),
              ),
          },
          options,
        );
      } catch (cause) {
        if (options.signal?.aborted) throw cause;
        readFailed(cause);
        throw cause;
      }
    },
    [readFailed],
  );

  const loadReference = useCallback(
    async (nativeReferenceId: string, options: { readonly signal?: AbortSignal } = {}) => {
      try {
        return await request(
          (client) => client.workstreams.reference({ headers: {}, params: { nativeReferenceId } }),
          options.signal,
        );
      } catch (cause) {
        if (options.signal?.aborted) throw cause;
        readFailed(cause);
        throw cause;
      }
    },
    [readFailed],
  );

  const loadActionSnapshot = useCallback(
    async (options: { readonly signal?: AbortSignal } = {}): Promise<WorkstreamActionSnapshot> => {
      const started = dataRef.current?.binding;
      if (!started) throw new WorkstreamActionError("stale");
      const live = await loadCompleteWorkstreamList(
        (cursor) =>
          request(
            (client) =>
              client.workstreams.list({
                headers: {},
                payload: { limit: 50, ...(cursor === undefined ? {} : { cursor }) },
              }),
            options.signal,
          ),
        options,
      );
      options.signal?.throwIfAborted();
      const current = dataRef.current?.binding;
      if (
        !started ||
        !current ||
        actionAuthorityKey(started) !== actionAuthorityKey(current) ||
        actionAuthorityKey(started) !== actionAuthorityKey(live.binding)
      )
        throw new WorkstreamActionError("stale");
      const references = await loadCompleteWorkstreamReferences(
        (cursor) =>
          request(
            (client) =>
              client.workstreams.references({
                headers: {},
                payload: { limit: 50, ...(cursor === undefined ? {} : { cursor }) },
              }),
            options.signal,
          ),
        options,
      );
      const context = await request(
        (client) => client.workstreams.registrationContext({ headers: {} }),
        options.signal,
      );
      options.signal?.throwIfAborted();
      if (
        !dataRef.current ||
        actionAuthorityKey(started) !== actionAuthorityKey(dataRef.current.binding)
      )
        throw new WorkstreamActionError("stale");
      const snapshot: WorkstreamActionSnapshot = {
        data: live,
        references,
        placements: null,
        registrationContext: context,
      };
      assertWorkstreamActionSnapshot(snapshot);
      return snapshot;
    },
    [],
  );
  const retry = useCallback(async () => {
    const started = dataRef.current?.binding;
    if (!started) throw new WorkstreamActionError("stale");
    const authority = actionAuthorityKey(started);
    await loadActionSnapshot();
    await reconcileWorkstreamCommands({
      commandIds: [...unresolvedCommands.current]
        .filter(([, key]) => key === authority)
        .map(([id]) => id),
      observe: async (id) => {
        if (!dataRef.current || actionAuthorityKey(dataRef.current.binding) !== authority)
          throw new WorkstreamActionError("stale");
        const receipt = await request((client) =>
          client.workstreams.command({ headers: {}, params: { commandId: id } }),
        );
        if (!dataRef.current || actionAuthorityKey(dataRef.current.binding) !== authority)
          throw new WorkstreamActionError("stale");
        return receipt;
      },
      resolved: (id) => unresolvedCommands.current.delete(id),
    });
    // Commands sent to another environment are observed on that environment only.
    await reconcileRemoteWorkstreamCommands({
      commands: remoteCommands.current,
      ownerRegistry: ownerRegistryKey(started),
      routesFor: (environmentId) =>
        environmentWorkstreamsRoutes(environmentId, requestEnvironmentWorkstreams),
    });
    const snapshot = await loadActionSnapshot();
    metadataCache.write(snapshot.data);
    setData(metadataCache.read(snapshot.data.binding));
    setRegistrationContext(snapshot.registrationContext);
    setError(null);
    // The action snapshot omits stored assignments; reload them at the list's new version.
    if (placementsEnabled) refresh();
  }, [loadActionSnapshot, placementsEnabled, refresh]);

  const assignsThroughOwnEnvironment = useCallback(
    (environmentId: string) =>
      primaryEnvironmentId !== null && environmentId !== primaryEnvironmentId,
    [primaryEnvironmentId],
  );

  return {
    assignsThroughOwnEnvironment,
    assignNativeThread: assignThread,
    registrationContext,
    loadActionSnapshot,
    retry,
    placementInventory,
    placements,
    assignmentIndex,
    stale,
    verificationNow,
    data,
    error,
    loading,
    refresh,
    submit,
    runBindingOperation,
    loadDetail,
    loadReference,
  };
}
