import type {
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
  WorkstreamsRegistrationContextResponse,
} from "@t3tools/contracts";
import {
  EnvironmentHttpConflictError,
  T3_PLACEMENT_MAX_IDENTITIES,
  T3_PLACEMENT_MAX_REQUEST_BYTES,
} from "@t3tools/contracts";
import {
  appendWorkstreamDtoPage,
  appendWorkstreamListResult,
  LiveWorkstreamMetadataCache,
  orderWorkstreamMetadata,
  type WorkstreamDtoPage,
  loadLiveT3Placements,
  type LiveT3Placements,
  workstreamBindingKey,
} from "@t3tools/client-runtime/state/workstreams";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { PrimaryEnvironmentHttpClient } from "../environments/primary/httpClient";
import { runPrimaryHttp } from "../lib/runtime";

type PrimaryClient = Effect.Success<typeof PrimaryEnvironmentHttpClient>;

const request = <A, E>(run: (client: PrimaryClient) => Effect.Effect<A, E>, signal?: AbortSignal) =>
  runPrimaryHttp(PrimaryEnvironmentHttpClient.pipe(Effect.flatMap(run)), { signal });

const metadataCache = new LiveWorkstreamMetadataCache();
const isCursorStale = Schema.is(EnvironmentHttpConflictError);
const CURSOR_RESTART_ATTEMPTS = 3;
const EMPTY_NATIVE_THREADS: readonly { readonly environmentId: string; readonly id: string }[] = [];

const isRestartableCursorStale = (cause: unknown): boolean =>
  isCursorStale(cause) && cause.message === "workstream_cursor_stale";

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
  readonly placementInventory: NativePlacementInventory;
  readonly placements: LiveT3Placements | null;
  readonly references: WorkstreamReferencePage | null;
  readonly registrationContext: WorkstreamsRegistrationContextResponse | null;
  readonly data: T3WorkstreamListResult | null;
  readonly error: string | null;
  readonly loading: boolean;
  readonly refresh: () => void;
  readonly retry: () => Promise<void>;
  readonly loadActionSnapshot: (options?: {
    readonly signal?: AbortSignal;
  }) => Promise<WorkstreamActionSnapshot>;
  readonly observeCommand: (
    commandId: string,
    options?: { readonly signal?: AbortSignal },
  ) => Promise<WorkstreamReceipt>;
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

export interface WorkstreamActionSnapshot {
  readonly data: T3WorkstreamListResult;
  readonly references: WorkstreamReferencePage;
  readonly placements: LiveT3Placements | null;
  readonly registrationContext: WorkstreamsRegistrationContextResponse | null;
}

export class WorkstreamActionError extends Error {
  constructor(
    readonly reason: "activation" | "stale" | "denied" | "unknown" | "ambiguous" | "invalid-pr",
  ) {
    super(workstreamActionMessage(reason));
    this.name = "WorkstreamActionError";
  }
}

function workstreamActionMessage(reason: WorkstreamActionError["reason"]): string {
  switch (reason) {
    case "activation":
      return "Reference preparation is unavailable until its source is activated.";
    case "stale":
      return "Workstreams or reference verification changed. Reload and take a new action.";
    case "denied":
      return "The Workstream action was denied. No further changes were submitted.";
    case "ambiguous":
      return "Conflicting references require resolution before this action can continue.";
    case "invalid-pr":
      return "Enter a full GitHub pull request URL.";
    case "unknown":
      return "The effect is unknown. Retry reloads metadata and checks the existing command; it does not resubmit.";
  }
}

export const workstreamFailureMessage = (cause: unknown): string =>
  cause instanceof WorkstreamActionError ? cause.message : workstreamActionMessage("unknown");

export function assertWorkstreamReadContext(
  data: T3WorkstreamListResult,
  context: WorkstreamReadContext,
): void {
  if (
    context.owner_id !== data.binding.ownerId ||
    context.server_generation !== data.binding.serverGeneration ||
    context.registry_version !== data.binding.registryVersion
  )
    throw new WorkstreamActionError("stale");
}

export function assertWorkstreamActionSnapshot(snapshot: WorkstreamActionSnapshot): void {
  const { data, references, placements, registrationContext } = snapshot;
  assertWorkstreamReadContext(data, references.context);
  for (const context of [placements?.context, registrationContext]) {
    if (!context) continue;
    assertWorkstreamReadContext(data, context);
    if (
      context.principal_id !== data.binding.principalId ||
      context.authorization_revision !== data.binding.authorizationRevision
    )
      throw new WorkstreamActionError("stale");
  }
  if (references.next_cursor !== null) throw new WorkstreamActionError("stale");
}

export function assertWorkstreamSnapshotAuthority(
  started: T3WorkstreamListResult["binding"] | undefined,
  current: T3WorkstreamListResult["binding"] | undefined,
  returned: T3WorkstreamListResult["binding"],
): void {
  const authorityKey = (binding: T3WorkstreamListResult["binding"]) =>
    workstreamBindingKey({ ...binding, registryVersion: 0 });
  if (started && (!current || authorityKey(started) !== authorityKey(current)))
    throw new WorkstreamActionError("stale");
  const expected = started ?? current;
  if (expected && authorityKey(expected) !== authorityKey(returned))
    throw new WorkstreamActionError("stale");
}

export async function loadCompleteWorkstreamReferences(
  load: (cursor?: string) => Promise<WorkstreamReferencePage>,
  options: CursorRestartOptions = {},
): Promise<WorkstreamReferencePage> {
  return withCursorRestart(() => loadAllPages(load, options.signal), options);
}

export async function retryWorkstreamCommands(input: {
  readonly load: () => Promise<unknown>;
  readonly commandIds: readonly string[];
  readonly observe: (commandId: string) => Promise<WorkstreamReceipt>;
}): Promise<void> {
  await input.load();
  for (const commandId of input.commandIds) await input.observe(commandId);
}

export interface NativePlacementInventory {
  readonly coverage: "complete" | "partial";
  readonly identities: readonly T3PlacementIdentity[];
  readonly json: string;
  readonly totalIdentities: number;
}

interface PlacementCandidate {
  readonly key: string;
  readonly identity: T3PlacementIdentity;
}

interface BindingOperationRequest {
  readonly controller: AbortController;
  readonly acceptedBindingKeys: Set<string>;
}

const addBoundedPlacementCandidate = (
  heap: PlacementCandidate[],
  candidate: PlacementCandidate,
): void => {
  const replacingMaximum = heap.length === T3_PLACEMENT_MAX_IDENTITIES;
  if (replacingMaximum && candidate.key >= heap[0]!.key) return;
  if (replacingMaximum) heap[0] = candidate;
  else heap.push(candidate);

  if (!replacingMaximum) {
    let index = heap.length - 1;
    while (index > 0) {
      const parent = Math.floor((index - 1) / 2);
      if (heap[parent]!.key >= heap[index]!.key) break;
      [heap[parent], heap[index]] = [heap[index]!, heap[parent]!];
      index = parent;
    }
    return;
  }

  let index = 0;
  while (true) {
    const left = index * 2 + 1;
    if (left >= heap.length) return;
    const right = left + 1;
    const child = right < heap.length && heap[right]!.key > heap[left]!.key ? right : left;
    if (heap[index]!.key >= heap[child]!.key) return;
    [heap[index], heap[child]] = [heap[child]!, heap[index]!];
    index = child;
  }
};

export function nativePlacementInventory(
  nativeThreads: readonly { readonly environmentId: string; readonly id: string }[],
): NativePlacementInventory {
  const identityKeys = new Set<string>();
  const candidates: PlacementCandidate[] = [];
  for (const thread of nativeThreads) {
    const key = JSON.stringify([thread.environmentId, thread.id]);
    if (identityKeys.has(key)) continue;
    identityKeys.add(key);
    addBoundedPlacementCandidate(candidates, {
      key,
      identity: {
        source_instance_id: thread.environmentId,
        native_thread_id: thread.id,
      },
    });
  }
  const totalIdentities = identityKeys.size;
  const selected: T3PlacementIdentity[] = [];
  const encoder = new TextEncoder();
  let requestBytes = encoder.encode(JSON.stringify({ identities: selected })).byteLength;
  candidates.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  for (const { identity: value } of candidates) {
    const nextBytes =
      requestBytes +
      encoder.encode(JSON.stringify(value)).byteLength +
      (selected.length > 0 ? 1 : 0);
    if (nextBytes > T3_PLACEMENT_MAX_REQUEST_BYTES) break;
    selected.push(value);
    requestBytes = nextBytes;
  }
  return {
    coverage: totalIdentities > selected.length ? "partial" : "complete",
    identities: selected,
    json: JSON.stringify(selected),
    totalIdentities,
  };
}

export function nativePlacementInventoryJson(
  nativeThreads: readonly { readonly environmentId: string; readonly id: string }[],
): string {
  return nativePlacementInventory(nativeThreads).json;
}

export function reuseNativePlacementIdentitySnapshot<
  Thread extends { readonly environmentId: string; readonly id: string },
>(previous: readonly Thread[], next: readonly Thread[]): readonly Thread[] {
  if (previous === next || previous.length !== next.length) return next;
  for (let index = 0; index < previous.length; index += 1) {
    const previousThread = previous[index]!;
    const nextThread = next[index]!;
    if (
      previousThread.environmentId !== nextThread.environmentId ||
      previousThread.id !== nextThread.id
    )
      return next;
  }
  return previous;
}

export function useWorkstreams(
  placementsEnabled = true,
  nativeThreads: readonly {
    readonly environmentId: string;
    readonly id: string;
  }[] = EMPTY_NATIVE_THREADS,
): WorkstreamListView {
  const nativeThreadsSnapshot = useRef(nativeThreads);
  nativeThreadsSnapshot.current = reuseNativePlacementIdentitySnapshot(
    nativeThreadsSnapshot.current,
    nativeThreads,
  );
  const stableNativeThreads = nativeThreadsSnapshot.current;
  const inventory = useMemo(
    () => nativePlacementInventory(placementsEnabled ? stableNativeThreads : EMPTY_NATIVE_THREADS),
    [placementsEnabled, stableNativeThreads],
  );
  const identities = useMemo(
    () => JSON.parse(inventory.json) as readonly T3PlacementIdentity[],
    [inventory.json],
  );
  const [placements, setPlacements] = useState<LiveT3Placements | null>(null);
  const [references, setReferences] = useState<WorkstreamReferencePage | null>(null);
  const [registrationContext, setRegistrationContext] =
    useState<WorkstreamsRegistrationContextResponse | null>(null);
  const [data, setData] = useState<T3WorkstreamListResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [revision, setRevision] = useState(0);
  const generation = useRef(0);
  const listRequest = useRef<AbortController | null>(null);
  const commandRequests = useRef(new Set<BindingOperationRequest>());
  const attemptedCommands = useRef(
    new Map<string, { readonly binding: string; receipt: WorkstreamReceipt | null }>(),
  );
  const currentBindingKey = data ? workstreamBindingKey(data.binding) : null;
  const currentBindingKeyRef = useRef(currentBindingKey);
  currentBindingKeyRef.current = currentBindingKey;
  const currentBindingRef = useRef(data?.binding);
  currentBindingRef.current = data?.binding;
  const refresh = useCallback(() => {
    listRequest.current?.abort();
    generation.current += 1;
    setPlacements(null);
    setRevision((value) => value + 1);
  }, []);

  const loadActionSnapshot = useCallback(
    async (options: { readonly signal?: AbortSignal } = {}): Promise<WorkstreamActionSnapshot> => {
      const signal = options.signal;
      const startedBinding = currentBindingRef.current;
      const value = await loadCompleteWorkstreamList(
        (cursor) =>
          request(
            (client) =>
              client.workstreams.list({
                headers: {},
                payload: { limit: 50, ...(cursor === undefined ? {} : { cursor }) },
              }),
            signal,
          ),
        options,
      );
      const [completeReferences, context, projection] = await Promise.all([
        loadCompleteWorkstreamReferences(
          (cursor) =>
            request(
              (client) =>
                client.workstreams.references({
                  headers: {},
                  payload: { limit: 50, ...(cursor === undefined ? {} : { cursor }) },
                }),
              signal,
            ),
          options,
        ),
        request((client) => client.workstreams.registrationContext({ headers: {} }), signal).catch(
          (cause: unknown) => {
            throwIfAborted(signal);
            void cause;
            return null;
          },
        ),
        placementsEnabled
          ? loadLiveT3Placements(value, identities, () =>
              request(
                (client) =>
                  client.workstreams.threadPlacements({ headers: {}, payload: { identities } }),
                signal,
              ),
            ).catch(() => {
              throwIfAborted(signal);
              return null;
            })
          : Promise.resolve(null),
      ]);
      throwIfAborted(signal);
      const snapshot = {
        data: value,
        references: completeReferences,
        registrationContext: context,
        placements: projection,
      };
      assertWorkstreamActionSnapshot(snapshot);
      const key = workstreamBindingKey(value.binding);
      assertWorkstreamSnapshotAuthority(startedBinding, currentBindingRef.current, value.binding);
      for (const operation of commandRequests.current) {
        operation.controller.signal.throwIfAborted();
        if (!operation.acceptedBindingKeys.has(key)) throw new WorkstreamActionError("stale");
      }
      metadataCache.write(value);
      currentBindingKeyRef.current = key;
      currentBindingRef.current = value.binding;
      setData(value);
      setReferences(completeReferences);
      setRegistrationContext(context);
      setPlacements(projection);
      setError(null);
      return snapshot;
    },
    [identities, placementsEnabled],
  );

  const observeCommand = useCallback(
    async (commandId: string, options: { readonly signal?: AbortSignal } = {}) => {
      const attempt = attemptedCommands.current.get(commandId);
      if (
        !attempt ||
        !data ||
        attempt.binding !== workstreamBindingKey({ ...data.binding, registryVersion: 0 })
      )
        throw new WorkstreamActionError("stale");
      const receipt = await request(
        (client) => client.workstreams.command({ headers: {}, params: { commandId } }),
        options.signal,
      );
      attempt.receipt = receipt;
      return receipt;
    },
    [data],
  );

  const retry = useCallback(async () => {
    await retryWorkstreamCommands({
      load: () => loadActionSnapshot(),
      commandIds: [...attemptedCommands.current]
        .filter(
          ([, attempt]) =>
            attempt.receipt === null ||
            attempt.receipt.state === "pending" ||
            attempt.receipt.state === "unresolved",
        )
        .map(([id]) => id),
      observe: (id) => observeCommand(id),
    });
    await loadActionSnapshot();
  }, [loadActionSnapshot, observeCommand]);

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

  useEffect(() => {
    listRequest.current?.abort();
    const controller = new AbortController();
    listRequest.current = controller;
    const current = ++generation.current;
    setPlacements(null);
    setReferences(null);
    setRegistrationContext(null);
    setLoading(true);
    void loadCompleteWorkstreamList(
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
    )
      .then(async (value) => {
        if (generation.current !== current) return;
        const normalized = { ...value, items: [...orderWorkstreamMetadata(value.items)] };
        metadataCache.write(normalized);
        setData(metadataCache.read(normalized.binding));
        setError(null);
        const [referencePage, sourceContext] = await Promise.all([
          loadCompleteWorkstreamReferences(
            (cursor) =>
              request(
                (client) =>
                  client.workstreams.references({
                    headers: {},
                    payload: { limit: 50, ...(cursor === undefined ? {} : { cursor }) },
                  }),
                controller.signal,
              ),
            { signal: controller.signal },
          ).catch(() => null),
          request(
            (client) => client.workstreams.registrationContext({ headers: {} }),
            controller.signal,
          ).catch(() => null),
        ]);
        if (generation.current !== current) return;
        if (referencePage) {
          try {
            assertWorkstreamReadContext(normalized, referencePage.context);
            setReferences(referencePage);
          } catch {
            setReferences(null);
          }
        }
        if (sourceContext) {
          try {
            assertWorkstreamActionSnapshot({
              data: normalized,
              references: referencePage ?? { context: sourceContext, items: [], next_cursor: null },
              placements: null,
              registrationContext: sourceContext,
            });
            setRegistrationContext(sourceContext);
          } catch {
            setRegistrationContext(null);
          }
        }
        if (placementsEnabled) {
          try {
            const projection = await loadLiveT3Placements(normalized, identities, () =>
              request(
                (client) =>
                  client.workstreams.threadPlacements({
                    headers: {},
                    payload: { identities },
                  }),
                controller.signal,
              ),
            );
            if (generation.current === current) setPlacements(projection);
          } catch {
            if (generation.current === current) setPlacements(null);
          }
        }
      })
      .catch((cause: unknown) => {
        if (generation.current !== current || controller.signal.aborted) return;
        // Authorization/session lifecycle failures must hide previously authorized content.
        metadataCache.purgeAuthorization();
        setData(null);
        setReferences(null);
        setRegistrationContext(null);
        setError(workstreamFailureMessage(cause));
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
  }, [revision, placementsEnabled, identities]);

  useEffect(() => {
    if (!placements || placements.items.length === 0) return;
    const expiry = Math.min(...placements.items.map((item) => Date.parse(item.expires_at)));
    const timer = window.setTimeout(
      () => setPlacements(null),
      Math.max(0, Math.min(2_147_483_647, expiry - Date.now())),
    );
    return () => window.clearTimeout(timer);
  }, [placements]);

  const runBindingOperation = useCallback(
    async <A>(
      operation: (submit: (command: WorkstreamCommand) => Promise<WorkstreamReceipt>) => Promise<A>,
    ) => {
      const startedBinding = data?.binding;
      if (startedBinding === undefined) throw new Error("Workstream binding is unavailable.");
      const authorityBinding = workstreamBindingKey({ ...startedBinding, registryVersion: 0 });
      if (
        [...attemptedCommands.current.values()].some(
          (attempt) =>
            attempt.binding === authorityBinding &&
            (attempt.receipt === null ||
              attempt.receipt.state === "pending" ||
              attempt.receipt.state === "unresolved"),
        )
      )
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
        if (!currentBindingIsAccepted() && !controller.signal.aborted) controller.abort();
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
        if (attemptedCommands.current.has(command.command_id))
          throw new WorkstreamActionError("unknown");
        const attempt = {
          binding: workstreamBindingKey({ ...startedBinding, registryVersion: 0 }),
          receipt: null as WorkstreamReceipt | null,
        };
        attemptedCommands.current.set(command.command_id, attempt);
        commandAttempted = true;
        const receipt = await request(
          (client) => client.workstreams.submit({ headers: {}, payload: { command } }),
          controller.signal,
        );
        attempt.receipt = receipt;
        acceptReceiptBinding(receipt);
        assertCurrentBinding();
        // Pending effects are retained for explicit GET-only Retry; commands are never resubmitted.
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
          setPlacements(null);
          refreshAfterOperation = commandAttempted;
        }
        throw cause;
      } finally {
        commandRequests.current.delete(operationRequest);
        if (refreshAfterOperation && !controller.signal.aborted && currentBindingIsAccepted())
          refresh();
      }
    },
    [data?.binding, refresh],
  );
  const submit = useCallback(
    (command: WorkstreamCommand) => runBindingOperation((submitCommand) => submitCommand(command)),
    [runBindingOperation],
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
        generation.current += 1;
        setPlacements(null);
        metadataCache.purgeAuthorization();
        setData(null);
        setError(workstreamFailureMessage(cause));
        throw cause;
      }
    },
    [],
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
        generation.current += 1;
        setPlacements(null);
        metadataCache.purgeAuthorization();
        setData(null);
        setError(workstreamFailureMessage(cause));
        throw cause;
      }
    },
    [],
  );

  return {
    placementInventory: inventory,
    placements,
    references,
    registrationContext,
    data,
    error,
    loading,
    refresh,
    retry,
    loadActionSnapshot,
    observeCommand,
    submit,
    runBindingOperation,
    loadDetail,
    loadReference,
  };
}
