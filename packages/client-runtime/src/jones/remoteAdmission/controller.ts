import type { RemoteAdmissionDecision, RemoteAdmissionRecord } from "@t3tools/contracts";

export type Duration = 3600 | 86400 | 172800 | 259200 | 604800;
export type View = "current" | "blocked" | "done" | "unresolved" | "inflight" | "access";
export interface Counts {
  currentHeldAdmissions: number;
  blockedHeldAdmissions: number;
  heldAdmissions: number;
  inFlightAdmissions: number;
  failedAdmissions: number;
  unresolvedAdmissions: number;
  activeAccessScopes: number;
  activeThreads: number;
}
export interface Access {
  id: string;
  generation: number;
  state: "pending" | "active" | "revoked";
  title: string;
  origin: string;
  destinationThreadId: string | null;
  expiresAt: string;
}
export interface Item {
  record: RemoteAdmissionRecord;
  attempt: string;
  blocked: boolean;
  accessId: string | null;
  grantEligible: boolean;
  outcome: "awaiting" | "created" | "failed" | "unknown" | "not_created" | null;
  availability: "available" | "unknown" | "deleted";
  threadId: string | null;
  messageStage: string | null;
  reason: string | null;
  canRetry: boolean;
  canRecheck: boolean;
}
export interface Page {
  revision: string;
  policyRevision: string;
  counts: Counts;
  records: readonly Item[];
  access: readonly Access[];
  nextCursor: string | null;
  recordCount: number;
  diagnostics?: readonly {
    id: string;
    reason: string;
    repeats: number;
    requestId: string | null;
  }[];
  diagnosticCounts?: { logicalEvents: number; repeatArrivals: number; summaryRows: number };
}
export interface Capabilities {
  canDecide: boolean;
  features: ReadonlySet<
    | "reviewV2"
    | "grants"
    | "conflicts"
    | "changeFeed"
    | "grantRestore"
    | "blockedAudit"
    | "blockedReview"
    | "history"
    | "launchRetry"
    | "recoveredResult"
    | "managedAccess"
  >;
}
export type Mutation =
  | { action: "decide"; input: RemoteAdmissionDecision & { grant?: { durationSeconds: Duration } } }
  | {
      action: "update" | "revoke" | "restore";
      id: string;
      expectedGeneration: number;
      durationSeconds?: Duration;
      commandId: string;
    }
  | {
      action: "retry" | "recheck";
      id: string;
      bindingDigest: string;
      commandId: string;
      durationSeconds?: Duration;
    };
export interface ChangeBatch {
  cursor: string;
  revision: string;
  results: readonly Item[];
}
/** Adapter over canonical owner HTTP contracts; no parallel wire schema or authority cache. */
export interface ReviewClient {
  capabilities(): Promise<Capabilities>;
  page(view: View, cursor?: string): Promise<Page>;
  lookup(id: string, bindingDigest?: string): Promise<Item>;
  lookupAccess(id: string): Promise<Access>;
  lookupRestore?(predecessorId: string, originalCommandId: string): Promise<Access>;
  mutate(input: Mutation): Promise<{ item?: Item; access?: Access }>;
  head(): Promise<string>;
  changes(after: string, signal: AbortSignal): Promise<ChangeBatch>;
}
export const bindingKey = (item: Pick<RemoteAdmissionRecord, "id" | "bindingDigest">) =>
  JSON.stringify([item.id, item.bindingDigest]);
export const attentionKey = (receiver: string, item: Item) =>
  JSON.stringify([receiver, item.record.id, item.record.bindingDigest, item.attempt]);
export type GroupView = "access" | "inflight" | "unresolved";
export interface ReviewGroup {
  page: Page | null;
  loading: boolean;
  error: string | null;
}
export interface AttentionSnapshot {
  cursor: string | null;
  results: readonly Item[];
  uncertainAccess?: readonly {
    id: string;
    action: string;
    generation: number;
    restoreCommandId?: string;
  }[];
}
export interface AttentionPersistence {
  read(): AttentionSnapshot | null;
  write(value: AttentionSnapshot): void;
}
export interface ReviewState {
  capabilities: Capabilities | null;
  groups: Record<GroupView, ReviewGroup>;
  view: View;
  page: Page | null;
  loading: boolean;
  error: string | null;
  pending: ReadonlySet<string>;
  uncertain: ReadonlySet<string>;
  results: readonly Item[];
  policyBarrier: boolean;
  loadFailed: boolean;
  incoming: readonly Item[];
  currentHeldCount: number;
  selectedItem: Item | null;
  selectedAccess: Access | null;
  selectedAccessFromId: string | null;
  accessSelectionRevision: number;
}
export function createReviewController(client: ReviewClient) {
  let state: ReviewState = {
    capabilities: null,
    groups: {
      access: { page: null, loading: false, error: null },
      inflight: { page: null, loading: false, error: null },
      unresolved: { page: null, loading: false, error: null },
    },
    view: "current",
    page: null,
    loading: false,
    error: null,
    pending: new Set(),
    uncertain: new Set(),
    results: [],
    policyBarrier: false,
    loadFailed: false,
    incoming: [],
    currentHeldCount: 0,
    selectedItem: null,
    selectedAccess: null,
    selectedAccessFromId: null,
    accessSelectionRevision: 0,
  };
  let readOrdinal = 0;
  let mutationEpoch = 0;
  let policyWriting = false;
  let cursor: string | null = null;
  let persistence: AttentionPersistence | null = null;
  const unknownAccess = new Map<
    string,
    { action: string; generation: number; restoreCommandId?: string }
  >();
  const conflictedAccess = new Set<string>();
  let accessReadOrdinal = 0;
  const groupOrdinals = { access: 0, inflight: 0, unresolved: 0 };
  let feed: AbortController | null = null;
  const committed = new Map<string, Item>();
  const listeners = new Set<() => void>();
  const publish = (patch: Partial<ReviewState>) => {
    state = { ...state, ...patch };
    for (const notify of listeners) notify();
  };
  const persistAttention = () =>
    persistence?.write({
      cursor,
      results: state.results,
      uncertainAccess: [...unknownAccess].map(([id, value]) => ({ id, ...value })),
    });
  const acceptItem = (item: Item) => {
    const uncertain = new Set(state.uncertain);
    if (item.record.status !== "held") uncertain.delete(bindingKey(item.record));
    const page = state.page;
    publish({
      uncertain,
      ...(state.selectedItem && bindingKey(state.selectedItem.record) === bindingKey(item.record)
        ? { selectedItem: item }
        : {}),
      incoming: state.incoming.map((old) =>
        bindingKey(old.record) === bindingKey(item.record) ? item : old,
      ),
      ...(page
        ? {
            page: {
              ...page,
              records: page.records.map((old) =>
                bindingKey(old.record) === bindingKey(item.record) ? item : old,
              ),
            },
          }
        : {}),
    });
  };
  const acceptAccess = (access: Access, fromId = access.id) => {
    const replace = (entries: readonly Access[]) =>
      entries.map((entry) => (entry.id === access.id || entry.id === fromId ? access : entry));
    const accessPage = state.groups.access.page;
    publish({
      selectedAccess: access,
      selectedAccessFromId: fromId,
      accessSelectionRevision: state.accessSelectionRevision + 1,
      groups: {
        ...state.groups,
        access: {
          ...state.groups.access,
          ...(accessPage ? { page: { ...accessPage, access: replace(accessPage.access) } } : {}),
        },
      },
      ...(state.page
        ? {
            page: {
              ...state.page,
              access:
                state.view === "access" &&
                !state.page.access.some((entry) => entry.id === access.id || entry.id === fromId)
                  ? [...state.page.access, access]
                  : replace(state.page.access),
            },
          }
        : {}),
    });
  };
  const lookupAccess = async (id: string) => {
    const ordinal = ++accessReadOrdinal;
    const epoch = mutationEpoch;
    const unknown = unknownAccess.get(id);
    const restoreReadback =
      unknown?.action === "restore" && unknown.restoreCommandId && client.lookupRestore;
    const access = restoreReadback
      ? await client.lookupRestore!(id, unknown!.restoreCommandId!)
      : await client.lookupAccess(id);
    if (!restoreReadback && access.id !== id) throw new Error("Access binding mismatch");
    if (restoreReadback && access.id === id) throw new Error("Restore successor unavailable");
    if (ordinal !== accessReadOrdinal || epoch !== mutationEpoch) return;
    acceptAccess(access, id);
    // Reading an original command's successor observes its current state; it never restores again.
    if (
      restoreReadback ||
      conflictedAccess.has(id) ||
      (unknown && unknown.action !== "restore" && access.generation > unknown.generation)
    ) {
      conflictedAccess.delete(id);
      unknownAccess.delete(id);
      const uncertain = new Set(state.uncertain);
      uncertain.delete(`access:${id}`);
      publish({ uncertain });
      persistAttention();
    }
  };
  const loadGroup = async (view: GroupView, older = false) => {
    if (
      !state.capabilities?.canDecide ||
      state.pending.size > 0 ||
      policyWriting ||
      state.groups[view].loading
    )
      return;
    const ordinal = ++groupOrdinals[view];
    const epoch = mutationEpoch;
    const previous = state.groups[view].page;
    if (older && !previous?.nextCursor) return;
    const update = (patch: Partial<ReviewGroup>) =>
      publish({ groups: { ...state.groups, [view]: { ...state.groups[view], ...patch } } });
    update({ loading: true, error: null });
    try {
      const page = await client.page(view, older ? previous!.nextCursor! : undefined);
      if (ordinal !== groupOrdinals[view] || !state.capabilities?.canDecide) return;
      if (epoch !== mutationEpoch) {
        update({ error: "State changed while loading this group. Refresh for current evidence." });
        return;
      }
      if (older && previous && previous.revision !== page.revision) {
        update({ error: "This group changed. Refresh it before loading more." });
        return;
      }
      update({
        page:
          older && previous
            ? {
                ...page,
                records: [
                  ...previous.records,
                  ...page.records.filter(
                    (item) =>
                      !previous.records.some(
                        (old) => bindingKey(old.record) === bindingKey(item.record),
                      ),
                  ),
                ],
                access: [
                  ...previous.access,
                  ...page.access.filter(
                    (entry) => !previous.access.some((old) => old.id === entry.id),
                  ),
                ],
              }
            : page,
      });
    } catch {
      if (ordinal === groupOrdinals[view])
        update({ error: "This group is unavailable. Refresh for current evidence." });
    } finally {
      if (ordinal === groupOrdinals[view]) update({ loading: false });
    }
  };
  const load = async (view = state.view, older = false) => {
    if (state.pending.size > 0 || policyWriting) return;
    const ordinal = ++readOrdinal;
    const epoch = mutationEpoch;
    const previous = state.page;
    publish({ loading: true, error: null, view });
    try {
      const capabilities = await client.capabilities();
      publish({ capabilities });
      if (!capabilities.canDecide) {
        publish({
          groups: {
            access: { page: null, loading: false, error: null },
            inflight: { page: null, loading: false, error: null },
            unresolved: { page: null, loading: false, error: null },
          },
          page: null,
          incoming: [],
          loadFailed: true,
          error: "Owner access is required to review requests.",
        });
        return;
      }
      const page = await client.page(
        view,
        older && previous?.nextCursor ? previous.nextCursor : undefined,
      );
      if (ordinal !== readOrdinal || epoch !== mutationEpoch) return;
      if (older && previous && previous.revision !== page.revision) {
        publish({ error: "Request history changed. Refresh to load a consistent page." });
        return;
      }
      const observed = page.records.map((item) => {
        const prior = committed.get(bindingKey(item.record));
        return prior &&
          item.record.status === "held" &&
          !item.blocked &&
          item.record.updatedAt <= prior.record.updatedAt
          ? prior
          : item;
      });
      const uncertain = new Set(state.uncertain);
      for (const item of observed)
        if (item.record.status !== "held") uncertain.delete(bindingKey(item.record));
      const records =
        older && previous
          ? [
              ...previous.records,
              ...observed.filter(
                (item) =>
                  !previous.records.some(
                    (old) => bindingKey(old.record) === bindingKey(item.record),
                  ),
              ),
            ]
          : [...observed];
      const refreshedAccess = page.access.find((entry) => entry.id === state.selectedAccess?.id);
      if (
        refreshedAccess &&
        state.selectedAccess &&
        (refreshedAccess.generation > state.selectedAccess.generation ||
          refreshedAccess.state !== state.selectedAccess.state)
      )
        acceptAccess(refreshedAccess);
      publish({
        page: { ...page, records },
        uncertain,
        loadFailed: false,
        ...(view === "current"
          ? {
              incoming: observed.filter((item) => !item.blocked && item.record.status === "held"),
              currentHeldCount: page.counts.currentHeldAdmissions,
            }
          : {}),
      });
      if (view === "current" && !older)
        await Promise.all(
          (["access", "inflight", "unresolved"] as const).map((group) => loadGroup(group)),
        );
    } catch {
      if (ordinal === readOrdinal)
        publish({
          loadFailed: true,
          error: "Request review is unavailable for this session. Refresh server state.",
        });
    } finally {
      if (ordinal === readOrdinal) publish({ loading: false });
    }
  };
  const mutate = async (input: Mutation) => {
    const key =
      input.action === "decide"
        ? bindingKey(input.input)
        : input.action === "retry" || input.action === "recheck"
          ? bindingKey(input)
          : `access:${input.id}`;
    if (
      !state.capabilities?.canDecide ||
      state.loadFailed ||
      state.policyBarrier ||
      state.pending.has(key) ||
      state.uncertain.has(key)
    )
      return;
    if (input.action === "decide") {
      const item = [
        ...state.incoming,
        ...(state.page?.records ?? []),
        ...(state.selectedItem ? [state.selectedItem] : []),
      ].find((item) => bindingKey(item.record) === key);
      if (!item || item.record.status !== "held" || state.loadFailed) return;
      if (
        input.input.decision === "approve" &&
        item.record.envelope.receiverPolicy.revision !== state.page?.policyRevision
      )
        return;
      if (input.input.grant && (!item.grantEligible || !state.capabilities.features.has("grants")))
        return;
    }
    const required =
      input.action === "restore"
        ? "grantRestore"
        : input.action === "retry"
          ? "launchRetry"
          : input.action === "recheck"
            ? "recoveredResult"
            : input.action === "update" || input.action === "revoke"
              ? "grants"
              : "reviewV2";
    if (!state.capabilities.features.has(required)) return;
    ++mutationEpoch;
    publish({ pending: new Set([...state.pending, key]), error: null });
    if (input.action === "restore") {
      unknownAccess.set(input.id, {
        action: "restore",
        generation: input.expectedGeneration,
        restoreCommandId: input.commandId,
      });
      persistAttention();
    }
    try {
      const result = await client.mutate(input);
      if (result.item) {
        committed.set(bindingKey(result.item.record), result.item);
        acceptItem(result.item);
      }
      if (result.access) {
        acceptAccess(result.access, input.action === "decide" ? result.access.id : input.id);
        if (input.action === "restore") {
          unknownAccess.delete(input.id);
          persistAttention();
          const uncertain = new Set(state.uncertain);
          uncertain.delete(key);
          publish({ uncertain });
        }
      }
    } catch (error) {
      if (
        typeof error === "object" &&
        error !== null &&
        "code" in error &&
        error.code === "conflict"
      ) {
        publish({
          uncertain: new Set([...state.uncertain, key]),
          error: "State changed elsewhere. Review the exact current binding before deciding.",
        });
        if (input.action === "update" || input.action === "revoke" || input.action === "restore") {
          unknownAccess.delete(input.id);
          persistAttention();
          conflictedAccess.add(input.id);
          publish({ accessSelectionRevision: state.accessSelectionRevision + 1 });
          const conflict = error as {
            current?: { grant?: Access | null; chainHead?: Access | null };
          };
          const current = conflict.current?.chainHead ?? conflict.current?.grant;
          if (current) {
            acceptAccess(current, input.id);
            conflictedAccess.delete(input.id);
            const uncertain = new Set(state.uncertain);
            uncertain.delete(key);
            publish({ uncertain });
          } else {
            await lookupAccess(input.id).catch(() => undefined);
          }
        }
        if (input.action === "decide") {
          const current = await client
            .lookup(input.input.id, input.input.bindingDigest)
            .catch(() => null);
          if (current && bindingKey(current.record) === key) acceptItem(current);
        }
      } else {
        if (input.action === "update" || input.action === "revoke" || input.action === "restore")
          unknownAccess.set(input.id, {
            action: input.action,
            generation: input.expectedGeneration,
            ...(input.action === "restore" ? { restoreCommandId: input.commandId } : {}),
          });
        persistAttention();
        // Neither omission nor a held snapshot proves a lost mutation had no effect.
        publish({
          uncertain: new Set([...state.uncertain, key]),
          error: "Outcome unknown. Do not repeat this action; refresh its exact binding.",
        });
      }
    } finally {
      const pending = new Set(state.pending);
      pending.delete(key);
      publish({ pending });
    }
    await load();
  };
  return {
    getSnapshot: () => state,
    subscribe: (notify: () => void) => {
      listeners.add(notify);
      return () => {
        listeners.delete(notify);
      };
    },
    load,
    loadGroup,
    mutate,
    lookupAccess,
    setAttentionPersistence(value: AttentionPersistence) {
      if (persistence) return;
      persistence = value;
      const retained = value.read();
      if (retained) {
        cursor = retained.cursor;
        for (const entry of retained.uncertainAccess ?? [])
          unknownAccess.set(entry.id, {
            action: entry.action,
            generation: entry.generation,
            ...(entry.restoreCommandId ? { restoreCommandId: entry.restoreCommandId } : {}),
          });
        publish({
          results: retained.results,
          uncertain: new Set([
            ...state.uncertain,
            ...[...unknownAccess.keys()].map((id) => `access:${id}`),
          ]),
        });
      }
    },
    async lookup(id: string, digest?: string) {
      const epoch = mutationEpoch;
      const item = await client.lookup(id, digest);
      if (item.record.id !== id || (digest !== undefined && item.record.bindingDigest !== digest))
        throw new Error("Request binding mismatch");
      if (epoch !== mutationEpoch) return;
      publish({ selectedItem: item });
      acceptItem(item);
    },
    async withPolicyBarrier(write: () => Promise<void>) {
      if (policyWriting || state.pending.size > 0 || state.uncertain.size > 0) return;
      policyWriting = true;
      publish({ policyBarrier: true });
      ++mutationEpoch;
      try {
        await write();
      } finally {
        policyWriting = false;
        publish({ policyBarrier: false });
        await load();
      }
    },
    async watchResults() {
      if (feed) return;
      const owner = new AbortController();
      feed = owner;
      try {
        const capabilities = await client.capabilities();
        if (owner.signal.aborted) return;
        publish({ capabilities });
        if (!capabilities.canDecide || !capabilities.features.has("changeFeed")) return;
        // First admission starts at head. Retained history never creates a toast.
        if (cursor === null) {
          const head = await client.head();
          if (owner.signal.aborted) return;
          cursor = head;
        }
        persistAttention();
        while (!owner.signal.aborted) {
          const batch = await client.changes(cursor, owner.signal);
          if (owner.signal.aborted) break;
          cursor = batch.cursor;
          const results = new Map(
            state.results.map((item) => [
              JSON.stringify([bindingKey(item.record), item.attempt]),
              item,
            ]),
          );
          for (const item of batch.results)
            if (item.outcome && item.outcome !== "awaiting")
              results.set(JSON.stringify([bindingKey(item.record), item.attempt]), item);
          publish({ results: [...results.values()] });
          persistAttention();
          await load();
          if (state.view !== "current" && state.pending.size === 0) {
            const attention = await client.page("current");
            if (!owner.signal.aborted)
              publish({
                incoming: attention.records.filter(
                  (item) => !item.blocked && item.record.status === "held",
                ),
                currentHeldCount: attention.counts.currentHeldAdmissions,
              });
          }
        }
      } catch {
        if (!owner.signal.aborted)
          publish({
            error:
              "Receiver result updates are unavailable. Refresh Requests for current evidence.",
          });
      } finally {
        if (feed === owner) feed = null;
      }
    },
    stopResults() {
      const owner = feed;
      feed = null;
      owner?.abort();
    },
  };
}

/** Pin membership and order until BOTH pointer and keyboard focus leave the action area. */
export function createActionFreeze<T>(key: (item: T) => string) {
  let pointer = false;
  let focus = false;
  let shown: readonly T[] = [];
  return {
    pointer(value: boolean) {
      pointer = value;
    },
    focus(value: boolean) {
      focus = value;
    },
    reconcile(next: readonly T[]) {
      if (!pointer && !focus) shown = next;
      else {
        const current = new Map(next.map((item) => [key(item), item]));
        shown = shown.map((item) => current.get(key(item)) ?? item);
      }
      return shown;
    },
  };
}
