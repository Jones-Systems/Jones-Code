import type {
  LibraryAccount,
  LibraryDetail,
  LibraryReply,
  LibraryRequest,
  LibrarySummary,
  LibraryView,
} from "@t3tools/contracts/conversationLibrary";

export interface LibraryReaderBinding {
  readonly environmentId: string;
  readonly generation: number;
}

export interface LibraryReaderFilter {
  readonly accountId: string | null;
  readonly query: string;
  readonly view: LibraryView;
}

type ListReply = Extract<LibraryReply, { readonly kind: "list" }>;
type HelloReply = Extract<LibraryReply, { readonly kind: "hello" }>;
type DetailRequest = Extract<LibraryRequest, { readonly kind: "detail" }>;
type UpdateRequest = Extract<LibraryRequest, { readonly kind: "update" }>;
type MutationRequest = Extract<
  LibraryRequest,
  { readonly kind: "update" | "selectSnapshot" | "remove" }
>;
type ReadRequest = Extract<
  LibraryRequest,
  { readonly kind: "hello" | "accounts" | "list" | "detail" }
>;
type Lane = ReadRequest["kind"] | "mutation";

export interface LibraryReaderPage<A> {
  readonly status: "idle" | "loading" | "ready" | "stale" | "error";
  readonly value: A | null;
  readonly error: string | null;
}

export interface LibraryReaderState {
  readonly binding: LibraryReaderBinding | null;
  readonly filter: LibraryReaderFilter;
  readonly canWrite: boolean;
  readonly revision: number;
  readonly accounts: LibraryReaderPage<readonly LibraryAccount[]>;
  readonly list: LibraryReaderPage<ListReply>;
  readonly detail: LibraryReaderPage<LibraryDetail>;
  readonly selection: DetailRequest | null;
  readonly displayed: boolean;
  readonly mutationPending: boolean;
  readonly error: string | null;
}

export interface LibraryReaderTicket<R extends LibraryRequest = LibraryRequest> {
  readonly binding: LibraryReaderBinding;
  readonly request: R;
}

const idle = <A>(): LibraryReaderPage<A> => ({ status: "idle", value: null, error: null });
const initialFilter: LibraryReaderFilter = { accountId: null, query: "", view: "all" };
const initialState = (binding: LibraryReaderBinding | null): LibraryReaderState => ({
  binding,
  filter: initialFilter,
  canWrite: false,
  revision: 0,
  accounts: idle(),
  list: idle(),
  detail: idle(),
  selection: null,
  displayed: false,
  mutationPending: false,
  error: null,
});

function stale<A>(page: LibraryReaderPage<A>): LibraryReaderPage<A> {
  return { status: page.value === null ? "idle" : "stale", value: page.value, error: null };
}

function sameBinding(a: LibraryReaderBinding | null, b: LibraryReaderBinding | null): boolean {
  return (
    a === b ||
    (a !== null &&
      b !== null &&
      a.environmentId === b.environmentId &&
      a.generation === b.generation)
  );
}

function detailMatches(
  request: DetailRequest,
  detail: LibraryDetail,
  accountId: string | null,
): boolean {
  return (
    detail.conversation.key === request.key &&
    detail.account.id === detail.conversation.accountId &&
    (accountId === null || detail.account.id === accountId) &&
    (request.snapshotId === undefined || request.snapshotId === detail.snapshotId) &&
    (request.nodeId === undefined || request.nodeId === detail.nodeId) &&
    (request.offset === undefined || request.offset === detail.offset) &&
    (request.snapshotOffset ?? 0) === detail.snapshotOffset &&
    (request.branchOffset ?? 0) === detail.branchOffset &&
    (request.showHidden ?? false) === detail.showHidden
  );
}

/**
 * Client-only presentation state. Transport adapters must decode replies first and
 * rebind whenever T3's environment connection generation changes. Tickets identify
 * pending work; they are not authorization and never execute a request themselves.
 */
export class ConversationLibraryReader {
  private state: LibraryReaderState = initialState(null);
  private readonly listeners = new Set<() => void>();
  private readonly pending = new Map<Lane, LibraryReaderTicket>();
  private appendList = false;

  readonly getSnapshot = (): LibraryReaderState => this.state;

  readonly subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  private publish(state: LibraryReaderState): void {
    this.state = state;
    for (const listener of this.listeners) listener();
  }

  bind(binding: LibraryReaderBinding | null): void {
    if (sameBinding(this.state.binding, binding)) return;
    this.pending.clear();
    this.appendList = false;
    this.publish(initialState(binding === null ? null : { ...binding }));
  }

  setFilter(filter: LibraryReaderFilter): void {
    const next = { ...filter, query: filter.query.trim() };
    const current = this.state.filter;
    if (
      current.accountId === next.accountId &&
      current.query === next.query &&
      current.view === next.view
    )
      return;
    this.pending.delete("list");
    this.pending.delete("detail");
    this.appendList = false;
    this.publish({
      ...this.state,
      filter: next,
      list: idle(),
      detail: idle(),
      selection: null,
      displayed: false,
      error: null,
    });
  }

  clearSelection(): void {
    this.pending.delete("detail");
    this.publish({ ...this.state, detail: idle(), selection: null, displayed: false });
  }

  private begin<R extends LibraryRequest>(lane: Lane, request: R): LibraryReaderTicket<R> | null {
    const binding = this.state.binding;
    if (binding === null) return null;
    const ticket = { binding, request };
    this.pending.set(lane, ticket);
    return ticket;
  }

  private owns(lane: Lane, ticket: LibraryReaderTicket): boolean {
    return this.pending.get(lane) === ticket && sameBinding(ticket.binding, this.state.binding);
  }

  /** Check immediately before dispatching through the matching T3 connection. */
  isPending(ticket: LibraryReaderTicket): boolean {
    return [...this.pending.keys()].some((lane) => this.owns(lane, ticket));
  }

  requestHello(): LibraryReaderTicket<Extract<LibraryRequest, { kind: "hello" }>> | null {
    return this.begin("hello", { kind: "hello" });
  }

  acceptHello(ticket: LibraryReaderTicket, reply: HelloReply): boolean {
    if (!this.owns("hello", ticket)) return false;
    this.pending.delete("hello");
    if (reply.revision < this.state.revision) {
      this.publish({
        ...this.state,
        canWrite: false,
        error: "The library capability reply is stale. Refresh the connection.",
      });
      return false;
    }
    const state = this.advanceRevision(reply.revision);
    this.publish({ ...state, canWrite: reply.canWrite, error: null });
    return true;
  }

  requestAccounts(): LibraryReaderTicket<Extract<LibraryRequest, { kind: "accounts" }>> | null {
    const ticket = this.begin("accounts", { kind: "accounts" });
    if (ticket)
      this.publish({
        ...this.state,
        accounts: { ...this.state.accounts, status: "loading", error: null },
      });
    return ticket;
  }

  acceptAccounts(ticket: LibraryReaderTicket, accounts: readonly LibraryAccount[]): boolean {
    if (!this.owns("accounts", ticket)) return false;
    this.pending.delete("accounts");
    this.publish({ ...this.state, accounts: { status: "ready", value: accounts, error: null } });
    return true;
  }

  requestList(
    append = false,
  ): LibraryReaderTicket<Extract<LibraryRequest, { kind: "list" }>> | null {
    const list = this.state.list;
    if (append && (list.status !== "ready" || list.value?.cursor == null)) return null;
    const { accountId, query, view } = this.state.filter;
    const ticket = this.begin("list", {
      kind: "list",
      ...(accountId === null ? {} : { accountId }),
      query,
      view,
      ...(append && list.value?.cursor ? { cursor: list.value.cursor } : {}),
    });
    if (ticket) {
      this.appendList = append;
      this.publish({ ...this.state, list: { ...list, status: "loading", error: null } });
    }
    return ticket;
  }

  acceptList(ticket: LibraryReaderTicket, reply: ListReply): boolean {
    if (!this.owns("list", ticket)) return false;
    const prior = this.state.list.value;
    if (
      reply.revision < this.state.revision ||
      (this.appendList && prior?.revision !== reply.revision)
    ) {
      this.reject(ticket, "The library changed. Refresh before loading another page.");
      return false;
    }
    const rows = this.appendList && prior ? [...prior.rows, ...reply.rows] : reply.rows;
    if (
      new Set(rows.map((row) => row.key)).size !== rows.length ||
      rows.some(
        (row) =>
          this.state.filter.accountId !== null && row.accountId !== this.state.filter.accountId,
      )
    ) {
      this.reject(ticket, "The library returned an inconsistent conversation page.");
      return false;
    }
    this.pending.delete("list");
    const state = this.advanceRevision(reply.revision);
    this.publish({ ...state, list: { status: "ready", value: { ...reply, rows }, error: null } });
    return true;
  }

  requestDetail(request: DetailRequest): LibraryReaderTicket<DetailRequest> | null {
    const ticket = this.begin("detail", { ...request });
    if (ticket)
      this.publish({
        ...this.state,
        detail: { status: "loading", value: null, error: null },
        selection: ticket.request,
        displayed: false,
      });
    return ticket;
  }

  acceptDetail(ticket: LibraryReaderTicket, reply: LibraryDetail): boolean {
    if (!this.owns("detail", ticket) || ticket.request.kind !== "detail") return false;
    if (!detailMatches(ticket.request, reply, this.state.filter.accountId)) {
      this.reject(
        ticket,
        "The library returned a different conversation, snapshot, branch, or page.",
      );
      return false;
    }
    this.pending.delete("detail");
    const state = this.advanceRevision(reply.conversation.revision);
    this.publish({
      ...state,
      detail: { status: "ready", value: reply, error: null },
      selection: libraryDetailPage(reply, {}),
      displayed: false,
    });
    return true;
  }

  /** Call after rendering this exact page, not on network receipt or prefetch. */
  acknowledgeDisplayed(detail: LibraryDetail): boolean {
    if (this.state.detail.status !== "ready" || this.state.detail.value !== detail) return false;
    if (!this.state.displayed) this.publish({ ...this.state, displayed: true });
    return true;
  }

  private advanceRevision(revision: number): LibraryReaderState {
    if (revision <= this.state.revision) return this.state;
    this.pending.delete("list");
    this.pending.delete("detail");
    return {
      ...this.state,
      revision,
      list: stale(this.state.list),
      detail: stale(this.state.detail),
      displayed: false,
    };
  }

  observeRevision(revision: number): void {
    const state = this.advanceRevision(revision);
    if (state !== this.state) this.publish(state);
  }

  private mutate(request: MutationRequest): LibraryReaderTicket<MutationRequest> | null {
    const detail = this.state.detail;
    if (
      !this.state.canWrite ||
      this.state.mutationPending ||
      detail.status !== "ready" ||
      detail.value?.conversation.key !== request.key
    )
      return null;
    const ticket = this.begin("mutation", request);
    if (ticket) this.publish({ ...this.state, mutationPending: true, error: null });
    return ticket;
  }

  requestFlags(
    flags: Pick<UpdateRequest, "pinned" | "archived" | "attention">,
  ): LibraryReaderTicket<MutationRequest> | null {
    const conversation = this.state.detail.value?.conversation;
    if (
      !conversation ||
      (flags.pinned === undefined && flags.archived === undefined && flags.attention === undefined)
    )
      return null;
    return this.mutate({
      kind: "update",
      key: conversation.key,
      ...(flags.pinned === undefined ? {} : { pinned: flags.pinned }),
      ...(flags.archived === undefined ? {} : { archived: flags.archived }),
      ...(flags.attention === undefined ? {} : { attention: flags.attention }),
    });
  }

  requestMarkRead(): LibraryReaderTicket<MutationRequest> | null {
    const detail = this.state.detail.value;
    if (
      !detail ||
      !this.state.displayed ||
      !detail.conversation.unread ||
      detail.conversation.conflicts ||
      detail.snapshotId !== detail.conversation.snapshotId ||
      detail.nodeId === null ||
      detail.messages.length === 0 ||
      detail.nextOffset !== null ||
      detail.offset + detail.messages.length !== detail.totalMessages ||
      detail.readThrough <= 0 ||
      detail.readThrough !== detail.conversation.revision
    )
      return null;
    return this.mutate({
      kind: "update",
      key: detail.conversation.key,
      readThrough: detail.readThrough,
    });
  }

  requestSelectSnapshot(snapshotId: string): LibraryReaderTicket<MutationRequest> | null {
    const conversation = this.state.detail.value?.conversation;
    return conversation
      ? this.mutate({
          kind: "selectSnapshot",
          key: conversation.key,
          snapshotId,
          expectedRevision: conversation.revision,
        })
      : null;
  }

  requestRemoveLocalCopy(): LibraryReaderTicket<MutationRequest> | null {
    const conversation = this.state.detail.value?.conversation;
    return conversation
      ? this.mutate({
          kind: "remove",
          key: conversation.key,
          expectedRevision: conversation.revision,
        })
      : null;
  }

  acceptMutation(
    ticket: LibraryReaderTicket<MutationRequest>,
    reply: Extract<LibraryReply, { kind: "updated" | "removed" }>,
  ): boolean {
    if (!this.owns("mutation", ticket)) return false;
    const expected = ticket.request.kind === "remove" ? "removed" : "updated";
    if (reply.kind !== expected) {
      this.reject(
        ticket,
        "The library mutation response is inconsistent. Refresh to reconcile its outcome.",
      );
      return false;
    }
    this.pending.delete("mutation");
    this.pending.delete("list");
    this.pending.delete("detail");
    const removedSelected =
      ticket.request.kind === "remove" && this.state.selection?.key === ticket.request.key;
    this.publish({
      ...this.state,
      revision: Math.max(this.state.revision, reply.revision),
      mutationPending: false,
      list: stale(this.state.list),
      detail: removedSelected ? idle() : stale(this.state.detail),
      selection: removedSelected ? null : this.state.selection,
      displayed: false,
    });
    return true;
  }

  /** A failed write is not retried here: its outcome may be unknown to the client. */
  reject(ticket: LibraryReaderTicket, message: string): boolean {
    const lane = [...this.pending].find(([, pending]) => pending === ticket)?.[0];
    if (lane === undefined || !this.owns(lane, ticket)) return false;
    this.pending.delete(lane);
    if (lane === "hello") this.publish({ ...this.state, canWrite: false, error: message });
    else if (lane === "mutation") {
      this.pending.delete("list");
      this.pending.delete("detail");
      this.publish({
        ...this.state,
        mutationPending: false,
        list: stale(this.state.list),
        detail: stale(this.state.detail),
        displayed: false,
        error: message,
      });
    } else if (lane === "accounts") {
      this.publish({
        ...this.state,
        accounts: { ...this.state.accounts, status: "error", error: message },
      });
    } else if (lane === "list") {
      this.publish({
        ...this.state,
        list: { ...this.state.list, status: "error", error: message },
      });
    } else
      this.publish({
        ...this.state,
        detail: { status: "error", value: null, error: message },
        displayed: false,
      });
    return true;
  }
}

/** Keep every page tied to the snapshot and branch actually being read. */
export function libraryDetailPage(
  detail: LibraryDetail,
  page: Partial<Pick<DetailRequest, "offset" | "snapshotOffset" | "branchOffset" | "showHidden">>,
): DetailRequest {
  return {
    kind: "detail",
    key: detail.conversation.key,
    snapshotId: detail.snapshotId,
    ...(detail.nodeId === null ? {} : { nodeId: detail.nodeId }),
    offset: page.offset ?? detail.offset,
    snapshotOffset: page.snapshotOffset ?? detail.snapshotOffset,
    branchOffset: page.branchOffset ?? detail.branchOffset,
    showHidden: page.showHidden ?? detail.showHidden,
  };
}

/** Conversation IDs are source-local; never use one alone as a rendered row key. */
export function libraryReaderRowKey(binding: LibraryReaderBinding, row: LibrarySummary): string {
  return JSON.stringify([binding.environmentId, binding.generation, row.accountId, row.key]);
}
