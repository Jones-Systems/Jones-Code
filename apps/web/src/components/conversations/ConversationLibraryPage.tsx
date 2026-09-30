import { useAtomCommand } from "~/state/use-atom-command";
import { useEnvironmentQuery } from "~/state/query";
import { environmentCatalog } from "~/connection/catalog";
import { useEnvironments, usePrimaryEnvironmentId } from "~/state/environments";
import { usePreparedConnection } from "~/state/session";
import {
  ConversationLibraryImport,
  ConversationLibraryReader,
  libraryDetailPage,
  readConversationExportFile,
  type ConversationImportTicket,
  type LibraryReaderBinding,
  type LibraryReaderTicket,
} from "@t3tools/client-runtime/conversations";
import { EnvironmentId } from "@t3tools/contracts";
import type {
  LibraryAccount,
  LibraryDetail,
  LibraryReply,
  LibraryRequest,
  LibrarySummary,
  LibraryView,
} from "@t3tools/contracts/conversationLibrary";
import {
  conversationLibraryErrorMessage,
  conversationLibraryReplyMatchesRequest,
  requestConversationLibrary,
} from "~/state/conversations";
import {
  acknowledgeVisibleConversationDetail,
  conversationLibraryDate,
  conversationLibraryImportTargetAccount,
  conversationLibrarySnapshotRequest,
  createConversationLibraryAccountRequest,
} from "./ConversationLibraryPage.logic";
import * as Cause from "effect/Cause";
import * as Option from "effect/Option";
import {
  AlertCircleIcon,
  ArchiveIcon,
  ArrowDownIcon,
  ArrowLeftIcon,
  ArrowUpIcon,
  FileUpIcon,
  FlagIcon,
  PinIcon,
  RefreshCwIcon,
  Trash2Icon,
} from "lucide-react";
import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";

import { WorkspacePageContainer } from "../WorkspacePageContainer";
import { WorkspacePageHeader } from "../WorkspacePageHeader";
import {
  AlertDialog,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogPopup,
  AlertDialogTitle,
} from "../ui/alert-dialog";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { SidebarInset } from "../ui/sidebar";
import {
  Dialog,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogPopup,
  DialogTitle,
} from "../ui/dialog";
import { isElectron } from "~/env";

const VIEWS: ReadonlyArray<{ readonly id: LibraryView; readonly label: string }> = [
  { id: "all", label: "All" },
  { id: "unread", label: "Unread" },
  { id: "pinned", label: "Pinned" },
  { id: "archived", label: "Archived" },
  { id: "attention", label: "Needs attention" },
];

function bindingKey(binding: LibraryReaderBinding | null): string | null {
  return binding === null ? null : `${binding.environmentId}:${binding.generation}`;
}

function formatDate(value: number | null): string {
  return conversationLibraryDate(value)?.toLocaleString() ?? "Date unknown";
}

function BindingIdentity({
  environmentName,
  account,
}: {
  readonly environmentName: string;
  readonly account: LibraryAccount | null;
}) {
  return (
    <p className="text-xs text-muted-foreground">
      Environment: <span className="font-medium text-foreground">{environmentName}</span>
      {account ? (
        <>
          {" "}
          · Account: <span className="font-medium text-foreground">{account.label}</span>
        </>
      ) : null}
    </p>
  );
}

export function ConversationLibraryPage() {
  const { environments, isReady: environmentsReady } = useEnvironments();
  const primaryEnvironmentId = usePrimaryEnvironmentId();
  const [requestedEnvironmentId, setRequestedEnvironmentId] = useState(primaryEnvironmentId);
  const [importOpen, setImportOpen] = useState(false);
  const [createAccountOpen, setCreateAccountOpen] = useState(false);
  const [createAccountBinding, setCreateAccountBinding] = useState<string | null>(null);
  const [createAccountLabel, setCreateAccountLabel] = useState("");
  const [createAccountWorkspace, setCreateAccountWorkspace] = useState("");
  const [createAccountPending, setCreateAccountPending] = useState(false);
  const [createAccountError, setCreateAccountError] = useState<string | null>(null);
  const [removeOpen, setRemoveOpen] = useState(false);
  const [smallScreenDetailRequested, setSmallScreenDetailRequested] = useState(false);
  const helloStartedFor = useRef<string | null>(null);
  const preferredAccountId = useRef<string | null>(null);
  const detailPanelRef = useRef<HTMLDivElement>(null);
  const reader = useMemo(() => new ConversationLibraryReader(), []);
  const importer = useMemo(() => new ConversationLibraryImport(), []);
  const state = useSyncExternalStore(reader.subscribe, reader.getSnapshot, reader.getSnapshot);
  const importState = useSyncExternalStore(
    importer.subscribe,
    importer.getSnapshot,
    importer.getSnapshot,
  );
  const runRequest = useAtomCommand(requestConversationLibrary, {
    reportFailure: false,
    reportDefect: false,
  });

  const selectedEnvironmentId =
    requestedEnvironmentId !== null &&
    environments.some((item) => item.environmentId === requestedEnvironmentId)
      ? requestedEnvironmentId
      : (primaryEnvironmentId ?? environments[0]?.environmentId ?? null);

  const selectedEnvironment =
    environments.find((item) => item.environmentId === selectedEnvironmentId) ?? null;
  const connection = useEnvironmentQuery(
    selectedEnvironmentId === null ? null : environmentCatalog.stateAtom(selectedEnvironmentId),
  );
  const prepared = usePreparedConnection(selectedEnvironmentId);
  const binding = useMemo(
    (): LibraryReaderBinding | null =>
      selectedEnvironmentId === null
        ? null
        : { environmentId: selectedEnvironmentId, generation: connection.data?.generation ?? 0 },
    [connection.data?.generation, selectedEnvironmentId],
  );
  const currentBindingKey = bindingKey(binding);
  const readerBindingKey = bindingKey(state.binding);
  const connected = connection.data?.phase === "connected" && Option.isSome(prepared);
  const smallScreenDetail = state.selection !== null && smallScreenDetailRequested;
  const stateIsBound = currentBindingKey !== null && readerBindingKey === currentBindingKey;
  const libraryHandshakeComplete = stateIsBound && state.accounts.status !== "idle";
  const accountDialogOpen =
    createAccountOpen && createAccountBinding === currentBindingKey && connected && state.canWrite;
  const environmentName = selectedEnvironment?.label ?? "No environment selected";
  const detail = state.detail.value;
  const selectedAccount =
    detail?.account ??
    state.accounts.value?.find((account) => account.id === state.filter.accountId) ??
    null;

  useLayoutEffect(() => {
    reader.bind(binding);
    importer.bind(binding, false);
    helloStartedFor.current = null;
    preferredAccountId.current = null;
    setImportOpen(false);
    setCreateAccountOpen(false);
    setCreateAccountPending(false);
    setRemoveOpen(false);
    setSmallScreenDetailRequested(false);
  }, [binding, importer, reader]);

  useLayoutEffect(() => {
    if (readerBindingKey === currentBindingKey) importer.bind(state.binding, state.canWrite);
  }, [currentBindingKey, importer, readerBindingKey, state.binding, state.canWrite]);

  const executeReaderTicket = useCallback(
    async function executeReaderTicket(ticket: LibraryReaderTicket) {
      if (!reader.isPending(ticket)) return;
      const result = await runRequest({
        environmentId: EnvironmentId.make(ticket.binding.environmentId),
        input: ticket.request,
      });
      if (!reader.isPending(ticket)) return;
      if (result._tag === "Failure") {
        reader.reject(ticket, conversationLibraryErrorMessage(Cause.squash(result.cause)));
        return;
      }
      const reply = result.value;
      if (!conversationLibraryReplyMatchesRequest(ticket.request, reply)) {
        reader.reject(ticket, "The library returned a reply that did not match the request.");
        return;
      }

      switch (ticket.request.kind) {
        case "hello": {
          if (reply.kind !== "hello" || !reader.acceptHello(ticket, reply)) return;
          const accountsTicket = reader.requestAccounts();
          if (accountsTicket) void executeReaderTicket(accountsTicket);
          return;
        }
        case "accounts": {
          if (reply.kind !== "accounts" || !reader.acceptAccounts(ticket, reply.accounts)) return;
          const filter = reader.getSnapshot().filter;
          const preferred = preferredAccountId.current;
          const accountId =
            preferred !== null && reply.accounts.some((account) => account.id === preferred)
              ? preferred
              : filter.accountId !== null &&
                  reply.accounts.some((account) => account.id === filter.accountId)
                ? filter.accountId
                : (reply.accounts[0]?.id ?? null);
          if (preferredAccountId.current === accountId) preferredAccountId.current = null;
          reader.setFilter({ ...filter, accountId });
          return;
        }
        case "list":
          if (reply.kind === "list" && reader.acceptList(ticket, reply)) {
            const current = reader.getSnapshot();
            if (current.detail.status === "stale" && current.selection !== null) {
              const detailTicket = reader.requestDetail(current.selection);
              if (detailTicket) void executeReaderTicket(detailTicket);
            }
          }
          return;
        case "detail":
          if (reply.kind === "detail" && reader.acceptDetail(ticket, reply)) {
            const current = reader.getSnapshot();
            if (current.list.status === "stale") {
              const listTicket = reader.requestList();
              if (listTicket) void executeReaderTicket(listTicket);
            }
          }
          return;
        case "update":
        case "selectSnapshot":
        case "remove": {
          if (reply.kind !== "updated" && reply.kind !== "removed") return;
          if (
            !reader.acceptMutation(
              ticket as LibraryReaderTicket<
                Extract<LibraryRequest, { kind: "update" | "selectSnapshot" | "remove" }>
              >,
              reply as Extract<LibraryReply, { kind: "updated" | "removed" }>,
            )
          )
            return;
          const listTicket = reader.requestList();
          if (listTicket) void executeReaderTicket(listTicket);
          const selection =
            ticket.request.kind === "selectSnapshot"
              ? {
                  kind: "detail" as const,
                  key: ticket.request.key,
                  snapshotId: ticket.request.snapshotId,
                  offset: 0,
                }
              : reader.getSnapshot().selection;
          if (selection !== null) {
            const detailTicket = reader.requestDetail(selection);
            if (detailTicket) void executeReaderTicket(detailTicket);
          }
          return;
        }
        case "preview":
        case "import":
        case "createAccount":
          return;
      }
    },
    [reader, runRequest],
  );

  const executeImportTicket = useCallback(
    async (ticket: ConversationImportTicket) => {
      if (!importer.isPending(ticket)) return;
      const result = await runRequest({
        environmentId: EnvironmentId.make(ticket.binding.environmentId),
        input: ticket.request,
      });
      if (!importer.isPending(ticket)) return;
      if (result._tag === "Failure") {
        importer.reject(ticket, conversationLibraryErrorMessage(Cause.squash(result.cause)));
        return;
      }
      if (!conversationLibraryReplyMatchesRequest(ticket.request, result.value)) {
        importer.reject(ticket, "The library returned a reply that did not match the request.");
        return;
      }
      if (ticket.request.kind === "preview" && result.value.kind === "preview") {
        importer.acceptPreview(ticket, result.value);
      } else if (ticket.request.kind === "import" && result.value.kind === "imported") {
        if (importer.acceptImport(ticket, result.value)) {
          const listTicket = reader.requestList();
          if (listTicket) void executeReaderTicket(listTicket);
        }
      }
    },
    [executeReaderTicket, importer, reader, runRequest],
  );

  useEffect(() => {
    if (!connected) {
      helloStartedFor.current = null;
      return;
    }
    if (currentBindingKey === null || readerBindingKey !== currentBindingKey) return;
    if (helloStartedFor.current === currentBindingKey) return;
    helloStartedFor.current = currentBindingKey;
    const ticket = reader.requestHello();
    if (ticket) void executeReaderTicket(ticket);
  }, [connected, currentBindingKey, executeReaderTicket, reader, readerBindingKey]);

  useEffect(() => {
    if (
      !connected ||
      state.accounts.status !== "ready" ||
      currentBindingKey === null ||
      readerBindingKey !== currentBindingKey
    )
      return;
    const timeout = window.setTimeout(
      () => {
        const ticket = reader.requestList();
        if (ticket) void executeReaderTicket(ticket);
      },
      state.filter.query.length === 0 ? 0 : 180,
    );
    return () => window.clearTimeout(timeout);
  }, [
    connected,
    currentBindingKey,
    executeReaderTicket,
    reader,
    state.accounts.status,
    readerBindingKey,
    state.filter.accountId,
    state.filter.query,
    state.filter.view,
  ]);

  useEffect(() => {
    const detail = state.detail.value;
    if (state.detail.status !== "ready" || detail === null || state.displayed) return;
    const frame = window.requestAnimationFrame(() => {
      if (
        !acknowledgeVisibleConversationDetail(detailPanelRef.current, detail, (visibleDetail) =>
          reader.acknowledgeDisplayed(visibleDetail),
        )
      )
        return;
      const ticket = reader.requestMarkRead();
      if (ticket) void executeReaderTicket(ticket);
    });
    return () => window.cancelAnimationFrame(frame);
  }, [
    executeReaderTicket,
    reader,
    smallScreenDetailRequested,
    state.detail.status,
    state.detail.value,
    state.displayed,
  ]);

  const reload = useCallback(() => {
    const ticket = reader.requestList();
    if (ticket) void executeReaderTicket(ticket);
  }, [executeReaderTicket, reader]);

  const chooseRow = useCallback(
    (row: LibrarySummary) => {
      const ticket = reader.requestDetail({
        kind: "detail",
        key: row.key,
        snapshotId: row.snapshotId,
      });
      setSmallScreenDetailRequested(true);
      if (ticket) void executeReaderTicket(ticket);
    },
    [executeReaderTicket, reader],
  );

  const openPage = useCallback(
    (detail: LibraryDetail, changes: Parameters<typeof libraryDetailPage>[1]) => {
      const ticket = reader.requestDetail(libraryDetailPage(detail, changes));
      if (ticket) void executeReaderTicket(ticket);
    },
    [executeReaderTicket, reader],
  );

  const onImportFile = useCallback(
    async (file: File | undefined) => {
      const accountId = state.filter.accountId;
      if (file === undefined || accountId === null) return;
      const fileTicket = importer.beginFileRead(accountId);
      if (fileTicket === null) return;
      try {
        const conversations = await readConversationExportFile(file);
        const ticket = importer.requestPreviewForFile(fileTicket, conversations);
        if (ticket) await executeImportTicket(ticket);
      } catch (error) {
        importer.rejectFileRead(
          fileTicket,
          error instanceof Error ? error.message : "The export could not be read.",
        );
      }
    },
    [executeImportTicket, importer, state.filter.accountId],
  );

  const startImport = useCallback(() => {
    const ticket = importer.requestImport();
    if (ticket) void executeImportTicket(ticket);
  }, [executeImportTicket, importer]);

  const createAccountRequest = useMemo(
    () =>
      createConversationLibraryAccountRequest(
        createAccountLabel,
        createAccountWorkspace,
        state.canWrite,
        connected,
      ),
    [connected, createAccountLabel, createAccountWorkspace, state.canWrite],
  );
  const createAccount = useCallback(async () => {
    if (!createAccountRequest || binding === null) {
      setCreateAccountError(
        !state.canWrite
          ? "This environment does not allow library changes."
          : !connected
            ? "Connect the environment before creating an account."
            : "Enter a label and workspace, each no longer than 200 characters.",
      );
      return;
    }
    const requestedBinding = bindingKey(binding);
    setCreateAccountPending(true);
    setCreateAccountError(null);
    const result = await runRequest({
      environmentId: EnvironmentId.make(binding.environmentId),
      input: createAccountRequest,
    });
    if (bindingKey(reader.getSnapshot().binding) !== requestedBinding) return;
    setCreateAccountPending(false);
    if (result._tag === "Failure") {
      setCreateAccountError(conversationLibraryErrorMessage(Cause.squash(result.cause)));
      return;
    }
    if (result.value.kind !== "account") {
      setCreateAccountError("The environment returned an unexpected account response.");
      return;
    }
    preferredAccountId.current = result.value.account.id;
    setCreateAccountLabel("");
    setCreateAccountWorkspace("");
    setCreateAccountOpen(false);
    const ticket = reader.requestAccounts();
    if (ticket) void executeReaderTicket(ticket);
  }, [
    binding,
    connected,
    createAccountRequest,
    executeReaderTicket,
    reader,
    runRequest,
    state.canWrite,
  ]);

  const runMutation = useCallback(
    (makeTicket: () => LibraryReaderTicket | null) => {
      const ticket = makeTicket();
      if (ticket) void executeReaderTicket(ticket);
    },
    [executeReaderTicket],
  );

  const list = state.list.value;
  const accountLabel = selectedAccount
    ? `${selectedAccount.label} · ${selectedAccount.workspace}`
    : "No account selected";
  const importTargetAccount = conversationLibraryImportTargetAccount(
    state.accounts.value,
    selectedAccount,
    importState.accountId,
  );
  const importTargetLabel = importTargetAccount
    ? `${importTargetAccount.label} · ${importTargetAccount.workspace}`
    : "Account unavailable";

  return (
    <SidebarInset className="h-dvh min-h-0 overflow-hidden overscroll-y-none">
      <div className="flex min-h-0 min-w-0 flex-1 flex-col">
        <WorkspacePageHeader electron={isElectron} className="border-b border-border">
          <div className="flex min-w-0 flex-1 items-center gap-3">
            <h1 className="truncate text-sm font-semibold sm:text-base">Conversation Library</h1>
            <div className="ml-auto flex min-w-0 items-center gap-2">
              <label className="sr-only" htmlFor="conversation-library-environment">
                Environment
              </label>
              <select
                id="conversation-library-environment"
                className="h-8 max-w-[34vw] rounded-lg border border-input bg-background px-2 text-xs sm:max-w-64 sm:text-sm"
                value={selectedEnvironmentId ?? ""}
                onChange={(event) =>
                  setRequestedEnvironmentId(
                    event.currentTarget.value
                      ? EnvironmentId.make(event.currentTarget.value)
                      : null,
                  )
                }
                disabled={!environmentsReady || environments.length === 0}
              >
                <option value="">Choose environment</option>
                {environments.map((environment) => (
                  <option key={environment.environmentId} value={environment.environmentId}>
                    {environment.label}
                  </option>
                ))}
              </select>
              <Button
                size="sm"
                variant="outline"
                onClick={() => setImportOpen(true)}
                disabled={!connected || !state.canWrite || state.filter.accountId === null}
              >
                <FileUpIcon /> <span className="hidden sm:inline">Import</span>
              </Button>
            </div>
          </div>
        </WorkspacePageHeader>

        <WorkspacePageContainer
          width="expanded"
          className="min-h-0 flex-1 gap-4 overflow-y-auto pt-4 sm:pt-6"
        >
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div>
              <p className="text-sm text-muted-foreground">
                A local library of imported conversation snapshots.
              </p>
              <BindingIdentity environmentName={environmentName} account={selectedAccount} />
            </div>
            <div
              className="flex items-center gap-2 text-xs text-muted-foreground"
              aria-live="polite"
            >
              <span>
                {!connected
                  ? connection.data?.phase === "connecting"
                    ? "Connecting…"
                    : "Environment offline"
                  : state.canWrite
                    ? "Read and write access"
                    : libraryHandshakeComplete
                      ? "Read-only access"
                      : "Checking library access…"}
              </span>
              <Button
                size="icon-sm"
                variant="ghost"
                aria-label="Refresh conversations"
                onClick={reload}
                disabled={!connected || state.list.status === "loading"}
              >
                <RefreshCwIcon className={state.list.status === "loading" ? "animate-spin" : ""} />
              </Button>
            </div>
          </div>

          {!environmentsReady ? <StatusCard title="Loading environments" /> : null}
          {environmentsReady && environments.length === 0 ? (
            <StatusCard
              title="No environment is available"
              detail="Connect an environment to read its conversation library."
            />
          ) : null}
          {selectedEnvironmentId !== null && !connected ? (
            <StatusCard
              title="Waiting for this environment"
              detail={
                connection.data?.phase === "blocked"
                  ? (connection.data.lastFailure?.message ?? "The connection is blocked.")
                  : "Conversation data is shown only from a connected environment."
              }
            />
          ) : null}
          {state.error ? <ErrorCard message={state.error} onRetry={reload} /> : null}
          {libraryHandshakeComplete && state.canWrite === false && state.error === null ? (
            <div
              className="rounded-xl border border-border/70 bg-muted/30 px-4 py-3 text-sm text-muted-foreground"
              role="status"
            >
              This library is read-only. Import and library changes are disabled.
            </div>
          ) : null}
          {connected && state.accounts.status === "error" ? (
            <ErrorCard
              message={state.accounts.error ?? "Could not load accounts."}
              onRetry={() => {
                const ticket = reader.requestAccounts();
                if (ticket) void executeReaderTicket(ticket);
              }}
            />
          ) : null}

          <section
            className="grid min-h-[34rem] flex-1 gap-4 lg:grid-cols-[minmax(18rem,0.86fr)_minmax(0,1.6fr)]"
            aria-label="Conversation library"
          >
            <div
              className={`flex min-h-0 flex-col overflow-hidden rounded-2xl border border-border/70 bg-card/35 ${smallScreenDetail ? "max-lg:hidden" : ""}`}
            >
              <div className="space-y-3 border-b border-border/60 p-3 sm:p-4">
                <div className="flex flex-wrap gap-2">
                  <label className="sr-only" htmlFor="conversation-library-account">
                    Account
                  </label>
                  <select
                    id="conversation-library-account"
                    className="h-8 min-w-0 flex-1 rounded-lg border border-input bg-background px-2 text-sm"
                    value={state.filter.accountId ?? ""}
                    onChange={(event) =>
                      reader.setFilter({
                        ...reader.getSnapshot().filter,
                        accountId: event.currentTarget.value || null,
                      })
                    }
                    disabled={state.accounts.status !== "ready"}
                  >
                    <option value="">All accounts</option>
                    {(state.accounts.value ?? []).map((account) => (
                      <option key={account.id} value={account.id}>
                        {account.label} · {account.workspace}
                      </option>
                    ))}
                  </select>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => {
                      setCreateAccountError(null);
                      setCreateAccountLabel("");
                      setCreateAccountWorkspace("");
                      setCreateAccountBinding(currentBindingKey);
                      setCreateAccountOpen(true);
                    }}
                    disabled={!connected || !state.canWrite || createAccountPending}
                  >
                    Create account
                  </Button>
                </div>
                <Input
                  nativeInput
                  type="search"
                  aria-label="Search conversations"
                  placeholder="Search conversations"
                  value={state.filter.query}
                  onChange={(event) =>
                    reader.setFilter({
                      ...reader.getSnapshot().filter,
                      query: event.currentTarget.value,
                    })
                  }
                  disabled={!connected || state.accounts.status !== "ready"}
                />
                <div
                  className="flex gap-1 overflow-x-auto"
                  role="tablist"
                  aria-label="Conversation views"
                >
                  {VIEWS.map((view) => (
                    <button
                      key={view.id}
                      role="tab"
                      aria-selected={state.filter.view === view.id}
                      className={`shrink-0 rounded-lg px-2.5 py-1.5 text-xs transition-colors ${state.filter.view === view.id ? "bg-primary/12 text-foreground" : "text-muted-foreground hover:bg-muted/70 hover:text-foreground"}`}
                      onClick={() =>
                        reader.setFilter({ ...reader.getSnapshot().filter, view: view.id })
                      }
                    >
                      {view.label}
                    </button>
                  ))}
                </div>
              </div>

              <div className="min-h-0 flex-1 overflow-y-auto p-2" aria-live="polite">
                {state.list.status === "loading" && list === null ? (
                  <StatusCard title="Loading conversations" />
                ) : null}
                {state.list.status === "error" ? (
                  <ErrorCard
                    message={state.list.error ?? "Could not load conversations."}
                    onRetry={reload}
                  />
                ) : null}
                {state.list.status === "ready" && list?.rows.length === 0 ? (
                  <StatusCard
                    title="No conversations found"
                    detail="Try another account, view, or search."
                  />
                ) : null}
                {list?.rows.map((row) => (
                  <ConversationRow
                    key={`${row.accountId}:${row.key}`}
                    row={row}
                    selected={state.selection?.key === row.key}
                    onClick={() => chooseRow(row)}
                  />
                ))}
                {list?.cursor ? (
                  <Button
                    className="mt-2 w-full"
                    size="sm"
                    variant="outline"
                    disabled={state.list.status === "loading"}
                    onClick={() => {
                      const ticket = reader.requestList(true);
                      if (ticket) void executeReaderTicket(ticket);
                    }}
                  >
                    Load more
                  </Button>
                ) : null}
                {!connected ||
                state.accounts.status === "loading" ||
                state.accounts.status === "idle" ? (
                  <StatusCard title="Connect to load conversations" />
                ) : null}
              </div>
            </div>

            <div
              ref={detailPanelRef}
              className={`flex min-h-0 flex-col overflow-hidden rounded-2xl border border-border/70 bg-card/35 ${!smallScreenDetail ? "max-lg:hidden" : ""}`}
            >
              {detail ? (
                <>
                  <div className="border-b border-border/60 p-3 sm:p-4">
                    <div className="mb-2 flex items-center gap-2">
                      <Button
                        className="lg:hidden"
                        size="icon-sm"
                        variant="ghost"
                        aria-label="Back to conversations"
                        onClick={() => setSmallScreenDetailRequested(false)}
                      >
                        <ArrowLeftIcon />
                      </Button>
                      <div className="min-w-0 flex-1">
                        <h2 className="truncate text-base font-semibold sm:text-lg">
                          {detail.conversation.title || "Untitled conversation"}
                        </h2>
                        <p className="truncate text-xs text-muted-foreground">
                          {accountLabel} · Imported {formatDate(detail.snapshotImportedAt)}
                        </p>
                      </div>
                      <Button
                        size="icon-sm"
                        variant={detail.conversation.pinned ? "secondary" : "ghost"}
                        aria-label={
                          detail.conversation.pinned ? "Unpin conversation" : "Pin conversation"
                        }
                        disabled={!state.canWrite || state.mutationPending}
                        onClick={() =>
                          runMutation(() =>
                            reader.requestFlags({ pinned: !detail.conversation.pinned }),
                          )
                        }
                      >
                        <PinIcon />
                      </Button>
                      <Button
                        size="icon-sm"
                        variant={detail.conversation.archived ? "secondary" : "ghost"}
                        aria-label={
                          detail.conversation.archived
                            ? "Unarchive conversation"
                            : "Archive conversation"
                        }
                        disabled={!state.canWrite || state.mutationPending}
                        onClick={() =>
                          runMutation(() =>
                            reader.requestFlags({ archived: !detail.conversation.archived }),
                          )
                        }
                      >
                        <ArchiveIcon />
                      </Button>
                      <Button
                        size="icon-sm"
                        variant={detail.conversation.attention ? "secondary" : "ghost"}
                        aria-label={
                          detail.conversation.attention ? "Clear attention" : "Mark for attention"
                        }
                        disabled={!state.canWrite || state.mutationPending}
                        onClick={() =>
                          runMutation(() =>
                            reader.requestFlags({ attention: !detail.conversation.attention }),
                          )
                        }
                      >
                        <FlagIcon />
                      </Button>
                      <Button
                        size="icon-sm"
                        variant="ghost"
                        aria-label="Remove local copy"
                        disabled={!state.canWrite || state.mutationPending}
                        onClick={() => setRemoveOpen(true)}
                      >
                        <Trash2Icon />
                      </Button>
                    </div>
                    <div className="grid gap-2 sm:grid-cols-2">
                      <label className="grid gap-1 text-xs text-muted-foreground">
                        Snapshot
                        <select
                          className="h-8 rounded-lg border border-input bg-background px-2 text-sm text-foreground"
                          value={detail.snapshotId}
                          disabled={state.detail.status === "loading"}
                          onChange={(event) => {
                            const ticket = reader.requestDetail(
                              conversationLibrarySnapshotRequest(detail, event.currentTarget.value),
                            );
                            if (ticket) void executeReaderTicket(ticket);
                          }}
                        >
                          {detail.snapshots.map((snapshot) => (
                            <option key={snapshot.id} value={snapshot.id}>
                              {formatDate(snapshot.sourceUpdatedAt)} · {snapshot.messageCount}{" "}
                              messages
                            </option>
                          ))}
                        </select>
                      </label>
                      <label className="grid gap-1 text-xs text-muted-foreground">
                        Branch
                        <select
                          className="h-8 rounded-lg border border-input bg-background px-2 text-sm text-foreground"
                          value={detail.nodeId ?? ""}
                          onChange={(event) => {
                            const nodeId = event.currentTarget.value || undefined;
                            const ticket = reader.requestDetail({
                              ...libraryDetailPage(detail, { offset: 0 }),
                              ...(nodeId === undefined ? {} : { nodeId }),
                            });
                            if (ticket) void executeReaderTicket(ticket);
                          }}
                        >
                          <option value="">Current branch</option>
                          {detail.branches.map((branch) => (
                            <option key={branch.id} value={branch.id}>
                              {branch.preview || branch.id}
                            </option>
                          ))}
                        </select>
                      </label>
                    </div>
                    {detail.snapshotId !== detail.conversation.snapshotId ? (
                      <Button
                        className="mt-2"
                        size="xs"
                        variant="outline"
                        disabled={!state.canWrite || state.mutationPending}
                        onClick={() =>
                          runMutation(() => reader.requestSelectSnapshot(detail.snapshotId))
                        }
                      >
                        Make this the current snapshot
                      </Button>
                    ) : null}
                    <div className="mt-2 flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
                      <span>
                        {detail.totalMessages} messages · Snapshot {detail.snapshotOffset + 1}–
                        {Math.min(
                          detail.snapshotOffset + detail.snapshots.length,
                          detail.snapshotCount,
                        )}{" "}
                        of {detail.snapshotCount} · Branch {detail.branchOffset + 1}–
                        {Math.min(detail.branchOffset + detail.branches.length, detail.branchCount)}{" "}
                        of {detail.branchCount}
                      </span>
                      <div className="flex items-center gap-1">
                        <Button
                          size="xs"
                          variant="ghost"
                          disabled={detail.snapshotOffset <= 0}
                          onClick={() =>
                            openPage(detail, {
                              snapshotOffset: Math.max(0, detail.snapshotOffset - 10),
                            })
                          }
                        >
                          <ArrowUpIcon />
                          Snapshots
                        </Button>
                        <Button
                          size="xs"
                          variant="ghost"
                          disabled={
                            detail.snapshotOffset + detail.snapshots.length >= detail.snapshotCount
                          }
                          onClick={() =>
                            openPage(detail, { snapshotOffset: detail.snapshotOffset + 10 })
                          }
                        >
                          <ArrowDownIcon />
                          Snapshots
                        </Button>
                        <Button
                          size="xs"
                          variant="ghost"
                          disabled={detail.branchOffset <= 0}
                          onClick={() =>
                            openPage(detail, {
                              branchOffset: Math.max(0, detail.branchOffset - 10),
                            })
                          }
                        >
                          <ArrowUpIcon />
                          Branches
                        </Button>
                        <Button
                          size="xs"
                          variant="ghost"
                          disabled={
                            detail.branchOffset + detail.branches.length >= detail.branchCount
                          }
                          onClick={() =>
                            openPage(detail, { branchOffset: detail.branchOffset + 10 })
                          }
                        >
                          <ArrowDownIcon />
                          Branches
                        </Button>
                      </div>
                    </div>
                  </div>
                  <div
                    className="min-h-0 flex-1 overflow-y-auto p-3 sm:p-5"
                    aria-label="Conversation messages"
                  >
                    {detail.warnings.length > 0 ? (
                      <div className="mb-3 rounded-lg border border-warning/30 bg-warning-surface/40 p-3 text-xs text-warning-foreground">
                        {detail.warnings.join(" ")}
                      </div>
                    ) : null}
                    <label className="mb-3 flex items-center gap-2 text-xs text-muted-foreground">
                      <input
                        type="checkbox"
                        checked={detail.showHidden}
                        onChange={(event) =>
                          openPage(detail, { showHidden: event.currentTarget.checked, offset: 0 })
                        }
                      />
                      Show hidden messages
                    </label>
                    <div className="space-y-3">
                      {detail.messages.map((message) => (
                        <ConversationLibraryMessage key={message.id} message={message} />
                      ))}
                    </div>
                    <div className="mt-4 flex justify-between gap-2">
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={
                          detail.previousOffset === null || state.detail.status === "loading"
                        }
                        onClick={() =>
                          openPage(detail, { offset: detail.previousOffset ?? detail.offset })
                        }
                      >
                        Previous messages
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={detail.nextOffset === null || state.detail.status === "loading"}
                        onClick={() =>
                          openPage(detail, { offset: detail.nextOffset ?? detail.offset })
                        }
                      >
                        Next messages
                      </Button>
                    </div>
                  </div>
                </>
              ) : state.detail.status === "loading" ? (
                <StatusCard title="Loading conversation" />
              ) : state.detail.status === "error" ? (
                <ErrorCard
                  message={state.detail.error ?? "Could not load this conversation."}
                  onRetry={() => {
                    if (state.selection) {
                      const ticket = reader.requestDetail(state.selection);
                      if (ticket) void executeReaderTicket(ticket);
                    }
                  }}
                />
              ) : (
                <StatusCard
                  title="Select a conversation"
                  detail="Choose an item from the library to read its saved snapshot."
                />
              )}
            </div>
          </section>
        </WorkspacePageContainer>
      </div>

      <Dialog
        open={importOpen}
        onOpenChange={(open) => {
          setImportOpen(open);
          if (!open) importer.cancel();
        }}
      >
        <DialogPopup className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Import conversations</DialogTitle>
            <DialogDescription>
              Choose a conversations.json export to preview it before adding it to this library.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 px-6 pb-5">
            <BindingIdentity environmentName={environmentName} account={importTargetAccount} />
            <p className="text-xs text-muted-foreground">Target account: {importTargetLabel}</p>
            <label className="grid gap-2 text-sm font-medium">
              Export file
              <input
                type="file"
                accept="application/json,.json"
                disabled={
                  !state.canWrite ||
                  importState.status === "previewing" ||
                  importState.status === "importing"
                }
                onChange={(event) => {
                  void onImportFile(event.currentTarget.files?.[0]);
                  event.currentTarget.value = "";
                }}
              />
            </label>
            {importState.status === "previewing" ? (
              <p role="status" className="text-sm text-muted-foreground">
                Reading and validating the selected file…
              </p>
            ) : null}
            {importState.error ? (
              <div
                role="alert"
                className="rounded-lg border border-destructive/30 bg-destructive/5 p-3 text-sm"
              >
                {importState.error}
              </div>
            ) : null}
            {importState.preview ? (
              <div className="max-h-60 space-y-2 overflow-y-auto rounded-xl border border-border/60 p-3">
                <p className="text-sm font-medium">
                  Preview · {importState.preview.length} conversations
                </p>
                {importState.preview.map((item) => (
                  <div
                    key={item.id}
                    className="flex flex-wrap justify-between gap-2 border-t border-border/50 pt-2 text-sm"
                  >
                    <span className="min-w-0 flex-1 truncate">
                      {item.title || "Untitled conversation"}
                    </span>
                    <span className="text-xs text-muted-foreground">
                      {item.messageCount} messages
                      {item.warningCount ? ` · ${item.warningCount} warnings` : ""}
                    </span>
                  </div>
                ))}
                <p className="text-xs text-muted-foreground">
                  Confirming import sends this validated export to {environmentName} /{" "}
                  {importTargetLabel}.
                </p>
              </div>
            ) : null}
            {importState.status === "complete" && importState.result ? (
              <p role="status" className="text-sm text-foreground">
                Imported {importState.result.inserted}; skipped {importState.result.duplicates}{" "}
                duplicates and {importState.result.older} older snapshots;{" "}
                {importState.result.conflicts} conflicts.
              </p>
            ) : null}
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                importer.cancel();
                setImportOpen(false);
              }}
            >
              Close
            </Button>
            <Button
              onClick={startImport}
              disabled={
                !state.canWrite ||
                importState.status !== "ready" ||
                importState.preview === null ||
                importTargetAccount === null
              }
            >
              Confirm import
            </Button>
          </DialogFooter>
        </DialogPopup>
      </Dialog>

      <Dialog
        open={accountDialogOpen}
        onOpenChange={(open) => {
          setCreateAccountOpen(open);
          if (open) setCreateAccountBinding(currentBindingKey);
          else setCreateAccountError(null);
        }}
      >
        <DialogPopup className="max-w-md">
          <DialogHeader>
            <DialogTitle>Create a library account</DialogTitle>
            <DialogDescription>
              Create an account/workspace label for imported conversations in {environmentName}.
            </DialogDescription>
          </DialogHeader>
          <form
            className="space-y-4 px-6 pb-5"
            onSubmit={(event) => {
              event.preventDefault();
              void createAccount();
            }}
          >
            <BindingIdentity environmentName={environmentName} account={null} />
            <label className="grid gap-1 text-sm font-medium">
              Account label
              <Input
                nativeInput
                maxLength={200}
                value={createAccountLabel}
                onChange={(event) => setCreateAccountLabel(event.currentTarget.value)}
                disabled={!state.canWrite || createAccountPending}
              />
            </label>
            <label className="grid gap-1 text-sm font-medium">
              Workspace
              <Input
                nativeInput
                maxLength={200}
                value={createAccountWorkspace}
                onChange={(event) => setCreateAccountWorkspace(event.currentTarget.value)}
                disabled={!state.canWrite || createAccountPending}
              />
            </label>
            {createAccountError ? (
              <p role="alert" className="text-sm text-destructive">
                {createAccountError}
              </p>
            ) : null}
            <div className="flex flex-col-reverse gap-2 sm:flex-row sm:justify-end">
              <Button variant="outline" type="button" onClick={() => setCreateAccountOpen(false)}>
                Cancel
              </Button>
              <Button
                type="submit"
                disabled={!createAccountRequest || createAccountPending || !state.canWrite}
              >
                {createAccountPending ? "Creating…" : "Create account"}
              </Button>
            </div>
          </form>
        </DialogPopup>
      </Dialog>

      <AlertDialog open={removeOpen} onOpenChange={setRemoveOpen}>
        <AlertDialogPopup>
          <AlertDialogHeader>
            <AlertDialogTitle>Remove this local copy?</AlertDialogTitle>
            <AlertDialogDescription>
              This removes the selected conversation snapshot from {environmentName} /{" "}
              {accountLabel}. It does not change the source export.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <Button variant="outline" onClick={() => setRemoveOpen(false)}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={() => {
                setRemoveOpen(false);
                runMutation(() => reader.requestRemoveLocalCopy());
              }}
            >
              Remove local copy
            </Button>
          </AlertDialogFooter>
        </AlertDialogPopup>
      </AlertDialog>
    </SidebarInset>
  );
}

function ConversationRow({
  row,
  selected,
  onClick,
}: {
  readonly row: LibrarySummary;
  readonly selected: boolean;
  readonly onClick: () => void;
}) {
  return (
    <button
      type="button"
      className={`mb-1 flex w-full flex-col items-start gap-2 rounded-xl p-3 text-left transition-colors hover:bg-accent/60 ${selected ? "bg-accent/70 ring-1 ring-ring/30" : ""}`}
      onClick={onClick}
    >
      <span className="flex w-full min-w-0 items-start justify-between gap-3">
        <span className="min-w-0 flex-1 truncate text-sm font-medium">
          {row.title || "Untitled conversation"}
        </span>
        {row.unread ? (
          <span className="mt-1 size-2 shrink-0 rounded-full bg-primary" aria-label="Unread" />
        ) : null}
      </span>
      <span className="flex flex-wrap gap-x-2 gap-y-1 text-xs text-muted-foreground">
        <span>{row.messageCount} messages</span>
        <span>·</span>
        <span>{formatDate(row.sourceUpdatedAt)}</span>
        {row.pinned ? <span>· Pinned</span> : null}
        {row.archived ? <span>· Archived</span> : null}
        {row.attention ? <span>· Attention</span> : null}
        {row.conflicts ? <span>· Conflict</span> : null}
      </span>
    </button>
  );
}

export function ConversationLibraryMessage({
  message,
}: {
  readonly message: LibraryDetail["messages"][number];
}) {
  return (
    <article
      className={`rounded-xl border border-border/60 bg-background/55 p-3 sm:p-4 ${message.hidden ? "border-dashed opacity-70" : ""}`}
    >
      <div className="mb-2 flex items-center justify-between gap-3 text-xs text-muted-foreground">
        <span className="font-medium capitalize">
          {message.role ?? "Message"}
          {message.hidden ? " · hidden" : ""}
        </span>
        <time>{formatDate(message.createdAt)}</time>
      </div>
      <p className="whitespace-pre-wrap break-words text-sm leading-relaxed text-foreground">
        {message.text || "[No text content]"}
      </p>
      {message.unsupportedParts > 0 ? (
        <p className="mt-2 text-xs text-muted-foreground">
          {message.unsupportedParts} non-text content item
          {message.unsupportedParts === 1 ? "" : "s"} omitted.
        </p>
      ) : null}
    </article>
  );
}

function StatusCard({ title, detail }: { readonly title: string; readonly detail?: string }) {
  return (
    <div className="rounded-xl border border-border/50 bg-background/30 p-4 text-center">
      <p className="text-sm font-medium">{title}</p>
      {detail ? <p className="mt-1 text-xs text-muted-foreground">{detail}</p> : null}
    </div>
  );
}

function ErrorCard({
  message,
  onRetry,
}: {
  readonly message: string;
  readonly onRetry: () => void;
}) {
  return (
    <div
      role="alert"
      className="flex flex-wrap items-center gap-3 rounded-xl border border-destructive/25 bg-destructive/5 p-3 text-sm"
    >
      <AlertCircleIcon className="size-4 shrink-0 text-destructive" />
      <span className="min-w-0 flex-1">{message}</span>
      <Button size="xs" variant="outline" onClick={onRetry}>
        Retry
      </Button>
    </div>
  );
}
