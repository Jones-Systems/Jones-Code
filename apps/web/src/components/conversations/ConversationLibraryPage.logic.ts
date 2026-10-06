import type {
  LibraryAccount,
  LibraryDetail,
  LibraryRequest,
} from "@t3tools/contracts/conversationLibrary";

export function conversationLibraryImportTargetAccount(
  accounts: readonly LibraryAccount[] | null,
  selectedAccount: LibraryAccount | null,
  pendingAccountId: string | null,
): LibraryAccount | null {
  return pendingAccountId === null
    ? selectedAccount
    : (accounts?.find((account) => account.id === pendingAccountId) ?? null);
}

export function acknowledgeVisibleConversationDetail(
  panel: Pick<HTMLElement, "getClientRects"> | null,
  detail: LibraryDetail,
  acknowledge: (detail: LibraryDetail) => boolean,
): boolean {
  return panel !== null && panel.getClientRects().length > 0 && acknowledge(detail);
}

export function createConversationLibraryAccountRequest(
  label: string,
  workspace: string,
  canWrite: boolean,
  connected: boolean,
): Extract<LibraryRequest, { readonly kind: "createAccount" }> | null {
  const cleanLabel = label.trim();
  const cleanWorkspace = workspace.trim();
  if (!canWrite || !connected || cleanLabel.length === 0 || cleanWorkspace.length === 0)
    return null;
  if (cleanLabel.length > 200 || cleanWorkspace.length > 200) return null;
  return { kind: "createAccount", label: cleanLabel, workspace: cleanWorkspace };
}

export function conversationLibraryDate(value: number | null): Date | null {
  return value === null ? null : new Date(value);
}

export function conversationLibrarySnapshotRequest(
  detail: LibraryDetail,
  snapshotId: string,
): Extract<LibraryRequest, { readonly kind: "detail" }> {
  return {
    kind: "detail",
    key: detail.conversation.key,
    snapshotId,
    offset: 0,
    snapshotOffset: detail.snapshotOffset,
    branchOffset: 0,
    showHidden: detail.showHidden,
  };
}
