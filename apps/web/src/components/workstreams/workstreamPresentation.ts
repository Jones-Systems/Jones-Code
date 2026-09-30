import type {
  WorkstreamAssociation,
  WorkstreamMembershipCommand,
  WorkstreamReceiptPresentation,
  WorkstreamReceipts,
} from "./types";

export type WorkstreamMemberAction = "move" | "link" | "remove" | "reattach";

export interface WorkstreamMemberActionItem {
  readonly id: WorkstreamMemberAction;
  readonly label: string;
  readonly destructive?: boolean;
}

export function getWorkstreamMemberActions(input: {
  readonly association: WorkstreamAssociation;
  readonly isRemoved: boolean;
}): readonly WorkstreamMemberActionItem[] {
  if (input.isRemoved) {
    return [{ id: "reattach", label: "Reattach to workstream" }];
  }
  return [
    ...(input.association === "primary"
      ? ([
          { id: "move", label: "Move to another workstream" },
          { id: "link", label: "Link to another workstream" },
        ] as const)
      : []),
    {
      id: "remove",
      label:
        input.association === "secondary" ? "Unlink from workstream" : "Remove from workstream",
      destructive: true,
    },
  ];
}

export function planWorkstreamDrop(input: {
  readonly memberRef: string;
  readonly sourceWorkstreamId: string;
  readonly targetWorkstreamId: string;
}): WorkstreamMembershipCommand | null {
  if (input.sourceWorkstreamId === input.targetWorkstreamId) return null;
  return {
    type: "move",
    memberRef: input.memberRef,
    fromWorkstreamId: input.sourceWorkstreamId,
    toWorkstreamId: input.targetWorkstreamId,
  };
}

export function getReceiptSummary(receipts: WorkstreamReceipts): {
  readonly coordination: WorkstreamReceiptPresentation;
  readonly nativeSettlement: WorkstreamReceiptPresentation;
  readonly isPartial: boolean;
} {
  const unavailable = (operation: string): WorkstreamReceiptPresentation => ({
    id: `${operation}:not-requested`,
    operation,
    state: "pending",
    message: "Not requested",
  });
  const coordination = receipts.coordination ?? unavailable("Workstream coordination");
  const nativeSettlement = receipts.nativeSettlement ?? unavailable("Native T3 settlement");
  return {
    coordination,
    nativeSettlement,
    isPartial:
      coordination.state !== nativeSettlement.state ||
      (coordination.state === "failed") !== (nativeSettlement.state === "failed"),
  };
}
export function pageWindow<T>(input: {
  readonly items: readonly T[];
  readonly page: number;
  readonly pageSize: number;
}): { readonly items: readonly T[]; readonly page: number; readonly pageCount: number } {
  const pageSize = Math.max(1, Math.floor(input.pageSize));
  const pageCount = Math.max(1, Math.ceil(input.items.length / pageSize));
  const page = Math.min(Math.max(0, Math.floor(input.page)), pageCount - 1);
  return {
    items: input.items.slice(page * pageSize, page * pageSize + pageSize),
    page,
    pageCount,
  };
}
