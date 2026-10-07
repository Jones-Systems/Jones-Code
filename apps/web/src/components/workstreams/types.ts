export type WorkstreamMemberKind =
  | "t3-thread"
  | "conversation"
  | "prompt"
  | "run-group"
  | "job"
  | "pull-request"
  | "deployment"
  | "repository";

export type WorkstreamAssociation = "primary" | "secondary";

export interface WorkstreamMemberPresentation {
  readonly ref: string;
  readonly kind: WorkstreamMemberKind;
  readonly title: string;
  readonly subtitle?: string;
  readonly association: WorkstreamAssociation;
  readonly removedAt?: string;
}

export interface WorkstreamPresentation {
  readonly id: string;
  readonly name: string;
  readonly lifecycle: "planned" | "active" | "paused" | "completed" | "deferred" | "abandoned";
  readonly order: number;
  readonly members: readonly WorkstreamMemberPresentation[];
  readonly memberCount: number;
  readonly hasMoreMembers: boolean;
}

export type WorkstreamMembershipCommand =
  | {
      readonly type: "move";
      readonly memberRef: string;
      readonly fromWorkstreamId: string;
      readonly toWorkstreamId: string;
    }
  | {
      readonly type: "link";
      readonly memberRef: string;
      readonly workstreamId: string;
      readonly association: "secondary";
    }
  | {
      readonly type: "remove";
      readonly memberRef: string;
      readonly workstreamId: string;
    }
  | {
      readonly type: "reattach";
      readonly memberRef: string;
      readonly workstreamId: string;
      readonly association: WorkstreamAssociation;
    };

export interface WorkstreamHistoryEntry {
  readonly id: string;
  readonly occurredAt: string;
  readonly label: string;
  readonly detail?: string;
}

export interface WorkstreamReceiptPresentation {
  readonly id: string;
  readonly operation: string;
  readonly state: "pending" | "succeeded" | "failed";
  readonly message?: string;
}

export interface WorkstreamReceipts {
  readonly coordination: WorkstreamReceiptPresentation | null;
  readonly nativeSettlement: WorkstreamReceiptPresentation | null;
}
