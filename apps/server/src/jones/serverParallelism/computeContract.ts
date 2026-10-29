import type {
  ProjectEntryKind,
  ProjectListEntriesResult,
  ProjectSearchContentsInput,
  ProjectSearchContentsResult,
  ProjectSearchEntriesResult,
} from "@t3tools/contracts";

export type WorkspaceIndexVariant = "paths" | "content";

export interface WorkspaceKey {
  readonly cwd: string;
  readonly variant: WorkspaceIndexVariant;
}

export interface LogicalFinderHandle {
  readonly handleId: string;
  readonly key: WorkspaceKey;
}

/** Both worker replacement and same-worker finder recreation fence stale results. */
export interface FinderToken {
  readonly authorityId: string;
  readonly slotId: number;
  readonly slotGeneration: number;
  readonly keyId: string;
  readonly incarnation: number;
}

export interface WorkspaceJobs {
  readonly list: { readonly input: undefined; readonly output: ProjectListEntriesResult };
  readonly search: {
    readonly input: {
      readonly query: string;
      readonly limit: number;
      readonly kind?: ProjectEntryKind;
      readonly imageOnly?: boolean;
    };
    readonly output: ProjectSearchEntriesResult;
  };
  readonly searchContents: {
    readonly input: Omit<ProjectSearchContentsInput, "cwd">;
    readonly output: ProjectSearchContentsResult;
  };
  readonly refresh: { readonly input: undefined; readonly output: void };
}

export type ComputeFailureReason =
  | "queue-full"
  | "queue-timeout"
  | "capacity-held"
  | "unavailable"
  | "deadline"
  | "cancelled"
  | "worker-exit"
  | "protocol"
  | "oversize"
  | "finder-create"
  | "finder-search"
  | "finder-refresh"
  | "retirement-unknown";
