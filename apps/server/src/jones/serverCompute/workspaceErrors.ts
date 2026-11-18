import * as Schema from "effect/Schema";

export class WorkspaceSearchIndexCreateFailed extends Schema.TaggedError<
  WorkspaceSearchIndexCreateFailed,
  { readonly brand: unique symbol }
>()("WorkspaceSearchIndexCreateFailed", {
  cwd: Schema.String,
  reason: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `Failed to create the workspace search index for '${this.cwd}'.`;
  }
}

export class WorkspaceSearchIndexScanTimedOut extends Schema.TaggedError<
  WorkspaceSearchIndexScanTimedOut,
  { readonly brand: unique symbol }
>()("WorkspaceSearchIndexScanTimedOut", {
  cwd: Schema.String,
  timeout: Schema.String,
}) {
  override get message(): string {
    return `Workspace search index for '${this.cwd}' did not finish scanning within ${this.timeout}`;
  }
}

export class WorkspaceSearchIndexSearchFailed extends Schema.TaggedError<
  WorkspaceSearchIndexSearchFailed,
  { readonly brand: unique symbol }
>()("WorkspaceSearchIndexSearchFailed", {
  cwd: Schema.String,
  queryLength: Schema.Number,
  pageSize: Schema.Number,
  reason: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `Workspace search failed for '${this.cwd}'.`;
  }
}

export class WorkspaceSearchIndexRefreshFailed extends Schema.TaggedError<
  WorkspaceSearchIndexRefreshFailed,
  { readonly brand: unique symbol }
>()("WorkspaceSearchIndexRefreshFailed", {
  cwd: Schema.String,
  reason: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `Failed to refresh the workspace search index for '${this.cwd}'.`;
  }
}

export class WorkspaceSearchIndexDestroyFailed extends Schema.TaggedError<
  WorkspaceSearchIndexDestroyFailed,
  { readonly brand: unique symbol }
>()("WorkspaceSearchIndexDestroyFailed", {
  cwd: Schema.String,
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return `Failed to destroy the workspace search index for '${this.cwd}'.`;
  }
}
