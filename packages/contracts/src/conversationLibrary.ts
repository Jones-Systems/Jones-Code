import * as Schema from "effect/Schema";

/** The library is a read model of supplied records, never a coding provider. */
export const CONVERSATION_LIBRARY_PROTOCOL = "t3.conversation-library.v1";
export const CONVERSATION_LIBRARY_PATH = "/api/conversation-library";
export const LIBRARY_MAX_REQUEST_BYTES = 16 * 1024 * 1024;
export const LIBRARY_MAX_CONVERSATIONS = 1_000;
export const LIBRARY_MAX_NODES = 10_000;
export const LIBRARY_MAX_TEXT_BYTES = 8 * 1024 * 1024;
export const LIBRARY_PAGE_SIZE = 50;

export interface ExportMessage {
  readonly id?: string | null;
  readonly author: { readonly role: string };
  readonly create_time?: number | null;
  readonly content?: {
    readonly content_type?: string;
    readonly parts?: readonly unknown[];
    readonly text?: string;
  } | null;
  readonly metadata?: { readonly is_visually_hidden_from_conversation?: boolean };
}

export interface ExportNode {
  readonly id?: string;
  readonly parent?: string | null;
  readonly message?: ExportMessage | null;
}

export interface ExportConversation {
  readonly id?: string;
  readonly conversation_id?: string;
  readonly title: string;
  readonly create_time?: number | null;
  readonly update_time?: number | null;
  readonly current_node?: string | null;
  readonly mapping: Readonly<Record<string, ExportNode>>;
}

export interface LibraryNode {
  readonly id: string;
  readonly parentId: string | null;
  readonly messageId: string | null;
  readonly role: string | null;
  readonly text: string;
  readonly createdAt: number | null;
  readonly hidden: boolean;
  readonly unsupportedParts: number;
}

export interface LibrarySnapshot {
  readonly conversationId: string;
  readonly title: string;
  readonly sourceUpdatedAt: number | null;
  readonly currentNodeId: string | null;
  readonly nodes: readonly LibraryNode[];
  readonly warnings: readonly string[];
}

export interface LibraryAccount {
  readonly id: string;
  readonly label: string;
  readonly workspace: string;
}

export type LibraryView = "all" | "unread" | "pinned" | "archived" | "attention";

export interface LibrarySummary {
  readonly key: string;
  readonly accountId: string;
  readonly conversationId: string;
  readonly title: string;
  readonly snapshotId: string;
  readonly sourceUpdatedAt: number | null;
  readonly importedAt: number;
  readonly revision: number;
  readonly unread: boolean;
  readonly pinned: boolean;
  readonly archived: boolean;
  readonly attention: boolean;
  readonly conflicts: boolean;
  readonly messageCount: number;
  readonly warningCount: number;
}

export interface LibrarySnapshotSummary {
  readonly id: string;
  readonly sourceUpdatedAt: number | null;
  readonly importedAt: number;
  readonly messageCount: number;
}

export type LibraryRequest =
  | { readonly kind: "hello" }
  | { readonly kind: "accounts" }
  | { readonly kind: "createAccount"; readonly label: string; readonly workspace: string }
  | { readonly kind: "preview"; readonly conversations: readonly ExportConversation[] }
  | {
      readonly kind: "import";
      readonly accountId: string;
      readonly conversations: readonly ExportConversation[];
    }
  | {
      readonly kind: "list";
      readonly accountId?: string;
      readonly query?: string;
      readonly view?: LibraryView;
      readonly cursor?: string;
    }
  | {
      readonly kind: "detail";
      readonly key: string;
      readonly snapshotId?: string;
      readonly nodeId?: string;
      readonly offset?: number;
      readonly snapshotOffset?: number;
      readonly branchOffset?: number;
      readonly showHidden?: boolean;
    }
  | {
      readonly kind: "update";
      readonly key: string;
      readonly pinned?: boolean;
      readonly archived?: boolean;
      readonly attention?: boolean;
      readonly readThrough?: number;
    }
  | {
      readonly kind: "selectSnapshot";
      readonly key: string;
      readonly snapshotId: string;
      readonly expectedRevision: number;
    }
  | { readonly kind: "remove"; readonly key: string; readonly expectedRevision: number };

export interface LibraryDetail {
  readonly kind: "detail";
  readonly conversation: LibrarySummary;
  readonly account: LibraryAccount;
  readonly snapshotId: string;
  readonly snapshotSourceUpdatedAt: number | null;
  readonly snapshotImportedAt: number;
  readonly showHidden: boolean;
  readonly nodeId: string | null;
  readonly messages: readonly LibraryNode[];
  readonly totalMessages: number;
  readonly readThrough: number;
  readonly offset: number;
  readonly previousOffset: number | null;
  readonly nextOffset: number | null;
  readonly snapshots: readonly LibrarySnapshotSummary[];
  readonly snapshotOffset: number;
  readonly snapshotCount: number;
  readonly branches: readonly { readonly id: string; readonly preview: string }[];
  readonly branchOffset: number;
  readonly branchCount: number;
  readonly warnings: readonly string[];
}

export type LibraryReply =
  | {
      readonly kind: "hello";
      readonly protocol: typeof CONVERSATION_LIBRARY_PROTOCOL;
      readonly revision: number;
      readonly canWrite: boolean;
      readonly capture: "not-enabled";
    }
  | { readonly kind: "accounts"; readonly accounts: readonly LibraryAccount[] }
  | { readonly kind: "account"; readonly account: LibraryAccount }
  | {
      readonly kind: "preview";
      readonly conversations: readonly {
        readonly id: string;
        readonly title: string;
        readonly messageCount: number;
        readonly warningCount: number;
      }[];
    }
  | {
      readonly kind: "imported";
      readonly inserted: number;
      readonly duplicates: number;
      readonly older: number;
      readonly conflicts: number;
      readonly revision: number;
    }
  | {
      readonly kind: "list";
      readonly rows: readonly LibrarySummary[];
      readonly revision: number;
      readonly cursor: string | null;
    }
  | LibraryDetail
  | { readonly kind: "updated"; readonly revision: number }
  | { readonly kind: "removed"; readonly revision: number };

export type LibraryErrorCode =
  | "invalid"
  | "too-large"
  | "not-found"
  | "conflict"
  | "unsupported"
  | "storage"
  | "forbidden";

const ExportMessageSchema = Schema.Struct({
  id: Schema.optionalKey(Schema.NullOr(Schema.String)),
  author: Schema.Struct({ role: Schema.String }),
  create_time: Schema.optionalKey(Schema.NullOr(Schema.Number)),
  content: Schema.optionalKey(
    Schema.NullOr(
      Schema.Struct({
        content_type: Schema.optionalKey(Schema.String),
        parts: Schema.optionalKey(Schema.Array(Schema.Unknown)),
        text: Schema.optionalKey(Schema.String),
      }),
    ),
  ),
  metadata: Schema.optionalKey(
    Schema.Struct({
      is_visually_hidden_from_conversation: Schema.optionalKey(Schema.Boolean),
    }),
  ),
});

const ExportNodeSchema = Schema.Struct({
  id: Schema.optionalKey(Schema.String),
  parent: Schema.optionalKey(Schema.NullOr(Schema.String)),
  message: Schema.optionalKey(Schema.NullOr(ExportMessageSchema)),
});

const ExportConversationSchema = Schema.Struct({
  id: Schema.optionalKey(Schema.String),
  conversation_id: Schema.optionalKey(Schema.String),
  title: Schema.String,
  create_time: Schema.optionalKey(Schema.NullOr(Schema.Number)),
  update_time: Schema.optionalKey(Schema.NullOr(Schema.Number)),
  current_node: Schema.optionalKey(Schema.NullOr(Schema.String)),
  mapping: Schema.Record(Schema.String, ExportNodeSchema),
});

const LibraryNodeSchema = Schema.Struct({
  id: Schema.String,
  parentId: Schema.NullOr(Schema.String),
  messageId: Schema.NullOr(Schema.String),
  role: Schema.NullOr(Schema.String),
  text: Schema.String,
  createdAt: Schema.NullOr(Schema.Number),
  hidden: Schema.Boolean,
  unsupportedParts: Schema.Number,
});

const LibraryAccountSchema = Schema.Struct({
  id: Schema.String,
  label: Schema.String,
  workspace: Schema.String,
});

export const LibraryViewSchema = Schema.Literals([
  "all",
  "unread",
  "pinned",
  "archived",
  "attention",
]);

const LibrarySummarySchema = Schema.Struct({
  key: Schema.String,
  accountId: Schema.String,
  conversationId: Schema.String,
  title: Schema.String,
  snapshotId: Schema.String,
  sourceUpdatedAt: Schema.NullOr(Schema.Number),
  importedAt: Schema.Number,
  revision: Schema.Number,
  unread: Schema.Boolean,
  pinned: Schema.Boolean,
  archived: Schema.Boolean,
  attention: Schema.Boolean,
  conflicts: Schema.Boolean,
  messageCount: Schema.Number,
  warningCount: Schema.Number,
});

const LibrarySnapshotSummarySchema = Schema.Struct({
  id: Schema.String,
  sourceUpdatedAt: Schema.NullOr(Schema.Number),
  importedAt: Schema.Number,
  messageCount: Schema.Number,
});

const LibraryDetailSchema = Schema.Struct({
  kind: Schema.Literal("detail"),
  conversation: LibrarySummarySchema,
  account: LibraryAccountSchema,
  snapshotId: Schema.String,
  snapshotSourceUpdatedAt: Schema.NullOr(Schema.Number),
  snapshotImportedAt: Schema.Number,
  showHidden: Schema.Boolean,
  nodeId: Schema.NullOr(Schema.String),
  messages: Schema.Array(LibraryNodeSchema),
  totalMessages: Schema.Number,
  readThrough: Schema.Number,
  offset: Schema.Number,
  previousOffset: Schema.NullOr(Schema.Number),
  nextOffset: Schema.NullOr(Schema.Number),
  snapshots: Schema.Array(LibrarySnapshotSummarySchema),
  snapshotOffset: Schema.Number,
  snapshotCount: Schema.Number,
  branches: Schema.Array(Schema.Struct({ id: Schema.String, preview: Schema.String })),
  branchOffset: Schema.Number,
  branchCount: Schema.Number,
  warnings: Schema.Array(Schema.String),
});

export const LibraryRequestSchema = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("hello") }),
  Schema.Struct({ kind: Schema.Literal("accounts") }),
  Schema.Struct({
    kind: Schema.Literal("createAccount"),
    label: Schema.String,
    workspace: Schema.String,
  }),
  Schema.Struct({
    kind: Schema.Literal("preview"),
    conversations: Schema.Array(ExportConversationSchema),
  }),
  Schema.Struct({
    kind: Schema.Literal("import"),
    accountId: Schema.String,
    conversations: Schema.Array(ExportConversationSchema),
  }),
  Schema.Struct({
    kind: Schema.Literal("list"),
    accountId: Schema.optionalKey(Schema.String),
    query: Schema.optionalKey(Schema.String),
    view: Schema.optionalKey(LibraryViewSchema),
    cursor: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({
    kind: Schema.Literal("detail"),
    key: Schema.String,
    snapshotId: Schema.optionalKey(Schema.String),
    nodeId: Schema.optionalKey(Schema.String),
    offset: Schema.optionalKey(Schema.Number),
    snapshotOffset: Schema.optionalKey(Schema.Number),
    branchOffset: Schema.optionalKey(Schema.Number),
    showHidden: Schema.optionalKey(Schema.Boolean),
  }),
  Schema.Struct({
    kind: Schema.Literal("update"),
    key: Schema.String,
    pinned: Schema.optionalKey(Schema.Boolean),
    archived: Schema.optionalKey(Schema.Boolean),
    attention: Schema.optionalKey(Schema.Boolean),
    readThrough: Schema.optionalKey(Schema.Number),
  }),
  Schema.Struct({
    kind: Schema.Literal("selectSnapshot"),
    key: Schema.String,
    snapshotId: Schema.String,
    expectedRevision: Schema.Number,
  }),
  Schema.Struct({
    kind: Schema.Literal("remove"),
    key: Schema.String,
    expectedRevision: Schema.Number,
  }),
]) satisfies Schema.Schema<LibraryRequest>;

export const LibraryReplySchema = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("hello"),
    protocol: Schema.Literal(CONVERSATION_LIBRARY_PROTOCOL),
    revision: Schema.Number,
    canWrite: Schema.Boolean,
    capture: Schema.Literal("not-enabled"),
  }),
  Schema.Struct({ kind: Schema.Literal("accounts"), accounts: Schema.Array(LibraryAccountSchema) }),
  Schema.Struct({ kind: Schema.Literal("account"), account: LibraryAccountSchema }),
  Schema.Struct({
    kind: Schema.Literal("preview"),
    conversations: Schema.Array(
      Schema.Struct({
        id: Schema.String,
        title: Schema.String,
        messageCount: Schema.Number,
        warningCount: Schema.Number,
      }),
    ),
  }),
  Schema.Struct({
    kind: Schema.Literal("imported"),
    inserted: Schema.Number,
    duplicates: Schema.Number,
    older: Schema.Number,
    conflicts: Schema.Number,
    revision: Schema.Number,
  }),
  Schema.Struct({
    kind: Schema.Literal("list"),
    rows: Schema.Array(LibrarySummarySchema),
    revision: Schema.Number,
    cursor: Schema.NullOr(Schema.String),
  }),
  LibraryDetailSchema,
  Schema.Struct({ kind: Schema.Literal("updated"), revision: Schema.Number }),
  Schema.Struct({ kind: Schema.Literal("removed"), revision: Schema.Number }),
]) satisfies Schema.Schema<LibraryReply>;

export const LibraryErrorCodeSchema = Schema.Literals([
  "invalid",
  "too-large",
  "not-found",
  "conflict",
  "unsupported",
  "storage",
  "forbidden",
]) satisfies Schema.Schema<LibraryErrorCode>;
