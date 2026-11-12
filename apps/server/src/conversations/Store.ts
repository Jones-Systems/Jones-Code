// @effect-diagnostics-next-line nodeBuiltinImport:off -- Synchronous conversation identifiers retain their existing custody hash encoding.
import * as NodeCrypto from "node:crypto";
import {
  CONVERSATION_LIBRARY_PROTOCOL,
  LIBRARY_PAGE_SIZE,
  type LibraryAccount,
  type LibraryDetail,
  type LibraryNode,
  type LibraryReply,
  type LibraryRequest,
  type LibrarySnapshot,
  type LibrarySummary,
} from "@t3tools/contracts/conversationLibrary";
import {
  ConversationLibraryError,
  boundedLibraryString,
  libraryRequestMutates,
  normalizeConversationExport,
} from "@t3tools/shared/conversationLibrary";

type SqlValue = string | number | null;
type Row = Record<string, unknown>;

export interface LibraryDatabase {
  exec(sql: string): unknown;
  prepare(sql: string): {
    get(...values: SqlValue[]): Row | undefined;
    all(...values: SqlValue[]): Row[];
    run(...values: SqlValue[]): unknown;
  };
  close(): void;
}

const MAX_LIBRARY_BYTES = 512 * 1024 * 1024;
const MAX_LIBRARY_CONVERSATIONS = 50_000;
const MAX_SNAPSHOTS_PER_CONVERSATION = 250;

function text(row: Row, key: string): string {
  const value = row[key];
  if (typeof value !== "string") {
    throw new ConversationLibraryError("storage", "The library contains an invalid text field.");
  }
  return value;
}

function number(row: Row, key: string): number {
  const value = row[key];
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new ConversationLibraryError("storage", "The library contains an invalid numeric field.");
  }
  return value;
}

function nullableNumber(row: Row, key: string): number | null {
  return row[key] === null ? null : number(row, key);
}

function nullableText(row: Row, key: string): string | null {
  return row[key] === null ? null : text(row, key);
}

function digest(value: unknown): string {
  return NodeCrypto.createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

function offset(value: number | undefined): number {
  if (value === undefined) return 0;
  if (!Number.isSafeInteger(value) || value < 0 || value > 1_000_000) {
    throw new ConversationLibraryError("invalid", "The requested page offset is invalid.");
  }
  return value;
}

const summarySelect = `SELECT c.*, s.source_updated_at, s.imported_at,
  s.message_count, s.warning_count
  FROM conversations c LEFT JOIN snapshots s ON s.id = c.selected_snapshot
  AND s.conversation_key = c.key`;

function summary(row: Row): LibrarySummary {
  return {
    key: text(row, "key"),
    accountId: text(row, "account_id"),
    conversationId: text(row, "source_id"),
    title: text(row, "title"),
    snapshotId: text(row, "selected_snapshot"),
    sourceUpdatedAt: nullableNumber(row, "source_updated_at"),
    importedAt: number(row, "imported_at"),
    revision: number(row, "change_revision"),
    unread: number(row, "read_revision") < number(row, "change_revision"),
    pinned: number(row, "pinned") === 1,
    archived: number(row, "archived") === 1,
    attention: number(row, "attention") === 1,
    conflicts: number(row, "conflicts") === 1,
    messageCount: number(row, "message_count"),
    warningCount: number(row, "warning_count"),
  };
}

function node(row: Row): LibraryNode {
  return {
    id: text(row, "node_id"),
    parentId: nullableText(row, "parent_id"),
    messageId: nullableText(row, "message_id"),
    role: nullableText(row, "role"),
    text: text(row, "text"),
    createdAt: nullableNumber(row, "created_at"),
    hidden: number(row, "hidden") === 1,
    unsupportedParts: number(row, "unsupported_parts"),
  };
}

/** Each mutation and its catalog revision commit together; replies follow COMMIT. */
export class ConversationLibraryStore {
  private readonly db: LibraryDatabase;
  private readonly clock: () => number;

  constructor(db: LibraryDatabase, options: { initialize?: boolean; clock?: () => number } = {}) {
    this.db = db;
    this.clock = options.clock ?? Date.now;
    db.exec("PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 1000;");
    const version = number(this.one("PRAGMA user_version"), "user_version");
    if (version === 0 && options.initialize) {
      const existing = number(
        this.one("SELECT count(*) AS count FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"),
        "count",
      );
      if (existing !== 0)
        throw new ConversationLibraryError(
          "unsupported",
          "An unrecognized database occupies the library path.",
        );
      db.exec(`BEGIN IMMEDIATE;
        CREATE TABLE meta (id INTEGER PRIMARY KEY CHECK(id = 1), revision INTEGER NOT NULL);
        INSERT INTO meta VALUES (1, 0);
        CREATE TABLE accounts (id TEXT PRIMARY KEY, label TEXT NOT NULL, workspace TEXT NOT NULL);
        CREATE TABLE conversations (
          key TEXT PRIMARY KEY, account_id TEXT NOT NULL REFERENCES accounts(id),
          source_id TEXT NOT NULL, title TEXT NOT NULL, selected_snapshot TEXT NOT NULL,
          change_revision INTEGER NOT NULL, read_revision INTEGER NOT NULL DEFAULT 0,
          pinned INTEGER NOT NULL DEFAULT 0, archived INTEGER NOT NULL DEFAULT 0,
          attention INTEGER NOT NULL DEFAULT 0, conflicts INTEGER NOT NULL DEFAULT 0,
          UNIQUE(account_id, source_id)
        );
        CREATE INDEX conversation_order ON conversations(change_revision DESC, key);
        CREATE INDEX conversation_account ON conversations(account_id, change_revision DESC, key);
        CREATE TABLE snapshots (
          id TEXT PRIMARY KEY, conversation_key TEXT NOT NULL REFERENCES conversations(key) ON DELETE CASCADE,
          title TEXT NOT NULL, source_updated_at INTEGER, imported_at INTEGER NOT NULL,
          introduced_revision INTEGER NOT NULL, current_node TEXT, warnings TEXT NOT NULL,
          message_count INTEGER NOT NULL, warning_count INTEGER NOT NULL
        );
        CREATE INDEX snapshot_conversation ON snapshots(conversation_key, introduced_revision DESC);
        CREATE TABLE nodes (
          snapshot_id TEXT NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
          node_id TEXT NOT NULL, parent_id TEXT, message_id TEXT, role TEXT, text TEXT NOT NULL,
          created_at INTEGER, hidden INTEGER NOT NULL, unsupported_parts INTEGER NOT NULL,
          PRIMARY KEY(snapshot_id, node_id)
        );
        CREATE INDEX node_parent ON nodes(snapshot_id, parent_id);
        CREATE VIRTUAL TABLE library_search USING fts5(key UNINDEXED, title, body);
        PRAGMA user_version = 1;
        COMMIT;`);
    } else if (version !== 1) {
      throw new ConversationLibraryError(
        "unsupported",
        "This library database version is not supported.",
      );
    }
  }

  close(): void {
    this.db.close();
  }

  private one(sql: string, ...values: SqlValue[]): Row {
    const row = this.db.prepare(sql).get(...values);
    if (!row)
      throw new ConversationLibraryError(
        "not-found",
        "The requested library record does not exist.",
      );
    return row;
  }

  private revision(): number {
    return number(this.one("SELECT revision FROM meta WHERE id = 1"), "revision");
  }

  private advance(): number {
    this.db.exec("UPDATE meta SET revision = revision + 1 WHERE id = 1");
    return this.revision();
  }

  private atomic<A>(operation: () => A): A {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = operation();
      const pages = number(this.one("PRAGMA page_count"), "page_count");
      const free = number(this.one("PRAGMA freelist_count"), "freelist_count");
      const size = number(this.one("PRAGMA page_size"), "page_size");
      if ((pages - free) * size > MAX_LIBRARY_BYTES) {
        throw new ConversationLibraryError(
          "too-large",
          "The library reached its 512 MiB admission limit. Remove unneeded local records before importing more.",
        );
      }
      this.db.exec("COMMIT");
      return result;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  private account(id: string): LibraryAccount {
    const row = this.one("SELECT * FROM accounts WHERE id = ?", id);
    return { id: text(row, "id"), label: text(row, "label"), workspace: text(row, "workspace") };
  }

  private conversation(key: string): LibrarySummary {
    return summary(this.one(`${summarySelect} WHERE c.key = ?`, key));
  }

  private indexSnapshot(key: string, id: string, title: string): void {
    const rows = this.db
      .prepare("SELECT text FROM nodes WHERE snapshot_id = ? AND hidden = 0 ORDER BY node_id")
      .all(id);
    const body = rows.map((row) => text(row, "text")).join("\n");
    this.db.prepare("DELETE FROM library_search WHERE key = ?").run(key);
    this.db
      .prepare("INSERT INTO library_search (key, title, body) VALUES (?, ?, ?)")
      .run(key, title, body);
  }

  private importSnapshots(accountId: string, snapshots: readonly LibrarySnapshot[]): LibraryReply {
    this.account(accountId);
    return this.atomic(() => {
      let inserted = 0,
        duplicates = 0,
        older = 0,
        conflicts = 0;
      const insertNode = this.db.prepare(`INSERT INTO nodes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`);
      for (const snapshot of snapshots) {
        const key = digest(["chatgpt-export", accountId, snapshot.conversationId]);
        const id = digest(["snapshot-v1", key, snapshot]);
        if (
          this.db
            .prepare("SELECT id FROM snapshots WHERE id = ? AND conversation_key = ?")
            .get(id, key)
        ) {
          duplicates++;
          continue;
        }
        const prior = this.db.prepare(`${summarySelect} WHERE c.key = ?`).get(key);
        if (!prior) {
          const count = number(this.one("SELECT count(*) AS count FROM conversations"), "count");
          if (count >= MAX_LIBRARY_CONVERSATIONS)
            throw new ConversationLibraryError(
              "too-large",
              "The library conversation limit has been reached.",
            );
          this.db
            .prepare(`INSERT INTO conversations
            (key, account_id, source_id, title, selected_snapshot, change_revision)
            VALUES (?, ?, ?, ?, ?, 0)`)
            .run(key, accountId, snapshot.conversationId, snapshot.title, id);
        }
        const retained = this.one(
          "SELECT count(*) AS count, max(source_updated_at) AS maximum FROM snapshots WHERE conversation_key = ?",
          key,
        );
        if (number(retained, "count") >= MAX_SNAPSHOTS_PER_CONVERSATION) {
          throw new ConversationLibraryError(
            "too-large",
            "A conversation reached its 250-snapshot retention limit. No retained history was discarded.",
          );
        }
        const revision = this.advance();
        const messageCount = snapshot.nodes.filter(
          (entry) => entry.role !== null && !entry.hidden,
        ).length;
        this.db
          .prepare(`INSERT INTO snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
          .run(
            id,
            key,
            snapshot.title,
            snapshot.sourceUpdatedAt,
            this.clock(),
            revision,
            snapshot.currentNodeId,
            JSON.stringify(snapshot.warnings),
            messageCount,
            snapshot.warnings.length,
          );
        for (const entry of snapshot.nodes) {
          insertNode.run(
            id,
            entry.id,
            entry.parentId,
            entry.messageId,
            entry.role,
            entry.text,
            entry.createdAt,
            Number(entry.hidden),
            entry.unsupportedParts,
          );
        }
        const maximum = nullableNumber(retained, "maximum");
        const advanceSelection =
          !prior ||
          (snapshot.sourceUpdatedAt !== null &&
            maximum !== null &&
            nullableNumber(prior, "source_updated_at") !== null &&
            number(prior, "conflicts") === 0 &&
            snapshot.sourceUpdatedAt > maximum);
        if (advanceSelection) {
          this.db
            .prepare(
              `UPDATE conversations SET title = ?, selected_snapshot = ?, change_revision = ? WHERE key = ?`,
            )
            .run(snapshot.title, id, revision, key);
          this.indexSnapshot(key, id, snapshot.title);
        } else if (
          snapshot.sourceUpdatedAt !== null &&
          maximum !== null &&
          snapshot.sourceUpdatedAt < maximum
        ) {
          older++;
        } else {
          conflicts++;
          this.db
            .prepare("UPDATE conversations SET conflicts = 1, change_revision = ? WHERE key = ?")
            .run(revision, key);
        }
        inserted++;
      }
      return {
        kind: "imported",
        inserted,
        duplicates,
        older,
        conflicts,
        revision: this.revision(),
      };
    });
  }

  private list(request: Extract<LibraryRequest, { kind: "list" }>): LibraryReply {
    const revision = this.revision();
    const query = request.query?.trim() ?? "";
    if (query.length > 500)
      throw new ConversationLibraryError(
        "invalid",
        "Use a search phrase of at most 500 characters.",
      );
    const view = request.view ?? "all";
    const identity = digest([request.accountId ?? null, query, view]);
    const clauses = [view === "archived" ? "c.archived = 1" : "c.archived = 0"];
    const values: SqlValue[] = [];
    if (request.accountId !== undefined) {
      this.account(request.accountId);
      clauses.push("c.account_id = ?");
      values.push(request.accountId);
    }
    if (view === "unread") clauses.push("c.read_revision < c.change_revision");
    if (view === "pinned") clauses.push("c.pinned = 1");
    if (view === "attention") clauses.push("(c.attention = 1 OR c.conflicts = 1)");
    if (query) {
      clauses.push("c.key IN (SELECT key FROM library_search WHERE library_search MATCH ?)");
      values.push(`"${query.replaceAll('"', '""')}"`);
    }
    if (request.cursor) {
      const match = /^(\d+):(\d+):([a-f0-9]{64}):([a-f0-9]{64})$/.exec(request.cursor);
      if (
        !match ||
        !Number.isSafeInteger(Number(match[1])) ||
        !Number.isSafeInteger(Number(match[2]))
      ) {
        throw new ConversationLibraryError("invalid", "The conversation cursor is invalid.");
      }
      if (Number(match[1]) !== revision || match[4] !== identity) {
        throw new ConversationLibraryError(
          "conflict",
          "The conversation list changed. Refresh before loading another page.",
        );
      }
      clauses.push("(c.change_revision < ? OR (c.change_revision = ? AND c.key > ?))");
      values.push(Number(match[2]), Number(match[2]), match[3]!);
    }
    const found = this.db
      .prepare(`${summarySelect} WHERE ${clauses.join(" AND ")}
      ORDER BY c.change_revision DESC, c.key ASC LIMIT ?`)
      .all(...values, LIBRARY_PAGE_SIZE + 1);
    const rows = found.slice(0, LIBRARY_PAGE_SIZE).map(summary);
    const last = rows.at(-1);
    return {
      kind: "list",
      rows,
      revision,
      cursor:
        found.length > LIBRARY_PAGE_SIZE && last
          ? `${revision}:${last.revision}:${last.key}:${identity}`
          : null,
    };
  }

  private detail(request: Extract<LibraryRequest, { kind: "detail" }>): LibraryDetail {
    const conversation = this.conversation(request.key);
    const snapshotId = request.snapshotId ?? conversation.snapshotId;
    const snapshot = this.one(
      "SELECT * FROM snapshots WHERE id = ? AND conversation_key = ?",
      snapshotId,
      request.key,
    );
    const nodeId = request.nodeId ?? nullableText(snapshot, "current_node");
    if (nodeId !== null)
      this.one(
        "SELECT node_id FROM nodes WHERE snapshot_id = ? AND node_id = ?",
        snapshotId,
        nodeId,
      );
    const chain = `WITH RECURSIVE chain(node_id, parent_id, depth) AS (
      SELECT node_id, parent_id, 0 FROM nodes WHERE snapshot_id = ? AND node_id = ?
      UNION ALL SELECT n.node_id, n.parent_id, c.depth + 1 FROM nodes n JOIN chain c
      ON n.node_id = c.parent_id WHERE n.snapshot_id = ? AND c.depth < 10000
    )`;
    const pathValues: SqlValue[] = [snapshotId, nodeId, snapshotId, snapshotId];
    const visible = request.showHidden
      ? "n.role IS NOT NULL"
      : "n.role IS NOT NULL AND n.hidden = 0";
    const fromChain = `FROM chain c JOIN nodes n ON n.node_id = c.node_id AND n.snapshot_id = ? WHERE ${visible}`;
    const totalMessages = number(
      this.one(`${chain} SELECT count(*) AS count ${fromChain}`, ...pathValues),
      "count",
    );
    const start =
      request.offset === undefined
        ? Math.max(0, Math.floor((totalMessages - 1) / LIBRARY_PAGE_SIZE) * LIBRARY_PAGE_SIZE)
        : offset(request.offset);
    if (start > totalMessages)
      throw new ConversationLibraryError(
        "invalid",
        "The requested message page is outside this branch.",
      );
    const messages = this.db
      .prepare(`${chain} SELECT n.* ${fromChain} ORDER BY c.depth DESC LIMIT ? OFFSET ?`)
      .all(...pathValues, LIBRARY_PAGE_SIZE, start)
      .map(node);
    const snapshotOffset = offset(request.snapshotOffset);
    const snapshots = this.db
      .prepare(`SELECT * FROM snapshots WHERE conversation_key = ?
      ORDER BY introduced_revision DESC, id ASC LIMIT 50 OFFSET ?`)
      .all(request.key, snapshotOffset)
      .map((row) => ({
        id: text(row, "id"),
        sourceUpdatedAt: nullableNumber(row, "source_updated_at"),
        importedAt: number(row, "imported_at"),
        messageCount: number(row, "message_count"),
      }));
    const branchOffset = offset(request.branchOffset);
    const leafWhere = `FROM nodes n WHERE n.snapshot_id = ? AND NOT EXISTS
      (SELECT 1 FROM nodes child WHERE child.snapshot_id = n.snapshot_id AND child.parent_id = n.node_id)`;
    const branches = this.db
      .prepare(`SELECT n.node_id, substr(n.text, 1, 100) AS preview ${leafWhere}
      ORDER BY n.node_id LIMIT 50 OFFSET ?`)
      .all(snapshotId, branchOffset)
      .map((row) => ({ id: text(row, "node_id"), preview: text(row, "preview") }));
    return {
      kind: "detail",
      conversation,
      account: this.account(conversation.accountId),
      snapshotId,
      nodeId,
      snapshotSourceUpdatedAt: nullableNumber(snapshot, "source_updated_at"),
      snapshotImportedAt: number(snapshot, "imported_at"),
      showHidden: request.showHidden ?? false,
      messages,
      totalMessages,
      offset: start,
      readThrough:
        snapshotId === conversation.snapshotId &&
        nodeId !== null &&
        nodeId === nullableText(snapshot, "current_node") &&
        !conversation.conflicts
          ? conversation.revision
          : 0,
      previousOffset: start > 0 ? Math.max(0, start - LIBRARY_PAGE_SIZE) : null,
      nextOffset: start + messages.length < totalMessages ? start + messages.length : null,
      snapshots,
      snapshotOffset,
      snapshotCount: number(
        this.one("SELECT count(*) AS count FROM snapshots WHERE conversation_key = ?", request.key),
        "count",
      ),
      branches,
      branchOffset,
      branchCount: number(this.one(`SELECT count(*) AS count ${leafWhere}`, snapshotId), "count"),
      warnings: JSON.parse(text(snapshot, "warnings")) as string[],
    };
  }

  execute(request: LibraryRequest, canWrite: boolean): LibraryReply {
    if (libraryRequestMutates(request)) return this.dispatch(request, canWrite);
    this.db.exec("BEGIN");
    try {
      const reply = this.dispatch(request, canWrite);
      this.db.exec("COMMIT");
      return reply;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  private dispatch(request: LibraryRequest, canWrite: boolean): LibraryReply {
    if (libraryRequestMutates(request) && !canWrite) {
      throw new ConversationLibraryError(
        "forbidden",
        "This connection cannot change the conversation library.",
      );
    }
    switch (request.kind) {
      case "hello":
        return {
          kind: "hello",
          protocol: CONVERSATION_LIBRARY_PROTOCOL,
          revision: this.revision(),
          canWrite,
          capture: "not-enabled",
        };
      case "accounts":
        return {
          kind: "accounts",
          accounts: this.db
            .prepare("SELECT id FROM accounts ORDER BY label, workspace, id")
            .all()
            .map((row) => this.account(text(row, "id"))),
        };
      case "createAccount":
        return this.atomic(() => {
          const label = boundedLibraryString(request.label, "Account label", 200);
          const workspace = boundedLibraryString(request.workspace, "Workspace label", 200);
          if (number(this.one("SELECT count(*) AS count FROM accounts"), "count") >= 200)
            throw new ConversationLibraryError(
              "too-large",
              "The library account-binding limit has been reached.",
            );
          if (
            this.db
              .prepare(
                "SELECT id FROM accounts WHERE label = ? COLLATE NOCASE AND workspace = ? COLLATE NOCASE",
              )
              .get(label, workspace)
          ) {
            throw new ConversationLibraryError(
              "conflict",
              "That account/workspace label already exists. Select its existing binding.",
            );
          }
          const id = NodeCrypto.randomUUID();
          this.db.prepare("INSERT INTO accounts VALUES (?, ?, ?)").run(id, label, workspace);
          this.advance();
          return { kind: "account", account: { id, label, workspace } };
        });
      case "preview":
        return {
          kind: "preview",
          conversations: normalizeConversationExport(request.conversations).map((s) => ({
            id: s.conversationId,
            title: s.title,
            messageCount: s.nodes.filter((n) => n.role !== null && !n.hidden).length,
            warningCount: s.warnings.length,
          })),
        };
      case "import":
        return this.importSnapshots(
          request.accountId,
          normalizeConversationExport(request.conversations),
        );
      case "list":
        return this.list(request);
      case "detail":
        return this.detail(request);
      case "update":
        return this.atomic(() => {
          const current = this.conversation(request.key);
          if (
            request.readThrough !== undefined &&
            (!Number.isSafeInteger(request.readThrough) ||
              request.readThrough < 0 ||
              request.readThrough > current.revision)
          ) {
            throw new ConversationLibraryError(
              "invalid",
              "The read marker cannot acknowledge an unobserved future revision.",
            );
          }
          this.db
            .prepare(`UPDATE conversations SET pinned = ?, archived = ?, attention = ?,
          read_revision = max(read_revision, ?) WHERE key = ?`)
            .run(
              Number(request.pinned ?? current.pinned),
              Number(request.archived ?? current.archived),
              Number(request.attention ?? current.attention),
              request.readThrough ?? 0,
              request.key,
            );
          return { kind: "updated", revision: this.advance() };
        });
      case "selectSnapshot":
        return this.atomic(() => {
          const current = this.conversation(request.key);
          if (current.revision !== request.expectedRevision)
            throw new ConversationLibraryError(
              "conflict",
              "The conversation changed. Reload before choosing its default snapshot.",
            );
          const selected = this.one(
            "SELECT title FROM snapshots WHERE id = ? AND conversation_key = ?",
            request.snapshotId,
            request.key,
          );
          const revision = this.advance();
          this.db
            .prepare(
              "UPDATE conversations SET selected_snapshot = ?, title = ?, conflicts = 0, change_revision = ? WHERE key = ?",
            )
            .run(request.snapshotId, text(selected, "title"), revision, request.key);
          this.indexSnapshot(request.key, request.snapshotId, text(selected, "title"));
          return { kind: "updated", revision };
        });
      case "remove":
        return this.atomic(() => {
          if (this.conversation(request.key).revision !== request.expectedRevision)
            throw new ConversationLibraryError(
              "conflict",
              "The conversation changed. Reload before removing the local copy.",
            );
          this.db.prepare("DELETE FROM library_search WHERE key = ?").run(request.key);
          this.db.prepare("DELETE FROM conversations WHERE key = ?").run(request.key);
          return { kind: "removed", revision: this.advance() };
        });
    }
  }
}
