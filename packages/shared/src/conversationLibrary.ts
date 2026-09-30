import {
  LIBRARY_MAX_CONVERSATIONS,
  LIBRARY_MAX_NODES,
  LIBRARY_MAX_TEXT_BYTES,
  type ExportConversation,
  type LibraryNode,
  type LibrarySnapshot,
  type LibraryErrorCode,
  type LibraryRequest,
} from "@t3tools/contracts/conversationLibrary";

export class ConversationLibraryError extends Error {
  readonly code: LibraryErrorCode;

  constructor(code: LibraryErrorCode, message: string) {
    super(message);
    this.code = code;
    this.name = "ConversationLibraryError";
  }
}

export function boundedLibraryString(value: string, name: string, max = 512): string {
  const trimmed = value.trim();
  if (!trimmed || trimmed.length > max || /[\u0000-\u001f\u007f]/.test(trimmed)) {
    throw new ConversationLibraryError(
      "invalid",
      `${name} is empty, too long, or contains control characters.`,
    );
  }
  return trimmed;
}

function identifier(value: string, name: string): string {
  const checked = boundedLibraryString(value, name);
  if (checked !== value)
    throw new ConversationLibraryError("invalid", `${name} contains surrounding whitespace.`);
  return value;
}

function exportTime(value: number | null | undefined): number | null {
  if (value === undefined || value === null) return null;
  const milliseconds = value * 1_000;
  if (!Number.isFinite(milliseconds) || Math.abs(milliseconds) > 8_640_000_000_000_000) {
    throw new ConversationLibraryError("invalid", "An export contains an invalid timestamp.");
  }
  return Math.trunc(milliseconds);
}

function compareIds(a: { readonly id: string }, b: { readonly id: string }): number {
  return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
}

/** Structural decoding belongs at the boundary; this checks graph and size invariants. */
export function normalizeConversationExport(
  input: readonly ExportConversation[],
): readonly LibrarySnapshot[] {
  if (input.length === 0 || input.length > LIBRARY_MAX_CONVERSATIONS) {
    throw new ConversationLibraryError(
      "too-large",
      `Import between 1 and ${LIBRARY_MAX_CONVERSATIONS} conversations at a time.`,
    );
  }
  const ids = new Set<string>();
  const encoder = new TextEncoder();
  let totalBytes = 0;
  let totalNodes = 0;
  return input.map((conversation) => {
    const id = identifier(conversation.id ?? conversation.conversation_id ?? "", "Conversation ID");
    if (
      conversation.id &&
      conversation.conversation_id &&
      conversation.id !== conversation.conversation_id
    ) {
      throw new ConversationLibraryError(
        "invalid",
        "The export contains conflicting conversation IDs.",
      );
    }
    if (ids.has(id))
      throw new ConversationLibraryError(
        "conflict",
        "The same conversation occurs twice in one import. Import the snapshots separately.",
      );
    ids.add(id);
    const title = boundedLibraryString(
      conversation.title || "Untitled conversation",
      "Title",
      2_000,
    );
    const entries = Object.entries(conversation.mapping);
    totalNodes += entries.length;
    if (totalNodes > 20_000)
      throw new ConversationLibraryError("too-large", "Import at most 20,000 nodes at a time.");
    if (entries.length > LIBRARY_MAX_NODES) {
      throw new ConversationLibraryError(
        "too-large",
        `A conversation exceeds ${LIBRARY_MAX_NODES} nodes.`,
      );
    }
    const nodes = entries
      .map(([key, entry]): LibraryNode => {
        identifier(key, "Node ID");
        if (entry.id !== undefined && entry.id !== key) {
          throw new ConversationLibraryError(
            "invalid",
            "A node ID differs from its export mapping key.",
          );
        }
        const message = entry.message;
        const content = message?.content;
        const parts = content?.parts ?? [];
        const textParts = parts.filter((part): part is string => typeof part === "string");
        const text = textParts.length > 0 ? textParts.join("\n") : (content?.text ?? "");
        const unsupportedParts =
          parts.length -
          textParts.length +
          (content && !parts.length && !content.text && content.content_type !== "text" ? 1 : 0);
        totalBytes += encoder.encode(text).byteLength;
        if (totalBytes > LIBRARY_MAX_TEXT_BYTES) {
          throw new ConversationLibraryError(
            "too-large",
            "The import exceeds the text budget. Split the supplied export into smaller imports.",
          );
        }
        return {
          id: key,
          parentId: entry.parent == null ? null : identifier(entry.parent, "Parent ID"),
          messageId: message?.id == null ? null : identifier(message.id, "Message ID"),
          role: message ? boundedLibraryString(message.author.role, "Author role", 128) : null,
          text,
          createdAt: exportTime(message?.create_time),
          hidden: message?.metadata?.is_visually_hidden_from_conversation === true,
          unsupportedParts,
        };
      })
      .sort(compareIds);
    const byId = new Map(nodes.map((node) => [node.id, node]));
    const finished = new Set<string>();
    let missingParents = 0;
    for (const node of nodes) {
      if (node.parentId !== null && !byId.has(node.parentId)) missingParents++;
      const visiting = new Set<string>();
      let cursor: LibraryNode | undefined = node;
      while (cursor && !finished.has(cursor.id)) {
        if (visiting.has(cursor.id))
          throw new ConversationLibraryError(
            "invalid",
            "The export contains a cycle in its conversation graph.",
          );
        visiting.add(cursor.id);
        cursor = cursor.parentId === null ? undefined : byId.get(cursor.parentId);
      }
      for (const visited of visiting) finished.add(visited);
    }
    const currentNodeId =
      conversation.current_node && byId.has(conversation.current_node)
        ? conversation.current_node
        : null;
    const warnings: string[] = [];
    if (missingParents)
      warnings.push(
        `${missingParents} parent references are absent; those branches have a history gap.`,
      );
    if (conversation.current_node && !currentNodeId)
      warnings.push("The exported selected node is absent. Select a retained branch explicitly.");
    if (!conversation.current_node && nodes.length)
      warnings.push(
        "The export does not identify its selected branch. Select a retained branch explicitly.",
      );
    const unsupported = nodes.reduce((sum, node) => sum + node.unsupportedParts, 0);
    if (unsupported)
      warnings.push(
        `${unsupported} non-text parts are represented as unavailable, not downloaded or executed.`,
      );
    return {
      conversationId: id,
      title,
      sourceUpdatedAt: exportTime(conversation.update_time),
      currentNodeId,
      nodes,
      warnings,
    };
  });
}

export function libraryBranchIds(
  nodes: readonly LibraryNode[],
  selected: string | null,
): readonly string[] {
  const parents = new Set(nodes.flatMap((node) => (node.parentId === null ? [] : [node.parentId])));
  const leaves = nodes.filter((node) => !parents.has(node.id)).map((node) => node.id);
  return selected && !leaves.includes(selected) ? [selected, ...leaves] : leaves;
}

/** An explicit node selection never falls back to some other branch. */
export function libraryBranch(
  nodes: readonly LibraryNode[],
  nodeId: string | null,
  showHidden = false,
): readonly LibraryNode[] {
  if (nodeId === null) return [];
  const byId = new Map(nodes.map((node) => [node.id, node]));
  if (!byId.has(nodeId))
    throw new ConversationLibraryError("not-found", "The selected branch is not in this snapshot.");
  const seen = new Set<string>();
  const path: LibraryNode[] = [];
  let cursor = byId.get(nodeId);
  while (cursor) {
    if (seen.has(cursor.id))
      throw new ConversationLibraryError("invalid", "The retained graph contains a cycle.");
    seen.add(cursor.id);
    if (cursor.role !== null && (showHidden || !cursor.hidden)) path.push(cursor);
    cursor = cursor.parentId === null ? undefined : byId.get(cursor.parentId);
  }
  return path.reverse();
}

export function libraryRequestMutates(request: LibraryRequest): boolean {
  return (
    request.kind === "createAccount" ||
    request.kind === "import" ||
    request.kind === "update" ||
    request.kind === "selectSnapshot" ||
    request.kind === "remove"
  );
}

export function libraryOriginalUrl(conversationId: string): string | null {
  return /^[A-Za-z0-9_-]{1,512}$/.test(conversationId)
    ? `https://chatgpt.com/c/${encodeURIComponent(conversationId)}`
    : null;
}

/** Reusing an epoch is a rendering bug: account and page changes invalidate pending reads. */
export class LibraryRequestFence {
  private generation = 0;
  next(): number {
    return ++this.generation;
  }
  accepts(generation: number): boolean {
    return this.generation === generation;
  }
}
