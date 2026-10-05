import type {
  ExportConversation,
  LibraryReply,
  LibraryRequest,
} from "@t3tools/contracts/conversationLibrary";
import {
  LIBRARY_MAX_REQUEST_BYTES,
  LibraryRequestSchema,
} from "@t3tools/contracts/conversationLibrary";
import * as Schema from "effect/Schema";

import type { LibraryReaderBinding } from "./model.ts";

export type ConversationImportFileErrorCode = "invalid" | "too-large";
const decodeLibraryRequest = Schema.decodeUnknownSync(LibraryRequestSchema);

export class ConversationImportFileError extends Error {
  override readonly name = "ConversationImportFileError";
  readonly code: ConversationImportFileErrorCode;

  constructor(code: ConversationImportFileErrorCode, message: string) {
    super(message);
    this.code = code;
  }
}

export interface ConversationImportPreviewItem {
  readonly id: string;
  readonly title: string;
  readonly messageCount: number;
  readonly warningCount: number;
}

export interface ConversationImportResult {
  readonly inserted: number;
  readonly duplicates: number;
  readonly older: number;
  readonly conflicts: number;
  readonly revision: number;
}

export interface ConversationImportState {
  readonly binding: LibraryReaderBinding | null;
  readonly canWrite: boolean;
  readonly status: "idle" | "previewing" | "ready" | "importing" | "complete" | "error";
  readonly accountId: string | null;
  readonly preview: readonly ConversationImportPreviewItem[] | null;
  readonly result: ConversationImportResult | null;
  readonly error: string | null;
}

export interface ConversationImportTicket<R extends LibraryRequest = LibraryRequest> {
  readonly binding: LibraryReaderBinding;
  readonly request: R;
}

export interface ConversationImportFileReadTicket {
  readonly binding: LibraryReaderBinding;
  readonly accountId: string;
}

type PreviewRequest = Extract<LibraryRequest, { readonly kind: "preview" }>;
type ImportRequest = Extract<LibraryRequest, { readonly kind: "import" }>;
type PreviewReply = Extract<LibraryReply, { readonly kind: "preview" }>;
type ImportedReply = Extract<LibraryReply, { readonly kind: "imported" }>;

const idleState = (
  binding: LibraryReaderBinding | null,
  canWrite = false,
): ConversationImportState => ({
  binding,
  canWrite,
  status: "idle",
  accountId: null,
  preview: null,
  result: null,
  error: null,
});

function serializedRequestBytes(request: LibraryRequest): number {
  return new TextEncoder().encode(JSON.stringify(request)).byteLength;
}

function checkedRequest<R extends LibraryRequest>(request: R): R {
  if (serializedRequestBytes(request) > LIBRARY_MAX_REQUEST_BYTES) {
    throw new ConversationImportFileError(
      "too-large",
      "The selected export is too large to send to this conversation library.",
    );
  }
  return request;
}

export async function readConversationExportFile(
  file: Pick<File, "name" | "size" | "text">,
): Promise<readonly ExportConversation[]> {
  if (file.name !== "conversations.json") {
    throw new ConversationImportFileError("invalid", "Choose the conversations.json export file.");
  }
  if (file.size > LIBRARY_MAX_REQUEST_BYTES) {
    throw new ConversationImportFileError(
      "too-large",
      "The selected export exceeds the 16 MiB limit.",
    );
  }

  let value: unknown;
  try {
    value = JSON.parse(await file.text());
  } catch {
    throw new ConversationImportFileError(
      "invalid",
      "The selected conversations.json file is not valid JSON.",
    );
  }

  try {
    const parsed = decodeLibraryRequest({
      kind: "preview",
      conversations: value,
    });
    if (parsed.kind !== "preview") {
      throw new ConversationImportFileError(
        "invalid",
        "The selected export does not contain a conversations list.",
      );
    }
    checkedRequest(parsed);
    return parsed.conversations;
  } catch (error) {
    if (error instanceof ConversationImportFileError) throw error;
    throw new ConversationImportFileError(
      "invalid",
      "The selected conversations.json file does not match the supported export format.",
    );
  }
}

export class ConversationLibraryImport {
  private state: ConversationImportState = idleState(null);
  private readonly listeners = new Set<() => void>();
  private pending: ConversationImportTicket | null = null;
  private fileRead: ConversationImportFileReadTicket | null = null;
  private prepared: ImportRequest | null = null;

  readonly getSnapshot = (): ConversationImportState => this.state;

  readonly subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  private publish(state: ConversationImportState): void {
    this.state = state;
    for (const listener of this.listeners) listener();
  }

  bind(binding: LibraryReaderBinding | null, canWrite: boolean): void {
    const current = this.state.binding;
    if (
      current === binding ||
      (current !== null &&
        binding !== null &&
        current.environmentId === binding.environmentId &&
        current.generation === binding.generation)
    ) {
      if (this.state.canWrite !== canWrite) this.publish({ ...this.state, canWrite });
      return;
    }
    this.pending = null;
    this.fileRead = null;
    this.prepared = null;
    this.publish(idleState(binding === null ? null : { ...binding }, canWrite));
  }

  beginFileRead(accountId: string): ConversationImportFileReadTicket | null {
    const binding = this.state.binding;
    if (binding === null || accountId.trim().length === 0) return null;
    const ticket: ConversationImportFileReadTicket = {
      binding,
      accountId,
    };
    this.pending = null;
    this.fileRead = ticket;
    this.prepared = null;
    this.publish({ ...idleState(binding, this.state.canWrite), status: "previewing", accountId });
    return ticket;
  }

  requestPreviewForFile(
    ticket: ConversationImportFileReadTicket,
    conversations: readonly ExportConversation[],
  ): ConversationImportTicket<PreviewRequest> | null {
    if (this.fileRead !== ticket || !this.sameBinding(ticket.binding, this.state.binding))
      return null;
    this.fileRead = null;
    return this.requestPreview(ticket.accountId, conversations);
  }

  rejectFileRead(ticket: ConversationImportFileReadTicket, message: string): boolean {
    if (this.fileRead !== ticket || !this.sameBinding(ticket.binding, this.state.binding))
      return false;
    this.fileRead = null;
    this.publish({ ...this.state, status: "error", preview: null, error: message });
    return true;
  }

  requestPreview(
    accountId: string,
    conversations: readonly ExportConversation[],
  ): ConversationImportTicket<PreviewRequest> | null {
    const binding = this.state.binding;
    if (binding === null || accountId.trim().length === 0) return null;
    this.fileRead = null;
    let request: PreviewRequest;
    try {
      request = checkedRequest({ kind: "preview", conversations });
    } catch (error) {
      this.prepared = null;
      this.pending = null;
      this.publish({
        ...idleState(binding, this.state.canWrite),
        status: "error",
        error: error instanceof Error ? error.message : "The export is too large.",
      });
      return null;
    }
    this.fileRead = null;
    const ticket: ConversationImportTicket<PreviewRequest> = { binding, request };
    this.pending = ticket;
    this.prepared = null;
    this.publish({ ...idleState(binding, this.state.canWrite), status: "previewing", accountId });
    return ticket;
  }

  isPending(ticket: ConversationImportTicket): boolean {
    return this.pending === ticket && this.sameBinding(ticket.binding, this.state.binding);
  }

  acceptPreview(ticket: ConversationImportTicket, reply: PreviewReply): boolean {
    if (!this.isPending(ticket) || ticket.request.kind !== "preview" || reply.kind !== "preview")
      return false;
    const accountId = this.state.accountId;
    if (accountId === null) return false;
    let request: ImportRequest;
    try {
      request = checkedRequest({
        kind: "import",
        accountId,
        conversations: ticket.request.conversations,
      });
    } catch (error) {
      this.pending = null;
      this.prepared = null;
      this.publish({
        ...this.state,
        status: "error",
        preview: null,
        error: error instanceof Error ? error.message : "The export is too large.",
      });
      return false;
    }
    this.pending = null;
    this.prepared = request;
    this.publish({ ...this.state, status: "ready", preview: reply.conversations, error: null });
    return true;
  }

  requestImport(): ConversationImportTicket<ImportRequest> | null {
    const binding = this.state.binding;
    const request = this.prepared;
    if (
      binding === null ||
      request === null ||
      !this.state.canWrite ||
      this.state.status !== "ready"
    )
      return null;
    try {
      checkedRequest(request);
    } catch (error) {
      this.prepared = null;
      this.publish({
        ...this.state,
        status: "error",
        preview: null,
        error: error instanceof Error ? error.message : "The export is too large.",
      });
      return null;
    }
    const ticket: ConversationImportTicket<ImportRequest> = { binding, request };
    this.pending = ticket;
    this.publish({ ...this.state, status: "importing", error: null });
    return ticket;
  }

  acceptImport(ticket: ConversationImportTicket, reply: ImportedReply): boolean {
    if (!this.isPending(ticket) || ticket.request.kind !== "import" || reply.kind !== "imported")
      return false;
    this.pending = null;
    this.prepared = null;
    this.publish({
      ...this.state,
      status: "complete",
      preview: null,
      result: reply,
      error: null,
    });
    return true;
  }

  reject(ticket: ConversationImportTicket, message: string): boolean {
    if (!this.isPending(ticket)) return false;
    this.pending = null;
    this.prepared = null;
    this.publish({ ...this.state, status: "error", preview: null, error: message });
    return true;
  }

  cancel(): void {
    this.pending = null;
    this.fileRead = null;
    this.prepared = null;
    this.publish(
      idleState(
        this.state.binding === null ? null : { ...this.state.binding },
        this.state.canWrite,
      ),
    );
  }

  private sameBinding(a: LibraryReaderBinding | null, b: LibraryReaderBinding | null): boolean {
    return (
      a === b ||
      (a !== null &&
        b !== null &&
        a.environmentId === b.environmentId &&
        a.generation === b.generation)
    );
  }
}
