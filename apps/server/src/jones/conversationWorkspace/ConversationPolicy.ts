import { type ChatAttachment, type ThreadId, WorktreeMcpHandoffResult, WorktreeMcpSetupScriptStatus } from "@t3tools/contracts";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as Crypto from "effect/Crypto";
import * as Path from "effect/Path";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as ServerConfig from "../../config.ts";
import { createConversationDirectory, disposeConversationDirectory, inspectConversationDirectory, resumeConversationDirectory, ownsConversationPath, type DirectoryIdentity } from "./ConversationDirectory.ts";
import { readSource, resolveSource, type SourceRequest, type SourceSnapshot } from "./SourceBroker.ts";

export interface Binding extends DirectoryIdentity, SourceSnapshot {
  readonly threadId: string;
  readonly projectId: string;
  readonly state: "conversation" | "attached" | "disposed" | "conversation_dir_nonempty";
  readonly ownerThreadId: string;
}
export interface HandoffRecord {
  readonly revision: number;
  readonly members: readonly string[];
  readonly threadId: string;
  readonly requestId: string;
  readonly request: string;
  readonly commit: string;
  readonly baseRef: string;
  readonly startedFromOrigin: boolean;
  readonly phase: "reserved" | "checkout_issued" | "checkout_created" | "bound" | "setup_issued" | "setup_done" | "continuation_issued" | "done";
  readonly path: string | null;
  readonly setup: WorktreeMcpSetupScriptStatus | null;
  readonly result: WorktreeMcpHandoffResult | null;
}
const BindingRecord = Schema.Struct({
  path: Schema.String, device: Schema.String, inode: Schema.String,
  commonDirectory: Schema.String, commit: Schema.String, baseRef: Schema.String,
  threadId: Schema.String, projectId: Schema.String, ownerThreadId: Schema.String,
  state: Schema.Literals(["conversation", "attached", "disposed", "conversation_dir_nonempty"]),
});
const SnapshotRecord = Schema.Struct({ commonDirectory: Schema.String, commit: Schema.String, baseRef: Schema.String });
const HandoffRecordSchema = Schema.Struct({
  revision: Schema.Int,
  members: Schema.Array(Schema.String),
  threadId: Schema.String, requestId: Schema.String, request: Schema.String,
  commit: Schema.String, baseRef: Schema.String, startedFromOrigin: Schema.Boolean,
  phase: Schema.Literals(["reserved", "checkout_issued", "checkout_created", "bound", "setup_issued", "setup_done", "continuation_issued", "done"]),
  path: Schema.NullOr(Schema.String), setup: Schema.NullOr(WorktreeMcpSetupScriptStatus), result: Schema.NullOr(WorktreeMcpHandoffResult),
});
const decodeBinding = Schema.decodeEffect(Schema.fromJsonString(BindingRecord));
const decodeSnapshot = Schema.decodeEffect(Schema.fromJsonString(SnapshotRecord));
const decodeHandoff = Schema.decodeEffect(Schema.fromJsonString(HandoffRecordSchema));
export class ConversationWorkspaceError extends Schema.TaggedError<ConversationWorkspaceError>()("ConversationWorkspaceError", {
  reason: Schema.Literals(["storage", "source", "attach_required", "handoff_conflict", "handoff_unknown", "directory_nonempty"]),
  threadId: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message() { return `Conversation workspace: ${this.reason}.`; }
}
const io = <A>(threadId: string, reason: ConversationWorkspaceError["reason"], run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: (cause) => new ConversationWorkspaceError({ threadId, reason, cause }) });

export const ParentConversation = Context.Reference<string | undefined>("jones/conversationWorkspace/ParentConversation", { defaultValue: () => undefined });

type TurnAttachment = (threadId: ThreadId, runId: string, text: string, attachments: ReadonlyArray<ChatAttachment>) => Effect.Effect<void, unknown>;

export class ConversationPolicy extends Context.Service<ConversationPolicy, {
  readonly registerTurnAttachment: (attach: TurnAttachment) => Effect.Effect<void>;
  readonly attachForTurn: (threadId: ThreadId, runId: string, text: string, attachments: ReadonlyArray<ChatAttachment>) => Effect.Effect<void, ConversationWorkspaceError>;
  readonly lookup: (threadId: string) => Effect.Effect<Binding | null, ConversationWorkspaceError>;
  readonly allocate: (input: { threadId: string; projectId: string; projectRoot: string; baseRef?: string; excluded?: readonly string[] }) => Effect.Effect<Binding, ConversationWorkspaceError>;
  readonly fallback: (threadId: string, driver: string, reason: string) => Effect.Effect<void, ConversationWorkspaceError>;
  readonly sharing: (threadId: string) => Effect.Effect<readonly string[], ConversationWorkspaceError>;
  readonly inherit: (threadId: string, parentThreadId: string) => Effect.Effect<void, ConversationWorkspaceError>;
  readonly startTurn: (threadId: string, runId: string) => Effect.Effect<{ binding: Binding; context: string } | null, ConversationWorkspaceError>;
  readonly snapshot: (threadId: string) => Effect.Effect<SourceSnapshot, ConversationWorkspaceError>;
  readonly readHandoff: (threadId: string) => Effect.Effect<HandoffRecord | null, ConversationWorkspaceError>;
  readonly source: (threadId: string, request: SourceRequest) => Effect.Effect<{ commit: string; content: string }, ConversationWorkspaceError>;
  readonly ownsPath: (cwd: string | null) => boolean;
  readonly acceptsCwd: (threadId: string, cwd: string | null) => Effect.Effect<boolean, ConversationWorkspaceError>;
  readonly markAttached: (threadId: string) => Effect.Effect<void, ConversationWorkspaceError>;
  readonly dispose: (threadId: string, attached: boolean) => Effect.Effect<string | null, ConversationWorkspaceError>;
  readonly assertAttached: (threadId: string, cwd?: string) => Effect.Effect<void, ConversationWorkspaceError>;
  readonly reserveHandoff: (input: { threadId: string; requestId: string; request: string; commit: string; baseRef: string; startedFromOrigin: boolean; members?: readonly string[] }) => Effect.Effect<HandoffRecord, ConversationWorkspaceError>;
  readonly updateHandoff: (record: HandoffRecord, patch: Partial<Pick<HandoffRecord, "phase" | "path" | "setup" | "result">>) => Effect.Effect<HandoffRecord, ConversationWorkspaceError>;
}>()("jones/conversationWorkspace/ConversationPolicy") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const config = yield* ServerConfig.ServerConfig;
  const crypto = yield* Crypto.Crypto;
  const paths = yield* Path.Path;
  // Registration breaks the construction cycle: ThreadManagement owns the
  // orchestrator which constructs ProviderTurnStart before WorktreeMcpService.
  // No attachment can run until the outer production service layer is ready.
  let turnAttachment: TurnAttachment | undefined;
  const directoryLocks = yield* KeyedLock.make<string>();
  const wrap = (threadId: string) => Effect.mapError((cause: unknown) => Schema.is(ConversationWorkspaceError)(cause) ? cause : new ConversationWorkspaceError({ threadId, reason: "storage", cause }));
  const lookup = Effect.fn("ConversationPolicy.lookup")(function* (threadId: string) {
    const rows = yield* sql<{ binding: string }>`SELECT binding FROM jones_conversation_bindings WHERE thread_id = ${threadId}`;
    return rows[0] === undefined ? null : yield* decodeBinding(rows[0].binding);
  }, (effect, threadId) => effect.pipe(wrap(threadId)));
  const save = Effect.fn("ConversationPolicy.save")(function* (binding: Binding) {
    const owners = yield* sql<{ payload_json: string }>`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id=${binding.threadId} AND deleted_at IS NULL`;
    if (!owners[0]) return yield* new ConversationWorkspaceError({ threadId: binding.threadId, reason: "storage" });
    const owner = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Struct({ id: Schema.String, projectId: Schema.String })))(owners[0].payload_json);
    if (owner.id !== binding.threadId || owner.projectId !== binding.projectId) return yield* new ConversationWorkspaceError({ threadId: binding.threadId, reason: "storage" });
    yield* sql`INSERT INTO jones_conversation_bindings(thread_id,binding) VALUES(${binding.threadId},${JSON.stringify(binding)}) ON CONFLICT(thread_id) DO UPDATE SET binding=excluded.binding`;
  }, (effect, binding) => effect.pipe(sql.withTransaction, wrap(binding.threadId)));
  const allocate = Effect.fn("ConversationPolicy.allocate")(function* (input: { threadId: string; projectId: string; projectRoot: string; baseRef?: string; excluded?: readonly string[] }) {
    const previous = yield* lookup(input.threadId);
    if (previous !== null) return previous;
    const snapshot = yield* io(input.threadId, "source", () => resolveSource(input.projectRoot, input.baseRef));
    const projectRoots = yield* sql<{ root: string }>`SELECT workspace_root AS root FROM projection_projects WHERE deleted_at IS NULL`;
    const checkoutRoots = yield* sql<{ root: string | null }>`SELECT json_extract(payload_json, '$.worktreePath') AS root FROM orchestration_v2_projection_threads WHERE deleted_at IS NULL`;
    const excluded = [input.projectRoot, snapshot.commonDirectory, config.worktreesDir, ...projectRoots.map((row) => row.root), ...checkoutRoots.flatMap((row) => row.root === null ? [] : [row.root]), ...(input.excluded ?? [])];
    const directory = yield* io(input.threadId, "storage", () => createConversationDirectory(config.stateDir, excluded));
    const binding: Binding = { ...directory, ...snapshot, threadId: input.threadId, projectId: input.projectId, ownerThreadId: input.threadId, state: "conversation" };
    yield* save(binding).pipe(Effect.onError(() => io(input.threadId, "storage", () => disposeConversationDirectory(directory)).pipe(Effect.tap((outcome) => outcome === "conversation_dir_nonempty" ? Effect.logError("conversation_dir_nonempty", { threadId: input.threadId, path: directory.path, owner: input.threadId }) : Effect.void))));
    return binding;
  });
  const inherit = Effect.fn("ConversationPolicy.inherit")(function* (threadId: string, parentThreadId: string) {
    if ((yield* lookup(threadId)) !== null) return;
    const binding = yield* lookup(parentThreadId);
    if (binding !== null) {
      const handoff = yield* readHandoff(binding.ownerThreadId);
      if (handoff !== null && handoff.phase !== "done") return yield* new ConversationWorkspaceError({ threadId, reason: "attach_required" });
      yield* save({ ...binding, threadId });
    }
  });
  const sharing = Effect.fn("ConversationPolicy.sharing")(function* (threadId: string) {
    const binding = yield* lookup(threadId);
    if (binding === null) return [threadId];
    const rows = yield* sql<{ thread_id: string }>`SELECT b.thread_id FROM jones_conversation_bindings b JOIN orchestration_v2_projection_threads t ON t.thread_id=b.thread_id WHERE json_extract(b.binding, '$.path')=${binding.path} AND t.deleted_at IS NULL ORDER BY b.thread_id`;
    return rows.map((row) => row.thread_id);
  }, (effect, threadId) => effect.pipe(wrap(threadId)));
  const startTurn = Effect.fn("ConversationPolicy.startTurn")(function* (threadId: string, runId: string) {
    let binding = yield* lookup(threadId);
    if (binding === null || binding.state === "attached") return null;
    if (binding.state === "disposed") {
      const resumed = yield* io(threadId, "storage", () => resumeConversationDirectory(binding!));
      binding = { ...binding, ...resumed, state: "conversation" };
      yield* save(binding);
    }
    const turnBinding = binding;
    const directoryState = yield* io(threadId, "storage", () => inspectConversationDirectory(turnBinding));
    if (directoryState === "conversation_dir_nonempty" || binding.state === "conversation_dir_nonempty") return yield* new ConversationWorkspaceError({ threadId, reason: "directory_nonempty" });
    if (directoryState === "absent") return yield* new ConversationWorkspaceError({ threadId, reason: "storage" });
    const previous = yield* sql<{ snapshot: string }>`SELECT snapshot FROM jones_conversation_turns WHERE run_id=${runId} AND thread_id=${threadId}`;
    const snapshot = previous[0] === undefined
      ? yield* io(threadId, "source", () => resolveSource(turnBinding.commonDirectory, turnBinding.baseRef))
      : yield* decodeSnapshot(previous[0].snapshot);
    yield* Effect.gen(function* () {
      yield* sql`INSERT OR IGNORE INTO jones_conversation_turns(run_id,thread_id,snapshot) VALUES(${runId},${threadId},${JSON.stringify(snapshot)})`;
      yield* sql`UPDATE jones_conversation_bindings SET active_run_id=${runId} WHERE thread_id=${threadId}`;
    }).pipe(sql.withTransaction);
    const instructions: string[] = [];
    const rootListing = yield* io(threadId, "source", () => readSource(snapshot, { operation: "list" }));
    for (const file of ["AGENTS.md", "CLAUDE.md"]) {
      const exists = rootListing.split("\0").some((entry) => entry.endsWith(`\t${file}`));
      if (exists) instructions.push(file);
    }
    return { binding: { ...turnBinding, ...snapshot }, context: `Conversation workspace: cwd ${turnBinding.path}. Source commit ${snapshot.commit} (base ${snapshot.baseRef}), recorded for run ${runId}. Read source with t3_source_read, t3_source_list, t3_source_search and t3_source_history. Root instructions at this commit: ${instructions.join(", ") || "none"}; read them through t3_source_read. Before writing or opening a terminal, call t3_worktree_handoff with a stable clientRequestId and continuationPrompt; it checks out this recorded commit and ends this turn. Project instructions and memory are not automatically loaded in this directory. This is not an OS or provider filesystem barrier.` };
  }, (effect, threadId) => effect.pipe(wrap(threadId)));
  const snapshot = Effect.fn("ConversationPolicy.snapshot")(function* (threadId: string) {
    const rows = yield* sql<{ snapshot: string }>`SELECT t.snapshot FROM jones_conversation_turns t JOIN jones_conversation_bindings b ON b.active_run_id=t.run_id AND b.thread_id=t.thread_id WHERE b.thread_id=${threadId}`;
    if (rows[0]) return yield* decodeSnapshot(rows[0].snapshot);
    const binding = yield* lookup(threadId);
    if (binding === null) return yield* new ConversationWorkspaceError({ threadId, reason: "source" });
    return { commonDirectory: binding.commonDirectory, commit: binding.commit, baseRef: binding.baseRef };
  }, (effect, threadId) => effect.pipe(wrap(threadId)));
  const source = Effect.fn("ConversationPolicy.source")(function* (threadId: string, request: SourceRequest) {
    const recorded = yield* snapshot(threadId);
    return { commit: recorded.commit, content: yield* io(threadId, "source", () => readSource(recorded, request)) };
  });
  const dispose = Effect.fn("ConversationPolicy.dispose")(function* (threadId: string, attached: boolean) {
    const binding = yield* lookup(threadId);
    if (binding === null || (binding.state === "attached" && !attached)) return null;
    // Shared delegates keep the directory until every binding has attached or settled.
    const peers = yield* sql`SELECT 1 FROM jones_conversation_bindings WHERE thread_id != ${threadId} AND json_extract(binding, '$.path') = ${binding.path} AND json_extract(binding, '$.state') NOT IN ('attached','disposed') LIMIT 1`;
    const shared = peers.length !== 0;
    const outcome = shared ? "shared" : yield* io(threadId, "storage", () => disposeConversationDirectory(binding));
    const state = attached ? "attached" : outcome === "conversation_dir_nonempty" ? "conversation_dir_nonempty" : "disposed";
    yield* save({ ...binding, state });
    if (outcome === "conversation_dir_nonempty") {
      yield* sql`INSERT INTO jones_conversation_obligations(thread_id,path,reason) VALUES(${binding.ownerThreadId},${binding.path},'conversation_dir_nonempty') ON CONFLICT(path) DO NOTHING`;
      yield* Effect.logWarning("conversation_dir_nonempty", { threadId, owner: binding.ownerThreadId, path: binding.path });
    }
    return outcome;
  }, (effect, threadId) => effect.pipe(wrap(threadId)));
  const assertAttached = Effect.fn("ConversationPolicy.assertAttached")(function* (threadId: string, cwd?: string) {
    const binding = yield* lookup(threadId);
    if (ownsConversationPath(config.stateDir, cwd ?? null) || (binding !== null && binding.state !== "attached")) return yield* new ConversationWorkspaceError({ threadId, reason: "attach_required" });
  });
  const readHandoff = Effect.fn("ConversationPolicy.readHandoff")(function* (threadId: string) {
    const rows = yield* sql<{ record: string }>`SELECT record FROM jones_conversation_handoffs WHERE thread_id=${threadId}`;
    return rows[0] === undefined ? null : yield* decodeHandoff(rows[0].record);
  }, (effect, threadId) => effect.pipe(wrap(threadId)));
  const reserveHandoff = Effect.fn("ConversationPolicy.reserveHandoff")(function* (input: { threadId: string; requestId: string; request: string; commit: string; baseRef: string; startedFromOrigin: boolean; members?: readonly string[] }) {
    const rows = yield* sql<{ record: string }>`SELECT record FROM jones_conversation_handoffs WHERE thread_id=${input.threadId}`;
    if (rows[0]) {
      const record = yield* decodeHandoff(rows[0].record);
      if (record.requestId !== input.requestId || record.request !== input.request) return yield* new ConversationWorkspaceError({ threadId: input.threadId, reason: "handoff_conflict" });
      return record;
    }
    const record: HandoffRecord = { ...input, members: input.members ?? [input.threadId], revision: 0, phase: "reserved", path: paths.join(config.worktreesDir, `conversation-${yield* crypto.randomUUIDv4().pipe(Effect.orDie)}`), setup: null, result: null };
    yield* sql`INSERT INTO jones_conversation_handoffs(thread_id,record) VALUES(${input.threadId},${JSON.stringify(record)})`;
    return record;
  }, (effect, input) => effect.pipe(sql.withTransaction, wrap(input.threadId)));
  const updateHandoff = Effect.fn("ConversationPolicy.updateHandoff")(function* (record: HandoffRecord, patch: Partial<Pick<HandoffRecord, "phase" | "path" | "setup" | "result">>) {
    const next = { ...record, ...patch, revision: record.revision + 1 };
    const rows = yield* sql`UPDATE jones_conversation_handoffs SET record=${JSON.stringify(next)} WHERE thread_id=${record.threadId} AND json_extract(record, '$.revision')=${record.revision} RETURNING thread_id`;
    if (rows.length !== 1) return yield* new ConversationWorkspaceError({ threadId: record.threadId, reason: "handoff_conflict" });
    return next;
  }, (effect, record) => effect.pipe(wrap(record.threadId)));
  const acceptsCwd = Effect.fn("ConversationPolicy.acceptsCwd")(function* (threadId: string, cwd: string | null) {
    const binding = yield* lookup(threadId);
    if (binding === null || binding.threadId !== threadId || binding.state !== "conversation" || binding.path !== cwd) return false;
    return (yield* io(threadId, "storage", () => inspectConversationDirectory(binding))) === "empty";
  });
  const markAttached = Effect.fn("ConversationPolicy.markAttached")(function* (threadId: string) {
    const binding = yield* lookup(threadId);
    if (binding !== null) yield* save({ ...binding, state: "attached" });
  });
  return ConversationPolicy.of({
    registerTurnAttachment: (attach) => Effect.sync(() => { turnAttachment = attach; }),
    attachForTurn: (threadId, runId, text, attachments) => Effect.suspend(() => turnAttachment === undefined
      ? Effect.fail(new ConversationWorkspaceError({ threadId, reason: "attach_required" }))
      : turnAttachment(threadId, runId, text, attachments).pipe(wrap(threadId))),
    ownsPath: (cwd) => ownsConversationPath(config.stateDir, cwd), lookup, sharing, allocate: (input) => directoryLocks.withLock(input.threadId, allocate(input).pipe(Effect.uninterruptible, wrap(input.threadId))), inherit, startTurn: (threadId, runId) => directoryLocks.withLock(threadId, startTurn(threadId, runId)), snapshot, readHandoff, source, acceptsCwd, markAttached, dispose, assertAttached, reserveHandoff, updateHandoff,
    fallback: Effect.fn("ConversationPolicy.fallback")((threadId: string, driver: string, reason: string) => sql`INSERT OR IGNORE INTO jones_conversation_fallbacks(thread_id,driver,reason) VALUES(${threadId},${driver},${reason})`.pipe(Effect.asVoid, wrap(threadId))),
  });
});
export const layer = Layer.effect(ConversationPolicy, make);
