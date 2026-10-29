import { CommandId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";

export interface Identity {
  readonly threadId: ThreadId;
  readonly commandId: CommandId;
  readonly requestKey: string;
  readonly generation: string;
}

export interface Intent extends Identity {
  readonly claimType: "thread.create" | "thread.metadata.update";
}

// Internal command context only: neither the wire command nor stored events gain Jones fields.
export const LaunchIntent = Context.Reference<Intent | undefined>(
  "jones/IdleWorktreeAdmission/LaunchIntent",
  { defaultValue: () => undefined },
);

type Phase =
  | "pending"
  | "worktree_issued"
  | "workspace_committed"
  | "setup_issued"
  | "prepared"
  | "ready"
  | "unresolved";

interface Record extends Identity {
  readonly phase: Phase;
  readonly worktreePath: string | null;
  readonly cleanupOwned: boolean;
}

export class IdleWorktreeAdmissionError extends Schema.TaggedError<IdleWorktreeAdmissionError>()(
  "IdleWorktreeAdmissionError",
  {
    threadId: ThreadId,
    reason: Schema.Literals([
      "preparing",
      "identity_conflict",
      "unresolved",
      "unavailable",
      "persistence",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.reason === "preparing"
      ? "The requested worktree is still being prepared. Retry this message after preparation completes."
      : `Idle worktree preparation is ${this.reason}; preserve its checkout and unresolved admission hold.`;
  }
}

interface Row {
  readonly thread_id: string;
  readonly launch_command_id: string;
  readonly request_key: string;
  readonly generation: string;
  readonly phase: Phase;
  readonly worktree_path: string | null;
  readonly cleanup_owned: number;
}

export const sameIdentity = (record: Identity, identity: Identity): boolean =>
  record.threadId === identity.threadId &&
  record.commandId === identity.commandId &&
  record.requestKey === identity.requestKey &&
  record.generation === identity.generation;

export const makeIdleWorktreeAdmission = (sql: SqlClient.SqlClient) => {
  const present = makeJonesTablePresence(sql);
  const persistence = (threadId: ThreadId) => (cause: unknown) =>
    cause instanceof IdleWorktreeAdmissionError
      ? cause
      : new IdleWorktreeAdmissionError({ threadId, reason: "persistence", cause });
  const read = Effect.fn("IdleWorktreeAdmission.read")(
    function* (threadId: ThreadId) {
      if (!(yield* present("jones_idle_worktree_admission"))) return null;
      const rows = yield* sql<Row>`SELECT * FROM jones_idle_worktree_admission
      WHERE thread_id = ${threadId}`;
      const row = rows[0];
      if (row === undefined) return null;
      const upstream = yield* sql`SELECT 1 FROM orchestration_v2_projection_threads
      WHERE thread_id = ${threadId} AND deleted_at IS NULL`;
      if (upstream.length === 0) return null;
      return {
        threadId: ThreadId.make(row.thread_id),
        commandId: CommandId.make(row.launch_command_id),
        requestKey: row.request_key,
        generation: row.generation,
        phase: row.phase,
        worktreePath: row.worktree_path,
        cleanupOwned: row.cleanup_owned === 1,
      } satisfies Record;
    },
    (effect, threadId) => effect.pipe(Effect.mapError(persistence(threadId))),
  );

  const current = Effect.fn("IdleWorktreeAdmission.current")(function* (identity: Identity) {
    const record = yield* read(identity.threadId);
    if (record === null || !sameIdentity(record, identity))
      return yield* new IdleWorktreeAdmissionError({
        threadId: identity.threadId,
        reason: "identity_conflict",
      });
    return record;
  });
  const assertWorkspace = Effect.fn("IdleWorktreeAdmission.assertWorkspace")(function* (
    identity: Identity,
    path: string,
  ) {
    const rows = yield* sql`SELECT 1 FROM orchestration_v2_projection_threads
      WHERE thread_id = ${identity.threadId} AND deleted_at IS NULL
        AND json_extract(payload_json, '$.worktreePath') = ${path}`;
    if (rows.length !== 1)
      return yield* new IdleWorktreeAdmissionError({
        threadId: identity.threadId,
        reason: "identity_conflict",
      });
  });
  const transition = Effect.fn("IdleWorktreeAdmission.transition")(
    function* (identity: Identity, from: ReadonlyArray<Phase>, to: Phase, path?: string) {
      const record = yield* current(identity);
      if (!from.includes(record.phase))
        return yield* new IdleWorktreeAdmissionError({
          threadId: identity.threadId,
          reason: "unresolved",
        });
      if (path !== undefined) yield* assertWorkspace(identity, path);
      yield* sql`UPDATE jones_idle_worktree_admission SET phase = ${to},
      worktree_path = ${path ?? record.worktreePath},
      cleanup_owned = ${to === "prepared" || to === "ready" || to === "unresolved" ? 0 : record.cleanupOwned ? 1 : 0}
      WHERE thread_id = ${identity.threadId} AND generation = ${identity.generation}`;
    },
    (effect, identity) =>
      effect.pipe(sql.withTransaction, Effect.mapError(persistence(identity.threadId))),
  );

  return {
    read,
    // The receipt, projection and hold share EventSink's transaction, before publication.
    accept: Effect.fn("IdleWorktreeAdmission.accept")(
      function* (input: {
        readonly intent: Intent;
        readonly commandId: CommandId;
        readonly threadId: ThreadId;
        readonly commandType: string;
        readonly events: ReadonlyArray<{ readonly type: string; readonly threadId: ThreadId }>;
      }) {
        const { intent } = input;
        if (!(yield* present("jones_idle_worktree_admission")))
          return yield* new IdleWorktreeAdmissionError({
            threadId: input.threadId,
            reason: "unavailable",
          });
        if (
          intent.threadId !== input.threadId ||
          intent.commandId !== input.commandId ||
          intent.claimType !== input.commandType ||
          !input.events.some(
            (event) =>
              event.threadId === input.threadId &&
              event.type ===
                (intent.claimType === "thread.create" ? "thread.created" : "thread.metadata-updated"),
          )
        )
          return yield* new IdleWorktreeAdmissionError({
            threadId: input.threadId,
            reason: "identity_conflict",
          });
        yield* sql`INSERT INTO jones_idle_worktree_admission
        (thread_id, launch_command_id, request_key, generation, phase)
        VALUES (${intent.threadId}, ${intent.commandId}, ${intent.requestKey}, ${intent.generation}, 'pending')`;
        yield* current(intent);
      },
      (effect, input) => effect.pipe(Effect.mapError(persistence(input.threadId))),
    ),
    issueWorktree: (identity: Identity) => transition(identity, ["pending"], "worktree_issued"),
    recordWorkspace: (identity: Identity, path: string) =>
      transition(identity, ["worktree_issued"], "workspace_committed", path),
    issueSetup: (identity: Identity) => transition(identity, ["workspace_committed"], "setup_issued"),
    prepareRelease: (identity: Identity, path: string) =>
      transition(identity, ["workspace_committed", "setup_issued"], "prepared", path),
    release: Effect.fn("IdleWorktreeAdmission.release")(
      function* (identity: Identity) {
        const record = yield* current(identity);
        if (record.phase === "ready") return;
        if (record.worktreePath === null || record.cleanupOwned)
          return yield* new IdleWorktreeAdmissionError({
            threadId: identity.threadId,
            reason: "unresolved",
          });
        yield* transition(identity, ["prepared"], "ready", record.worktreePath);
      },
      (effect, identity) =>
        effect.pipe(sql.withTransaction, Effect.mapError(persistence(identity.threadId))),
    ),
    markUnresolved: (identity: Identity) =>
      Effect.gen(function* () {
        const record = yield* current(identity);
        if (record.phase === "pending" || record.phase === "prepared" || record.phase === "ready")
          return;
        yield* transition(identity, [record.phase], "unresolved");
      }).pipe(sql.withTransaction, Effect.mapError(persistence(identity.threadId))),
    cleanupAllowed: (identity: Identity) =>
      read(identity.threadId).pipe(
        Effect.map(
          (record) =>
            record !== null &&
            sameIdentity(record, identity) &&
            record.cleanupOwned &&
            // Unconfirmed external steps, including a claimed checkout, grant no deletion proof.
            record.phase === "workspace_committed" &&
            record.worktreePath !== null,
        ),
      ),
  };
};

export type Store = ReturnType<typeof makeIdleWorktreeAdmission>;
