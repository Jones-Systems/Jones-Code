import { CommandId, MessageId, type ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";

export const TitleState = Schema.Struct({
  status: Schema.Literals(["active", "sufficient", "capped", "stopped"]),
  accepted: Schema.Array(MessageId),
  covered: Schema.Array(MessageId),
  knownInsufficient: Schema.Boolean,
  lastRequestId: Schema.NullOr(CommandId),
  pending: Schema.NullOr(
    Schema.Struct({ requestId: CommandId, evidence: Schema.Array(MessageId) }),
  ),
});
export type TitleState = typeof TitleState.Type;

export class ThreadTitleStateError extends Schema.TaggedError<ThreadTitleStateError>()(
  "ThreadTitleStateError",
  { cause: Schema.Defect() },
) {}

const isThreadTitleStateError = Schema.is(ThreadTitleStateError);

// Construct against the command owner's SQL client, without opening another connection.
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const present = makeJonesTablePresence(sql);
  const encode = Schema.encodeEffect(Schema.fromJsonString(TitleState));
  const decode = Schema.decodeUnknownEffect(Schema.fromJsonString(TitleState));
  const read = Effect.fn("ThreadTitleState.read")(
    function* (threadId: ThreadId) {
      if (!(yield* present("jones_thread_title_refinement"))) return null;
      const rows = yield* sql<{ state_json: string }>`SELECT state_json
      FROM jones_thread_title_refinement WHERE thread_id = ${threadId}
      AND EXISTS (SELECT 1 FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId})`;
      return rows[0] === undefined ? null : yield* decode(rows[0].state_json);
    },
    Effect.mapError((cause) => new ThreadTitleStateError({ cause })),
  );
  const available = present("jones_thread_title_refinement").pipe(
    Effect.mapError((cause) => new ThreadTitleStateError({ cause })),
  );
  const write = Effect.fn("ThreadTitleState.commit")(
    function* (threadId: ThreadId, state: TitleState) {
      // Invoked only inside EventSink's command transaction, after its replay gate.
      const thread =
        yield* sql`SELECT 1 FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId}`;
      if (thread.length === 0) return yield* new ThreadTitleStateError({ cause: "missing_thread" });
      const stateJson = yield* encode(state);
      yield* sql`INSERT INTO jones_thread_title_refinement (thread_id, state_json)
      VALUES (${threadId}, ${stateJson})
      ON CONFLICT(thread_id) DO UPDATE SET state_json = excluded.state_json`;
    },
    Effect.mapError((cause) =>
      isThreadTitleStateError(cause) ? cause : new ThreadTitleStateError({ cause }),
    ),
  );
  return { read, write, available };
});

// The upstream worker can run without Jones state; production composition
// supplies this reader on the command database.
export const AutomaticTitleState = Context.Reference<
  | {
      readonly read: (
        threadId: ThreadId,
      ) => Effect.Effect<TitleState | null, ThreadTitleStateError>;
    }
  | undefined
>("jones/threadTitle/State", { defaultValue: () => undefined });
export const layer = Layer.effect(AutomaticTitleState, make);
