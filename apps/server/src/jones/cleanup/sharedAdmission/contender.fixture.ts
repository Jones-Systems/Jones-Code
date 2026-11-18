// Executed only by the independent-process test. One JSON input, one result,
// owned synthetic SQLite connections, then normal scoped shutdown.
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The standalone contender reads its one input from inherited descriptor 0; FileSystem.readFileString accepts paths, not that retained descriptor.
import * as NodeFS from "node:fs";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { makeGraph, openSql } from "./fixtures.ts";
import { JournalBinding } from "./Journal.ts";
import { ParticipantIdentity, ReservationBinding } from "./Protocol.ts";

const decodeInput = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      localPath: Schema.String,
      commonPath: Schema.String,
      identity: ParticipantIdentity,
      binding: JournalBinding,
      reservation: ReservationBinding,
    }),
  ),
);
const encodeResult = Schema.encodeSync(
  Schema.fromJsonString(
    Schema.Struct({
      outcome: Schema.Literals(["Success", "Failure"]),
      stage: Schema.String,
    }),
  ),
);
const input = decodeInput(NodeFS.readFileSync(0, "utf8"));
let stage = "construction";
await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const local = yield* openSql(input.localPath);
      const common = yield* openSql(input.commonPath);
      yield* common`PRAGMA busy_timeout=0`;
      yield* common`PRAGMA synchronous=FULL`;
      // Enrollment occurred before the parent intentionally takes the shared writer
      // lock. Building a graph also validates identity/pins without changing them.
      const graph = yield* makeGraph({
        local,
        common,
        identity: input.identity,
        binding: input.binding,
        initialization: "existing",
      });
      stage = "reserve";
      const result = yield* Effect.exit(graph.journal.reserveScopes(input.reservation));
      process.stdout.write(encodeResult({ outcome: result._tag, stage }) + "\n");
    }),
  ),
).catch(() => {
  process.stdout.write(encodeResult({ outcome: "Failure", stage }) + "\n");
});
