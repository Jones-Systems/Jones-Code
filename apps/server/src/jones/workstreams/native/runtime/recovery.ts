import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Clock from "effect/Clock";
import * as Schema from "effect/Schema";
import * as Entries from "../operations/durableEntries.ts";
import * as Operations from "../../operations/service.ts";
import * as Callers from "./recordedCaller.ts";

export class RecoveryError extends Schema.TaggedError<RecoveryError>()("NativeRecoveryError", {
  stage: Schema.Literals(["operations", "reservations"]),
}) {}
/** The parent EffectRunner selects one native reservation; imported effects use
 * their legacy reconciliation port or remain fenced. Never mount recover-all.
 */
export class RawRecovery extends Context.Service<
  RawRecovery,
  {
    readonly pending: () => Effect.Effect<
      readonly {
        readonly command_id: string;
        readonly origin: "native" | "legacy_open";
        readonly next_poll_at: string | null;
        readonly recorded_caller: Callers.RecordedCallerV1 | null;
      }[],
      RecoveryError
    >;
    readonly recoverOne: (
      commandId: string,
    ) => Effect.Effect<void, Callers.RecordedCallerFence, Callers.RecoveryCaller>;
  }
>()("t3/jones/workstreams/native/runtime/recovery/RawRecovery") {}
type Sweep = { readonly attempted: number; readonly fenced: number; readonly deferred: number };
export class NativeWorkstreamsRecovery extends Context.Service<
  NativeWorkstreamsRecovery,
  {
    readonly sweep: () => Effect.Effect<Sweep, RecoveryError>;
  }
>()("t3/jones/workstreams/native/runtime/recovery/NativeWorkstreamsRecovery") {}

const due = (at: string | null, now: number) =>
  at === null || (Number.isFinite(Date.parse(at)) && Date.parse(at) <= now);
const make = Effect.gen(function* () {
  const entries = yield* Entries.DurableEntries;
  const operations = yield* Operations.WorkstreamOperations;
  const callers = yield* Callers.RecordedCallers;
  const raw = yield* RawRecovery;
  let sweeping = false;
  const sweep = Effect.fn("NativeWorkstreamsRecovery.sweep")(() =>
    Effect.suspend(() => {
      if (sweeping) return Effect.succeed({ attempted: 0, fenced: 0, deferred: 0 });
      sweeping = true;
      return Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        const result = { attempted: 0, fenced: 0, deferred: 0 };
        const pending = yield* entries
          .open()
          .pipe(Effect.mapError(() => new RecoveryError({ stage: "operations" })));
        for (const saved of pending) {
          if (!due(saved.operation.next_poll_at, now)) {
            result.deferred++;
            continue;
          }
          if (!saved.operation.recorded_caller) {
            result.fenced++;
            continue;
          }
          const resumed = yield* operations
            .resumeRecorded(saved.operation.operation_id)
            .pipe(Effect.result);
          if (resumed._tag === "Failure") result.fenced++;
          else result.attempted++;
        }
        for (const item of yield* raw.pending()) {
          if (!due(item.next_poll_at, now)) {
            result.deferred++;
            continue;
          }
          if (item.origin !== "native" || !item.recorded_caller) {
            result.fenced++;
            continue;
          }
          const record = item.recorded_caller;
          const recovered = yield* Effect.gen(function* () {
            const caller = yield* callers.readmitRecorded(record);
            yield* caller
              .check("workstreams:write", null)
              .pipe(
                Effect.mapError(() => new Callers.RecordedCallerFence({ code: "not_granted" })),
              );
            yield* Callers.withRecordedCaller(record, caller, raw.recoverOne(item.command_id));
          }).pipe(Effect.result);
          if (recovered._tag === "Failure") result.fenced++;
          else result.attempted++;
        }
        return result;
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            sweeping = false;
          }),
        ),
      );
    }),
  );
  // One scoped fiber owns startup and cadence. Ordinary reads never trigger it.
  yield* Effect.gen(function* () {
    for (;;) {
      yield* sweep().pipe(Effect.catch(() => Effect.void));
      yield* Effect.sleep(30_000);
    }
  }).pipe(Effect.forkScoped);
  return NativeWorkstreamsRecovery.of({ sweep });
});
export const layer = Layer.effect(NativeWorkstreamsRecovery, make);
