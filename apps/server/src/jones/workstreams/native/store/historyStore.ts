import { canonicalWorkstreamsSnapshotJson } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import * as NativeSql from "./sql.ts";

export interface HistoryEvent {
  readonly event_id: string;
  readonly command_id: string;
  readonly registry_version: number;
  readonly occurred_at: string;
  readonly workstream_ids: readonly string[];
  readonly value: unknown;
  readonly legacy_event_sequence?: number;
}
export interface StoredHistoryEvent extends HistoryEvent {
  readonly event_sequence: number;
}
export interface HistoryPage {
  readonly server_generation: number;
  readonly registry_version: number;
  readonly high_water: number;
  readonly events: readonly StoredHistoryEvent[];
}
type Failure = SqlError | NativeSql.StoreError | NativeSql.CryptoError;
export class HistoryStore extends Context.Service<
  HistoryStore,
  {
    readonly append: (
      scope: NativeSql.OwnerScope,
      event: HistoryEvent,
    ) => Effect.Effect<number, Failure>;
    readonly page: (
      scope: NativeSql.OwnerScope,
      input: {
        scope_key?: string;
        limit: number;
        high_water?: number;
        after?: { registry_version: number; event_id: string };
        expected?: { server_generation: number; registry_version: number };
      },
    ) => Effect.Effect<HistoryPage, Failure>;
  }
>()("t3/jones/workstreams/native/store/historyStore") {}
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const db = yield* NativeSql.NativeSql;
  const append = Effect.fn("HistoryStore.append")(
    (scope: NativeSql.OwnerScope, event: HistoryEvent) =>
      db.transaction(
        scope,
        Effect.gen(function* () {
          const old = yield* sql<{
            event_sequence: number;
            envelope: string;
            registry_version: number;
          }>`SELECT event_sequence,envelope,registry_version FROM jones_workstreams_history WHERE registry_instance_id = ${scope.registry_instance_id} AND owner_id = ${scope.owner_id} AND event_id = ${event.event_id}`;
          if (old[0]) {
            const value = yield* db.openJson(
              {
                ...scope,
                purpose: "history_event",
                record_id: event.event_id,
                revision: old[0].registry_version,
              },
              old[0].envelope,
            );
            if (NativeSql.jsonText(value) !== NativeSql.jsonText(event))
              return yield* new NativeSql.StoreError({ reason: "idempotency_conflict" });
            return old[0].event_sequence;
          }
          const high = yield* sql<{
            high: number;
          }>`SELECT coalesce(max(event_sequence),0) AS high FROM jones_workstreams_history WHERE registry_instance_id = ${scope.registry_instance_id} AND owner_id = ${scope.owner_id}`;
          const sequence = event.legacy_event_sequence ?? (high[0]?.high ?? 0) + 1;
          const envelope = yield* db.sealJson(
            {
              ...scope,
              purpose: "history_event",
              record_id: event.event_id,
              revision: event.registry_version,
            },
            event,
          );
          const ids = [...new Set(event.workstream_ids)];
          yield* sql`INSERT INTO jones_workstreams_history ${sql.insert({ ...scope, event_id: event.event_id, event_sequence: sequence, legacy_event_sequence: event.legacy_event_sequence ?? null, command_id: event.command_id, registry_version: event.registry_version, occurred_at: event.occurred_at, workstream_ids: canonicalWorkstreamsSnapshotJson(ids), envelope })}`;
          for (const scopeKey of ["global", ...ids.map((id) => `workstream:${id}`)])
            yield* sql`INSERT INTO jones_workstreams_history_scopes ${sql.insert({ ...scope, scope_key: scopeKey, registry_version: event.registry_version, event_id: event.event_id, event_sequence: sequence })}`;
          return sequence;
        }),
      ),
  );
  const page = Effect.fn("HistoryStore.page")(
    (
      scope: NativeSql.OwnerScope,
      input: {
        scope_key?: string;
        limit: number;
        high_water?: number;
        after?: { registry_version: number; event_id: string };
        expected?: { server_generation: number; registry_version: number };
      },
    ) =>
      db.transaction(
        scope,
        Effect.gen(function* () {
          if (
            !Number.isSafeInteger(input.limit) ||
            input.limit < 1 ||
            input.limit > 1000 ||
            (input.high_water !== undefined &&
              (!Number.isSafeInteger(input.high_water) || input.high_water < 0))
          )
            return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
          const rows = yield* sql<{
            server_generation: number;
            registry_version: number;
            state: string;
          }>`SELECT server_generation,registry_version,state FROM jones_workstreams_registries WHERE registry_instance_id = ${scope.registry_instance_id} AND owner_id = ${scope.owner_id}`;
          const context = rows[0];
          if (!context || context.state !== "active")
            return yield* new NativeSql.StoreError({ reason: "fenced" });
          if (
            input.expected &&
            (input.expected.server_generation !== context.server_generation ||
              input.expected.registry_version !== context.registry_version)
          )
            return yield* new NativeSql.StoreError({ reason: "conflict" });
          const high = yield* sql<{
            high: number;
          }>`SELECT coalesce(max(event_sequence),0) AS high FROM jones_workstreams_history WHERE registry_instance_id = ${scope.registry_instance_id} AND owner_id = ${scope.owner_id}`;
          const highWater = input.high_water ?? high[0]?.high ?? 0;
          const events = yield* sql<{
            event_id: string;
            registry_version: number;
            event_sequence: number;
            envelope: string;
          }>`SELECT h.event_id,h.registry_version,h.event_sequence,h.envelope FROM jones_workstreams_history h JOIN jones_workstreams_history_scopes s ON s.registry_instance_id = h.registry_instance_id AND s.owner_id = h.owner_id AND s.event_id = h.event_id AND s.registry_version = h.registry_version
      WHERE h.registry_instance_id = ${scope.registry_instance_id} AND h.owner_id = ${scope.owner_id} AND s.scope_key = ${input.scope_key ?? "global"} AND h.event_sequence <= ${highWater}
      AND (h.registry_version > ${input.after?.registry_version ?? -1} OR (h.registry_version = ${input.after?.registry_version ?? -1} AND h.event_id COLLATE BINARY > ${input.after?.event_id ?? ""})) ORDER BY h.registry_version,h.event_id COLLATE BINARY LIMIT ${input.limit}`;
          const result: StoredHistoryEvent[] = [];
          for (const event of events) {
            const value = yield* db.openJson(
              {
                ...scope,
                purpose: "history_event",
                record_id: event.event_id,
                revision: event.registry_version,
              },
              event.envelope,
            );
            if (
              typeof value !== "object" ||
              value === null ||
              !("event_id" in value) ||
              value.event_id !== event.event_id
            )
              return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
            result.push({ ...(value as HistoryEvent), event_sequence: event.event_sequence });
          }
          return {
            server_generation: context.server_generation,
            registry_version: context.registry_version,
            high_water: highWater,
            events: result,
          };
        }),
      ),
  );
  return HistoryStore.of({ append, page });
});
/** @public Canonical service layer. */
export const layer = Layer.effect(HistoryStore, make);
