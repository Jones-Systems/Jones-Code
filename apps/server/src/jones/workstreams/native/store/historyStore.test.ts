import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as HistoryStore from "./historyStore.ts";
import { now, scope, seed, testLayer } from "./sql.testkit.ts";

it.effect(
  "history uses bytewise IDs at shared versions and excludes later appends at the captured high-water",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const history = yield* HistoryStore.HistoryStore;
      for (const eventId of ["event:z", "event:a", "event:Z"])
        yield* history.append(scope, {
          event_id: eventId,
          command_id: "rejected:command",
          registry_version: 0,
          occurred_at: now,
          workstream_ids: ["ws:a", "ws:b"],
          value: { sentinel: "audit" },
        });
      const first = yield* history.page(scope, { limit: 1, scope_key: "workstream:ws:a" });
      expect(first.events.map((event) => event.event_id)).toEqual(["event:Z"]);
      yield* history.append(scope, {
        event_id: "event:b",
        command_id: "noop",
        registry_version: 0,
        occurred_at: now,
        workstream_ids: ["ws:a"],
        value: {},
      });
      const rest = yield* history.page(scope, {
        limit: 7,
        scope_key: "workstream:ws:a",
        high_water: first.high_water,
        after: { registry_version: 0, event_id: "event:Z" },
      });
      expect(rest.events.map((event) => event.event_id)).toEqual(["event:a", "event:z"]);
      expect((yield* history.page(scope, { limit: 100 })).events).toHaveLength(4);
      expect(
        (yield* history.page(scope, { limit: 100, scope_key: "workstream:ws:b" })).events,
      ).toHaveLength(3);
    }).pipe(Effect.provide(testLayer())),
);

it.effect("native event sequences continue after the retained legacy sequence", () =>
  Effect.gen(function* () {
    yield* seed;
    const history = yield* HistoryStore.HistoryStore;
    const event = {
      event_id: "legacy:event",
      command_id: "legacy:command",
      registry_version: 0,
      occurred_at: now,
      workstream_ids: [],
      value: { preserved: true },
      legacy_event_sequence: 500,
    };
    expect(yield* history.append(scope, event)).toBe(500);
    expect(
      yield* history.append(scope, {
        ...event,
        event_id: "legacy:earlier",
        legacy_event_sequence: 499,
      }),
    ).toBe(499);
    const { legacy_event_sequence: _legacy, ...native } = event;
    expect(yield* history.append(scope, { ...native, event_id: "native:event" })).toBe(501);
    expect((yield* history.page(scope, { limit: 100 })).high_water).toBe(501);
  }).pipe(Effect.provide(testLayer())),
);
