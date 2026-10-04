import { ThreadId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { makeWorktreeOwnershipLeaseStore } from "./WorktreeOwnershipLease.ts";

const recordThreadCreation = Effect.fnUntraced(function* (
  threadId: ThreadId,
  eventId: string,
  streamVersion = 1,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO orchestration_events
    (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json)
    VALUES (${eventId}, 'thread', ${threadId}, ${streamVersion}, 'thread.created', '2026-01-01T00:00:00.000Z', 'user', '{}', '{}')`;
});

it.layer(SqlitePersistenceMemory)("WorktreeOwnershipLeaseStore", (it) => {
  it.effect("rotates same-owner generations and never grants expiry-only takeover", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const resourcePath = "/workspace/project";
      const ownerThreadId = ThreadId.make("thread-owner");
      const contenderThreadId = ThreadId.make("thread-contender");
      const ownerIncarnation = "event-owner-created";
      yield* recordThreadCreation(ownerThreadId, ownerIncarnation);
      yield* recordThreadCreation(contenderThreadId, "event-contender-created");

      const first = Option.getOrThrow(
        yield* store.acquire({
          resourcePath,
          leaseId: "lease-1",
          ownerThreadId,
          branch: "feature/owner",
          nowMs: 1_000,
          expiresAtMs: 2_000,
        }),
      );
      const recovered = Option.getOrThrow(
        yield* store.acquire({
          resourcePath,
          leaseId: "lease-2",
          ownerThreadId,
          branch: "feature/owner",
          nowMs: 1_500,
          expiresAtMs: 2_500,
        }),
      );

      assert.equal(recovered.acquiredAtMs, first.acquiredAtMs);
      assert.equal(recovered.leaseId, "lease-2");
      assert.equal(recovered.ownerIncarnation, ownerIncarnation);
      assert.equal(recovered.renewedAtMs, 1_500);
      assert.equal(recovered.expiresAtMs, 2_500);
      assert.isFalse(
        yield* store.renew({
          resourcePath,
          leaseId: "lease-1",
          ownerThreadId,
          ownerIncarnation,
          nowMs: 1_600,
          expiresAtMs: 2_600,
        }),
      );

      yield* store.release(first);
      assert.equal((yield* store.listAll())[0]?.leaseId, "lease-2");

      const expiredTakeover = yield* store.acquire({
        resourcePath,
        leaseId: "lease-3",
        ownerThreadId: contenderThreadId,
        branch: "feature/contender",
        nowMs: 10_000,
        expiresAtMs: 11_000,
      });
      assert.isTrue(Option.isNone(expiredTakeover));

      yield* store.release(recovered);
      const acquiredAfterRelease = yield* store.acquire({
        resourcePath,
        leaseId: "lease-4",
        ownerThreadId: contenderThreadId,
        branch: "feature/contender",
        nowMs: 10_001,
        expiresAtMs: 11_001,
      });
      assert.isTrue(Option.isSome(acquiredAfterRelease));
    }),
  );

  it.effect("does not recover a retained lease for a recreated thread id", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const resourcePath = "/workspace/recreated";
      const ownerThreadId = ThreadId.make("thread-reused-id");
      yield* recordThreadCreation(ownerThreadId, "event-old-incarnation");

      const retained = yield* store.acquire({
        resourcePath,
        leaseId: "lease-old-incarnation",
        ownerThreadId,
        branch: "feature/old",
        nowMs: 1_000,
        expiresAtMs: 2_000,
      });
      assert.isTrue(Option.isSome(retained));
      yield* recordThreadCreation(ownerThreadId, "event-new-incarnation", 2);

      const recreated = yield* store.acquire({
        resourcePath,
        leaseId: "lease-new-incarnation",
        ownerThreadId,
        branch: "feature/new",
        nowMs: 10_000,
        expiresAtMs: 11_000,
      });
      assert.isTrue(Option.isNone(recreated));
      assert.deepEqual(
        yield* store.getThreadIncarnation(ownerThreadId),
        Option.some("event-new-incarnation"),
      );
      assert.equal(
        (yield* store.listAll()).find((lease) => lease.resourcePath === resourcePath)?.leaseId,
        "lease-old-incarnation",
      );
    }),
  );

  it.effect("does not create a lease without an authoritative creation event", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const input = {
        resourcePath: "/workspace/missing-creation",
        leaseId: "lease-missing-creation",
        ownerThreadId: ThreadId.make("thread-without-creation"),
        branch: null,
        nowMs: 1_000,
        expiresAtMs: 2_000,
      };
      assert.isTrue(Option.isNone(yield* store.acquire(input)));
      assert.isFalse(
        (yield* store.listAll()).some((lease) => lease.resourcePath === input.resourcePath),
      );
    }),
  );
});
