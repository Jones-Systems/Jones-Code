import { ThreadId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { makeWorktreeOwnershipLeaseStore } from "./WorktreeOwnershipLease.ts";

it.layer(SqlitePersistenceMemory)("WorktreeOwnershipLeaseStore", (it) => {
  it.effect("rotates same-owner generations and never grants expiry-only takeover", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const resourcePath = "/workspace/project";
      const ownerThreadId = ThreadId.make("thread-owner");
      const contenderThreadId = ThreadId.make("thread-contender");
      const ownerIncarnation = "event-owner-created";

      const first = Option.getOrThrow(
        yield* store.acquire({
          resourcePath,
          leaseId: "lease-1",
          ownerThreadId,
          ownerIncarnation,
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
          ownerIncarnation,
          branch: "feature/owner",
          nowMs: 1_500,
          expiresAtMs: 2_500,
        }),
      );

      assert.equal(recovered.acquiredAtMs, first.acquiredAtMs);
      assert.equal(recovered.leaseId, "lease-2");
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
        ownerIncarnation: "event-contender-created",
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
        ownerIncarnation: "event-contender-created",
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

      const retained = yield* store.acquire({
        resourcePath,
        leaseId: "lease-old-incarnation",
        ownerThreadId,
        ownerIncarnation: "event-old-incarnation",
        branch: "feature/old",
        nowMs: 1_000,
        expiresAtMs: 2_000,
      });
      assert.isTrue(Option.isSome(retained));

      const recreated = yield* store.acquire({
        resourcePath,
        leaseId: "lease-new-incarnation",
        ownerThreadId,
        ownerIncarnation: "event-new-incarnation",
        branch: "feature/new",
        nowMs: 10_000,
        expiresAtMs: 11_000,
      });
      assert.isTrue(Option.isNone(recreated));
    }),
  );
});
