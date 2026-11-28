import { assert, it } from "@effect/vitest";
import { ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import migration from "../persistence/Migrations/169_JonesAutoAccountBindings.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import { accountIdentityDigest } from "./accountBinding.ts";
import { makeAccountBindingStore } from "./accountBindingStore.ts";

it.effect(
  "keeps the admission with the accepted creation across readers and rolls back with it",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const threadId = ThreadId.make("synthetic-new-thread");
      const instanceId = ProviderInstanceId.make("account-a");
      const another = ProviderInstanceId.make("account-b");
      const digest = accountIdentityDigest("codex", "synthetic-native-account");
      const store = makeAccountBindingStore(sql, makeJonesTablePresence(sql));
      const creation = {
        threadId,
        payload: { modelSelection: { instanceId, model: "synthetic-model" } },
      };
      assert.isUndefined(yield* store.read(threadId, instanceId));
      yield* migration;
      const aborted = yield* store
        .commit({ instanceId, digest }, [creation])
        .pipe(Effect.andThen(Effect.fail("rollback")), sql.withTransaction, Effect.exit);
      assert.equal(aborted._tag, "Failure");
      assert.isUndefined(yield* store.read(threadId, instanceId));
      const mismatched = yield* Effect.exit(
        store.commit({ instanceId: another, digest }, [creation]),
      );
      assert.equal(mismatched._tag, "Failure");
      yield* sql.withTransaction(store.commit({ instanceId, digest }, [creation]));

      const reopened = makeAccountBindingStore(sql, makeJonesTablePresence(sql));
      assert.equal(yield* reopened.read(threadId, instanceId), digest);
      assert.isUndefined(yield* reopened.read(threadId, another));
      assert.notEqual(digest, accountIdentityDigest("codex", "changed-native-account"));
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
