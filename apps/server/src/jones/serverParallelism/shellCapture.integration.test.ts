import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Statement from "effect/sql/Statement";
import * as Fixture from "../threadResultDependencies/testkit.ts";

it.effect("deferred shell decoding retains captured dependencies and watermark without acquiring SQL", () =>
  Effect.gen(function* () {
    const fixture = yield* Fixture.seed;
    yield* fixture.send("captured");
    const before = yield* fixture.projection.getShellSnapshot();
    const decode = yield* fixture.sql.withTransaction(fixture.projection.readShellSnapshot());
    yield* fixture.sql.withTransaction(fixture.store.release(fixture.waiter.id, "dependency:captured"));
    const rejectSql: Statement.Transformer = () => Effect.die("SQL during deferred decode");
    const captured = yield* decode.pipe(Effect.provideService(Statement.CurrentTransformer, rejectSql));
    assert.equal(captured.snapshotSequence, before.snapshotSequence);
    assert.deepEqual(captured, before);
    const fresh = yield* fixture.projection.getShellSnapshot();
    assert.equal(fresh.threads.find((thread) => thread.id === fixture.waiter.id)?.pendingBackgroundTasks?.length ?? 0, 0);
    assert.equal(captured.threads.find((thread) => thread.id === fixture.waiter.id)?.pendingBackgroundTasks?.length, 1);
    // The ordinary single-shell path continues to use the live dependency roster.
    const shell = yield* fixture.projection.getThreadShell(fixture.waiter.id);
    assert.deepEqual(shell?.pendingBackgroundTasks, []);
  }).pipe(Effect.provide(Layer.fresh(Fixture.layer))),
);
