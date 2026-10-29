import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as WorkspaceEntries from "../../workspace/WorkspaceEntries.ts";
import * as WorkspacePaths from "../../workspace/WorkspacePaths.ts";
import { ServerCompute } from "../serverCompute/ServerCompute.ts";
import * as Persistence from "../../persistence/Sqlite.ts";
import { readerPoolOptions } from "./persistenceOptions.ts";
import { observeSafely } from "./observations.ts";
import { makePoolObservers } from "./metrics.ts";

it.effect("standalone path, legacy and memory layers construct without ServerConfig", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "jones-persistence-callers-" });
      for (const layer of [
        Persistence.layerMemory,
        Persistence.layerFromPath(path.join(directory, "legacy.sqlite")),
        Persistence.layerFromPath(path.join(directory, "pool.sqlite"), readerPoolOptions(3)),
      ]) {
        const result = yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          return yield* sql<{ value: number }>`SELECT 1 AS value`;
        }).pipe(Effect.provide(layer));
        assert.equal(result[0]?.value, 1);
      }
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("one shared layer identity is acquired once across independent consumers", () =>
  Effect.scoped(
    Effect.gen(function* () {
      class First extends Context.Service<First, object>()(
        "t3/jones/serverParallelism/composition.test/First",
      ) {}
      class Second extends Context.Service<Second, object>()(
        "t3/jones/serverParallelism/composition.test/Second",
      ) {}
      let acquisitions = 0;
      const compute = Layer.effect(
        ServerCompute,
        Effect.sync(() => {
          acquisitions++;
          return ServerCompute.of({
            acquireLogical: () => Effect.die("No native acquisition in construction test"),
            run: () => Effect.die("No native job in construction test"),
          });
        }),
      );
      // Actual WorkspaceEntries -> WorkspaceSearchIndexMap service construction,
      // with only the external compute resource constructor counted here.
      const shared = WorkspaceEntries.layer.pipe(
        Layer.provide(WorkspacePaths.layer),
        Layer.provide(compute),
      );
      const consumers = Layer.merge(
        Layer.effect(First, WorkspaceEntries.WorkspaceEntries).pipe(Layer.provide(shared)),
        Layer.effect(Second, WorkspaceEntries.WorkspaceEntries).pipe(Layer.provide(shared)),
      );
      yield* Effect.gen(function* () {
        assert.strictEqual(yield* First, yield* Second);
        assert.equal(acquisitions, 1);
      }).pipe(Effect.provide(consumers.pipe(Layer.provide(NodeServices.layer))));
    }),
  ),
);

it.effect("observation callbacks never interfere and profile options are explicit", () =>
  Effect.gen(function* () {
    const options = readerPoolOptions(2, () => {
      throw new Error("sink failed");
    });
    assert.deepEqual(
      { ...options, observe: undefined },
      {
        size: 2,
        interactiveQueueCapacity: 64,
        backgroundQueueCapacity: 64,
        interactiveQueueTimeoutMs: 10_000,
        backgroundQueueTimeoutMs: 30_000,
        leaseWatchdogMs: 60_000,
        observe: undefined,
      },
    );
    const event = {
      phase: "queue" as const,
      priority: "interactive" as const,
      slotId: 0,
      generation: 1,
      durationMs: 2,
      outcome: "ok" as const,
      queueDepth: 0,
    };
    options.observe?.(event);
    observeSafely(() => {
      throw new Error("sink failed");
    })(event);
    const observers = yield* makePoolObservers("laptop");
    observers.reader(event);
  }),
);
