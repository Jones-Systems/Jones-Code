import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import {
  readProviderStatusCache,
  writeProviderStatusCache,
} from "../../../provider/providerStatusCache.ts";
import { makeCatalogCachePublication } from "./cachePublication.ts";
import { instanceA, makeGate, providerSnapshot } from "./catalog.testkit.ts";

it.effect.each([
  { cancelled: false, label: "completed" },
  { cancelled: true, label: "cancelled" },
])("a $label cache claim cannot land over its successor", ({ cancelled }) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = yield* fs.makeTempDirectoryScoped({ prefix: "jones-catalog-cache-order-" });
    const filePath = path.join(root, "provider.json");
    const rename = yield* makeGate();
    const claim = makeCatalogCachePublication();
    const older = claim(instanceA, () => true);
    const olderWrite = yield* older
      .run(
        writeProviderStatusCache({
          filePath,
          provider: providerSnapshot(instanceA, "older"),
          isCurrent: older.isCurrent,
        }),
      )
      .pipe(
        Effect.provideService(FileSystem.FileSystem, {
          ...fs,
          rename: (from, to) => rename.hold.pipe(Effect.andThen(fs.rename(from, to))),
        }),
        Effect.forkScoped,
      );
    yield* Effect.addFinalizer(() => rename.release);
    yield* Deferred.await(rename.started);
    if (cancelled) yield* Effect.sync(() => olderWrite.interruptUnsafe());
    const newer = claim(instanceA, () => true);
    const newerWrite = yield* newer
      .run(
        writeProviderStatusCache({
          filePath,
          provider: providerSnapshot(instanceA, "newer"),
          isCurrent: newer.isCurrent,
        }),
      )
      .pipe(Effect.forkScoped);
    yield* Effect.addFinalizer(() => rename.release);
    yield* rename.release;
    if (cancelled) {
      const exit = yield* Fiber.await(olderWrite);
      assert.isTrue(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause));
    } else yield* Fiber.join(olderWrite);
    yield* Fiber.join(newerWrite);
    assert.equal((yield* readProviderStatusCache(filePath))?.version, "newer");
    assert.deepStrictEqual(yield* fs.readDirectory(root), ["provider.json"]);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
