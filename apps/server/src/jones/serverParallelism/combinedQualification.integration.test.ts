import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { combinedReaderWriterWindow } from "./combinedQualification.ts";

it.effect.each([2, 3] as const)(
  "retains every actual reader acquisition at the maximum window with N=%s",
  (count) =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "jones-sp-reader-window-" });
        const report = yield* Effect.promise(() =>
          combinedReaderWriterWindow(path.join(directory, "synthetic.sqlite"), count, 2_000, () =>
            Promise.resolve(),
          ),
        );
        assert.equal(report.observationLimitReached, false);
        assert.equal(report.readerQueue.count, 4_000);
        assert.equal(report.readerExecute.count, 4_000);
        assert.equal(report.writer.count, 2_000);
        assert.equal(report.control.count, 2_000);
        const queued = report.readerObservations.filter((event) => event.phase === "queue");
        assert.equal(queued.filter((event) => event.priority === "background").length, 1_000);
        assert.equal(queued.filter((event) => event.priority === "interactive").length, 3_000);
        assert.isTrue(queued.every((event) => event.outcome === "ok"));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
