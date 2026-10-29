import * as NodeServices from "@effect/platform-node/NodeServices";
import { vi } from "vite-plus/test";
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

it.effect("retains reader/writer/control and setup boundaries while a diagnostic sink throws", () =>
  Effect.scoped(Effect.gen(function* () {
    vi.useFakeTimers({ toFake: ["Date", "performance"] });
    vi.setSystemTime(0);
    yield* Effect.addFinalizer(() => Effect.sync(() => { vi.useRealTimers(); }));
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "jones-sp-diagnostic-window-" });
    const events: import("./diagnosticObservation.ts").DiagnosticEvent[] = [];
    const report = yield* Effect.promise(() => combinedReaderWriterWindow(
      path.join(directory, "synthetic.sqlite"), 2, 4, () => Promise.resolve(), undefined,
      (event) => { events.push(event); throw new Error("diagnostic sink"); },
    ));
    assert.equal(report.readerQueue.count, 8);
    assert.equal(report.writer.count, 4);
    assert.equal(report.control.count, 4);
    assert.equal(events.filter((event) => event.kind === "combined" && event.event === "writer-begin").length, 4);
    assert.equal(events.filter((event) => event.kind === "combined" && event.event === "writer-end").length, 4);
    assert.equal(events.filter((event) => event.kind === "combined" && event.event === "control-end").length, 4);
    assert.equal(events.filter((event) => event.kind === "combined" && event.event === "reader-end").length, 4);
    const kinds = events.flatMap((event) => event.kind === "combined" ? [event.event] : []);
    assert.isTrue(kinds.indexOf("window-begin") < kinds.indexOf("database-ready"));
    assert.isTrue(kinds.indexOf("sql-load-end") < kinds.indexOf("window-end"));
    assert.isDefined(report.diagnosticCpu);
    assert.equal(vi.getMockedSystemTime()?.getTime(), 0);
    assert.equal(report.observationLimitReached, false);
  })).pipe(Effect.provide(NodeServices.layer)),
);
