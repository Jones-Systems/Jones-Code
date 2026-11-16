import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { acquireFixture } from "./Conformance.testkit.ts";
import * as ProjectProduction from "./ProjectProduction.ts";
import { outputObligations } from "./ProducerFinalizer.ts";
import { binding } from "./OutputAttestation.ts";

it.effect.each(["succeeded", "failed", "cancelled"] as const)(
  "generic project script finalizes %s once without requiring optional provenance",
  (status) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      yield* Effect.acquireUseRelease(
        Effect.promise(acquireFixture),
        ({ worktree }) =>
          Effect.gen(function* () {
            const finish = yield* ProjectProduction.begin({
              threadId: "fixture",
              worktreePath: worktree,
              trigger: "setup",
            });
            yield* finish(status);
            yield* finish("cancelled");
            const obligations = yield* Effect.promise(() => outputObligations(worktree));
            expect(obligations).toHaveLength(0);
            const bound = yield* Effect.promise(() => binding(worktree));
            const directory = path.join(bound.git_dir, "workspace-completions");
            const names = yield* fs.readDirectory(directory);
            expect(names).toHaveLength(1);
            const record = JSON.parse(yield* fs.readFileString(path.join(directory, names[0]!)));
            expect(record.status).toBe(status);
            expect(record.proofStatus).toBe("deferred");
            expect(record.lineage.threadId).toBe("fixture");
          }),
        ({ base }) => fs.remove(base, { recursive: true, force: true }),
      );
    }).pipe(Effect.provide(NodeServices.layer)),
);
it.effect("optional production recording never prevents a project script from running", () =>
  Effect.gen(function* () {
    const finish = yield* ProjectProduction.begin({
      threadId: "fixture",
      worktreePath: "/does-not-exist/jones-synthetic",
      trigger: "setup",
    });
    yield* finish("succeeded");
  }),
);
