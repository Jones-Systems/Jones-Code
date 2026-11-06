import { expect, it } from "vitest";
import * as Effect from "effect/Effect";
import { fixture } from "./Conformance.testkit.ts";
import * as ProjectProduction from "./ProjectProduction.ts";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import { outputObligations } from "./ProducerFinalizer.ts";
import { binding } from "./OutputAttestation.ts";

for (const status of ["succeeded", "failed", "cancelled"] as const)
  it(
    "generic project script finalizes " + status + " once without requiring optional provenance",
    async () => {
      await fixture(async ({ worktree }) => {
        await Effect.runPromise(
          Effect.gen(function* () {
            const finish = yield* ProjectProduction.begin({
              threadId: "fixture",
              worktreePath: worktree,
              trigger: "setup",
            });
            yield* finish(status);
            yield* finish("cancelled");
          }),
        );
        const obligations = await outputObligations(worktree);
        expect(obligations).toHaveLength(0);
        const bound = await binding(worktree);
        const directory = Path.join(bound.git_dir, "workspace-completions");
        const names = await FS.readdir(directory);
        expect(names).toHaveLength(1);
        const record = JSON.parse(await FS.readFile(Path.join(directory, names[0]!), "utf8"));
        expect(record.status).toBe(status);
        expect(record.proofStatus).toBe("deferred");
        expect(record.lineage.threadId).toBe("fixture");
      });
    },
  );
it("optional production recording never prevents a project script from running", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const finish = yield* ProjectProduction.begin({
        threadId: "fixture",
        worktreePath: "/does-not-exist/jones-synthetic",
        trigger: "setup",
      });
      yield* finish("succeeded");
    }),
  );
});
