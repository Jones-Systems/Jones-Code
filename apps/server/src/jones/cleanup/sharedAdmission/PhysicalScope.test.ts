import { assert, describe, it } from "@effect/vitest";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Physical alias and inode-replacement fixtures deliberately mutate exact owned native directories.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native fixture setup/teardown shares synchronous host paths with its retained file/process handles and keeps the existing fixture context.
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import { bindingFor, fixture, removalFor } from "./fixtures.ts";
import * as P from "./Protocol.ts";
import { overlaps } from "./PhysicalScope.ts";
import { fixedAccountJournalPath } from "./Journal.ts";

describe("physical scope observations", () => {
  it.effect.each(["alias", "ancestor", "descendant", "exact"] as const)(
    "%s reservation blocks removal across stores without blocking a sibling",
    (variant) =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          const path =
            variant === "alias"
              ? NodePath.join(f.directory, "alias/a")
              : variant === "ancestor"
                ? f.rootPath
                : variant === "descendant"
                  ? NodePath.join(f.aPath, "nested")
                  : f.aPath;
          const scope = yield* f.b.observations.capture(path);
          assert.isTrue(overlaps(f.scope, scope));
          if (variant !== "ancestor") assert.isFalse(overlaps(scope, f.unrelated));
          yield* P.reserveScopes(bindingFor(f.b.identity.storeId, [scope])).pipe(
            Effect.provide(f.b.context),
          );
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(P.reserveScopes(removalFor(f)).pipe(Effect.provide(f.a.context))),
            ),
          );
          // Root metadata is descriptive; it did not prevent enrolling this live
          // ancestor/alias writer, and cannot erase it by re-enrollment.
          assert.strictEqual(yield* f.a.journal.enrollRoot(f.root), f.slot.rootId);
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(P.reserveScopes(removalFor(f)).pipe(Effect.provide(f.a.context))),
            ),
          );
        }),
      ),
  );
  it.effect(
    "ancestor use predates new root enrollment; structural enrollment never grants exclusion",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          // The workspace ancestor excludes the synthetic stores' protected infrastructure.
          const parent = yield* f.b.observations.capture(f.rootPath);
          yield* P.reserveScopes(bindingFor(f.b.identity.storeId, [parent])).pipe(
            Effect.provide(f.b.context),
          );
          const newRootPath = NodePath.join(f.rootPath, "second-scratch");
          NodeFS.mkdirSync(newRootPath);
          const newRoot = yield* f.a.observations.capture(newRootPath);
          yield* f.a.journal.enrollRoot(newRoot);
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(P.reserveScopes(removalFor(f)).pipe(Effect.provide(f.a.context))),
            ),
          );
        }),
      ),
  );
  it.effect(
    "missing names retain nearest physical parent and normalized suffix; case variants conservatively overlap",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          const missing = yield* f.a.observations.capture(
            NodePath.join(f.rootPath, "Absent/child"),
          );
          const alias = yield* f.b.observations.capture(
            NodePath.join(f.directory, "alias/absent/CHILD"),
          );
          assert.deepStrictEqual(missing.unresolved, ["Absent", "child"]);
          assert.deepStrictEqual(missing.ancestors[0].identity, f.root.ancestors[0].identity);
          assert.isTrue(overlaps(missing, alias));
          assert.isTrue(overlaps(missing, f.scope));
          NodeFS.mkdirSync(NodePath.join(f.rootPath, "Absent"));
          assert.isTrue(Exit.isFailure(yield* Effect.exit(f.a.observations.revalidate(missing))));
        }),
      ),
  );
  it.effect("unsupported observations never fall back to raw path strings", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture();
        const plain = NodePath.join(f.directory, "plain-file");
        NodeFS.writeFileSync(plain, "synthetic");
        const dangling = NodePath.join(f.directory, "dangling");
        NodeFS.symlinkSync(NodePath.join(f.directory, "not-there"), dangling);
        for (const path of [
          "relative/path",
          `${f.rootPath}/../scratch`,
          plain,
          NodePath.join(plain, "child"),
          dangling,
        ])
          assert.isTrue(Exit.isFailure(yield* Effect.exit(f.a.observations.capture(path))));
      }),
    ),
  );
  it("fixed account location has no baseDir, XDG or runtime-home parameter", () => {
    assert.strictEqual(
      fixedAccountJournalPath({ home: "/synthetic/account", uid: "501" }),
      "/synthetic/account/.local/state/jones-code/workspace-admission-v1.sqlite",
    );
    assert.throws(() => fixedAccountJournalPath({ home: "relative", uid: "501" }));
  });
});
