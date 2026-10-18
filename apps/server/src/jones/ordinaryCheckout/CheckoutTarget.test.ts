import { assert, it } from "@effect/vitest";
import * as NodePath from "@effect/platform-node/NodePath";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import { CheckoutError } from "./CheckoutProtocol.ts";
import * as Target from "./CheckoutTarget.ts";

const input: Target.TargetInput = {
  projectId: "project",
  threadId: "thread",
  projectRoot: "/repo/sub",
  worktreePath: null,
  threadBranch: null,
  projectRevision: "project-1",
  threadRevision: "thread-1",
};
const observation = (
  options: {
    readonly branch?: string | null;
    readonly inode?: string;
    readonly missing?: string;
  } = {},
) =>
  Target.CheckoutTargetObservation.of({
    directory: (p) =>
      p === options.missing
        ? Effect.fail(new CheckoutError({ reason: "unavailable" }))
        : Effect.succeed({
            realPath: p.replace(/^\/alias/u, "/repo"),
            identity: {
              version: 1,
              device: "1",
              inode: options.inode ?? p.replace(/^\/alias/u, "/repo"),
              birth: "1",
            },
          }),
    git: (cwd) =>
      Effect.succeed(
        cwd.startsWith("/plain")
          ? null
          : {
              root: cwd.startsWith("/linked") ? "/linked" : "/repo",
              commonDirectory: "/repo/.git",
              gitDirectory: cwd.startsWith("/linked")
                ? "/repo/.git/worktrees/linked"
                : "/repo/.git",
              commonIdentity: { version: 1, device: "1", inode: "git-common", birth: "1" },
              directoryIdentity: {
                version: 1,
                device: "1",
                inode: cwd.startsWith("/linked") ? "git-linked" : "git-common",
                birth: "1",
              },
              branch: options.branch === undefined ? "refs/heads/work" : options.branch,
              registeredWorktrees: ["/repo", "/linked"],
            },
      ),
  });
const provide = <A, E>(effect: Effect.Effect<A, E, Target.CheckoutTarget>, obs = observation()) =>
  effect.pipe(
    Effect.provide(Target.layer),
    Effect.provideService(Target.CheckoutTargetObservation, obs),
    Effect.provideService(HostProcess.HomeDirectory, "/home/synthetic"),
    Effect.provide(NodePath.layer),
  );

it.effect(
  "null ordinary paths, nested projects and symlink aliases share the physical checkout",
  () =>
    provide(
      Effect.gen(function* () {
        const targets = yield* Target.CheckoutTarget;
        const ordinary = yield* targets.capture(input);
        const alias = yield* targets.capture({ ...input, projectRoot: "/alias/sub" });
        assert.strictEqual(ordinary.checkoutKey, "/repo");
        assert.strictEqual(ordinary.cwd, "/repo/sub");
        assert.strictEqual(alias.checkoutKey, ordinary.checkoutKey);
        assert.deepEqual(alias.directoryIdentity, ordinary.directoryIdentity);
      }),
    ),
);
it.effect("registered linked worktrees outside the project remain distinct resources", () =>
  provide(
    Effect.gen(function* () {
      const targets = yield* Target.CheckoutTarget;
      const ordinary = yield* targets.capture(input);
      const linked = yield* targets.capture({ ...input, worktreePath: "/linked" });
      assert.notEqual(linked.checkoutKey, ordinary.checkoutKey);
      assert.strictEqual(linked.gitCommonDirectory, ordinary.gitCommonDirectory);
      assert.isFalse(Target.overlaps(ordinary.checkoutKey, linked.checkoutKey, "/"));
    }),
  ),
);
it.effect("non-Git directories preserve a null branch, including an explicit ordinary path", () =>
  provide(
    Effect.gen(function* () {
      const targets = yield* Target.CheckoutTarget;
      const plain = yield* targets.capture({
        ...input,
        projectRoot: "/plain",
        worktreePath: "/plain",
      });
      assert.strictEqual(plain.branch, null);
      assert.strictEqual(plain.gitCommonDirectory, null);
      assert.strictEqual(plain.checkoutKey, "/plain");
    }),
  ),
);
it.effect("missing and relative roots never fall back to server cwd", () =>
  provide(
    Effect.gen(function* () {
      const targets = yield* Target.CheckoutTarget;
      for (const root of ["", "relative", "/missing"])
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(targets.capture({ ...input, projectRoot: root }))),
        );
    }),
    observation({ missing: "/missing" }),
  ),
);
it.effect("branch, directory replacement and symlink drift invalidate a captured target", () =>
  Effect.gen(function* () {
    const target = yield* provide(Effect.flatMap(Target.CheckoutTarget, (t) => t.capture(input)));
    for (const obs of [
      observation({ branch: "refs/heads/other" }),
      observation({ branch: null }),
      observation({ inode: "replacement" }),
    ])
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            provide(
              Effect.flatMap(Target.CheckoutTarget, (t) => t.revalidate(target)),
              obs,
            ),
          ),
        ),
      );
  }),
);
it("ancestor overlap treats root and separator boundaries exactly", () => {
  assert.isTrue(Target.overlaps("/", "/repo", "/"));
  assert.isTrue(Target.overlaps("/repo", "/repo/sub", "/"));
  assert.isFalse(Target.overlaps("/repo", "/repository", "/"));
  assert.isFalse(Target.overlaps("/repo/%", "/repo/other", "/"));
});
