import { assert, describe, expect, it, vi } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import { VcsRepositoryDetectionError } from "@t3tools/contracts";

import * as GitManager from "./GitManager.ts";
import * as GitWorkflowService from "./GitWorkflowService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as VcsDriver from "../vcs/VcsDriver.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";

function makeLayer(input: {
  readonly detect: VcsDriverRegistry.VcsDriverRegistry["Service"]["detect"];
}) {
  return GitWorkflowService.layer.pipe(
    Layer.provide(
      Layer.mock(VcsDriverRegistry.VcsDriverRegistry)({
        detect: input.detect,
      }),
    ),
    Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
    Layer.provide(Layer.mock(GitManager.GitManager)({})),
  );
}

describe("GitWorkflowService", () => {
  it.effect("reports a non-Git VCS repository as not a Git repository", () =>
    Effect.gen(function* () {
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      const isRepository = yield* workflow.isRepository("/jj-repo");

      assert.equal(isRepository, false);
    }).pipe(
      Effect.provide(
        makeLayer({
          detect: () =>
            Effect.succeed({
              kind: "jj",
              repository: {
                kind: "jj",
                rootPath: "/jj-repo",
                metadataPath: "/jj-repo/.jj",
                freshness: {
                  source: "live-local",
                  observedAt: DateTime.makeUnsafe("2026-01-01T00:00:00.000Z"),
                  expiresAt: Option.none(),
                },
              },
              driver: {} as VcsDriverRegistry.VcsDriverHandle["driver"],
            }),
        }),
      ),
    ),
  );

  it.effect("returns an empty local status when no VCS repository is detected", () =>
    Effect.gen(function* () {
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      const status = yield* workflow.localStatus({ cwd: "/not-a-repo" });

      assert.deepStrictEqual(status, {
        isRepo: false,
        hasPrimaryRemote: false,
        isDefaultRef: false,
        refName: null,
        hasWorkingTreeChanges: false,
        workingTree: {
          files: [],
          insertions: 0,
          deletions: 0,
        },
      });
    }).pipe(
      Effect.provide(
        makeLayer({
          detect: () => Effect.succeed(null),
        }),
      ),
    ),
  );

  it.effect("returns an empty full status when no VCS repository is detected", () =>
    Effect.gen(function* () {
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      const status = yield* workflow.status({ cwd: "/not-a-repo" });

      assert.deepStrictEqual(status, {
        isRepo: false,
        hasPrimaryRemote: false,
        isDefaultRef: false,
        refName: null,
        hasWorkingTreeChanges: false,
        workingTree: {
          files: [],
          insertions: 0,
          deletions: 0,
        },
        hasUpstream: false,
        aheadCount: 0,
        behindCount: 0,
        aheadOfDefaultCount: 0,
        pr: null,
      });
    }).pipe(
      Effect.provide(
        makeLayer({
          detect: () => Effect.succeed(null),
        }),
      ),
    ),
  );

  it.effect("does not call GitManager status methods when no VCS repository is detected", () => {
    const localStatus = vi.fn();
    const remoteStatus = vi.fn();
    const status = vi.fn();

    const testLayer = GitWorkflowService.layer.pipe(
      Layer.provide(
        Layer.mock(VcsDriverRegistry.VcsDriverRegistry)({
          detect: () => Effect.succeed(null),
        }),
      ),
      Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
      Layer.provide(
        Layer.mock(GitManager.GitManager)({
          localStatus,
          remoteStatus,
          status,
        }),
      ),
    );

    return Effect.gen(function* () {
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      yield* workflow.localStatus({ cwd: "/not-a-repo" });
      yield* workflow.remoteStatus({ cwd: "/not-a-repo" });
      yield* workflow.status({ cwd: "/not-a-repo" });

      assert.equal(localStatus.mock.calls.length, 0);
      assert.equal(remoteStatus.mock.calls.length, 0);
      assert.equal(status.mock.calls.length, 0);
    }).pipe(Effect.provide(testLayer));
  });

  it.effect("returns an empty ref list when no VCS repository is detected", () =>
    Effect.gen(function* () {
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      const refs = yield* workflow.listRefs({ cwd: "/not-a-repo" });

      assert.deepStrictEqual(refs, {
        refs: [],
        isRepo: false,
        hasPrimaryRemote: false,
        nextCursor: null,
        totalCount: 0,
      });
    }).pipe(
      Effect.provide(
        makeLayer({
          detect: () => Effect.succeed(null),
        }),
      ),
    ),
  );

  it.effect("structures workflow detection failures without exposing upstream details", () => {
    const cause = new VcsRepositoryDetectionError({
      operation: "VcsDriverRegistry.detect",
      cwd: "/repo",
      detail: "upstream detail must stay in the cause chain",
    });

    return Effect.gen(function* () {
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      const error = yield* workflow.status({ cwd: "/repo" }).pipe(Effect.flip);

      expect(error).toMatchObject({
        _tag: "GitManagerError",
        operation: "GitWorkflowService.status",
        cwd: "/repo",
        detail: "Failed to detect a VCS repository for this Git workflow.",
      });
      expect(error.message).not.toContain(cause.detail);
    }).pipe(
      Effect.provide(
        makeLayer({
          detect: () => Effect.fail(cause),
        }),
      ),
    );
  });

  it.effect("structures command detection failures without exposing upstream details", () => {
    const cause = new VcsRepositoryDetectionError({
      operation: "VcsDriverRegistry.detect",
      cwd: "/repo",
      detail: "upstream command detail must stay in the cause chain",
    });

    return Effect.gen(function* () {
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      const error = yield* workflow.listRefs({ cwd: "/repo" }).pipe(Effect.flip);

      expect(error).toMatchObject({
        _tag: "GitCommandError",
        operation: "GitWorkflowService.listRefs",
        command: "vcs-route",
        cwd: "/repo",
        detail: "Failed to detect a VCS repository for this Git command.",
      });
      expect(error.message).not.toContain(cause.detail);
    }).pipe(
      Effect.provide(
        makeLayer({
          detect: () => Effect.fail(cause),
        }),
      ),
    );
  });
});

class WorkflowMutationGuard extends Context.Service<
  WorkflowMutationGuard,
  { readonly current: boolean }
>()("t3/git/GitWorkflowService.test/WorkflowMutationGuard") {}

it.effect("forwards typed mutation guards only after resolving the Git workflow", () =>
  Effect.gen(function* () {
    const resolving = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const stale = { _tag: "StaleWorkflowMutation" } as const;
    let checks = 0;
    const driver = yield* VcsDriver.VcsDriver;
    const workflow = yield* GitWorkflowService.make.pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.mock(GitManager.GitManager)({}),
          Layer.mock(VcsDriverRegistry.VcsDriverRegistry)({
            resolve: () =>
              Deferred.succeed(resolving, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.as({
                  kind: "git" as const,
                  driver,
                  repository: {
                    kind: "git" as const,
                    rootPath: "/repo",
                    metadataPath: "/repo/.git",
                    freshness: {
                      source: "live-local" as const,
                      observedAt: DateTime.makeUnsafe("2026-01-01T00:00:00.000Z"),
                      expiresAt: Option.none(),
                    },
                  },
                }),
              ),
          }),
          Layer.mock(GitVcsDriver.GitVcsDriver)({
            createWorktree: (input, options) =>
              (options?.revalidateMutation ?? Effect.void).pipe(
                Effect.as({
                  worktree: { path: input.path ?? "/worktree", refName: input.refName },
                }),
              ),
            pruneWorktrees: (input) => input.revalidateMutation ?? Effect.void,
          }),
        ),
      ),
    );
    const guard = Effect.gen(function* () {
      checks++;
      if (!(yield* WorkflowMutationGuard).current) return yield* Effect.fail(stale);
    });
    const fiber = yield* workflow
      .createWorktree(
        { cwd: "/repo", path: "/worktree", refName: "main" },
        { revalidateMutation: guard },
      )
      .pipe(
        Effect.provideService(WorkflowMutationGuard, { current: false }),
        Effect.flip,
        Effect.forkChild,
      );
    yield* Deferred.await(resolving);
    assert.equal(checks, 0);
    yield* Deferred.succeed(release, undefined);
    assert.strictEqual(yield* Fiber.join(fiber), stale);
    assert.strictEqual(
      yield* workflow
        .pruneWorktrees({ cwd: "/repo", revalidateMutation: guard })
        .pipe(Effect.provideService(WorkflowMutationGuard, { current: false }), Effect.flip),
      stale,
    );
    assert.equal(checks, 2);
  }).pipe(
    Effect.provide(
      Layer.mock(VcsDriver.VcsDriver)({
        capabilities: {
          kind: "git",
          supportsWorktrees: true,
          supportsBookmarks: false,
          supportsAtomicSnapshot: false,
          supportsPushDefaultRemote: true,
          ignoreClassifier: "native",
        },
      }),
    ),
  ),
);
