// @effect-diagnostics nodeBuiltinImport:off -- Private finite Git/SQLite fixtures exercise durable records without an app or compiler.
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import {
  beginProduction,
  finishProduction,
  outputObligations,
} from "../../../apps/server/src/jones/workspaceRetirement/ProducerFinalizer.ts";
import { binding } from "../../../apps/server/src/jones/workspaceRetirement/OutputAttestation.ts";
import { withFiniteProduction } from "./finite-production.ts";

export const finiteScratchReceipts: Array<{
  path: string;
  uid: number;
  gid: number;
  cleaned: boolean;
}> = [];

async function git(cwd: string, args: string[]) {
  await new Promise<void>((resolve, reject) => {
    NodeChildProcess.execFile(
      "git",
      ["-C", cwd, ...args],
      {
        timeout: 10000,
        maxBuffer: 65536,
        env: {
          PATH: process.env.PATH,
          GIT_CONFIG_NOSYSTEM: "1",
          GIT_CONFIG_GLOBAL: "/dev/null",
          GIT_NO_LAZY_FETCH: "1",
          GIT_OPTIONAL_LOCKS: "0",
          GIT_AUTHOR_NAME: "Fixture",
          GIT_AUTHOR_EMAIL: "fixture@example.invalid",
          GIT_COMMITTER_NAME: "Fixture",
          GIT_COMMITTER_EMAIL: "fixture@example.invalid",
        },
      },
      (error) => (error ? reject(error) : resolve()),
    );
  });
}

export async function finiteFixture<A>(
  run: (fixture: {
    worktree: string;
    root: string;
    records: () => Promise<
      Array<{
        id: string;
        owner: string;
        status: string;
        outcome: string;
        roots: string[];
        reasons: string[];
        proofStatus: string;
        binding: unknown;
      }>
    >;
  }) => Promise<A>,
): Promise<A> {
  const temporary = await NodeFSP.realpath(NodeOS.tmpdir());
  const storage = await NodeFSP.statfs(temporary);
  NodeAssert.ok(
    storage.bavail * storage.bsize >= 1536 * 1024 ** 2,
    "512 MiB peak plus 1 GiB reserve",
  );
  const base = await NodeFSP.mkdtemp(NodePath.join(temporary, "jfp-"));
  let owner: { ino: number; uid: number; gid: number } | undefined;
  let receipt: (typeof finiteScratchReceipts)[number] | undefined;
  try {
    await NodeFSP.chmod(base, 0o700);
    owner = await NodeFSP.stat(base);
    receipt = { path: base, uid: owner.uid, gid: owner.gid, cleaned: false };
    finiteScratchReceipts.push(receipt);
    NodeAssert.equal(owner.uid, process.getuid?.());
    NodeAssert.equal(owner.gid, process.getgid?.());
    const repo = NodePath.join(base, "r");
    const worktree = NodePath.join(base, "w");
    await NodeFSP.mkdir(repo);
    await git(repo, ["init", "-b", "main"]);
    await NodeFSP.writeFile(NodePath.join(repo, "tracked"), "finite fixture\n");
    await git(repo, ["add", "tracked"]);
    await git(repo, ["commit", "-m", "fixture"]);
    await git(repo, ["worktree", "add", "-b", "task", worktree]);
    const bound = await binding(worktree);
    const directory = NodePath.join(bound.git_dir, "workspace-output-obligations");
    return await run({
      worktree,
      root: NodePath.join(worktree, "apps/server/dist"),
      async records() {
        const names = await NodeFSP.readdir(directory).catch((error: NodeJS.ErrnoException) => {
          if (error.code === "ENOENT") return [];
          throw error;
        });
        return Promise.all(
          names.map(async (name) =>
            JSON.parse(await NodeFSP.readFile(NodePath.join(directory, name), "utf8")),
          ),
        );
      },
    });
  } finally {
    const current = await NodeFSP.stat(base);
    NodeAssert.equal(current.uid, process.getuid?.());
    if (owner) {
      NodeAssert.equal(current.ino, owner.ino);
      NodeAssert.equal(current.uid, owner.uid);
      NodeAssert.equal(current.gid, owner.gid);
    }
    await NodeFSP.rm(base, { recursive: true });
    await NodeAssert.rejects(NodeFSP.stat(base), { code: "ENOENT" });
    if (receipt) receipt.cleaned = true;
  }
}

const captureUnavailable = () => ({
  status: "unavailable" as const,
  reason: "fixture_unavailable",
});
const input = (worktree: string, root: string) => ({
  producer: "jones-server-build" as const,
  worktree,
  root,
});

export const finiteProductionCases: Record<string, () => Promise<void>> = {
  "active exact-incarnation duty precedes every output write and completion encloses postprocessing/cleanup":
    async () =>
      finiteFixture(async ({ worktree, root, records }) => {
        const events: string[] = [];
        const result = await Effect.runPromise(
          withFiniteProduction(
            input(worktree, root),
            (evidence) =>
              Effect.promise(async () => {
                const [active] = await records();
                NodeAssert.equal(active?.status, "active");
                NodeAssert.equal(active?.outcome, "unresolved");
                NodeAssert.equal(active?.owner, "jones-server-build");
                NodeAssert.deepEqual(active?.binding, await binding(worktree));
                NodeAssert.deepEqual(active?.roots, [root]);
                evidence.commandStarted();
                await NodeFSP.mkdir(NodePath.join(root, "client"), { recursive: true });
                evidence.leaderExited();
                await NodeFSP.writeFile(NodePath.join(root, "client/icon"), "overridden");
                events.push("copy-and-icons");
                return "native-result";
              }).pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    events.push("cleanup");
                  }),
                ),
              ),
            {
              capture: captureUnavailable,
              report: () => {},
              finish: async (...args) => {
                NodeAssert.deepEqual(events, ["copy-and-icons", "cleanup"]);
                NodeAssert.equal(args[3], undefined, "no content proof requested");
                return finishProduction(...args);
              },
            },
          ),
        );
        NodeAssert.equal(result, "native-result");
        const [stored] = await records();
        NodeAssert.equal(stored?.status, "succeeded");
        NodeAssert.equal(stored?.outcome, "unresolved");
        NodeAssert.equal(stored?.proofStatus, "deferred");
        NodeAssert.ok(stored?.reasons.includes("writer_completion_not_observed"));
        NodeAssert.equal((await outputObligations(worktree)).length, 1);
      }),
  "partial copy and executable validation failures preserve the original error and partial output duty":
    async () => {
      for (const stage of ["copy", "validation"])
        await finiteFixture(async ({ worktree, root, records }) => {
          const failure = { _tag: "FixtureFailure", stage };
          const result = await Effect.runPromise(
            Effect.exit(
              withFiniteProduction(
                input(worktree, root),
                (evidence) =>
                  Effect.gen(function* () {
                    yield* Effect.promise(async () => {
                      evidence.commandStarted();
                      await NodeFSP.mkdir(root, { recursive: true });
                      await NodeFSP.writeFile(NodePath.join(root, "partial"), stage);
                      evidence.leaderExited();
                    });
                    return yield* Effect.fail(failure);
                  }),
                { capture: captureUnavailable, report: () => {} },
              ),
            ),
          );
          NodeAssert.ok(Exit.isFailure(result));
          NodeAssert.equal(Cause.squash(result.cause), failure);
          const [stored] = await records();
          NodeAssert.equal(stored?.status, "failed");
          NodeAssert.equal(stored?.outcome, "unresolved");
          NodeAssert.ok(stored?.reasons.includes("writer_completion_not_observed"));
        });
    },
  "leader exit with an absent root and a delayed writer never grants disposal": async () =>
    finiteFixture(async ({ worktree, root, records }) => {
      await Effect.runPromise(
        withFiniteProduction(
          input(worktree, root),
          (evidence) =>
            Effect.sync(() => {
              evidence.commandStarted();
              evidence.leaderExited();
            }),
          { capture: captureUnavailable, report: () => {} },
        ),
      );
      NodeAssert.equal((await records())[0]?.outcome, "unresolved");
      // Deterministically model a writer which survives the leader; no timer or child remains.
      await NodeFSP.mkdir(root, { recursive: true });
      await NodeFSP.writeFile(NodePath.join(root, "late"), "late writer");
      NodeAssert.equal((await outputObligations(worktree)).length, 1);
    }),
  "interruption and an unknown native terminal preserve the active duty and exact diagnostic":
    async () => {
      for (const interrupt of [false, true])
        await finiteFixture(async ({ worktree, root, records }) => {
          const messages: string[] = [];
          let finalized = false;
          const result = await Effect.runPromise(
            Effect.exit(
              withFiniteProduction(
                input(worktree, root),
                (evidence) => {
                  evidence.commandStarted();
                  return interrupt
                    ? Effect.interrupt
                    : Effect.fail({ _tag: "UnknownNativeTerminal" });
                },
                {
                  finish: async (...args) => {
                    finalized = true;
                    return finishProduction(...args);
                  },
                  capture: () => {
                    throw new Error("unknown status must not be captured");
                  },
                  report: (message) => {
                    messages.push(message);
                  },
                },
              ),
            ),
          );
          NodeAssert.ok(Exit.isFailure(result));
          NodeAssert.equal(finalized, false);
          const [stored] = await records();
          NodeAssert.equal(stored?.status, "active");
          NodeAssert.equal(stored?.outcome, "unresolved");
          NodeAssert.ok(messages[0]?.includes(`id=${stored?.id}; root=${root}; path=`));
          NodeAssert.ok(messages[0]?.includes("native_terminal_not_observed"));
        });
    },
  "admission failure prevents allocation and is never retried after uncertain persistence":
    async () =>
      finiteFixture(async ({ worktree, root, records }) => {
        let allocations = 0;
        let attempts = 0;
        const result = await Effect.runPromise(
          Effect.exit(
            withFiniteProduction(
              input(worktree, root),
              () =>
                Effect.sync(() => {
                  allocations++;
                }),
              {
                begin: async () => {
                  attempts++;
                  throw new Error("uncertain admission");
                },
                capture: captureUnavailable,
                report: () => {},
              },
            ),
          ),
        );
        NodeAssert.ok(Exit.isFailure(result));
        NodeAssert.equal(attempts, 1);
        NodeAssert.equal(allocations, 0);
        NodeAssert.deepEqual(await records(), []);
        await NodeAssert.rejects(NodeFSP.stat(root), { code: "ENOENT" });
      }),
  "admission rejection after publication preserves the original uncertainty without allocating or retrying":
    async () =>
      finiteFixture(async ({ worktree, root, records }) => {
        const failure = new Error("admission rejected after publication");
        let attempts = 0;
        let allocations = 0;
        const result = await Effect.runPromise(
          Effect.exit(
            withFiniteProduction(
              input(worktree, root),
              () =>
                Effect.sync(() => {
                  allocations++;
                }),
              {
                begin: async (...args) => {
                  attempts++;
                  await beginProduction(...args);
                  throw failure;
                },
                capture: () => {
                  throw new Error("admission never reached optional capture");
                },
                report: () => {},
              },
            ),
          ),
        );
        NodeAssert.ok(Exit.isFailure(result));
        NodeAssert.equal(attempts, 1);
        NodeAssert.equal(allocations, 0);
        const admission = Cause.squash(result.cause);
        NodeAssert.ok(admission instanceof Error);
        NodeAssert.equal(admission.cause, failure);
        NodeAssert.equal("producer" in admission && admission.producer, "jones-server-build");
        NodeAssert.equal("worktree" in admission && admission.worktree, worktree);
        NodeAssert.equal("root" in admission && admission.root, root);
        const stored = await records();
        NodeAssert.equal(stored.length, 1);
        NodeAssert.equal(stored[0]?.status, "active");
        NodeAssert.equal(stored[0]?.outcome, "unresolved");
        // The fixture can inspect its private records; the rejected caller did
        // not receive a session and must never guess or fabricate this id.
        NodeAssert.equal("id" in admission, false);
        await NodeAssert.rejects(NodeFSP.stat(root), { code: "ENOENT" });
      }),
  "finalization and optional capture/report failure preserve success or failure without new duties":
    async () => {
      for (const fails of [false, true])
        await finiteFixture(async ({ worktree, root, records }) => {
          const failure = { _tag: "NativeFixtureFailure" };
          let attempts = 0;
          const messages: string[] = [];
          const result = await Effect.runPromise(
            Effect.exit(
              withFiniteProduction(
                input(worktree, root),
                (evidence) => {
                  evidence.commandStarted();
                  evidence.leaderExited();
                  return fails ? Effect.fail(failure) : Effect.succeed("success");
                },
                {
                  finish: async () => {
                    attempts++;
                    throw new Error("uncertain finalization");
                  },
                  capture: () => {
                    throw new Error("optional failure");
                  },
                  report: (message) => {
                    messages.push(message);
                    throw new Error("diagnostic failure");
                  },
                },
              ),
            ),
          );
          NodeAssert.equal(attempts, 1);
          if (Exit.isFailure(result)) NodeAssert.equal(Cause.squash(result.cause), failure);
          else NodeAssert.equal(result.value, "success");
          NodeAssert.equal(Exit.isFailure(result), fails);
          const stored = await records();
          NodeAssert.equal(stored.length, 1);
          NodeAssert.equal(stored[0]?.status, "active");
          NodeAssert.ok(messages[0]?.includes(stored[0]!.id));
          NodeAssert.ok(messages[0]?.includes("finalization_not_confirmed"));
        });
    },
  "optional capture alone cannot change native status or add an allocation duty": async () => {
    for (const fails of [false, true])
      await finiteFixture(async ({ worktree, root, records }) => {
        const failure = { _tag: "NativeFixtureFailure" };
        const result = await Effect.runPromise(
          Effect.exit(
            withFiniteProduction(
              input(worktree, root),
              (evidence) => {
                evidence.commandStarted();
                evidence.leaderExited();
                return fails ? Effect.fail(failure) : Effect.succeed("success");
              },
              {
                capture: () => {
                  throw new Error("optional failure");
                },
                report: () => {},
              },
            ),
          ),
        );
        NodeAssert.equal(Exit.isFailure(result), fails);
        if (Exit.isFailure(result)) NodeAssert.equal(Cause.squash(result.cause), failure);
        const stored = await records();
        NodeAssert.equal(stored.length, 1);
        NodeAssert.equal(stored[0]?.status, fails ? "failed" : "succeeded");
        NodeAssert.equal(stored[0]?.outcome, "unresolved");
      });
  },
  "existing finalizer retains or disposes only after explicitly sufficient writer completion":
    async () =>
      finiteFixture(async ({ worktree, root }) => {
        const retained = await beginProduction(worktree, "fixture-proved", [root]);
        await NodeFSP.mkdir(root, { recursive: true });
        NodeAssert.equal((await finishProduction(retained, "succeeded")).outcome, "retained");
        const disposed = await beginProduction(worktree, "fixture-proved", [root]);
        await NodeFSP.rm(root, { recursive: true });
        NodeAssert.equal((await finishProduction(disposed, "succeeded")).outcome, "disposed");
        NodeAssert.deepEqual(await outputObligations(worktree), []);
      }),
};
