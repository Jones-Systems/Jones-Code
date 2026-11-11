// @effect-diagnostics nodeBuiltinImport:off -- Private finite Git/SQLite fixtures exercise durable records without an app or compiler.
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import NodeFSPMutable from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import {
  beginProduction,
  finishProduction,
  outputObligations,
  resolveProductions,
  productionSpawnIntent,
  productionSpawnResult,
  type ProductionSession,
} from "../../../apps/server/src/jones/workspaceRetirement/ProducerFinalizer.ts";
import {
  canonical,
  nativePlatform,
} from "../../../apps/server/src/jones/workspaceRetirement/Filesystem.ts";
import { binding } from "../../../apps/server/src/jones/workspaceRetirement/OutputAttestation.ts";
import { resolveCommand } from "./resolve.ts";
import {
  producerIdentity,
  observeProducerSelf,
  observeProducerSession,
} from "../../../apps/server/src/jones/workspaceRetirement/ProducerSessionObservation.ts";
import { reservationTransaction } from "../../../apps/server/src/jones/workspaceRetirement/ParticipantGuard.ts";
import { withFiniteProduction, type FiniteProductionEvidence } from "./finite-production.ts";

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
    retainOnUnknownCleanup: () => void;
    records: () => Promise<Array<ProductionSession["record"]>>;
  }) => Promise<A>,
): Promise<A> {
  const temporary = await NodeFSP.realpath(NodeOS.tmpdir());
  const storage = await NodeFSP.statfs(temporary);
  NodeAssert.ok(
    storage.bavail * storage.bsize >= 2560 * 1024 ** 2,
    "512 MiB peak plus 2 GiB reserve",
  );
  const base = await NodeFSP.mkdtemp(NodePath.join(temporary, "jfp-"));
  let cleanupAllowed = true;
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
      retainOnUnknownCleanup: () => {
        cleanupAllowed = false;
      },
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
    if (cleanupAllowed) {
      await NodeFSP.rm(base, { recursive: true });
      await NodeAssert.rejects(NodeFSP.stat(base), { code: "ENOENT" });
      if (receipt) receipt.cleaned = true;
    }
  }
}

const captureUnavailable = () => ({
  status: "unavailable" as const,
  reason: "fixture_unavailable",
});
const fixtureSpawn = (evidence: FiniteProductionEvidence) =>
  evidence.spawn(() => Effect.succeed({ pid: 123 }), true);
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
                await Effect.runPromise(fixtureSpawn(evidence));
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
                NodeAssert.equal(args[3]?.verifyContent, undefined, "no content proof requested");
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
                      await Effect.runPromise(fixtureSpawn(evidence));
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
            fixtureSpawn(evidence).pipe(Effect.tap(() => Effect.sync(evidence.leaderExited))),
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
                (evidence) =>
                  fixtureSpawn(evidence).pipe(
                    Effect.andThen(
                      interrupt ? Effect.interrupt : Effect.fail({ _tag: "UnknownNativeTerminal" }),
                    ),
                  ),
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
          NodeAssert.equal(finalized, true);
          const [stored] = await records();
          NodeAssert.equal(stored?.status, "active");
          NodeAssert.equal(stored?.outcome, "unresolved");
          NodeAssert.ok(messages[0]?.includes(`id=${stored?.id}; root=${root}; path=`));
          NodeAssert.ok(
            messages.some((message) => message.includes("native_terminal_not_observed")),
          );
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
                (evidence) =>
                  fixtureSpawn(evidence).pipe(
                    Effect.tap(() => Effect.sync(evidence.leaderExited)),
                    Effect.andThen(fails ? Effect.fail(failure) : Effect.succeed("success")),
                  ),
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
              (evidence) =>
                fixtureSpawn(evidence).pipe(
                  Effect.tap(() => Effect.sync(evidence.leaderExited)),
                  Effect.andThen(fails ? Effect.fail(failure) : Effect.succeed("success")),
                ),
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

async function rewrite(session: ProductionSession) {
  await NodeFSP.writeFile(session.path, JSON.stringify(session.record));
}
const freshInput = (worktree: string, root: string) => ({
  worktree,
  root,
  producer: "jones-server-build" as const,
});

export const resolutionCases: Record<string, () => Promise<void>> = {
  "supported holders precede allocation; no-spawn completion resolves and three sequential builds keep one record":
    async () =>
      finiteFixture(async ({ worktree, root, records }) => {
        for (let n = 0; n < 3; n++) {
          await Effect.runPromise(
            withFiniteProduction(
              freshInput(worktree, root),
              () =>
                Effect.promise(async () => {
                  const [record] = await records();
                  NodeAssert.equal(record?.holders?.in_process, "running");
                  NodeAssert.equal(record?.holders?.admission_closed, false);
                  NodeAssert.equal(record?.rev, 1);
                  await NodeFSP.mkdir(root, { recursive: true });
                  await NodeFSP.writeFile(NodePath.join(root, "output"), String(n));
                }),
              { report: () => {}, capture: captureUnavailable },
            ),
          );
          const stored = await records();
          NodeAssert.equal(stored.length, 1);
          NodeAssert.equal(stored[0]?.status, "succeeded");
          NodeAssert.equal(stored[0]?.outcome, "retained");
          NodeAssert.equal(stored[0]?.resolution?.rev, stored[0]?.rev);
          NodeAssert.deepEqual(await outputObligations(worktree), []);
        }
      }),
  "same-key admission and explicit matching reconciliation preserve a different root set's open admission":
    async () =>
      finiteFixture(async ({ worktree, root }) => {
        const first = await beginProduction(worktree, "fixture-roots", [root]);
        const otherRoot = NodePath.join(worktree, "other-output");
        const other = await beginProduction(worktree, "fixture-roots", [otherRoot]);
        NodeAssert.equal(
          JSON.parse(await NodeFSP.readFile(first.path, "utf8")).holders.admission_closed,
          false,
        );
        const results = await resolveProductions(worktree, {
          producer: "fixture-roots",
          roots: [root, root],
        });
        NodeAssert.deepEqual(
          results.map((record) => record.id),
          [first.record.id],
        );
        NodeAssert.equal(
          JSON.parse(await NodeFSP.readFile(other.path, "utf8")).holders.admission_closed,
          false,
        );
      }),
  "mixed-version active resolution prunes only behind a durable same-key active successor; legacy active records survive":
    async () =>
      finiteFixture(async ({ worktree, root, records }) => {
        const prior = await beginProduction(worktree, "fixture-mixed", [root]);
        prior.record.holders!.self.boot = "fixture-previous-boot";
        await rewrite(prior);
        await resolveProductions(worktree);
        const stored = JSON.parse(await NodeFSP.readFile(prior.path, "utf8"));
        NodeAssert.equal(stored.status, "active");
        NodeAssert.equal(stored.outcome, "disposed");
        const successor = await beginProduction(worktree, "fixture-mixed", [root]);
        await NodeAssert.rejects(NodeFSP.stat(prior.path), { code: "ENOENT" });
        NodeAssert.equal(successor.record.status, "active");
        NodeAssert.equal((await outputObligations(worktree))[0]?.id, successor.record.id);
        NodeAssert.equal(successor.record.supersedes?.id, prior.record.id);
        await finishProduction(successor, "succeeded");
        const legacy = structuredClone(stored);
        delete legacy.holders;
        delete legacy.rev;
        delete legacy.resolution;
        await NodeFSP.writeFile(prior.path, JSON.stringify(legacy));
        await beginProduction(worktree, "fixture-mixed", [root]);
        NodeAssert.equal(
          (await records()).some((record) => record.id === prior.record.id),
          true,
        );
        NodeAssert.equal(
          (await outputObligations(worktree)).some((record) => record.id === prior.record.id),
          true,
        );
      }),
  "durable intent precedes each spawn; proved zero balances, uncertain and non-detached capture stay open":
    async () =>
      finiteFixture(async ({ worktree, root }) => {
        const session = await beginProduction(worktree, "fixture-intents", [root]);
        const first = await productionSpawnIntent(session);
        NodeAssert.equal(
          JSON.parse(await NodeFSP.readFile(session.path, "utf8")).holders.spawns[0].state,
          "pending",
        );
        await productionSpawnResult(session, first, { kind: "none" });
        await NodeAssert.rejects(
          productionSpawnResult(session, first, { kind: "none" }),
          /spawn_binding_uncertain/,
        );
        const uncertain = await productionSpawnIntent(session);
        await productionSpawnResult(session, uncertain, {
          kind: "session",
          pid: 42,
          detached: false,
        });
        NodeAssert.equal(session.record.holders?.spawns[1]?.state, "pending");
        await finishProduction(session, "succeeded");
        await NodeAssert.rejects(productionSpawnIntent(session), /spawn_admission_closed/);
        const [result] = await resolveProductions(worktree);
        NodeAssert.ok(result?.reasons.includes("untracked_spawn_possible"));
        NodeAssert.equal(result?.outcome, "unresolved");
      }),
  "fast-exit detached PID is captured without a post-exit PID lookup and never proves session absence":
    async () =>
      finiteFixture(async ({ worktree, root }) => {
        const session = await beginProduction(worktree, "fixture-fast", [root]);
        const intent = await productionSpawnIntent(session);
        const child = NodeChildProcess.spawn(process.execPath, ["-e", "process.exit(0)"], {
          detached: true,
          stdio: "ignore",
        });
        const observed = await new Promise<number | null>((resolve, reject) => {
          child.once("error", reject);
          child.once("exit", resolve);
        });
        NodeAssert.equal(observed, 0);
        NodeAssert.ok(child.pid);
        await productionSpawnResult(session, intent, {
          kind: "session",
          pid: child.pid,
          detached: true,
        });
        NodeAssert.equal(session.record.holders?.spawns[0]?.state, "session");
        const recorded = session.record.holders?.spawns[0];
        NodeAssert.equal(recorded?.state === "session" ? recorded.sid : undefined, child.pid);
        await finishProduction(session, "succeeded");
        NodeAssert.equal(session.record.outcome, "unresolved");
        const observation = await observeProducerSession(session.record.holders!.self, child.pid);
        NodeAssert.equal(observation.state, "unknown");
        if (nativePlatform === "darwin")
          NodeAssert.equal(observation.reason, "darwin_unpublished_allocation_unproved");
      }),
  "guarded CD-1 preserves an unobserved terminal, known proof and unrelated reasons": async () =>
    finiteFixture(async ({ worktree, root }) => {
      const session = await beginProduction(worktree, "fixture-cd1", [root]);
      session.record.holders!.self.boot = "fixture-prior-boot";
      session.record.proofStatus = "attested";
      session.record.sidecars = ["fixture-proof-kept"];
      session.record.reasons.push("owner_followup");
      await rewrite(session);
      const [held] = await resolveProductions(worktree);
      NodeAssert.equal(held?.outcome, "unresolved");
      let stored = JSON.parse(await NodeFSP.readFile(session.path, "utf8"));
      NodeAssert.equal(stored.status, "active");
      NodeAssert.equal(stored.finished_at, undefined);
      NodeAssert.equal(stored.proofStatus, "attested");
      NodeAssert.deepEqual(stored.sidecars, ["fixture-proof-kept"]);
      NodeAssert.deepEqual(stored.reasons, ["owner_followup"]);
      stored.reasons = [];
      await NodeFSP.writeFile(session.path, JSON.stringify(stored));
      const [resolved] = await resolveProductions(worktree);
      NodeAssert.equal(resolved?.outcome, "disposed");
      NodeAssert.deepEqual(await outputObligations(worktree), []);
      stored = JSON.parse(await NodeFSP.readFile(session.path, "utf8"));
      NodeAssert.equal(stored.status, "active");
      NodeAssert.equal(stored.finished_at, undefined);
      const supported = structuredClone(stored);
      const invalid = [
        { ...supported, rev: supported.rev + 1 },
        { ...supported, finished_at: "2026-01-01T00:00:00.000Z" },
        { ...supported, holders: { ...supported.holders, version: 2 } },
        { ...supported, holders: { ...supported.holders, admission_closed: false } },
        { ...supported, holders: { ...supported.holders, overflow: true } },
        { ...supported, holders: { ...supported.holders, self: {} } },
        { ...supported, reasons: ["another_duty"] },
      ];
      for (const record of invalid) {
        await NodeFSP.writeFile(session.path, JSON.stringify(record));
        NodeAssert.equal((await outputObligations(worktree)).length, 1);
      }
      const legacy = { ...supported };
      delete legacy.holders;
      delete legacy.rev;
      delete legacy.resolution;
      await NodeFSP.writeFile(session.path, JSON.stringify(legacy));
      NodeAssert.equal((await outputObligations(worktree)).length, 1);
      NodeAssert.deepEqual((await resolveProductions(worktree))[0]?.reasons, [
        "legacy_no_holder_identity",
      ]);
      await NodeFSP.writeFile(
        session.path,
        JSON.stringify({ ...supported, binding: { ...supported.binding, ino: 1 } }),
      );
      await NodeAssert.rejects(outputObligations(worktree), /identity_mismatch/);
    }),
  "known terminal and finished_at survive resolution; sibling incarnation cannot resolve":
    async () =>
      finiteFixture(async ({ worktree, root }) => {
        const session = await beginProduction(worktree, "fixture-known", [root]);
        await finishProduction(session, "failed", [], { inProcessSettled: false });
        NodeAssert.equal(session.record.outcome, "unresolved");
        const finished = session.record.finished_at;
        session.record.holders!.self.boot = "fixture-older-boot";
        await rewrite(session);
        await resolveProductions(worktree);
        const stored = JSON.parse(await NodeFSP.readFile(session.path, "utf8"));
        NodeAssert.equal(stored.status, "failed");
        NodeAssert.equal(stored.finished_at, finished);
        NodeAssert.equal(stored.outcome, "disposed");
        session.record.binding = { ...session.record.binding, ino: session.record.binding.ino + 1 };
        await rewrite(session);
        await NodeAssert.rejects(resolveProductions(worktree), /identity_mismatch/);
      }),
  "in-flight copy cancellation keeps a live self holder until exact incarnation absence":
    async () =>
      finiteFixture(async ({ worktree, root }) => {
        const session = await beginProduction(worktree, "fixture-copy", [root]);
        const copying = NodeFSP.mkdir(root, { recursive: true }).then(() =>
          NodeFSP.writeFile(NodePath.join(root, "partial"), "pending"),
        );
        await finishProduction(session, undefined, [], { inProcessSettled: false });
        const [held] = await resolveProductions(worktree);
        NodeAssert.ok(held?.reasons.includes("holder_present"));
        NodeAssert.equal(session.record.holders?.in_process, "running");
        await copying;
        NodeAssert.equal((await outputObligations(worktree)).length, 1);
        const stored = JSON.parse(await NodeFSP.readFile(session.path, "utf8"));
        stored.holders.self.start = "different-incarnation";
        await NodeFSP.writeFile(session.path, JSON.stringify(stored));
        NodeAssert.equal((await resolveProductions(worktree))[0]?.outcome, "retained");
        NodeAssert.deepEqual(await outputObligations(worktree), []);
      }),
  "overflow and outstanding intents hold on the same boot and discharge only after a verified different boot":
    async () =>
      finiteFixture(async ({ worktree, root }) => {
        const session = await beginProduction(worktree, "fixture-overflow", [root]);
        await productionSpawnIntent(session);
        // Binding paths and host identity vary; measure the actual canonical payload
        // before padding past the spawn watermark, still below the record bound.
        session.record.sidecars = [""];
        const unpaddedBytes = Buffer.byteLength(canonical(session.record));
        NodeAssert.ok(unpaddedBytes < 56000);
        session.record.sidecars = ["x".repeat(56001 - unpaddedBytes)];
        NodeAssert.equal(Buffer.byteLength(canonical(session.record)), 56001);
        await rewrite(session);
        const overflow = await productionSpawnIntent(session);
        NodeAssert.equal(overflow, null);
        NodeAssert.equal(session.record.holders!.overflow, true);
        NodeAssert.equal(session.record.holders!.spawns.length, 1);
        NodeAssert.ok(Buffer.byteLength(await NodeFSP.readFile(session.path)) < 60000);
        await finishProduction(session, undefined, [], { inProcessSettled: false });
        NodeAssert.ok(
          (await resolveProductions(worktree))[0]?.reasons.includes("untracked_spawn_possible"),
        );
        let stored = JSON.parse(await NodeFSP.readFile(session.path, "utf8"));
        stored.holders.self.host = "not-this-host";
        await NodeFSP.writeFile(session.path, JSON.stringify(stored));
        NodeAssert.ok(
          (await resolveProductions(worktree))[0]?.reasons.includes("host_or_namespace_mismatch"),
        );
        stored = JSON.parse(await NodeFSP.readFile(session.path, "utf8"));
        stored.holders.self = { ...(await producerIdentity()), boot: "fixture-prior-boot" };
        await NodeFSP.writeFile(session.path, JSON.stringify(stored));
        NodeAssert.equal((await resolveProductions(worktree))[0]?.outcome, "disposed");
      }),
  "an acquired reservation blocks all writes; missing common storage is never an exclusivity fallback":
    async () =>
      finiteFixture(async ({ worktree, root }) => {
        const session = await beginProduction(worktree, "fixture-reservation", [root]);
        const bound = session.record.binding;
        await reservationTransaction(bound, async (db) => {
          db.prepare("INSERT INTO reservations VALUES (?,?,?)").run(
            bound.admin_id,
            "fixture",
            "{}",
          );
        });
        await NodeAssert.rejects(productionSpawnIntent(session), /retiring/);
        await NodeAssert.rejects(resolveProductions(worktree), /retiring/);
        await reservationTransaction(bound, async (db) => {
          db.prepare("DELETE FROM reservations WHERE admin_id=?").run(bound.admin_id);
        });
        session.record.binding = {
          ...bound,
          git_common_dir: NodePath.join(worktree, "missing-common"),
        };
        await NodeAssert.rejects(productionSpawnIntent(session), /reservation_store_absent/);
      }),
  "CAS refuses stale mutation and never overwrites a newer admission closure": async () =>
    finiteFixture(async ({ worktree, root }) => {
      const session = await beginProduction(worktree, "fixture-cas", [root]);
      const stale = structuredClone(session);
      await productionSpawnIntent(session);
      await NodeAssert.rejects(productionSpawnIntent(stale), /revision_changed/);
      await resolveProductions(worktree);
      await NodeAssert.rejects(productionSpawnIntent(session), /revision_changed/);
    }),
  "a real exited self leaves an active record; self resolution never fabricates its terminal":
    async () =>
      finiteFixture(async ({ worktree, root, retainOnUnknownCleanup }) => {
        const source = new URL(
          "../../../apps/server/src/jones/workspaceRetirement/ProducerFinalizer.ts",
          import.meta.url,
        ).href;
        let childPid: number | undefined;
        const result = await new Promise<string>((resolve, reject) => {
          const child = NodeChildProcess.execFile(
            process.execPath,
            [
              "--input-type=module",
              "-e",
              `const { beginProduction } = await import(${JSON.stringify(source)}); const session = await beginProduction(process.argv[1], 'fixture-exited-self', [process.argv[2]]); process.stdout.write(JSON.stringify(session));`,
              worktree,
              root,
            ],
            { timeout: 15000, maxBuffer: 16384 },
            (error, stdout) => {
              if (error) {
                retainOnUnknownCleanup();
                reject(error);
              } else resolve(stdout);
            },
          );
          childPid = child.pid;
        });
        const session: ProductionSession = JSON.parse(result);
        NodeAssert.equal(session.record.holders?.self.pid, childPid);
        const [resolved] = await resolveProductions(worktree);
        NodeAssert.equal(resolved?.status, "active");
        const stored = JSON.parse(await NodeFSP.readFile(session.path, "utf8"));
        NodeAssert.equal(stored.finished_at, undefined);
        if (nativePlatform === "linux") {
          NodeAssert.equal(resolved?.outcome, "unresolved");
          NodeAssert.ok(resolved?.reasons.includes("session_observation_unavailable"));
          NodeAssert.equal((await outputObligations(worktree)).length, 1);
        } else {
          NodeAssert.equal(resolved?.outcome, "disposed");
          NodeAssert.deepEqual(await outputObligations(worktree), []);
        }
      }),
  "observer errors and unqualified hosts never synthesize session absence": async () => {
    const identity = await producerIdentity();
    NodeAssert.equal((await observeProducerSelf(identity)).state, "present");
    NodeAssert.equal((await observeProducerSession(identity, -1)).state, "unknown");
    NodeAssert.equal(
      (await observeProducerSession({ ...identity, namespace: "foreign" }, process.pid)).reason,
      "host_or_namespace_mismatch",
    );
    NodeAssert.equal(
      (await observeProducerSession(identity, Number.MAX_SAFE_INTEGER)).state,
      "unknown",
    );
  },
};

export const resolveCases: Record<string, () => Promise<void>> = {
  "explicit resolve is target-scoped, reports the exact live duty, and rejects assertion flags":
    async () =>
      finiteFixture(async ({ worktree, root }) => {
        const session = await beginProduction(worktree, "fixture-cli", [root]);
        const result = await resolveCommand(["--worktree", worktree, "--json"]);
        NodeAssert.equal(result.records[0]?.id, session.record.id);
        NodeAssert.equal(result.records[0]?.path, session.path);
        NodeAssert.ok(result.records[0]?.reasons.includes("holder_present"));
        await NodeAssert.rejects(
          resolveCommand(["--worktree", worktree, "--assert-absent"]),
          /usage/,
        );
      }),
  "the real resolver CLI returns nonzero for a blocked legacy active record even when its old outcome says retained":
    async () =>
      finiteFixture(async ({ worktree, root, retainOnUnknownCleanup }) => {
        const session = await beginProduction(worktree, "fixture-cli-legacy", [root]);
        const legacy = JSON.parse(await NodeFSP.readFile(session.path, "utf8"));
        delete legacy.holders;
        delete legacy.rev;
        legacy.outcome = "retained";
        await NodeFSP.writeFile(session.path, JSON.stringify(legacy));
        const source = new URL("./resolve.ts", import.meta.url).pathname;
        const output = await new Promise<string>((resolve, reject) => {
          NodeChildProcess.execFile(
            process.execPath,
            [source, "--worktree", worktree, "--json"],
            { timeout: 15000, maxBuffer: 16384 },
            (error, stdout) => {
              if (error?.code === 1) resolve(stdout);
              else {
                if (error?.killed) retainOnUnknownCleanup();
                reject(error ?? new Error("Resolver incorrectly reported success"));
              }
            },
          );
        });
        const result = JSON.parse(output);
        NodeAssert.equal(result.blocked, true);
        NodeAssert.deepEqual(result.records[0].reasons, ["legacy_no_holder_identity"]);
        NodeAssert.equal((await outputObligations(worktree)).length, 1);
      }),
  "only lstat .git absence admits unowned execution; present invalid Git and binding failures block":
    async () =>
      finiteFixture(async ({ worktree, root }) => {
        const distribution = NodePath.join(worktree, "distribution");
        await NodeFSP.mkdir(distribution);
        const messages: string[] = [];
        let allocations = 0;
        const run = () =>
          Effect.runPromise(
            Effect.exit(
              withFiniteProduction(
                freshInput(distribution, root),
                () =>
                  Effect.sync(() => {
                    allocations++;
                  }),
                {
                  report: (message) => {
                    messages.push(message);
                  },
                  capture: captureUnavailable,
                },
              ),
            ),
          );
        NodeAssert.ok(Exit.isSuccess(await run()));
        NodeAssert.equal(allocations, 1);
        NodeAssert.ok(messages[0]?.includes("git_absent"));
        await NodeFSP.writeFile(NodePath.join(distribution, ".git"), "gitdir: missing\n");
        NodeAssert.ok(Exit.isFailure(await run()));
        NodeAssert.equal(allocations, 1);
        await NodeFSP.unlink(NodePath.join(distribution, ".git"));
        await NodeFSP.symlink("missing", NodePath.join(distribution, ".git"));
        NodeAssert.ok(Exit.isFailure(await run()));
        NodeAssert.equal(allocations, 1);
      }),
};

async function padRecords(directory: string, count: number, prefix = "zz-pad") {
  for (let n = 0; n < count; n++)
    await NodeFSP.writeFile(
      NodePath.join(directory, prefix + "-" + n.toString(16).padStart(32, "0") + ".json"),
      "{}",
    );
}
export const capacityCases: Record<string, () => Promise<void>> = {
  "the advisory cursor advances past preserved malformed filenames without treating them as deletion authority":
    async () =>
      finiteFixture(async ({ worktree, root }) => {
        const prior = await beginProduction(worktree, "zz-cursor", [root]);
        await finishProduction(prior, "succeeded");
        const directory = NodePath.dirname(prior.path);
        for (let n = 0; n < 16; n++)
          await NodeFSP.writeFile(
            NodePath.join(directory, "!preserved-" + String(n).padStart(2, "0")),
            "{}",
          );
        await NodeAssert.rejects(resolveProductions(worktree), /observation_failed/);
        const cursor = NodePath.join(
          prior.record.binding.git_dir,
          "workspace-output-obligations-cursor.json",
        );
        NodeAssert.equal(JSON.parse(await NodeFSP.readFile(cursor, "utf8")).after, "!preserved-15");
        const successor = await beginProduction(worktree, "zz-cursor", [root]);
        NodeAssert.equal(successor.record.supersedes?.id, prior.record.id);
        await NodeAssert.rejects(NodeFSP.stat(prior.path), { code: "ENOENT" });
        NodeAssert.equal((await NodeFSP.readdir(directory)).length, 17);
        NodeAssert.equal(
          await NodeFSP.readFile(NodePath.join(directory, "!preserved-00"), "utf8"),
          "{}",
        );
      }),

  "prune failure reports its exact path and count; uncertain successor admission never allocates or deletes the old duty":
    async () =>
      finiteFixture(async ({ worktree, root, records }) => {
        const prior = await beginProduction(worktree, "jones-server-build", [root]);
        await finishProduction(prior, "succeeded");
        const unlink = NodeFSPMutable.unlink;
        let allocations = 0;
        try {
          NodeFSPMutable.unlink = async (path) => {
            if (path === prior.path) throw new Error("fixture unlink unavailable");
            return unlink(path);
          };
          syncBuiltinESMExports();
          const exit = await Effect.runPromise(
            Effect.exit(
              withFiniteProduction(
                freshInput(worktree, root),
                () =>
                  Effect.sync(() => {
                    allocations++;
                  }),
                { report: () => {} },
              ),
            ),
          );
          NodeAssert.ok(Exit.isFailure(exit));
          if (!Exit.isFailure(exit)) throw new Error("Expected blocked successor admission");
          const failure = Cause.squash(exit.cause);
          NodeAssert.ok(failure instanceof Error);
          NodeAssert.ok(failure.cause instanceof Error);
          NodeAssert.equal(
            failure.cause.message,
            `prune_not_confirmed; path=${prior.path}; count=2`,
          );
          NodeAssert.equal(allocations, 0);
          NodeAssert.equal((await records()).length, 2);
          NodeAssert.equal(
            JSON.parse(await NodeFSP.readFile(prior.path, "utf8")).outcome,
            "disposed",
          );
        } finally {
          NodeFSPMutable.unlink = unlink;
          syncBuiltinESMExports();
        }
      }),

  "9999 and 10000 records admit only with an exact validated successor and keep the reader bound":
    async () => {
      for (const total of [9999, 10000])
        await finiteFixture(async ({ worktree, root }) => {
          const prior = await beginProduction(worktree, "aa-capacity", [root]);
          await finishProduction(prior, "succeeded");
          const directory = NodePath.dirname(prior.path);
          await padRecords(directory, total - 1);
          const next = await beginProduction(worktree, "aa-capacity", [root]);
          NodeAssert.equal((await NodeFSP.readdir(directory)).length, total);
          NodeAssert.equal(next.record.supersedes?.id, prior.record.id);
          await NodeAssert.rejects(NodeFSP.stat(prior.path), { code: "ENOENT" });
          NodeAssert.equal(next.record.status, "active");
        });
    },
  "10000 preserved files block before allocation; a 10065th name forbids every deletion including exact temporaries":
    async () =>
      finiteFixture(async ({ worktree, root }) => {
        const bound = await binding(worktree);
        const directory = NodePath.join(bound.git_dir, "workspace-output-obligations");
        await NodeFSP.mkdir(directory);
        await padRecords(directory, 10000);
        await NodeAssert.rejects(
          beginProduction(worktree, "fixture-capacity", [root]),
          /count=10000; no_supersession_candidate/,
        );
        NodeAssert.equal((await NodeFSP.readdir(directory)).length, 10000);
        for (let n = 10000; n < 10064; n++)
          await NodeFSP.writeFile(
            NodePath.join(directory, "zz-pad-" + n.toString(16).padStart(32, "0") + ".json"),
            "{}",
          );
        const temporary = NodePath.join(
          directory,
          ".fixture-capacity-" + "0".repeat(32) + "-" + "1".repeat(16),
        );
        await NodeFSP.writeFile(temporary, "exact but above the ceiling");
        await NodeAssert.rejects(
          beginProduction(worktree, "fixture-capacity", [root]),
          /count>=10065/,
        );
        NodeAssert.equal(await NodeFSP.readFile(temporary, "utf8"), "exact but above the ceiling");
        await NodeAssert.rejects(NodeFSP.stat(root), { code: "ENOENT" });
      }),
  "10001 crash recovery advances past sixteen preserved candidates and a corrupt cursor authorizes no deletion":
    async () =>
      finiteFixture(async ({ worktree, root }) => {
        const prior = await beginProduction(worktree, "aa-pad", [root]);
        await finishProduction(prior, "succeeded");
        const directory = NodePath.dirname(prior.path);
        const oldPath = prior.path;
        const oldId = "aa-pad-" + "0".repeat(32);
        prior.record.id = oldId;
        prior.path = NodePath.join(directory, oldId + ".json");
        await rewrite(prior);
        await NodeFSP.unlink(oldPath);
        for (let n = 1; n < 10001; n++)
          await NodeFSP.writeFile(
            NodePath.join(directory, "aa-pad-" + n.toString(16).padStart(32, "0") + ".json"),
            "{}",
          );
        const successor = structuredClone(prior);
        successor.record.id = "aa-pad-" + (16).toString(16).padStart(32, "0");
        successor.path = NodePath.join(directory, successor.record.id + ".json");
        successor.record.supersedes = { id: oldId, rev: prior.record.rev! };
        successor.record.status = "active";
        successor.record.outcome = "unresolved";
        delete successor.record.finished_at;
        delete successor.record.resolution;
        successor.record.rev = 1;
        successor.record.reasons = ["producer_active"];
        successor.record.holders!.self_discharge = null;
        successor.record.holders!.in_process = "running";
        successor.record.holders!.admission_closed = false;
        await rewrite(successor);
        const cursor = NodePath.join(
          prior.record.binding.git_dir,
          "workspace-output-obligations-cursor.json",
        );
        await NodeFSP.writeFile(cursor, "corrupt cursor");
        await NodeAssert.rejects(resolveProductions(worktree), /count=10001/);
        NodeAssert.equal((await NodeFSP.readdir(directory)).length, 10001);
        NodeAssert.equal(
          JSON.parse(await NodeFSP.readFile(cursor, "utf8")).after,
          "aa-pad-" + (15).toString(16).padStart(32, "0") + ".json",
        );
        // Recovery succeeds before the malformed preserved records correctly block listing.
        await NodeAssert.rejects(resolveProductions(worktree), /identity_mismatch/);
        NodeAssert.equal((await NodeFSP.readdir(directory)).length, 10000);
        await NodeAssert.rejects(NodeFSP.stat(prior.path), { code: "ENOENT" });
        NodeAssert.equal(
          await NodeFSP.readFile(
            NodePath.join(directory, "aa-pad-" + (1).toString(16).padStart(32, "0") + ".json"),
            "utf8",
          ),
          "{}",
        );
      }),
};
