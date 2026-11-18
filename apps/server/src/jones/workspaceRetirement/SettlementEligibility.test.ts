import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import { CommandId, DEFAULT_SERVER_SETTINGS, ThreadId } from "@t3tools/contracts";
import { resolveWorktreeCleanup } from "@t3tools/shared/projectSettings";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Config from "../../config.ts";
import {
  fixture,
  threadId,
  projectId,
  selection,
} from "../workspaceLifecycle/ManagedConsumer.testkit.ts";
import * as Registry from "../workspaceLifecycle/RegistryCli.ts";
import { LifecycleError } from "../workspaceLifecycle/Protocol.ts";
import { nativePlatform } from "./Filesystem.ts";
import { NativeObservations } from "./NativeObservations.ts";
import { evaluate } from "./Runtime.ts";
import { Receipts, layer } from "./Receipts.ts";
import { binding } from "./OutputAttestation.ts";
import { beginProduction } from "./ProducerFinalizer.ts";

it.effect.each([
  "clean",
  "dirty",
  "untracked",
  "active",
  "terminal",
  "output",
  "configured-failure",
  "registered-output",
  "lifecycle-only",
  "legacy",
  "recent-legacy",
  "pending",
  "running",
  "failed",
] as const)("default-unconfigured retirement qualification keeps the real %s protections", (mode) =>
  Effect.scoped(
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        yield* f.seed;
        yield* f.fs.writeFileString(f.linked + "/tracked", "original");
        yield* f.runGit(["add", "tracked"], f.linked);
        yield* f.runGit(["commit", "-m", "tracked fixture"], f.linked);
        yield* f.dispatch({
          type: "thread.settle",
          commandId: CommandId.make("qualify"),
          threadId,
        });
        const sql = yield* SqlClient.SqlClient;
        const store = yield* Receipts.pipe(Effect.provide(layer));
        const [intent] = yield* store.recoveryBatch(0);
        if (mode !== "pending") {
          yield* store.startScript(intent!.event_id);
          if (mode !== "running")
            yield* store.finishScript(
              intent!.event_id,
              mode === "failed" ? "failed" : "completed",
              mode === "failed" ? 1 : 0,
            );
        }
        if (mode === "dirty") yield* f.fs.writeFileString(f.linked + "/tracked", "dirty");
        if (mode === "untracked") yield* f.fs.writeFileString(f.linked + "/novel", "untracked");
        if (mode === "active")
          yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.status','running') WHERE thread_id=${threadId}`;
        if (mode === "terminal")
          yield* store.claim({
            id: "terminal",
            worktree: f.linked,
            kind: "terminal",
            owner: "fixture",
            holder: JSON.stringify("fixture"),
          });
        if (mode === "output")
          yield* Effect.promise(() => beginProduction(f.linked, "fixture", [f.linked + "/build"]));
        if (["legacy", "recent-legacy", "pending", "running", "failed"].includes(mode)) {
          const historical = ThreadId.make("historical");
          yield* f.dispatch({
            type: "thread.create",
            commandId: CommandId.make("historical-create"),
            threadId: historical,
            projectId,
            createdBy: "user",
            creationSource: "web",
            title: "Historical",
            modelSelection: selection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: "work",
            worktreePath: f.linked,
          });
          yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.settledOverride','settled','$.settledAt',${mode !== "recent-legacy" ? "2000-01-01T00:00:00.000Z" : DateTime.formatIso(DateTime.nowUnsafe())}) WHERE thread_id=${historical}`;
        }
        const probe = () => ({
          observedAt: DateTime.toEpochMillis(DateTime.nowUnsafe()),
          status: "complete" as const,
          paths: [],
          limits: ["synthetic native adapter"],
        });
        let request = evaluate({
          worktree: f.linked,
          projectRoot: f.repo,
          threadId,
          cause: "owner_action",
          execute: false,
          rules: resolveWorktreeCleanup(DEFAULT_SERVER_SETTINGS, projectId),
          admit: Effect.void,
          revalidate: Effect.void,
        }).pipe(
          Effect.provideService(Config.ServerConfig, {
            ...f.config,
            workspaceLifecycle: undefined,
          }),
          Effect.provideService(NativeObservations, {
            temporaryRoots: [],
            services: async () => probe(),
            processes: async () => probe(),
            workloads: () => Effect.succeed(probe()),
          }),
        );
        if (mode === "configured-failure")
          request = request.pipe(
            Effect.provideService(Registry.LifecycleRegistry, {
              show: () => Effect.fail(new LifecycleError({ reason: "unavailable" })),
              resume: () => Effect.die("not used"),
            }),
          );
        if (mode === "registered-output" || mode === "lifecycle-only") {
          const bound = yield* Effect.promise(() => binding(f.linked));
          request = request.pipe(
            Effect.provideService(Registry.LifecycleRegistry, {
              show: () =>
                Effect.succeed({
                  worktree: f.linked,
                  generation: null,
                  binding: null,
                  assessment: null,
                  disposition: "cleanup_candidate",
                  assessment_status: "absent",
                  stored_readiness: null,
                  physical_readiness: "unknown",
                  binding_evidence: "caller_asserted",
                  authority_effect: "none",
                }),
              resume: () => Effect.die("not used"),
              ...(mode === "registered-output"
                ? {
                    outputs: () =>
                      Effect.succeed([
                        {
                          id: "artifact",
                          path: f.linked + "/evidence",
                          owner: "fixture",
                          class: "evidence",
                          state: "retained",
                          worktree: {
                            path: f.linked,
                            dev: bound.dev,
                            ino: bound.ino,
                            git_dir: bound.git_dir,
                            git_common_dir: bound.git_common_dir,
                          },
                        },
                      ]),
                  }
                : {}),
            }),
          );
        }
        const result = yield* request;
        if (["clean", "legacy", "lifecycle-only"].includes(mode))
          expect(result.outcome.state).toBe("proposed");
        else expect(result.outcome.state, JSON.stringify(result.outcome)).toBe("held");
        if (mode === "legacy")
          expect(
            yield* sql`SELECT * FROM jones_workspace_retirement_intents WHERE thread_id='historical'`,
          ).toEqual([]);
        if (mode === "configured-failure")
          expect(result.outcome.reasonCodes.length).toBeGreaterThan(0);
      }).pipe(Effect.provide(f.layer));
    }),
  ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);

// A thread may record its checkout through a symlinked spelling (a linked T3 home or
// worktrees directory, or macOS /tmp and /var). Retirement binds the realpath, so every
// protection observed under the recorded spelling must still hold the canonical target.
it.effect.each(["clean", "active", "terminal", "terminal-canonical", "registered-output"] as const)(
  "symlinked worktree spelling keeps the %s protection",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        const alias = f.root + "/alias";
        yield* f.fs.symlink(f.config.worktreesDir, alias);
        const spelled = alias + "/project/linked";
        expect(yield* f.fs.realPath(spelled)).toBe(f.linked);
        yield* Effect.gen(function* () {
          yield* f.seedThread(threadId, "work", spelled);
          yield* f.fs.writeFileString(f.linked + "/tracked", "original");
          yield* f.runGit(["add", "tracked"], f.linked);
          yield* f.runGit(["commit", "-m", "tracked fixture"], f.linked);
          yield* f.dispatch({
            type: "thread.settle",
            commandId: CommandId.make("qualify"),
            threadId,
          });
          const sql = yield* SqlClient.SqlClient;
          const store = yield* Receipts.pipe(Effect.provide(layer));
          const [intent] = yield* store.recoveryBatch(0);
          yield* store.startScript(intent!.event_id);
          yield* store.finishScript(intent!.event_id, "completed", 0);
          if (mode === "active")
            yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.status','running') WHERE thread_id=${threadId}`;
          if (mode === "terminal" || mode === "terminal-canonical")
            yield* store.claim({
              id: "terminal",
              worktree: mode === "terminal" ? spelled : f.linked,
              kind: "terminal",
              owner: "fixture",
              holder: JSON.stringify("fixture"),
            });
          const probe = () => ({
            observedAt: DateTime.toEpochMillis(DateTime.nowUnsafe()),
            status: "complete" as const,
            paths: [],
            limits: ["synthetic native adapter"],
          });
          let request = evaluate({
            worktree: spelled,
            projectRoot: f.repo,
            threadId,
            cause: "owner_action",
            execute: false,
            rules: resolveWorktreeCleanup(DEFAULT_SERVER_SETTINGS, projectId),
            admit: Effect.void,
            revalidate: Effect.void,
          }).pipe(
            Effect.provideService(Config.ServerConfig, {
              ...f.config,
              workspaceLifecycle: undefined,
            }),
            Effect.provideService(NativeObservations, {
              temporaryRoots: [],
              services: async () => probe(),
              processes: async () => probe(),
              workloads: () => Effect.succeed(probe()),
            }),
          );
          if (mode === "registered-output") {
            const bound = yield* Effect.promise(() => binding(f.linked));
            request = request.pipe(
              Effect.provideService(Registry.LifecycleRegistry, {
                show: () =>
                  Effect.succeed({
                    worktree: spelled,
                    generation: null,
                    binding: null,
                    assessment: null,
                    disposition: "cleanup_candidate",
                    assessment_status: "absent",
                    stored_readiness: null,
                    physical_readiness: "unknown",
                    binding_evidence: "caller_asserted",
                    authority_effect: "none",
                  }),
                resume: () => Effect.die("not used"),
                outputs: () =>
                  Effect.succeed([
                    {
                      id: "artifact",
                      path: spelled + "/evidence",
                      owner: "fixture",
                      class: "evidence",
                      state: "retained",
                      worktree: {
                        path: f.linked,
                        dev: bound.dev,
                        ino: bound.ino,
                        git_dir: bound.git_dir,
                        git_common_dir: bound.git_common_dir,
                      },
                    },
                  ]),
              }),
            );
          }
          const result = yield* request;
          if (mode === "clean") expect(result.outcome.state).toBe("proposed");
          else {
            expect(result.outcome.state, JSON.stringify(result.outcome)).toBe("held");
            expect(result.outcome.reasonCodes).toContain("active_consumer");
          }
        }).pipe(Effect.provide(f.layer));
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);

describe.skipIf(nativePlatform !== "darwin")("native Darwin owner retirement", () => {
  it.effect(
    "owner propose then retire removes a real linked worktree using native Darwin observations",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture;
          yield* Effect.gen(function* () {
            yield* f.seed;
            yield* f.fs.writeFileString(f.linked + "/tracked", "original");
            yield* f.runGit(["add", "tracked"], f.linked);
            yield* f.runGit(["commit", "-m", "tracked fixture"], f.linked);
            // A commit left only in this worktree's reflog must be preserved by a recovery ref.
            yield* f.fs.writeFileString(f.linked + "/tracked", "abandoned");
            yield* f.runGit(["commit", "-am", "abandoned fixture"], f.linked);
            const abandoned = (yield* f.runGit(["rev-parse", "HEAD"], f.linked)).stdout.trim();
            yield* f.runGit(["reset", "--hard", "HEAD~1"], f.linked);
            const head = (yield* f.runGit(["rev-parse", "HEAD"], f.linked)).stdout.trim();
            // The fixture repository lives under the OS temporary root, which native observation
            // never treats as durable; a pushed branch is what establishes restoration here.
            yield* f.runGit(["update-ref", "refs/remotes/origin/work", head]);
            yield* f.dispatch({
              type: "thread.settle",
              commandId: CommandId.make("native-darwin-settle"),
              threadId,
            });
            const store = yield* Receipts.pipe(Effect.provide(layer));
            const [intent] = yield* store.recoveryBatch(0);
            yield* store.startScript(intent!.event_id);
            yield* store.finishScript(intent!.event_id, "completed", 0);
            const request = (ownerCoverage?: { fingerprint: string; paths: readonly string[] }) =>
              evaluate({
                worktree: f.linked,
                projectRoot: f.repo,
                threadId,
                cause: "owner_action",
                execute: ownerCoverage !== undefined,
                ...(ownerCoverage ? { ownerCoverage } : {}),
                rules: resolveWorktreeCleanup(DEFAULT_SERVER_SETTINGS, projectId),
                admit: Effect.void,
                revalidate: Effect.void,
                // No NativeObservations override: services, processes and workloads are native.
              }).pipe(
                Effect.provideService(Config.ServerConfig, {
                  ...f.config,
                  workspaceLifecycle: undefined,
                }),
              );
            const complete = (
              coverage: Record<string, { status: string; reason: string | null }>,
            ) =>
              expect(
                Object.fromEntries(
                  Object.entries(coverage).map(([name, probe]) => [
                    name,
                    [probe.status, probe.reason],
                  ]),
                ),
              ).toEqual({
                native: ["complete", null],
                services: ["complete", null],
                registry: ["complete", null],
                workloads: ["complete", null],
                obligations: ["complete", null],
                process: ["complete", null],
              });
            const proposal = yield* request();
            expect(proposal.outcome.state, JSON.stringify(proposal.outcome)).toBe("proposed");
            if (!("qualification" in proposal)) throw new Error("proposal has no qualification");
            complete(proposal.qualification.consumers!.coverage);
            const retired = yield* request({
              fingerprint: proposal.outcome.fingerprint!,
              paths: proposal.outcome.paths,
            });
            expect(retired.outcome.state, JSON.stringify(retired.outcome)).toBe("retired");
            if (!("qualification" in retired)) throw new Error("receipt has no qualification");
            complete(retired.qualification.consumers!.coverage);
            expect(yield* f.fs.exists(f.linked)).toBe(false);
            expect((yield* f.runGit(["rev-parse", "refs/heads/work"])).stdout.trim()).toBe(head);
            const recovery = (yield* f.runGit([
              "for-each-ref",
              "--format=%(refname) %(objectname)",
              "refs/retired-worktrees/",
            ])).stdout
              .trim()
              .split("\n");
            expect(recovery).toHaveLength(1);
            expect(recovery[0]).toMatch(new RegExp(`^refs/retired-worktrees/\\S+ ${abandoned}$`));
            expect(
              (yield* f.runGit(["worktree", "list", "--porcelain"])).stdout.includes(f.linked),
            ).toBe(false);
          }).pipe(Effect.provide(f.layer));
        }),
      ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );
});
