import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { CommandId, DEFAULT_SERVER_SETTINGS } from "@t3tools/contracts";
import { resolveWorktreeCleanup } from "@t3tools/shared/projectSettings";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Config from "../../config.ts";
import { fixture, threadId, projectId } from "../workspaceLifecycle/ManagedConsumer.testkit.ts";
import * as Registry from "../workspaceLifecycle/RegistryCli.ts";
import { NativeObservations } from "./NativeObservations.ts";
import { evaluate } from "./Runtime.ts";
import { Receipts, layer } from "./Receipts.ts";
import { binding } from "./OutputAttestation.ts";

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
