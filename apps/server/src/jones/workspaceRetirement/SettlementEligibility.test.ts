import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
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
