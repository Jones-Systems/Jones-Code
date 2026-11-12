import { it } from "vite-plus/test";
import {
  resolutionCases,
  capacityCases,
} from "../../../../../scripts/jones/direct-producers/finite-production.testkit.ts";

import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it as effectIt, expect } from "@effect/vitest";
import { DEFAULT_SERVER_SETTINGS } from "@t3tools/contracts";
import { resolveWorktreeCleanup } from "@t3tools/shared/projectSettings";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import { fixture, projectId } from "../workspaceLifecycle/ManagedConsumer.testkit.ts";
import { LifecycleRegistry } from "../workspaceLifecycle/RegistryCli.ts";
import { NativeObservations } from "./NativeObservations.ts";
import { beginProduction } from "./ProducerFinalizer.ts";
import { evaluate } from "./Runtime.ts";

it.each(Object.entries({ ...resolutionCases, ...capacityCases }))("%s", (_name, run) => run());

// Configured-runtime oracle only. It uses real private Git/SQLite and never
// launches a provider or grants removal authority; the Node seam does not run it.
effectIt.effect(
  "Runtime reconciles only its target and reports exact duties with retirement disabled",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        const path = yield* Path.Path;
        yield* Effect.gen(function* () {
          yield* f.seed;
          const worktree = yield* f.fs.realPath(f.linked);
          const root = path.join(worktree, "dist");
          const session = yield* Effect.promise(() =>
            beginProduction(worktree, "fixture-runtime", [root]),
          );
          const rules = {
            ...resolveWorktreeCleanup(DEFAULT_SERVER_SETTINGS, projectId),
            worktreeSettledAfterDays: null,
          };
          const result = yield* evaluate({
            worktree,
            projectRoot: f.repo,
            threadId: null,
            rules,
            cause: "settlement",
            execute: false,
            admit: Effect.die("Disabled retirement must not acquire removal authority"),
            revalidate: Effect.die("Disabled retirement must not remove a checkout"),
          });
          expect(result.outcome.outputObligations).toContainEqual({
            id: session.record.id,
            path: root,
            owner: session.record.owner,
            state: "active",
          });
          const detail = result.outcome.details.find((entry) =>
            entry.lineage.some((lineage) => lineage.productionId === session.record.id),
          );
          expect(detail?.fact).toContain("status active and outcome unresolved");
          expect(detail?.nextAction).toContain("direct-producers/resolve.ts --worktree");
          expect(yield* f.fs.exists(f.linked)).toBe(true);
          const record = JSON.parse(yield* f.fs.readFileString(session.path));
          expect(record.holders.admission_closed).toBe(true);
          expect(record.status).toBe("active");
          expect(record.finished_at).toBeUndefined();
        }).pipe(
          Effect.provideService(NativeObservations, {
            services: async () => ({
              observedAt: DateTime.toEpochMillis(DateTime.nowUnsafe()),
              status: "complete",
              paths: [],
              limits: [],
            }),
            processes: async () => ({
              observedAt: DateTime.toEpochMillis(DateTime.nowUnsafe()),
              status: "complete",
              paths: [],
              limits: [],
            }),
            workloads: () =>
              Effect.succeed({
                observedAt: DateTime.toEpochMillis(DateTime.nowUnsafe()),
                status: "complete",
                paths: [],
                limits: [],
              }),
            temporaryRoots: [],
          }),
          Effect.provideService(LifecycleRegistry, {
            resume: () => Effect.die("No provider or checkout resume"),
            show: () => Effect.sync(f.state.view),
            outputs: () => Effect.succeed([]),
          }),
          Effect.provide(f.layer.pipe(Layer.provideMerge(f.configured))),
        );
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);
