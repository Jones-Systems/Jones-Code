import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { DEFAULT_SERVER_SETTINGS, ProjectId, ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Config from "../../config.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Projects from "../../orchestration-v2/ProjectStore.ts";
import * as Settings from "../../serverSettings.ts";
import * as OwnerAction from "./OwnerAction.ts";
import { TargetObservation } from "./TargetPresence.ts";

it.effect(
  "absent historical entries do not consume the owner listing's expensive candidate budget",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const candidates = Array.from({ length: 206 }, (_, i) => ({
          threadId: ThreadId.make(`old-${i}`),
          worktreePath: `/synthetic/checkout-${i}`,
        }));
        let observations = 0;
        const action = yield* OwnerAction.make.pipe(
          Effect.provideService(TargetObservation, async (path) => {
            observations++;
            return {
              status: path === candidates[205]!.worktreePath ? "present" : "absent_unregistered",
            };
          }),
          Effect.provide(
            Layer.mergeAll(
              Layer.mock(Settings.ServerSettingsService)({
                getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS),
              }),
              Layer.mock(Projects.ProjectStoreV2)({
                get: () => Effect.succeedSome({ workspaceRoot: "/synthetic/repo" } as never),
              }),
              Layer.mock(Projections.ProjectionStoreV2)({
                getThreadWorktreePaths: () => Effect.succeed(candidates),
                getThread: (id) =>
                  Effect.succeed({
                    id,
                    title: "historical",
                    projectId: ProjectId.make("project"),
                  } as never),
              }),
            ),
          ),
        );
        const report = yield* action({ action: "propose" });
        expect(observations).toBe(206);
        expect(report.entries).toHaveLength(206);
        expect(report.omittedCount).toBe(0);
        expect(
          report.entries
            .slice(0, 205)
            .every((e) => e.retirement?.reasonCodes[0] === "worktree_already_absent"),
        ).toBe(true);
        expect(report.entries[205]!.retirement?.reasonCodes).not.toContain(
          "worktree_already_absent",
        );
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(
          Config.layerTest(process.cwd(), { prefix: "retirement-owner-listing-" }),
          NodeSqliteClient.layer({ filename: ":memory:" }),
        ).pipe(Layer.provide(NodeServices.layer)),
      ),
    ),
);
