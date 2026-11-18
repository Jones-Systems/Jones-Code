import { it } from "vite-plus/test";
import { DEFAULT_SERVER_SETTINGS, ProjectId } from "@t3tools/contracts";
import { resolveWorktreeCleanup } from "@t3tools/shared/projectSettings";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as Config from "../../config.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Registry from "../workspaceLifecycle/RegistryCli.ts";
import { NativeObservations } from "./NativeObservations.ts";
import { evaluate } from "./Runtime.ts";
import {
  absentCheckoutCases,
  retirementCases,
  type EvaluateRetirement,
} from "./Retirement.testkit.ts";
it.each(Object.entries(retirementCases))("%s", (_name, run) => run());

const probe = () => ({
  observedAt: DateTime.toEpochMillis(DateTime.nowUnsafe()),
  status: "complete" as const,
  paths: [],
  limits: ["synthetic native adapter"],
});
// Settings evaluation with no lifecycle registry, thread or native workload configured.
const settingsEvaluation: EvaluateRetirement = (sql, { registry, admit, ...request }) =>
  evaluate({
    ...request,
    threadId: null,
    rules: resolveWorktreeCleanup(DEFAULT_SERVER_SETTINGS, ProjectId.make("synthetic-project")),
    admit: admit ?? Effect.void,
    revalidate: Effect.void,
  }).pipe(
    registry
      ? Effect.provideService(
          Registry.LifecycleRegistry,
          registry as unknown as Registry.LifecycleRegistry["Service"],
        )
      : (effect) => effect,
    Effect.provideService(SqlClient.SqlClient, sql),
    Effect.provideService(Config.ServerConfig, {
      workspaceLifecycle: undefined,
    } as unknown as Config.ServerConfig["Service"]),
    Effect.provideService(
      Projections.ProjectionStoreV2,
      {} as unknown as Projections.ProjectionStoreV2["Service"],
    ),
    Effect.provideService(NativeObservations, {
      temporaryRoots: [],
      services: async () => probe(),
      processes: async () => probe(),
      workloads: () => Effect.succeed(probe()),
    }),
  );
it.each(Object.entries(absentCheckoutCases(settingsEvaluation)))("%s", (_name, run) => run());
