import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { DEFAULT_SERVER_SETTINGS, ProjectId } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { resolveWorktreeCleanup } from "@t3tools/shared/projectSettings";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import { it } from "vite-plus/test";
import * as Config from "../../config.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as GitVcsDriver from "../../vcs/GitVcsDriver.ts";
import { NativeObservations } from "./NativeObservations.ts";
import {
  HERMETIC_GIT_ENV,
  preserveCases,
  preserveRuntimeCases,
  type PreserveEvaluation,
} from "./PreserveBranch.testkit.ts";
import { evaluate } from "./Runtime.ts";

it.each(Object.entries(preserveCases))("%s", (_name, run) => run());

const probe = () => ({
  observedAt: DateTime.toEpochMillis(DateTime.nowUnsafe()),
  status: "complete" as const,
  paths: [],
  limits: ["synthetic native adapter"],
});
// The server's own Git adapter; its configuration directory is a scoped temporary directory.
const gitLayer = GitVcsDriver.layer.pipe(
  Layer.provideMerge(Config.layerTest(process.cwd(), { prefix: "preserve-branch-" })),
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);
// Settings evaluation as the owner action runs it. Every Git call is recorded and runs with the
// fixture's hermetic configuration merged into the adapter's `env`, as the temporary index is.
const evaluation: PreserveEvaluation = (sql, { registry: _registry, admit, ...request }, git) =>
  Effect.gen(function* () {
    const server = yield* GitVcsDriver.GitVcsDriver;
    const recording: GitVcsDriver.GitVcsDriver["Service"] = {
      ...server,
      // Recorded when the command runs; Runtime builds its removal effect before deciding to run it.
      execute: (input) =>
        Effect.suspend(() => {
          git.calls.push([...input.args]);
          return server.execute({
            ...input,
            env: { ...HERMETIC_GIT_ENV, HOME: git.home, ...input.env },
          });
        }),
    };
    return yield* evaluate({
      ...request,
      threadId: null,
      rules: resolveWorktreeCleanup(DEFAULT_SERVER_SETTINGS, ProjectId.make("synthetic-project")),
      admit: admit ?? Effect.void,
      revalidate: Effect.void,
    }).pipe(
      Effect.provideService(GitVcsDriver.GitVcsDriver, recording),
      Effect.provideService(HostProcess.Hostname, "mini.local"),
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
  }).pipe(Effect.provide(gitLayer));
it.each(Object.entries(preserveRuntimeCases(evaluation)))("%s", (_name, run) => run());
