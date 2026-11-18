import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Net from "@t3tools/shared/Net";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import { resolveServerConfig } from "../../cli/config.ts";
import * as ServerConfig from "../../config.ts";
import type { Configuration as FleetCheckpointConfiguration } from "../fleetCheckpoint/Configuration.ts";
import type { Configuration as WorkspaceLifecycleConfiguration } from "../workspaceLifecycle/Configuration.ts";

const checkpoint = {
  executable: "/synthetic/host-tooling",
  arguments: ["fleet-controller"],
  binding: "/synthetic/client.json",
  cwd: "/synthetic",
  controllerId: "synthetic-controller",
} satisfies FleetCheckpointConfiguration;

const lifecycle = {
  environmentId: "synthetic-environment",
  projects: [
    {
      projectId: "synthetic-project",
      repository: "Jones-Systems/Synthetic",
      projectRoot: "/synthetic/repository",
    },
  ],
  registry: {
    executable: "/synthetic/v3",
    arguments: ["workspace-lifecycle"],
    database: "/synthetic/registry.sqlite",
    cwd: "/synthetic",
  },
} satisfies WorkspaceLifecycleConfiguration;

const resolve = (baseDir: string, cwd: string, env: Record<string, string>) =>
  resolveServerConfig(
    {
      mode: Option.some("desktop"),
      port: Option.some(4888),
      host: Option.none(),
      baseDir: Option.some(baseDir),
      cwd: Option.some(cwd),
      devUrl: Option.none(),
      noBrowser: Option.none(),
      bootstrapFd: Option.none(),
      autoBootstrapProjectFromCwd: Option.none(),
      logWebSocketEvents: Option.none(),
      tailscaleServeEnabled: Option.none(),
      tailscaleServePort: Option.none(),
      lan: Option.none(),
    },
    Option.none(),
  ).pipe(Effect.provide([Net.layer, ConfigProvider.layer(ConfigProvider.fromEnv({ env }))]));

it.layer(NodeServices.layer)("retained owner policy configuration", (it) => {
  it.effect("keeps both owners disabled when neither environment setting is supplied", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "jones-policy-defaults-" });
      const config = yield* resolve(path.join(root, "app"), root, {});
      expect(config.fleetCheckpoint).toBeUndefined();
      expect(config.workspaceLifecycle).toBeUndefined();
      expect(config.serverParallelism.profile).toBe("laptop");
      expect(config.serverParallelism.readerCount).toBe(2);
      expect(config.serverParallelism.computeWorkerCount).toBe(2);
    }),
  );

  it.effect.each([false, true])(
    "preserves explicit lifecycle and independent checkpoint selection through CLI/service composition (checkpoint: %s)",
    (includeCheckpoint) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "jones-policy-configured-" });
        const authorityStateDir = path.join(root, "authority");
        const config = yield* resolve(path.join(root, "app"), root, {
          ...(includeCheckpoint
            ? { JONES_FLEET_CHECKPOINT_COMMAND: JSON.stringify(checkpoint) }
            : {}),
          JONES_WORKSPACE_LIFECYCLE_CONFIG: JSON.stringify(lifecycle),
          JONES_SERVER_PARALLELISM_PROFILE: "vps",
          T3CODE_NATIVE_AUTHORITY_STATE_DIR: `  ${authorityStateDir}  `,
        });
        const provided = yield* ServerConfig.ServerConfig.pipe(
          Effect.provide(ServerConfig.layer(ServerConfig.make(config))),
        );
        expect(provided.fleetCheckpoint).toEqual(includeCheckpoint ? checkpoint : undefined);
        expect(provided.workspaceLifecycle).toEqual(lifecycle);
        expect(provided.authorityStateDir).toBe(authorityStateDir);
        expect(provided.serverParallelism.readerCount).toBe(3);
        expect(provided.serverParallelism.computeWorkerCount).toBe(3);
        expect(yield* fs.exists(provided.dbPath)).toBe(false);
      }),
  );

  it.effect(
    "rejects malformed and out-of-schema policies before creating cwd or server state",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "jones-policy-invalid-" });
        const invalid: ReadonlyArray<Record<string, string>> = [
          { JONES_FLEET_CHECKPOINT_COMMAND: "not JSON" },
          {
            JONES_FLEET_CHECKPOINT_COMMAND: JSON.stringify({
              ...checkpoint,
              executable: "relative",
            }),
          },
          {
            JONES_FLEET_CHECKPOINT_COMMAND: JSON.stringify({ ...checkpoint, controllerId: "" }),
          },
          {
            JONES_FLEET_CHECKPOINT_COMMAND: JSON.stringify({ ...checkpoint, unknownPolicy: true }),
          },
          { JONES_WORKSPACE_LIFECYCLE_CONFIG: "not JSON" },
          {
            JONES_WORKSPACE_LIFECYCLE_CONFIG: JSON.stringify({ ...lifecycle, environmentId: "" }),
          },
          {
            JONES_WORKSPACE_LIFECYCLE_CONFIG: JSON.stringify({
              ...lifecycle,
              projects: [...lifecycle.projects, ...lifecycle.projects],
            }),
          },
          {
            JONES_WORKSPACE_LIFECYCLE_CONFIG: JSON.stringify({
              ...lifecycle,
              registry: { ...lifecycle.registry, executable: "relative" },
            }),
          },
          {
            JONES_WORKSPACE_LIFECYCLE_CONFIG: JSON.stringify({ ...lifecycle, unknownPolicy: true }),
          },
        ];
        for (const [index, env] of invalid.entries()) {
          const baseDir = path.join(root, `app-${index}`);
          const cwd = path.join(root, `cwd-${index}`);
          const error = yield* resolve(baseDir, cwd, env).pipe(Effect.flip);
          expect(error._tag).toBe("ConfigError");
          expect(yield* fs.exists(baseDir)).toBe(false);
          expect(yield* fs.exists(cwd)).toBe(false);
        }
      }),
  );
});
