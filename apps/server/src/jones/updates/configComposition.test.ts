// @effect-diagnostics nodeBuiltinImport:off -- Real symlinks and permission reads exercise authority placement.
import * as NodeFS from "node:fs";

import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { DEFAULT_SIGNAL_EXPORT } from "@t3tools/shared/observability";
import * as OtelEnvironment from "@t3tools/shared/otelEnvironment";
import * as ServerConfig from "../../config.ts";
import { resolveServerParallelismProfile } from "../serverParallelism/config.ts";

const configInput = Effect.fn(function* () {
  const baseDir = "/config-composition-fixture";
  const paths = yield* ServerConfig.deriveServerPaths(baseDir, undefined);
  return {
    ...paths,
    logLevel: "Error",
    traceMinLevel: "Info",
    traceTimingEnabled: true,
    traceBatchWindowMs: 200,
    traceMaxBytes: 10 * 1024 * 1024,
    traceMaxFiles: 10,
    otlpTracesUrl: undefined,
    otlpMetricsUrl: undefined,
    otlpLogsUrl: undefined,
    otlpTracesExport: DEFAULT_SIGNAL_EXPORT,
    otlpMetricsExport: DEFAULT_SIGNAL_EXPORT,
    otlpLogsExport: DEFAULT_SIGNAL_EXPORT,
    otelEnvironment: OtelEnvironment.none,
    mode: "web",
    port: 0,
    host: undefined,
    cwd: baseDir,
    baseDir,
    staticDir: undefined,
    devUrl: undefined,
    devAllowedOrigins: [],
    noBrowser: true,
    startupPresentation: "headless",
    desktopBootstrapToken: undefined,
    autoBootstrapProjectFromCwd: false,
    logWebSocketEvents: false,
    tailscaleServeEnabled: false,
    tailscaleServePort: 443,
  } as const satisfies Parameters<typeof ServerConfig.make>[0];
});

it.layer(NodeServices.layer)("server config composition", (it) => {
  it.effect("supplies the existing default profile for standalone config construction", () =>
    Effect.gen(function* () {
      const input = yield* configInput();
      const config = ServerConfig.make(input);
      expect(config.serverParallelism).toEqual({
        profile: "laptop",
        selectionSource: "default",
        readerCount: 2,
        computeWorkerCount: 2,
      });
      expect(config.authorityStateDir).toBe(input.authorityStateDir);
      expect(config.startupPresentation).toBe("headless");
      expect(config.fleetCheckpoint).toBeUndefined();
      expect(config.workspaceLifecycle).toBeUndefined();
    }),
  );

  it.effect("preserves the selected profile and counts through make and the service layer", () =>
    Effect.gen(function* () {
      const input = yield* configInput();
      for (const profile of ["laptop", "vps"] as const) {
        const selected = resolveServerParallelismProfile(profile);
        const config = ServerConfig.make({ ...input, serverParallelism: selected });
        expect(config.serverParallelism).toBe(selected);
        const provided = yield* ServerConfig.ServerConfig.pipe(
          Effect.provide(ServerConfig.layer(config)),
        );
        expect(provided.serverParallelism).toBe(selected);
        expect(provided.serverParallelism.selectionSource).toBe("environment");
        expect(provided.serverParallelism.readerCount).toBe(profile === "vps" ? 3 : 2);
        expect(provided.serverParallelism.computeWorkerCount).toBe(profile === "vps" ? 3 : 2);
      }
    }),
  );

  it.effect("keeps authority state stable across upstream development-state selection", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const baseDir = "/config-composition-fixture";
      const devUrl = new URL("http://127.0.0.1:5173");
      const regular = yield* ServerConfig.deriveServerPaths(baseDir, undefined);
      const implicitDev = yield* ServerConfig.deriveServerPaths(baseDir, devUrl);
      const explicitDev = yield* ServerConfig.deriveServerPaths(baseDir, devUrl, {
        baseDirIsExplicit: true,
      });
      expect(regular.stateDir).toBe(path.join(baseDir, "userdata"));
      expect(implicitDev.stateDir).toBe(path.join(baseDir, "dev"));
      expect(explicitDev.stateDir).toBe(regular.stateDir);
      for (const paths of [regular, implicitDev, explicitDev]) {
        expect(paths.authorityStateDir).toBe(path.join(baseDir, "native-store-authority"));
        expect(paths.dbPath).toBe(path.join(paths.stateDir, "statev2.sqlite"));
        expect(paths.secretsDir).toBe(path.join(paths.stateDir, "secrets"));
        expect(paths.worktreeCheckoutCacheDir).toBe(
          path.join(baseDir, "caches", "worktree-checkouts"),
        );
      }
      const override = path.join(baseDir, "separate-authority");
      const overridden = yield* ServerConfig.deriveServerPaths(baseDir, devUrl, {
        baseDirIsExplicit: true,
        authorityStateDir: override,
      });
      expect(overridden).toEqual({ ...explicitDev, authorityStateDir: override });
    }),
  );

  it.effect("rejects overlapping authority roots before creating application directories", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "jones-config-overlap-" });
      const baseDir = path.join(root, "uncreated-home");
      for (const authorityStateDir of [
        baseDir,
        path.join(baseDir, "userdata"),
        path.join(baseDir, "userdata", "nested"),
        path.join(baseDir, "dev"),
        path.join(baseDir, "dev", "nested"),
        path.join(baseDir, "runtime"),
        path.join(baseDir, "runtime", "db-backup"),
        path.join(baseDir, "runtime", "db-backup", "nested"),
      ]) {
        const paths = yield* ServerConfig.deriveServerPaths(baseDir, undefined, {
          authorityStateDir,
        });
        const error = yield* ServerConfig.ensureServerDirectories(paths).pipe(Effect.flip);
        expect(error._tag).toBe("AuthorityStateDirValidationError");
        expect(yield* fs.exists(baseDir)).toBe(false);
      }
    }),
  );

  it.effect("rejects symlink aliases with missing descendants in either path direction", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "jones-config-alias-" });
      for (const protectedName of ["userdata", "dev", path.join("runtime", "db-backup")]) {
        const baseDir = path.join(root, protectedName.replaceAll(path.sep, "-"));
        const protectedDir = path.join(baseDir, protectedName);
        yield* fs.makeDirectory(protectedDir, { recursive: true });
        const alias = path.join(baseDir, "authority-alias");
        NodeFS.symlinkSync(protectedDir, alias, "dir");
        const paths = yield* ServerConfig.deriveServerPaths(baseDir, undefined, {
          authorityStateDir: path.join(alias, "missing", "authority"),
        });
        const error = yield* ServerConfig.ensureServerDirectories(paths).pipe(Effect.flip);
        expect(error._tag).toBe("AuthorityStateDirValidationError");
        expect(yield* fs.exists(path.join(protectedDir, "missing"))).toBe(false);
        expect(yield* fs.exists(paths.logsDir)).toBe(false);
      }

      const baseDir = path.join(root, "protected-alias");
      const storage = path.join(root, "external-state");
      yield* fs.makeDirectory(baseDir);
      yield* fs.makeDirectory(storage);
      NodeFS.symlinkSync(storage, path.join(baseDir, "userdata"), "dir");
      const paths = yield* ServerConfig.deriveServerPaths(baseDir, undefined, {
        authorityStateDir: path.join(storage, "missing", "authority"),
      });
      const error = yield* ServerConfig.ensureServerDirectories(paths).pipe(Effect.flip);
      expect(error._tag).toBe("AuthorityStateDirValidationError");
      expect(yield* fs.exists(path.join(storage, "missing"))).toBe(false);
      expect(yield* fs.exists(paths.logsDir)).toBe(false);
    }),
  );

  it.effect("honors private explicit authority placement and safe sibling prefixes", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "jones-config-authority-" });
      const baseDir = path.join(root, "home");
      for (const authorityStateDir of [
        path.join(baseDir, "userdata-authority"),
        path.join(root, "external-authority"),
      ]) {
        const paths = yield* ServerConfig.deriveServerPaths(baseDir, undefined, {
          authorityStateDir,
        });
        yield* ServerConfig.ensureServerDirectories(paths);
        expect(paths.authorityStateDir).toBe(authorityStateDir);
        expect(NodeFS.statSync(authorityStateDir).mode & 0o777).toBe(0o700);
        expect(yield* fs.exists(paths.stateDir)).toBe(true);
        expect(yield* fs.exists(paths.logsDir)).toBe(true);
        expect(yield* fs.exists(paths.dbPath)).toBe(false);
      }
      expect(yield* fs.exists(path.join(baseDir, "native-store-authority"))).toBe(false);
    }),
  );

  it.effect("constructs the test layer with default parallelism and private authority state", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.realPath(
        yield* fs.makeTempDirectoryScoped({ prefix: "jones-config-layer-" }),
      );
      const config = yield* ServerConfig.ServerConfig.pipe(
        Effect.provide(ServerConfig.layerTest(root, path.join(root, "home"))),
      );
      expect(config.serverParallelism).toEqual({
        profile: "laptop",
        selectionSource: "default",
        readerCount: 2,
        computeWorkerCount: 2,
      });
      expect(config.authorityStateDir).toBe(path.join(root, "home", "native-store-authority"));
      expect(NodeFS.statSync(config.authorityStateDir).mode & 0o777).toBe(0o700);
    }),
  );
});
