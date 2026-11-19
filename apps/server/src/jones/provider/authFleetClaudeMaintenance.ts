import * as HostProcess from "@t3tools/shared/HostProcess";
import { resolveCommandPath } from "@t3tools/shared/shell";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

const AuthFleetBinding = Schema.fromJsonString(
  Schema.Struct({
    schema: Schema.Literal("host-tooling.auth-fleet-binding/v1"),
    home: Schema.String,
    accounts: Schema.Array(
      Schema.Struct({
        provider: Schema.String,
        alias: Schema.String,
        native_cli: Schema.String,
      }),
    ),
  }),
);

export const resolveAuthFleetClaudeMaintenanceTarget = Effect.fn(
  "AuthFleetClaudeMaintenance.resolveTarget",
)(
  function* (
    options: { readonly binaryPath?: string | null; readonly env?: NodeJS.ProcessEnv } | undefined,
  ) {
    const platform = yield* HostProcess.Platform;
    if (platform !== "darwin" && platform !== "linux") return undefined;

    const env = yield* HostProcess.Environment;
    const home = env.HOME ?? (yield* HostProcess.HomeDirectory);
    const path = yield* Path.Path;
    if (!path.isAbsolute(home) || (options?.env?.HOME ?? home) !== home) return undefined;

    const tooling = path.join(home, ".local/share/host-tooling");
    const launcher = options?.binaryPath?.trim();
    const alias = (["default", "jace"] as const).find(
      (name) => launcher === path.join(tooling, "auth-fleet/bin", `claude-fleet-${name}`),
    );
    if (!alias || !launcher) return undefined;

    const fs = yield* FileSystem.FileSystem;
    const canonicalHome = yield* fs.realPath(home);
    const bindingPath = path.join(tooling, "runtime/auth-fleet-binding.json");
    const isHomeFile = (file: string, realPath: string) =>
      realPath === path.join(canonicalHome, path.relative(home, file));
    if (
      !isHomeFile(launcher, yield* fs.realPath(launcher)) ||
      !(yield* resolveCommandPath(launcher, { env })) ||
      !isHomeFile(bindingPath, yield* fs.realPath(bindingPath))
    ) {
      return undefined;
    }

    const info = yield* fs.stat(bindingPath);
    if (info.type !== "File" || (info.mode & 0o777) !== 0o600 || Number(info.size) > 128 * 1024) {
      return undefined;
    }
    const binding = yield* fs
      .readFileString(bindingPath)
      .pipe(Effect.flatMap(Schema.decodeUnknownEffect(AuthFleetBinding)));
    const accounts = binding.accounts.filter(
      (account) => account.provider === "claude" && account.alias === alias,
    );
    const binaryPath = path.join(home, ".local/bin/claude");
    if (binding.home !== home || accounts.length !== 1 || accounts[0]?.native_cli !== binaryPath) {
      return undefined;
    }

    const data = path.join(home, ".local/share");
    if (env.XDG_DATA_HOME && env.XDG_DATA_HOME !== data) return undefined;
    const realBinary = yield* fs.realPath(binaryPath);
    const versions = path.join(canonicalHome, ".local/share/claude/versions");
    if (
      path.dirname(realBinary) !== versions ||
      !/^\d+\.\d+\.\d+$/.test(path.basename(realBinary))
    ) {
      return undefined;
    }

    // Both account launchers follow this symlink. Run maintenance with the host
    // environment so it never needs to load either account's fleet credentials.
    return { binaryPath, env: { ...env, HOME: home } };
  },
  Effect.orElseSucceed(() => undefined),
);
