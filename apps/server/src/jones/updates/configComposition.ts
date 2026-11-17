import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { validateNativeStoreAuthorityPath } from "../../environment/nativeStoreAuthorityPath.ts";
import { Configuration as FleetCheckpointConfiguration } from "../fleetCheckpoint/Configuration.ts";
import { Configuration as WorkspaceLifecycleConfiguration } from "../workspaceLifecycle/Configuration.ts";

const decodeFleetCheckpointConfiguration = Schema.decodeUnknownEffect(
  Schema.fromJsonString(FleetCheckpointConfiguration),
  { onExcessProperty: "error" },
);
const decodeWorkspaceLifecycleConfiguration = Schema.decodeUnknownEffect(
  Schema.fromJsonString(WorkspaceLifecycleConfiguration),
  { onExcessProperty: "error" },
);

// Only explicit owner configuration selects these policies; malformed input must not disable them.
export const retainedPolicyConfig = Config.all({
  fleetCheckpoint: Config.String("JONES_FLEET_CHECKPOINT_COMMAND").pipe(
    Config.mapEffect((raw) =>
      decodeFleetCheckpointConfiguration(raw).pipe(
        Effect.mapError((cause) => new Config.ConfigError(cause)),
      ),
    ),
    Config.option,
    Config.map(Option.getOrUndefined),
  ),
  workspaceLifecycle: Config.String("JONES_WORKSPACE_LIFECYCLE_CONFIG").pipe(
    Config.mapEffect((raw) =>
      decodeWorkspaceLifecycleConfiguration(raw).pipe(
        Effect.mapError((cause) => new Config.ConfigError(cause)),
      ),
    ),
    Config.option,
    Config.map(Option.getOrUndefined),
  ),
});

export class AuthorityStateDirValidationError extends Schema.TaggedError<AuthorityStateDirValidationError>()(
  "AuthorityStateDirValidationError",
  {
    authorityStateDir: Schema.String,
    stateDir: Schema.String,
    databaseBackupDir: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export const validateAuthorityStateDir = Effect.fn("ServerConfig.validateAuthorityStateDir")(
  function* (authorityStateDir: string, stateDir: string) {
    const path = yield* Path.Path;
    const baseDir = path.dirname(stateDir);
    // Share the launcher's canonical-path guard, including both userdata and dev.
    yield* Effect.try({
      try: () => validateNativeStoreAuthorityPath(baseDir, authorityStateDir),
      catch: (cause) =>
        new AuthorityStateDirValidationError({
          authorityStateDir,
          stateDir,
          databaseBackupDir: path.join(baseDir, "runtime", "db-backup"),
          cause,
        }),
    });
  },
);
