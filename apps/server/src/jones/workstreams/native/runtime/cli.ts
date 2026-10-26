import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as FileSystem from "effect/FileSystem";
import * as ServerConfig from "../../../../config.ts";
import * as SecretStore from "../../../../auth/ServerSecretStore.ts";
import * as Persistence from "../../../../persistence/Sqlite.ts";
import { resolveCliAuthConfig } from "../../../../cli/config.ts";
import * as Admin from "../cli.ts";
import { nativePersistenceLayer } from "../integration.ts";
import { StartupError } from "./startup.ts";

/** Only the explicit init command opens/migrates this selected home. No source enrollment or keys are created. */
const bootstrap = (selection: Admin.InitSelection) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const path = yield* Path.Path,
        fs = yield* FileSystem.FileSystem;
      if (
        selection.keyProvider !== "server-secret-store" ||
        !path.isAbsolute(selection.home) ||
        !(yield* fs.exists(selection.home))
      )
        return yield* new StartupError({ code: "explicit_selection_required" });
      const config = yield* resolveCliAuthConfig(
        { baseDir: Option.some(path.resolve(selection.home)) },
        Option.none(),
      );
      if (path.resolve(config.baseDir) !== path.resolve(selection.home))
        return yield* new StartupError({ code: "database_mismatch" });
      const selectedConfig = ServerConfig.layer(config);
      const storage = Persistence.layerFromPath(config.dbPath);
      const secrets = SecretStore.layer.pipe(Layer.provide(selectedConfig));
      return Admin.layer.pipe(
        Layer.provide(nativePersistenceLayer),
        Layer.provide(secrets),
        Layer.provide(storage),
        Layer.provide(selectedConfig),
      );
    }),
  );

export const registryCommand = Admin.registryCommand(bootstrap);
