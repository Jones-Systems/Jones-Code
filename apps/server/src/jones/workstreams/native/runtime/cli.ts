import * as Effect from "effect/Effect";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as SqlClient from "effect/sql/SqlClient";
import * as AuthSessions from "../../../../persistence/AuthSessions.ts";
import * as SourceSetup from "./sourceSetup.ts";
import * as SourceEnrollments from "./sourceEnrollment.ts";
import { makeTrustedNativeEnrollmentCliContext } from "../../enrollment/trustedCliContext.ts";
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

const sourceBootstrap = (selection: Admin.InitSelection, action: "plan" | "apply" | "readback") =>
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
      const trusted = yield* makeTrustedNativeEnrollmentCliContext(
        { baseDir: config.baseDir, authorityDir: config.authorityStateDir },
        action === "apply" ? "apply" : "strict_inspection",
      );
      if (
        path.resolve(config.baseDir) !== path.resolve(selection.home) ||
        config.dbPath !== trusted.dbPath
      )
        return yield* new StartupError({ code: "database_mismatch" });
      yield* trusted.verifyUnchanged;
      const qualification = Layer.effect(
        SourceEnrollments.SourceIssuer,
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const authority =
            action === "apply" ? yield* trusted.bindAuthorityToSqlClient(sql) : trusted.authority;
          return { authority, build: Effect.succeed(Option.some(trusted.build)) };
        }),
      );
      // Bound authority rechecks owner/source metadata and qualifies main through the owned SQL transaction.
      const owner = Layer.effect(
        SourceEnrollments.SourceSetupOwner,
        Effect.map(SourceEnrollments.SourceIssuer, (issuer) => ({
          verifyUnchanged: issuer.authority.readCurrent.pipe(
            Effect.asVoid,
            Effect.mapError(
              () => new SourceEnrollments.SourceEnrollmentError({ code: "owner_unqualified" }),
            ),
          ),
        })),
      );
      const selectedConfig = ServerConfig.layer(config);
      return SourceSetup.layer.pipe(
        Layer.provideMerge(SourceEnrollments.layer),
        Layer.provideMerge(AuthSessions.layer),
        Layer.provideMerge(nativePersistenceLayer),
        Layer.provide(owner),
        Layer.provide(qualification),
        Layer.provide(SecretStore.layer.pipe(Layer.provide(selectedConfig))),
        Layer.provide(NodeSqliteClient.layer(trusted.sqlite)),
        Layer.provide(selectedConfig),
      );
    }),
  );

export const registryCommand = Admin.registryCommand(bootstrap, sourceBootstrap);
