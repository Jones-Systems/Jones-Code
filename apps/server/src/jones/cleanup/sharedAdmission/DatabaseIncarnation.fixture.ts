// @effect-diagnostics nodeBuiltinImport:off -- Synthetic account files stay inside the caller's exact cleanup-owned root.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as FileSystem from "effect/FileSystem";
import * as NodeSqlite from "node:sqlite";
import {
  beginCheckedServerOwnership,
  invalidateCheckedServerOwnership,
} from "../CheckedRuntimeOwner.ts";
import type { HeldRuntimeOwnerToken } from "../CheckedRuntimeOwner.ts";
import { inspectHeldDatabaseOwner } from "../CheckedRuntimeOwner.ts";
import * as Bootstrap from "./AccountJournalBootstrap.ts";
import { NativeJournalHost, nativeHost } from "./NativeAccount.ts";
import * as Incarnation from "./DatabaseIncarnation.ts";
import * as Physical from "./PhysicalScope.ts";
import { ScopeObservation } from "./Protocol.ts";
import { validateInfrastructurePermit } from "./EnrollmentJournal.ts";

/** Real bootstrap/infrastructure/owner protocol under an explicit synthetic account.
 * Caller owns the root lifetime; no permissive permit or cleanup capability issuer. */
export const prepareSyntheticOpen = Effect.fn("DatabaseIncarnationFixture.prepare")(function* (
  owner: HeldRuntimeOwnerToken,
  root: string,
) {
  const home = NodePath.join(root, "synthetic-account");
  NodeFS.mkdirSync(home, { mode: 0o700 });
  const bootstrap = yield* Bootstrap.makeForHost().pipe(
    Effect.provideService(NativeJournalHost, {
      account: Effect.succeed({ home, uid: String(process.getuid!()) }),
      open: nativeHost.open,
    }),
  );
  const observations = Context.get(yield* Layer.build(Physical.layer), ScopeObservation);
  const scope = yield* observations.capture(inspectHeldDatabaseOwner(owner).stateDir);
  const permit = yield* bootstrap.beginInfrastructure(scope);
  const infrastructure = Incarnation.OpenInfrastructure.of({
    permit,
    scope,
    assertRetained: validateInfrastructurePermit(permit, scope),
  });
  const open = yield* Incarnation.prepareOpen(owner, permit).pipe(
    Effect.provideService(Incarnation.OpenInfrastructure, infrastructure),
  );
  return { open, infrastructure, bootstrap };
});

/** The original native acquisition, retained until the caller's scope closes. */
export const makeHeldFixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const root = yield* fs.realPath(
    yield* fs.makeTempDirectoryScoped({ prefix: "jones-incarnation-" }),
  );
  const stateDir = NodePath.join(root, "state");
  NodeFS.mkdirSync(stateDir, { mode: 0o700 });
  const lockPath = NodePath.join(stateDir, "server-owner.sqlite");
  const scope = yield* Effect.scope;
  const held = yield* Effect.acquireRelease(
    Effect.sync(() => {
      const connection = new NodeSqlite.DatabaseSync(lockPath);
      try {
        return {
          connection,
          owner: beginCheckedServerOwnership(connection, stateDir, lockPath, scope),
        };
      } catch (cause) {
        connection.close();
        throw cause;
      }
    }),
    ({ connection, owner }) =>
      Effect.sync(() => {
        invalidateCheckedServerOwnership(owner);
        connection.close();
      }),
  );
  return { root, stateDir, dbPath: NodePath.join(stateDir, "statev2.sqlite"), ...held };
});
export const makeOwnedFixture = Effect.gen(function* () {
  const held = yield* makeHeldFixture;
  return { ...held, ...(yield* prepareSyntheticOpen(held.owner, held.root)) };
});
