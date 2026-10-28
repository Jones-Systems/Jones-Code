// @effect-diagnostics nodeBuiltinImport:off -- Owner evidence reads only fixed metadata under the selected qualified home.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import packageJson from "../../../../../package.json" with { type: "json" };
import type { WorkstreamsSetupOwnerDescriptor as Descriptor } from "@t3tools/contracts/jones/workstreamsPeerSetup";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Context from "effect/Context";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import type { DesktopNativeEnrollmentOwner } from "../../enrollment/desktopOwner.ts";
import {
  makeTrustedNativeEnrollmentRuntimeContext,
  type NativeEnrollmentCliLocation,
  type TrustedNativeEnrollmentCliContext,
} from "../../enrollment/trustedCliContext.ts";
import { sha256EnrollmentBytes as sha } from "../../enrollment/request.ts";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Source from "./sourceEnrollment.ts";
import * as ServerConfig from "../../../../config.ts";
import * as DesktopOwner from "../../DesktopNativeOwner.ts";

declare const ownerBrand: unique symbol;
declare const operationBrand: unique symbol;
export interface QualifiedSetupOwner {
  readonly [ownerBrand]: true;
}
export interface QualifiedSetupOperation {
  readonly [operationBrand]: true;
}
type OwnerFacts = {
  readonly location: NativeEnrollmentCliLocation;
  readonly sql: SqlClient.SqlClient;
  readonly desktop?: DesktopNativeEnrollmentOwner;
  readonly live?: Authority.NativeStoreAuthority["Service"];
  readonly descriptor: Descriptor;
};
type OperationFacts = {
  readonly descriptor: Descriptor;
  readonly home: string;
  readonly sql: SqlClient.SqlClient;
  readonly check: Effect.Effect<void, Source.SourceEnrollmentError>;
  readonly authority: Authority.NativeStoreAuthority["Service"];
};
const owners = new WeakMap<QualifiedSetupOwner, OwnerFacts>();
const operations = new WeakMap<QualifiedSetupOperation, OperationFacts>();
const refused = (cause?: unknown) =>
  new Source.SourceEnrollmentError({
    code: "owner_unqualified",
    ...(cause === undefined ? {} : { cause }),
  });
const same = (a: unknown, b: unknown) => canonical(a) === canonical(b);
const descriptor = Effect.fn("SetupOwner.descriptor")(function* (
  location: NativeEnrollmentCliLocation,
  trusted: TrustedNativeEnrollmentCliContext,
  desktop?: DesktopNativeEnrollmentOwner,
) {
  const authority = yield* trusted.authority.readCurrent.pipe(Effect.mapError(refused));
  // Private evidence preserves path/runtime/writer facts; the peer receives only its digest.
  const evidence = yield* Effect.try({
    try: () =>
      desktop
        ? { writer: desktop.binding }
        : {
            state: NodeFS.readFileSync(
              NodePath.join(location.baseDir, "runtime", "service-state.json"),
              "utf8",
            ),
            receipt: NodeFS.readFileSync(
              NodePath.join(
                location.baseDir,
                "runtime",
                "versions",
                packageJson.version,
                ".jones-runtime-receipt.json",
              ),
              "utf8",
            ),
          },
    catch: refused,
  });
  const database = yield* Effect.try({
    try: () => {
      const s = NodeFS.lstatSync(trusted.dbPath);
      return { device: s.dev, inode: s.ino, uid: s.uid, mode: s.mode };
    },
    catch: refused,
  });
  const base = {
    kind: desktop ? ("desktop" as const) : ("standalone" as const),
    authority: {
      environment_id: authority.environmentId,
      authority_namespace: authority.authorityNamespace,
      store_generation: authority.storeGeneration,
    },
    build: trusted.build,
    runtime_version: packageJson.version,
  };
  yield* trusted.verifyUnchanged.pipe(Effect.mapError(refused));
  return Object.freeze({
    ...base,
    authority: Object.freeze(base.authority),
    build: Object.freeze({ ...base.build }),
    qualification_sha256: sha(
      canonical({ ...base, location, dbPath: trusted.dbPath, database, evidence }),
    ),
  }) satisfies Descriptor;
});
const qualify = Effect.fn("SetupOwner.qualify")(function* (facts: Omit<OwnerFacts, "descriptor">) {
  const trusted = yield* makeTrustedNativeEnrollmentRuntimeContext(
    facts.location,
    facts.sql,
    facts.desktop,
  ).pipe(Effect.mapError(refused));
  if (facts.live) {
    const live = yield* facts.live.readCurrent.pipe(Effect.mapError(refused));
    if (!same(live, yield* trusted.authority.readCurrent.pipe(Effect.mapError(refused))))
      return yield* refused();
  }
  return { trusted, descriptor: yield* descriptor(facts.location, trusted, facts.desktop) };
});
export const makeStandaloneSetupOwner = Effect.fn("SetupOwner.standalone")(function* (
  location: NativeEnrollmentCliLocation,
  sql: SqlClient.SqlClient,
  liveAuthority: Authority.NativeStoreAuthority["Service"],
) {
  const facts = { location: Object.freeze({ ...location }), sql, live: liveAuthority };
  const q = yield* qualify(facts);
  const token = Object.freeze({}) as QualifiedSetupOwner;
  owners.set(token, { ...facts, descriptor: q.descriptor });
  return token;
});
export const makeDesktopSetupOwner = Effect.fn("SetupOwner.desktop")(function* (
  location: NativeEnrollmentCliLocation,
  sql: SqlClient.SqlClient,
  owner: DesktopNativeEnrollmentOwner,
) {
  const facts = { location: Object.freeze({ ...location }), sql, desktop: owner };
  const q = yield* qualify(facts);
  const token = Object.freeze({}) as QualifiedSetupOwner;
  owners.set(token, { ...facts, descriptor: q.descriptor });
  return token;
});
export const refreshSetupOwner = Effect.fn("SetupOwner.refresh")(function* (
  owner: QualifiedSetupOwner,
) {
  const facts = owners.get(owner);
  if (!facts) return yield* refused();
  const q = yield* qualify(facts);
  if (!same(q.descriptor, facts.descriptor)) return yield* refused();
  const local = yield* q.trusted.authority.readCurrent.pipe(Effect.mapError(refused));
  const check = q.trusted.verifyUnchanged.pipe(Effect.mapError(refused));
  const authority = Object.freeze(
    Authority.NativeStoreAuthority.of({
      readCurrent: q.trusted.authority.readCurrent.pipe(
        Effect.map((value) => Object.freeze({ ...value })),
      ),
      trustProvider: Object.freeze({
        readTrustSnapshot: () => ({
          readiness: "ready" as const,
          trustedEnvironments: [Object.freeze({ ...local })],
        }),
      }),
    }),
  );
  const token = Object.freeze({}) as QualifiedSetupOperation;
  operations.set(
    token,
    Object.freeze({
      descriptor: q.descriptor,
      home: facts.location.baseDir,
      sql: facts.sql,
      check,
      authority,
    }),
  );
  return token;
});
/** Runtime membership is checked on every access; copied brands and descriptors confer nothing. */
export const setupOperationFacts = Effect.fn("SetupOwner.operation")(function* (
  operation: QualifiedSetupOperation,
) {
  const facts = operations.get(operation);
  if (!facts) return yield* refused();
  yield* facts.check;
  return facts;
});
export const setupOwnerLayer = (operation: QualifiedSetupOperation) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const facts = yield* setupOperationFacts(operation);
      return Layer.mergeAll(
        Layer.succeed(Authority.NativeStoreAuthority, facts.authority),
        Layer.succeed(Source.SourceSetupOwner, { verifyUnchanged: facts.check }),
        Layer.succeed(Source.SourceIssuer, {
          authority: facts.authority,
          build: Effect.succeed(Option.some(facts.descriptor.build)),
        }),
      );
    }),
  );
/** Application composition supplies an opaque owner, never a wire descriptor. */
export class SetupOwner extends Context.Service<
  SetupOwner,
  {
    readonly current: Effect.Effect<QualifiedSetupOwner, Source.SourceEnrollmentError>;
  }
>()("t3/jones/workstreams/native/runtime/setupOwner") {}

/** Captures selected application resources; ordinary startup performs no qualification or writes.
 * Each explicit setup operation refreshes its actual standalone/desktop owner before writers. */
export const runtimeLayer = Layer.effect(
  SetupOwner,
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig,
      sql = yield* SqlClient.SqlClient,
      live = yield* Authority.NativeStoreAuthority,
      desktop = yield* DesktopOwner.DesktopNativeOwner;
    const location = { baseDir: config.baseDir, authorityDir: config.authorityStateDir };
    const current = Effect.gen(function* () {
      if (config.mode !== "desktop") return yield* makeStandaloneSetupOwner(location, sql, live);
      yield* desktop.refresh.pipe(Effect.mapError(refused));
      const binding = desktop.binding;
      if (!desktop.available || !binding) return yield* refused();
      return yield* makeDesktopSetupOwner(location, sql, {
        binding,
        assertCurrent: desktop.assertCurrent,
        refresh: desktop.refresh,
      });
    }).pipe(Effect.provideService(SqlClient.SqlClient, sql), Effect.mapError(refused));
    return SetupOwner.of({ current });
  }),
);
