// @effect-diagnostics nodeBuiltinImport:off -- Own only unique synthetic SQLite/credential roots on disk.
// @effect-diagnostics globalFetchInEffect:off -- Exercise the existing real signed loopback transport.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { expect, it } from "@effect/vitest";
import { beforeAll, afterAll } from "vite-plus/test";
import {
  artifactBuilds,
  prepareSourceRenewalArtifacts,
  openArtifactStore,
  rejectMismatchedConsumer,
  type SourceRenewalArtifacts,
} from "../testing/sourceRenewalArtifacts.ts";
import {
  AuthSessionId,
  T3WorkstreamListResult,
  WorkstreamMembershipPage,
  WorkstreamReceipt,
  WorkstreamReferencePage,
  WorkstreamsRegistrationContextV2Response,
  type WorkstreamsNativeContext,
  type WorkstreamsRegistrationContextT3Source,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as ServerSecretStore from "../../../auth/ServerSecretStore.ts";
import { makeReservedBearerSessionMaterializer } from "../../../auth/SessionStore.ts";
import * as AuthSessions from "../../../persistence/AuthSessions.ts";
import { runMigrations } from "../../../persistence/Migrations.ts";
import migration from "../../persistence/Migrations/006_JonesWorkstreamsProviderEnrollments.ts";
import { makeNativeCredentialWriter } from "../enrollment/credentialFile.ts";
import { makeNativeEnrollmentOperations } from "../enrollment/operations.ts";
import {
  deriveNativeEnrollmentSession,
  nativeEnrollmentRequestSha256,
  sha256EnrollmentBytes,
  type NativeEnrollmentRequest,
} from "../enrollment/request.ts";
import { makeNativeEnrollments, NativeEnrollmentError } from "../enrollment/service.ts";
import { makeWorkstreamsNativeProvider } from "../nativeProvider/service.ts";
import { enrolledContext, makeProviderFixture } from "../nativeProvider/testFixtures.ts";
import {
  sources,
  startEnvironment,
  startLoopbackControlPlane,
} from "../testing/loopbackControlPlane.ts";

const now = "2026-01-01T00:00:00.000Z";
const artifactMode = process.env.JONES_SOURCE_RENEWAL_ARTIFACTS;
if (artifactMode !== undefined && artifactMode !== "1")
  throw new Error("Invalid source-renewal artifact mode");
let artifacts: SourceRenewalArtifacts | undefined;
beforeAll(async () => {
  if (artifactMode === "1") artifacts = await prepareSourceRenewalArtifacts();
});
afterAll(async () => {
  await artifacts?.close();
});

const buildA =
  artifactMode === "1"
    ? artifactBuilds.old
    : {
        repository: "Jones-Systems/Jones-Code" as const,
        sha: "a".repeat(40),
        tree: "b".repeat(40),
      };
const buildB =
  artifactMode === "1"
    ? artifactBuilds.next
    : { ...buildA, sha: "c".repeat(40), tree: "d".repeat(40) };
const nextBuildA =
  artifactMode === "1"
    ? artifactBuilds.next
    : { ...buildA, sha: "e".repeat(40), tree: "f".repeat(40) };
const credentialBytes = (token: string) =>
  JSON.stringify({ authorization_header: `Bearer ${token}` });
// Keep synthetic receipts visible even when the Effect test logger suppresses successful runs.
const record = (event: string, data: unknown) =>
  Effect.sync(() => {
    process.stdout.write(`${JSON.stringify({ event, data })}\n`);
  });

const harness = Effect.gen(function* () {
  artifacts?.assertActive();
  const parent = NodePath.resolve(import.meta.dirname, "../../../../node_modules");
  const root = yield* Effect.acquireRelease(
    Effect.promise(() => NodeFSP.mkdtemp(NodePath.join(parent, ".source-renewal-"))),
    (root) =>
      Effect.promise(async () => {
        artifacts?.assertRemoval();
        await NodeFSP.rm(root, { recursive: true });
        await expect(NodeFSP.stat(root)).rejects.toMatchObject({ code: "ENOENT" });
      }).pipe(Effect.tap(() => record("source-renewal root absent", root))),
  );
  const originals = sources.map((source) => ({ ...source }));
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      sources.forEach((source, index) => Object.assign(source, originals[index]));
      expect(sources).toEqual(originals);
    }),
  );
  const listener = <A extends { origin: string; close: () => Promise<void> }>(
    open: () => Promise<A>,
  ) =>
    Effect.acquireRelease(Effect.promise(open), (resource) =>
      Effect.promise(async () => {
        await resource.close();
        await expect(
          fetch(resource.origin, { signal: AbortSignal.timeout(1_000) }),
        ).rejects.toThrow();
      }).pipe(Effect.tap(() => record("source-renewal listener closed", resource.origin))),
    );
  const control = yield* listener(startLoopbackControlPlane);
  yield* record("source-renewal fixture", {
    root,
    pid: process.pid,
    registry: control.origin,
  });
  const stores = new Map<string, Effect.Success<ReturnType<typeof openArtifactStore>>>();
  return {
    root,
    control,
    store: (id: string, build: typeof buildA) =>
      artifacts
        ? openArtifactStore(artifacts, root, id, build).pipe(
            Effect.tap((store) =>
              Effect.sync(() => {
                stores.set(id, store);
              }),
            ),
          )
        : sourceStore(root, id, build),
    environment: (id: string, build: typeof buildA) =>
      artifacts
        ? stores.get(id)!.environment(control)
        : listener(() => startEnvironment(control, id, build)),
  };
});

const sourceStore = Effect.fn("SourceRenewalTest.sourceStore")(function* (
  root: string,
  sourceId: string,
  initialBuild: typeof buildA,
) {
  const context = yield* Layer.build(
    NodeSqliteClient.layer({
      filename: NodePath.join(root, `${sourceId}.sqlite`),
      readerWorker: false,
      checkpointWorker: false,
    }),
  );
  yield* runMigrations({ toMigrationInclusive: 54 }).pipe(Effect.provide(context));
  yield* migration.pipe(Effect.provide(context));
  const sql = yield* SqlClient.SqlClient.pipe(Effect.provide(context));
  const sessions = yield* AuthSessions.make.pipe(Effect.provide(context));
  // This is the synthetic build-port seam: enrollment and provider read the same bound value.
  let boundBuild = initialBuild;
  const ports = {
    authority: {
      readCurrent: Effect.succeed({
        environmentId: sourceId,
        authorityNamespace: "synthetic-authority",
        storeGeneration: 1,
      }),
    },
    build: Effect.sync(() => Option.some(boundBuild)),
  };
  const enrollments = yield* makeNativeEnrollments(ports).pipe(
    Effect.provide(context),
    Effect.provideService(AuthSessions.AuthSessionRepository, sessions),
  );
  const materialize = yield* makeReservedBearerSessionMaterializer.pipe(
    Effect.provideService(AuthSessions.AuthSessionRepository, sessions),
    Effect.provideService(ServerSecretStore.ServerSecretStore, {
      get: () => Effect.succeedSome(new Uint8Array(32).fill(7)),
      set: () => Effect.die("fixture must not initialize keys"),
      create: () => Effect.die("fixture must not initialize keys"),
      getOrCreateRandom: () => Effect.die("fixture must not initialize keys"),
      remove: () => Effect.die("fixture must not remove keys"),
    }),
  );
  const providerPorts = {
    ...makeProviderFixture().ports,
    ...ports,
    engine: { dispatch: () => Effect.die("renewal must not dispatch native commands") },
  };
  const provider = makeWorkstreamsNativeProvider(providerPorts);
  const providerV2 = makeWorkstreamsNativeProvider(providerPorts, "v2");
  const request = (suffix: string): NativeEnrollmentRequest => ({
    schema: "jones-code.workstreams-native-enrollment/v1",
    enrollment_id: `${sourceId}-${suffix}`,
    registry_origin: "https://workstreams.test",
    context: {
      ...enrolledContext,
      owner_id: "owner-fixture",
      principal_id: "22222222-2222-4222-8222-222222222222",
      source_instance_id: sourceId,
      authority_namespace: "synthetic-authority",
      store_generation: 1,
      enrollment_id: `${sourceId}-${suffix}`,
      build: boundBuild,
    },
    session: {
      session_id: AuthSessionId.make(`${sourceId}-${suffix}-session`),
      issued_at: now,
      expires_at: "2026-01-02T00:00:00.000Z",
    },
  });
  const credential = (request: NativeEnrollmentRequest) =>
    makeNativeCredentialWriter({
      baseDir: root,
      credentialPath: NodePath.join(root, `${request.enrollment_id}.json`),
    });
  const binding = (request: NativeEnrollmentRequest) =>
    enrollments.getBySessionId(request.session.session_id).pipe(Effect.map(Option.getOrThrow));
  const rows = Effect.gen(function* () {
    return {
      sessions: yield* sql`SELECT * FROM auth_sessions ORDER BY session_id`,
      enrollments: yield* sql`SELECT * FROM workstreams_native_enrollments ORDER BY enrollment_id`,
    };
  });
  return {
    sql,
    sessions,
    enrollments,
    materialize,
    provider,
    providerV2,
    request: (suffix: string) => Effect.succeed(request(suffix)),
    credential,
    binding,
    rows,
    changeBuild: (build: typeof buildA) =>
      Effect.sync(() => {
        boundBuild = build;
      }),
  };
});

const qualifiedDescriptor = Effect.fn("SourceRenewalTest.qualifiedDescriptor")(function* (
  store: Pick<Effect.Success<ReturnType<typeof sourceStore>>, "binding"> & {
    readonly provider: Pick<Effect.Success<ReturnType<typeof sourceStore>>["provider"], "context">;
  },
  request: NativeEnrollmentRequest,
) {
  const response = yield* store.provider.context(yield* store.binding(request));
  expect(response.state).toBe("ready");
  if (response.state !== "ready") return yield* Effect.die("provider did not qualify descriptor");
  const context: WorkstreamsNativeContext = response.context;
  const template = sources.find(
    (source) => source.source_instance_id === context.source_instance_id,
  )!;
  const descriptor: WorkstreamsRegistrationContextT3Source = {
    ...template,
    source_instance_id: context.source_instance_id,
    authority_namespace: context.authority_namespace,
    store_generation: context.store_generation,
    native_protocol: context.protocol,
    build: context.build,
  };
  // The existing signed fixture owns its descriptor array; adoption is a test-only producer port.
  // A single scoped journey changes it, and the harness restores it even on interruption.
  Object.assign(template, descriptor);
  return descriptor;
});

const read = <A>(origin: string, path: string, schema: Schema.ConstraintDecoder<A, never>) =>
  Effect.promise(async () => {
    const response = await fetch(`${origin}/api/workstreams${path}`, {
      signal: AbortSignal.timeout(5_000),
    });
    expect(response.status).toBe(200);
    return response.json() as Promise<unknown>;
  }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(schema)));

it.effect(
  "renews only source A after an exact build change, reconciling lost native replies before signed registration and membership readback",
  () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(now));
      const f = yield* harness;
      const a = yield* f.store("source-a", buildA);
      const b = yield* f.store("source-b", buildB);
      const oldA = yield* a.request("old");
      const oldB = yield* b.request("old");
      for (const [store, request] of [
        [a, oldA],
        [b, oldB],
      ] as const) {
        const receipt = yield* makeNativeEnrollmentOperations({
          enrollments: store.enrollments,
          materialize: store.materialize,
          credential: store.credential(request),
        }).apply(request, null);
        expect(receipt.state).toBe("applied");
        yield* qualifiedDescriptor(store, request);
      }
      const environmentA = yield* f.environment("source-a", buildA);
      const environmentB = yield* f.environment("source-b", buildB);
      const references = yield* read(environmentA.origin, "/references", WorkstreamReferencePage);
      let version = 3;
      for (const sourceId of ["source-a", "source-b"]) {
        const reference = references.items.find(
          (item) =>
            item.identity.source_instance_id === sourceId &&
            item.identity.native_id === "shared-thread",
        )!;
        const list = yield* read(environmentA.origin, "", T3WorkstreamListResult);
        const receipt = yield* Effect.promise(async () => {
          const response = await fetch(`${environmentA.origin}/api/workstreams/commands`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              command: {
                command_id: `renewal-attach-${sourceId}`,
                expected_server_generation: list.binding.serverGeneration,
                expected_registry_version: list.binding.registryVersion,
                action: {
                  operation: "attach_primary",
                  workstream_id: "ws-core-v1",
                  expected_version: version++,
                  native_reference_id: reference.native_reference_id,
                },
              },
            }),
            signal: AbortSignal.timeout(5_000),
          });
          expect(response.status).toBe(200);
          return response.json() as Promise<unknown>;
        }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(WorkstreamReceipt)));
        expect(receipt.state).toBe("committed");
      }
      const memberships = yield* read(
        environmentB.origin,
        "/ws-core-v1/memberships",
        WorkstreamMembershipPage,
      );
      expect(memberships.items.filter((item) => item.closed === null)).toHaveLength(2);
      const registrationA = yield* read(
        environmentA.origin,
        "/registration-context",
        WorkstreamsRegistrationContextV2Response,
      );
      const registrationB = yield* read(
        environmentB.origin,
        "/registration-context",
        WorkstreamsRegistrationContextV2Response,
      );
      expect(registrationA.state).toBe("ready");
      expect(registrationB.state).toBe("ready");
      const registryBinding = (yield* read(environmentB.origin, "", T3WorkstreamListResult))
        .binding;
      const authority = { ...f.control.authority };
      const historicalA = yield* a.rows;
      const historicalB = yield* b.rows;
      const oldCredentialA = yield* a.credential(oldA).observe();
      const oldCredentialB = yield* b.credential(oldB).observe();
      const descriptorB = { ...sources[1]! };
      const bindingB = yield* b.binding(oldB);

      const bIdentity = "identity" in b ? b.identity() : null;
      yield* a.changeBuild(nextBuildA);
      const oldBindingA = yield* a.binding(oldA);
      expect(oldBindingA.build).toEqual(buildA);
      expect(yield* a.provider.context(oldBindingA)).toMatchObject({
        state: "rejected",
        reason: "contract_mismatch",
      });
      // Compatible v2 observes a changed build without renewing the strict-v1 enrollment.
      expect(yield* a.providerV2.context(oldBindingA)).toMatchObject({
        state: "ready",
        enrollment: oldA.context,
        observed_build: nextBuildA,
      });
      const upgraded = yield* f.environment("source-a", nextBuildA);
      const refused = yield* read(
        upgraded.origin,
        "/registration-context",
        WorkstreamsRegistrationContextV2Response,
      );
      expect(refused.state).toBe("ready");
      if (refused.state === "ready") expect(refused.sources).toEqual([]);
      expect(
        yield* read(upgraded.origin, "/ws-core-v1/memberships", WorkstreamMembershipPage),
      ).toEqual(memberships);
      expect(yield* a.rows).toEqual(historicalA);
      expect(yield* b.rows).toEqual(historicalB);
      expect(yield* a.credential(oldA).observe()).toEqual(oldCredentialA);
      expect(yield* b.credential(oldB).observe()).toEqual(oldCredentialB);
      expect(f.control.authority).toEqual(authority);
      expect((yield* read(upgraded.origin, "", T3WorkstreamListResult)).binding).toEqual(
        registryBinding,
      );

      const renewal = yield* a.request("renewed");
      const renewalWriter = a.credential(renewal);
      let reserves = 0;
      let publications = 0;
      const operations = makeNativeEnrollmentOperations({
        enrollments: {
          ...a.enrollments,
          reserve: (request) =>
            a.enrollments.reserve(request).pipe(
              Effect.flatMap(() => {
                reserves++;
                return Effect.fail(new NativeEnrollmentError({ code: "unknown_commit" }));
              }),
            ),
        },
        materialize: (expected) => {
          expect(expected).toEqual(deriveNativeEnrollmentSession(renewal));
          return a.materialize(expected);
        },
        credential: {
          observe: renewalWriter.observe,
          publish: (token, preimage) =>
            renewalWriter.publish(token, preimage).pipe(
              Effect.map((result) => {
                publications++;
                expect(result.state).toBe("published");
                return {
                  state: "unknown" as const,
                  beforeSha256: result.beforeSha256,
                  afterSha256: null,
                };
              }),
            ),
        },
      });
      const plan = yield* operations.plan(renewal);
      expect(plan).toMatchObject({ state: "planned", file_pre_sha256: null, completed_phases: [] });
      const lostReserve = yield* operations.apply(renewal, plan.file_pre_sha256);
      expect(lostReserve).toMatchObject({
        state: "unknown",
        reason: "commit_unresolved",
        request_sha256: nativeEnrollmentRequestSha256(renewal),
      });
      expect(publications).toBe(0);
      const issued = yield* a.materialize(deriveNativeEnrollmentSession(renewal));
      const expectedSha = sha256EnrollmentBytes(credentialBytes(issued.token));
      expect(yield* operations.readback(renewal, expectedSha)).toMatchObject({
        state: "unknown",
        reason: "credential_absent",
        completed_phases: ["native_records_reserved"],
      });
      // Resume the original request only after exact record readback; no second reservation.
      const lostPublication = yield* operations.apply(renewal, plan.file_pre_sha256);
      expect(lostPublication).toMatchObject({
        state: "unknown",
        reason: "publication_unresolved",
        enrollment_id: lostReserve.enrollment_id,
        request_sha256: lostReserve.request_sha256,
        session_id: lostReserve.session_id,
      });
      const recovered = yield* operations.readback(renewal, expectedSha);
      expect(recovered).toMatchObject({
        state: "unchanged",
        reason: "complete",
        native_context: renewal.context,
        request_sha256: lostReserve.request_sha256,
        file_post_sha256: expectedSha,
        completed_phases: ["native_records_reserved", "native_credential_published"],
      });
      expect(yield* operations.readback(renewal, "0".repeat(64))).toMatchObject({
        state: "conflict",
        reason: "credential_conflict",
      });
      expect(
        yield* operations.readback(
          { ...renewal, registry_origin: "https://unrelated.invalid" },
          expectedSha,
        ),
      ).toMatchObject({ state: "conflict", reason: "enrollment_conflict" });
      expect(reserves).toBe(1);
      expect(publications).toBe(1);
      expect(JSON.stringify([lostReserve, lostPublication, recovered])).not.toContain(issued.token);
      expect(yield* a.credential(oldA).observe()).toEqual(oldCredentialA);
      expect(yield* b.credential(oldB).observe()).toEqual(oldCredentialB);
      const renewedRows = yield* a.rows;
      expect(renewedRows.sessions).toHaveLength(2);
      expect(renewedRows.enrollments).toHaveLength(2);
      expect(renewedRows.sessions).toEqual(expect.arrayContaining([...historicalA.sessions]));
      expect(renewedRows.enrollments).toEqual(expect.arrayContaining([...historicalA.enrollments]));
      expect(yield* b.rows).toEqual(historicalB);

      const adopted = yield* qualifiedDescriptor(a, renewal);
      expect(adopted.build).toEqual(nextBuildA);
      expect(sources[1]).toEqual(descriptorB);
      if ("identity" in b) expect(b.identity()).toEqual(bIdentity);
      expect(yield* b.binding(oldB)).toEqual(bindingB);
      expect(
        yield* read(
          environmentB.origin,
          "/registration-context",
          WorkstreamsRegistrationContextV2Response,
        ),
      ).toEqual(registrationB);
      const registration = yield* read(
        upgraded.origin,
        "/registration-context",
        WorkstreamsRegistrationContextV2Response,
      );
      expect(registration).toEqual({ ...registrationA, sources: [adopted] });
      expect((yield* read(upgraded.origin, "", T3WorkstreamListResult)).binding).toEqual(
        registryBinding,
      );
      expect(registryBinding.registryId).toBe("https://workstreams.test");
      expect(f.control.authority).toEqual(authority);
      expect(
        yield* read(upgraded.origin, "/ws-core-v1/memberships", WorkstreamMembershipPage),
      ).toEqual(memberships);
      expect(
        yield* read(environmentB.origin, "/ws-core-v1/memberships", WorkstreamMembershipPage),
      ).toEqual(memberships);
      expect(f.control.observations.submissions).toBe(2);
      expect(f.control.observations.rejected).toBe(0);
      expect(f.control.observations.accepted).toBeGreaterThan(0);
      yield* record("source-renewal joined readback", {
        sources: [oldA.context.source_instance_id, oldB.context.source_instance_id],
        builds: [buildA, buildB, nextBuildA],
        enrollments: [oldA.enrollment_id, oldB.enrollment_id, renewal.enrollment_id],
        sessions: [oldA.session.session_id, oldB.session.session_id, renewal.session.session_id],
        routes: [environmentA.origin, environmentB.origin, upgraded.origin],
        registryBinding,
        membershipIds: memberships.items.map((item) => item.membership_id),
        reserves,
        publications,
      });
      expect(yield* a.provider.context(yield* a.binding(oldA))).toMatchObject({
        state: "rejected",
        reason: "contract_mismatch",
      });
      // Expiry and revocation remove session lookup; neither is the changed-build refusal above.
      yield* TestClock.setTime(Date.parse(renewal.session.expires_at));
      expect(Option.isNone(yield* a.enrollments.getBySessionId(renewal.session.session_id))).toBe(
        true,
      );
      yield* TestClock.setTime(Date.parse(now));
      yield* a.sessions.revoke({
        sessionId: renewal.session.session_id,
        revokedAt: DateTime.makeUnsafe(now),
      });
      expect(Option.isNone(yield* a.enrollments.getBySessionId(renewal.session.session_id))).toBe(
        true,
      );
    }).pipe(Effect.scoped),
);

it.effect("releases its exact synthetic roots and listeners on failure and cancellation", () =>
  Effect.gen(function* () {
    const failure = new Error("deliberate source-renewal fixture failure");
    expect(
      yield* Effect.gen(function* () {
        const f = yield* harness;
        if (artifacts) {
          yield* f.store("source-a", buildA);
          yield* f.environment("source-a", buildA);
        }
        return yield* Effect.fail(failure);
      }).pipe(Effect.scoped, Effect.flip),
    ).toBe(failure);
    const acquired = yield* Deferred.make<void>();
    let cancelled: { root: string; origin: string } | undefined;
    const fiber = yield* Effect.gen(function* () {
      const f = yield* harness;
      if (artifacts) {
        yield* f.store("source-a", buildA);
        yield* f.environment("source-a", buildA);
      }
      cancelled = { root: f.root, origin: f.control.origin };
      yield* Deferred.succeed(acquired, undefined);
      return yield* Effect.never;
    }).pipe(Effect.scoped, Effect.forkChild);
    yield* Deferred.await(acquired);
    yield* Fiber.interrupt(fiber);
    expect(cancelled).toBeDefined();
    yield* Effect.promise(async () => {
      await expect(NodeFSP.stat(cancelled!.root)).rejects.toMatchObject({ code: "ENOENT" });
      await expect(
        fetch(cancelled!.origin, { signal: AbortSignal.timeout(1_000) }),
      ).rejects.toThrow();
    });
  }),
);

if (artifactMode === "1")
  it.effect("refuses a mismatched artifact consumer before opening a database", () =>
    Effect.gen(function* () {
      expect(artifacts).toBeDefined();
      const f = yield* harness;
      yield* rejectMismatchedConsumer(artifacts!, f.root);
    }).pipe(Effect.scoped),
  );
