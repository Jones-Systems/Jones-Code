// @effect-diagnostics nodeBuiltinImport:off -- Synthetic receipt fixtures hash their exact retained bytes.
import * as NodeCrypto from "node:crypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import {
  AuthOrchestrationReadScope,
  AuthSessionId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentHttpApi,
  EnvironmentId,
  type AuthEnvironmentScope,
  type JonesUpdateState,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Etag from "effect/http/Etag";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import { describe, expect, it as test } from "vite-plus/test";

import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import * as AuthHttp from "../../auth/http.ts";
import * as ServerConfig from "../../config.ts";
import * as SelfUpdate from "../../cloud/selfUpdate.ts";
import * as Launcher from "../../cloud/serviceLauncherClient.ts";
import * as Startup from "../../serverRuntimeStartup.ts";
import * as DesktopReceiver from "../../resourceTelemetry/DesktopTelemetryReceiver.ts";
import { jonesUpdatesHttpApiLayer } from "./http.ts";
import * as JonesUpdates from "./service.ts";
import * as UpdateAdmissionFence from "./UpdateAdmissionFence.ts";
import * as SourceRelation from "./sourceRelation.ts";
import { retainStagedSelection } from "./stagedSelection.ts";
import {
  QUALIFIED_RUNTIME_RECEIPT,
  qualifiedPayloadDigest,
  readQualifiedRuntimeReceipt,
  stageQualifiedRuntime,
  currentQualifiedRuntimeBinding,
} from "../cloud/qualifiedRuntime.ts";
import {
  reserveUpdateOperation,
  publishUpdateOperationSettlement,
  writeUpdateOperationCause,
} from "./launcherOperation.ts";

class JonesUpdatesTestApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.jonesUpdates,
) {}

it.layer(NodeServices.layer)("settlement service and native evidence", (it) => {
  it.effect.each([
    { supported: false, restoreStage: false },
    { supported: true, restoreStage: false },
    { supported: false, restoreStage: true },
    { supported: true, restoreStage: true },
  ])(
    "preserves settlement and plan ownership (capability $supported, restored $restoreStage)",
    ({ supported, restoreStage }) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "jones-settlement-http-" });
        const config = yield* ServerConfig.ServerConfig.pipe(
          Effect.provide(ServerConfig.layerTest(process.cwd(), baseDir)),
        );
        const version = "0.1.0-preview.20261022.1.1";
        const targetVersion = "0.1.0-preview.20261023.2.1";
        const active = `${baseDir}/runtime/versions/${version}`;
        yield* fs.makeDirectory(`${active}/client`, { recursive: true });
        yield* fs.writeFileString(`${active}/t3`, "#!/bin/sh\nexit 0\n");
        yield* fs.chmod(`${active}/t3`, 0o700);
        yield* fs.writeFileString(`${active}/client/index.html`, "synthetic");
        yield* fs.makeDirectory(`${baseDir}/userdata`, { recursive: true });
        yield* fs.writeFileString(
          `${baseDir}/userdata/statev2.sqlite`,
          "synthetic unused database",
        );
        yield* fs.writeFileString(
          `${baseDir}/userdata/environment-id`,
          "synthetic-settlement-host",
        );
        yield* fs.writeFileString(
          `${active}/${QUALIFIED_RUNTIME_RECEIPT}`,
          JSON.stringify({
            protocol: 1,
            repository: "Jones-Systems/Jones-Code",
            channel: "jones-main",
            version,
            sourceSha: "a".repeat(40),
            sourceTree: "b".repeat(40),
            installedSourceSha: "c".repeat(40),
            runId: 1,
            runAttempt: 1,
            artifactId: 2,
            workflow: ".github/workflows/artifact-cli-linux.yml",
            artifactDigest: `sha256:${"d".repeat(64)}`,
            archiveSha256: "e".repeat(64),
            platform: "linux",
            architecture: "x64",
            payloadSha256: yield* Effect.promise(() => qualifiedPayloadDigest(active)),
          }),
        );
        const installedReceipt = yield* Effect.promise(() =>
          readQualifiedRuntimeReceipt(
            baseDir,
            version,
            { platform: "linux", architecture: "x64" },
            { retainVerification: true },
          ),
        );
        const previousMigrationPlan = { pendingUpstream: [901], pendingJones: [902] };
        if (restoreStage) {
          const payloadDirectory = `${baseDir}/candidate`;
          yield* fs.makeDirectory(payloadDirectory);
          yield* fs.writeFileString(`${payloadDirectory}/t3`, "#!/bin/sh\nexit 0\n");
          yield* fs.chmod(`${payloadDirectory}/t3`, 0o700);
          yield* Effect.promise(async () => {
            const { protocol: _protocol, payloadSha256: _digest, ...metadata } = installedReceipt;
            const staged = await stageQualifiedRuntime({
              artifact: {
                ...metadata,
                version: targetVersion,
                sourceSha: "f".repeat(40),
                installedSourceSha: installedReceipt.sourceSha,
                payloadDirectory,
              },
              binding: await currentQualifiedRuntimeBinding(baseDir, version, {
                platform: "linux",
                architecture: "x64",
              }),
              host: { platform: "linux", architecture: "x64" },
              validate: async () => {},
            });
            await retainStagedSelection(staged);
          });
        }
        const operationId = "11111111-1111-4111-8111-111111111111";
        const input = { operationId, requestId: "22222222-2222-4222-8222-222222222222" };
        const binding = {
          baseDir,
          dbPath: `${baseDir}/userdata/statev2.sqlite`,
          environmentId: "synthetic-settlement-host",
          currentVersion: version,
          targetVersion,
          expectedInstalledSource: "a".repeat(40),
          targetSource: "f".repeat(40),
          stagedHandle: "a".repeat(64),
        };
        yield* Effect.promise(() => reserveUpdateOperation(baseDir, operationId, binding));
        const ledger = `${baseDir}/runtime/jones-update-operations/${operationId}`;
        const failedBytes = JSON.stringify({ schema: 1, operationId, binding, status: "failed" });
        yield* fs.writeFileString(`${ledger}.outcome.json`, failedBytes);
        yield* fs.writeFileString(
          `${baseDir}/runtime/service-state.json`,
          JSON.stringify({ protocol: 4, activeVersion: version }),
        );
        const reservationBytes = yield* fs.readFileString(`${ledger}.json`);
        const digests = {
          reservationSha256: NodeCrypto.createHash("sha256").update(reservationBytes).digest("hex"),
          outcomeSha256: NodeCrypto.createHash("sha256").update(failedBytes).digest("hex"),
        };
        yield* Effect.promise(() =>
          writeUpdateOperationCause(baseDir, {
            schema: 1,
            operationId,
            step: "backup",
            code: "ENOSPC",
            message: "backup refused",
          }),
        );
        const calls: unknown[] = [];
        let response: "refused" | "settled" | "unknown" = "refused";
        let latestOutcome = {
          id: operationId,
          status: "failed" as const,
          fromVersion: version,
          targetVersion,
          reason: "launcher-restarted",
        };
        const dependencies = Layer.mergeAll(
          ServerConfig.layer({ ...config, mode: "web" }),
          UpdateAdmissionFence.layer,
          Layer.succeed(HostProcess.Platform, "linux"),
          Layer.succeed(HostProcess.Architecture, "x64"),
          Layer.succeed(HostProcess.Arguments, ["synthetic", "unbundled-test.ts"]),
          Layer.succeed(HostProcess.IsExecutable, false),
          Layer.mock(SelfUpdate.ServerSelfUpdate)({
            update: () => Effect.die("No install in settlement test"),
          }),
          Layer.mock(Startup.ServerRuntimeStartup)({}),
          Layer.mock(DesktopReceiver.DesktopTelemetryReceiver)({}),
          Layer.succeed(Launcher.ServiceLauncherClient, {
            managed: true,
            currentVersion: version,
            qualifiedUpdateMigrationPlan: previousMigrationPlan,
            requiresQualifiedTrialGate: false,
            prepareTrial: Effect.undefined,
            prepareQualifiedTrial: () => Effect.die("No trial in settlement test"),
            requestUpdate: () => Effect.die("No install in settlement test"),
            ...(supported ? { launcherVersion: "0.1.0", qualifiedSettlement: true } : {}),
            get qualifiedStartupOutcome() {
              return latestOutcome;
            },
            reconcileFailedUpdate: (request) =>
              Effect.gen(function* () {
                calls.push({ action: "reconcile", input: request });
                if (response === "unknown")
                  return yield* new Launcher.ServiceLauncherClientError({ operation: "timeout" });
                if (response === "refused")
                  return { result: "refused" as const, reason: "restore-pending" as const };
                yield* Effect.promise(() =>
                  publishUpdateOperationSettlement(baseDir, {
                    schema: 1,
                    operationId,
                    basis: "legacy-reconciled",
                    status: "failed",
                    settlement: "no-effect",
                    ...digests,
                    evidence: {
                      phaseAtFailure: "accepted",
                      nativeReason: "launcher-restarted",
                      candidateStarted: false,
                      activeVersion: version,
                      activeSourceSha: binding.expectedInstalledSource,
                      recoveryPayload: "absent",
                      restorePending: false,
                      cause: "backup refused",
                      writerLauncherVersion: "0.1.0",
                      recordedAt: "2026-10-23T00:00:00.000Z",
                    },
                  }),
                );
                return { result: "settled" as const };
              }),
            recordRetiredOperation: (request) =>
              Effect.sync(() => {
                calls.push({ action: "recordRetired", input: request });
                return { result: "refused" as const, reason: "retired-evidence-mismatch" as const };
              }),
          }),
        );
        yield* Effect.gen(function* () {
          const updates = yield* JonesUpdates.JonesUpdates;
          expect((yield* updates.state())?.migrationPlan).toEqual(
            restoreStage ? undefined : previousMigrationPlan,
          );
          if (restoreStage) expect((yield* updates.state())?.phase).toBe("staged");
          expect(yield* updates.state()).toMatchObject({
            settlement: "none",
            cause: "backup refused",
            childVersion: version,
            launcherSettlement: supported ? "supported" : "unsupported",
            launcherRefresh: supported ? "none" : "recommended",
          });
          if (!supported) expect((yield* updates.state())?.launcherVersion).toBeUndefined();
          const beforeReplacement = yield* updates.state();
          expect(
            (yield* updates.downloadExact({
              targetSource: binding.targetSource,
              replace: {
                stagedHandle: binding.stagedHandle,
                sourceSha: binding.targetSource,
                operationId,
                environmentId: EnvironmentId.make(binding.environmentId),
                currentVersion: version,
                expectedInstalledSource: binding.expectedInstalledSource,
              },
            })).message,
          ).toBe("Exact-source download is unavailable on this host.");
          expect(yield* updates.state()).toEqual(beforeReplacement);
          expect((yield* updates.reconcile(input)).settlementRefusal).toBe(
            supported ? "restore-pending" : "capability-unsupported",
          );
          const retired = { ...input, retiredLocation: "synthetic-retired/attempt" };
          expect((yield* updates.recordRetired(retired)).settlementRefusal).toBe(
            supported ? "retired-evidence-mismatch" : "capability-unsupported",
          );
          if (!supported) {
            expect(calls).toEqual([]);
            return;
          }
          expect(calls).toEqual([
            { action: "reconcile", input },
            { action: "recordRetired", input: retired },
          ]);
          response = "settled";
          const settled = yield* updates.reconcile({
            ...input,
            requestId: "33333333-3333-4333-8333-333333333333",
          });
          expect(settled).toMatchObject({
            settlement: "legacy-reconciled",
            updateId: operationId,
            outcome: { status: "blocked" },
            message: restoreStage
              ? "Downloaded and verified. Install is a separate action."
              : "Update did not start: backup refused. Nothing changed.",
          });
          if (restoreStage) {
            expect(settled.phase).toBe("staged");
            expect(settled.migrationPlan).toBeUndefined();
          }
          expect(settled.settlementRefusal).toBeUndefined();
          expect((yield* updates.reconcileOperation(operationId)).state).toBe("blocked");
          expect(
            (yield* updates.installForOperation({
              ...binding,
              environmentId: EnvironmentId.make(binding.environmentId),
              operationId,
            })).message,
          ).toBe("The operation is ambiguous or its immutable installation binding changed.");
          response = "unknown";
          const before = calls.length;
          expect((yield* updates.reconcile(input)).message).toContain("outcome is unknown");
          expect(calls.length).toBe(before + 1);
          expect(yield* fs.readFileString(`${ledger}.json`)).toBe(reservationBytes);
          expect(yield* fs.readFileString(`${ledger}.outcome.json`)).toBe(failedBytes);
          latestOutcome = { ...latestOutcome, id: "44444444-4444-4444-8444-444444444444" };
          const next = yield* updates.state();
          expect(next).toMatchObject({ updateId: latestOutcome.id, settlement: "none" });
          expect(next?.cause).toBeUndefined();
          expect(next?.settlementRefusal).toBeUndefined();
        }).pipe(Effect.provide(JonesUpdates.layer.pipe(Layer.provide(dependencies))));
      }),
  );
});

const unusedService = <A>(): A =>
  new Proxy({} as object, {
    get(_target, key) {
      throw new Error(`Ordinary Release must not invoke native update operation ${String(key)}`);
    },
  }) as A;

it.layer(NodeServices.layer)("Jones update Release compatibility", (it) => {
  it.effect.each(["web", "desktop"] as const)(
    "ordinary Release %s host returns absent Jones state without displacing Release updates",
    (mode) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "jones-release-http-" });
        const config = yield* ServerConfig.ServerConfig.pipe(
          Effect.provide(ServerConfig.layerTest(process.cwd(), baseDir)),
        );
        const authenticate: EnvironmentAuth.EnvironmentAuth["Service"]["authenticateHttpRequest"] =
          () =>
            Effect.succeed({
              sessionId: AuthSessionId.make("jones-release-http"),
              subject: "synthetic-release-user",
              method: "browser-session-cookie",
              scopes: [AuthOrchestrationReadScope],
            });
        const auth = new Proxy(unusedService<EnvironmentAuth.EnvironmentAuth["Service"]>(), {
          get(target, key) {
            return key === "authenticateHttpRequest" ? authenticate : Reflect.get(target, key);
          },
        });
        const updates = JonesUpdates.layer.pipe(
          Layer.provide(UpdateAdmissionFence.layer),
          Layer.provide(ServerConfig.layer({ ...config, mode })),
          Layer.provide(Layer.succeed(HostProcess.Platform, "linux")),
          Layer.provide(Layer.succeed(HostProcess.Architecture, "x64")),
          Layer.provide(
            Layer.succeed(
              SelfUpdate.ServerSelfUpdate,
              unusedService<SelfUpdate.ServerSelfUpdate["Service"]>(),
            ),
          ),
          Layer.provide(
            Layer.succeed(
              Startup.ServerRuntimeStartup,
              unusedService<Startup.ServerRuntimeStartup["Service"]>(),
            ),
          ),
          Layer.provide(
            Layer.succeed(
              DesktopReceiver.DesktopTelemetryReceiver,
              unusedService<DesktopReceiver.DesktopTelemetryReceiver["Service"]>(),
            ),
          ),
          Layer.provide(
            Layer.succeed(Launcher.ServiceLauncherClient, {
              managed: false,
              requiresQualifiedTrialGate: false,
              prepareQualifiedTrial: () =>
                Effect.die("Release state must not prepare a qualified trial"),
              requestUpdate: () => Effect.die("Release state must not request Install"),
              prepareTrial: Effect.undefined,
            }),
          ),
        );
        const routes = HttpApiBuilder.layer(JonesUpdatesTestApi).pipe(
          Layer.provide(
            jonesUpdatesHttpApiLayer.pipe(
              Layer.provide(
                Layer.succeed(
                  SourceRelation.JonesSourceRelation,
                  unusedService<SourceRelation.JonesSourceRelation["Service"]>(),
                ),
              ),
            ),
          ),
          Layer.provide(AuthHttp.layerAuthenticatedAuth),
          Layer.provide(Layer.succeed(EnvironmentAuth.EnvironmentAuth, auth)),
          Layer.provide(updates),
          Layer.provideMerge(
            HttpPlatform.layer.pipe(
              Layer.provideMerge(NodeServices.layer),
              Layer.provideMerge(Etag.layerWeak),
            ),
          ),
          Layer.provide(NodeServices.layer),
        );
        yield* Effect.acquireUseRelease(
          Effect.sync(() => HttpRouter.toWebHandler(routes, { disableLogger: true })),
          (app) =>
            Effect.promise(async () => {
              const response = await app.handler(
                new Request("http://fixture/api/jones-updates", {
                  headers: { cookie: "synthetic-release-session=fixture" },
                }),
              );
              expect(response.status).toBe(200);
              expect(await response.json()).toBeNull();
              expect(response.headers.get("cache-control")).toBe("no-store");
            }),
          (app) => Effect.promise(() => app.dispose()),
        );
      }),
  );
});

function fixture(scopes: ReadonlyArray<AuthEnvironmentScope>) {
  const calls: unknown[] = [];
  const state: JonesUpdateState = {
    source: "jones-actions",
    channel: "jones-main",
    phase: "staged",
    capability: { check: true, download: true, install: false, reason: "bootstrap-required" },
    stagedHandle: "11111111-1111-4111-8111-111111111111",
    currentVersion: "0.0.0-preview.20261002.100",
    environmentId: EnvironmentId.make("synthetic-environment"),
  };
  const record = (action: string, input?: unknown) =>
    Effect.sync(() => {
      calls.push({ action, input });
      return state;
    });
  const service = Layer.succeed(JonesUpdates.JonesUpdates, {
    fleetOperationsSupported: false,
    refreshPolicy: Effect.undefined,
    retireStagedOperation: () => Effect.succeed({ retired: false }),
    state: (after) => record("state", after),
    check: record("check"),
    download: (input) => record("download", input),
    downloadExact: (input) => record("downloadExact", input),
    prepareNative: (input) => record("prepareNative", input),
    prepareQuit: Effect.sync(() => {
      calls.push({ action: "prepareQuit", input: undefined });
      return { prepared: true, threadCount: 2 };
    }),
    install: (input) => record("install", input),
    reconcile: (input) => record("reconcile", input),
    recordRetired: (input) => record("recordRetired", input),
    stageExact: (input) => record("stageExact", input),
    installForOperation: (input) => record("installForOperation", input),
    reconcileOperation: (operationId) => Effect.succeed({ state: "absent" as const, operationId }),
  });
  const relation = Layer.succeed(SourceRelation.JonesSourceRelation, {
    relation: (pair) =>
      Effect.sync(() => {
        calls.push({ action: "sourceRelation", input: pair });
        return {
          repository: "Jones-Systems/Jones-Code" as const,
          ...pair,
          relation: "ahead" as const,
          observedAt: "2026-10-20T00:00:00.000Z",
        };
      }),
  });
  const auth = Layer.succeed(EnvironmentAuthenticatedAuth, (effect) =>
    effect.pipe(
      Effect.provideService(EnvironmentAuthenticatedPrincipal, {
        sessionId: AuthSessionId.make("synthetic-session"),
        subject: "synthetic-user",
        method: "bearer-access-token",
        scopes: new Set(scopes),
      }),
    ),
  );
  const routes = HttpApiBuilder.layer(JonesUpdatesTestApi).pipe(
    Layer.provide(jonesUpdatesHttpApiLayer.pipe(Layer.provide(Layer.mergeAll(service, relation)))),
    Layer.provide(auth),
    Layer.provide(HttpServer.layerServices),
  );
  return { ...HttpRouter.toWebHandler(routes), calls, state };
}

describe("qualified update HTTP authority and decoding", () => {
  test("requires operate scope for empty-body quit preparation and returns no-store", async () => {
    for (const scopes of [[], ["orchestration:read"], ["orchestration:operate"]] as const) {
      const app = fixture(scopes);
      try {
        const response = await app.handler(
          new Request("http://fixture/api/jones-updates/prepare-quit", { method: "POST" }),
        );
        const permitted = scopes.some((scope) => scope === "orchestration:operate");
        expect(response.status).toBe(permitted ? 200 : 403);
        expect(app.calls).toEqual(permitted ? [{ action: "prepareQuit", input: undefined }] : []);
        if (permitted) {
          expect(response.headers.get("cache-control")).toBe("no-store");
          expect(await response.json()).toEqual({ prepared: true, threadCount: 2 });
        }
      } finally {
        await app.dispose();
      }
    }
  });

  const reconciliation = {
    operationId: "11111111-1111-4111-8111-111111111111",
    requestId: "22222222-2222-4222-8222-222222222222",
  };
  test("gates reconciliation and operator-only retirement independently", async () => {
    for (const scopes of [[], ["orchestration:operate"], ["environment:maintain"]] as const) {
      const app = fixture(scopes);
      try {
        for (const [action, input, permitted] of [
          ["reconcile", reconciliation, scopes.some((scope) => scope === "orchestration:operate")],
          [
            "record-retired",
            { ...reconciliation, retiredLocation: "synthetic-retired/attempt" },
            scopes.some((scope) => scope === "environment:maintain"),
          ],
        ] as const) {
          const before = app.calls.length;
          const response = await app.handler(
            new Request(`http://fixture/api/jones-updates/${action}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify(input),
            }),
          );
          expect(response.status).toBe(permitted ? 200 : 403);
          expect(app.calls.length).toBe(before + (permitted ? 1 : 0));
          if (permitted) {
            expect(response.headers.get("cache-control")).toBe("no-store");
            expect(await response.json()).toEqual(app.state);
            expect(app.calls.at(-1)).toEqual({
              action: action === "reconcile" ? action : "recordRetired",
              input,
            });
          }
        }
      } finally {
        await app.dispose();
      }
    }
  });

  test("rejects malformed reconciliation IDs and retirement locations before dispatch", async () => {
    const app = fixture(["orchestration:operate", "environment:maintain"]);
    try {
      for (const [action, input] of [
        ["reconcile", { ...reconciliation, operationId: "../attempt" }],
        ["reconcile", { operationId: reconciliation.operationId }],
        ["reconcile", { ...reconciliation, requestId: "not-a-uuid" }],
        ["record-retired", reconciliation],
        ["record-retired", { ...reconciliation, retiredLocation: "a".repeat(1025) }],
      ] as const) {
        const response = await app.handler(
          new Request(`http://fixture/api/jones-updates/${action}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(input),
          }),
        );
        expect(response.status).toBe(400);
      }
      expect(app.calls).toEqual([]);
    } finally {
      await app.dispose();
    }
  });

  test("round trips fixed download, install, and native preparation attempt inputs with no-store", async () => {
    const app = fixture(["orchestration:read", "orchestration:operate"]);
    try {
      const download = { artifactId: 101, sourceSha: "a".repeat(40) };
      const downloadExact = { targetSource: "b".repeat(40) };
      const replacement = {
        ...downloadExact,
        replace: {
          stagedHandle: app.state.stagedHandle,
          sourceSha: "c".repeat(40),
          operationId: "11111111-1111-4111-8111-111111111111",
          environmentId: app.state.environmentId,
          currentVersion: app.state.currentVersion,
          expectedInstalledSource: "a".repeat(40),
        },
      };
      const install = {
        stagedHandle: app.state.stagedHandle,
        environmentId: app.state.environmentId,
        currentVersion: app.state.currentVersion,
        continueRunningThreads: true,
      };
      const preparation = {
        stagedHandle: "b".repeat(64),
        transactionId: "c".repeat(64),
        environmentId: app.state.environmentId,
        currentVersion: app.state.currentVersion,
      };
      for (const [action, input] of [
        ["download", download],
        ["download-exact", downloadExact],
        ["download-exact", replacement],
        ["install", install],
        ["prepare-native", preparation],
      ] as const) {
        const response = await app.handler(
          new Request(`http://fixture/api/jones-updates/${action}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(input),
          }),
        );
        expect(response.status).toBe(200);
        expect(response.headers.get("cache-control")).toBe("no-store");
        expect(await response.json()).toEqual(app.state);
      }
      expect(app.calls).toEqual([
        { action: "download", input: download },
        { action: "downloadExact", input: downloadExact },
        { action: "downloadExact", input: replacement },
        { action: "install", input: install },
        { action: "prepareNative", input: preparation },
      ]);
    } finally {
      await app.dispose();
    }
  });

  test("rejects malformed install and missing preparation attempt before calling the service", async () => {
    const app = fixture(["orchestration:operate"]);
    try {
      const response = await app.handler(
        new Request("http://fixture/api/jones-updates/install", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ stagedHandle: 123, environmentId: "synthetic-environment" }),
        }),
      );
      expect(response.status).toBe(400);
      const preparation = await app.handler(
        new Request("http://fixture/api/jones-updates/prepare-native", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            stagedHandle: "b".repeat(64),
            environmentId: app.state.environmentId,
            currentVersion: app.state.currentVersion,
          }),
        }),
      );
      expect(preparation.status).toBe(400);
      // A version string or abbreviated commit never selects an exact source.
      for (const targetSource of ["0.1.0-preview.20261019.1.1", "b".repeat(12), "B".repeat(40)]) {
        const exact = await app.handler(
          new Request("http://fixture/api/jones-updates/download-exact", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ targetSource }),
          }),
        );
        expect(exact.status).toBe(400);
      }
      expect(app.calls).toEqual([]);
    } finally {
      await app.dispose();
    }
  });

  test("requires operate scope for every mutation while allowing read-only state", async () => {
    const app = fixture(["orchestration:read"]);
    try {
      const input = {
        stagedHandle: app.state.stagedHandle,
        environmentId: app.state.environmentId,
        currentVersion: app.state.currentVersion,
      };
      for (const action of ["check", "download", "download-exact", "prepare-native", "install"]) {
        const response = await app.handler(
          new Request(`http://fixture/api/jones-updates/${action}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(
              action === "download"
                ? { artifactId: 101, sourceSha: "a".repeat(40) }
                : action === "download-exact"
                  ? { targetSource: "a".repeat(40) }
                  : action === "prepare-native"
                    ? { ...input, stagedHandle: "b".repeat(64), transactionId: "c".repeat(64) }
                    : input,
            ),
          }),
        );
        expect(response.status).toBe(403);
      }
      expect(app.calls).toEqual([]);
      const response = await app.handler(new Request("http://fixture/api/jones-updates?after=2"));
      expect(response.status).toBe(200);
      expect(app.calls).toEqual([{ action: "state", input: 2 }]);
    } finally {
      await app.dispose();
    }
  });

  test("serves exact-pair source relation with read scope only and no-store", async () => {
    const base = "a".repeat(40),
      head = "b".repeat(40);
    const app = fixture(["orchestration:read"]);
    try {
      const response = await app.handler(
        new Request(`http://fixture/api/jones-updates/source-relation?base=${base}&head=${head}`),
      );
      expect(response.status).toBe(200);
      expect(response.headers.get("cache-control")).toBe("no-store");
      expect(await response.json()).toEqual({
        repository: "Jones-Systems/Jones-Code",
        base,
        head,
        relation: "ahead",
        observedAt: "2026-10-20T00:00:00.000Z",
      });
      expect(app.calls).toEqual([{ action: "sourceRelation", input: { base, head } }]);
    } finally {
      await app.dispose();
    }
  });

  test("rejects malformed or unauthorized source relation reads before calling the service", async () => {
    const base = "a".repeat(40);
    const app = fixture(["orchestration:read"]);
    try {
      for (const query of [
        `base=${base}`,
        `base=${base}&head=${base.slice(0, 7)}`,
        `base=${base}&head=${"B".repeat(40)}`,
        `base=${base}&head=repos%2Fother%2Frepo`,
      ]) {
        const response = await app.handler(
          new Request(`http://fixture/api/jones-updates/source-relation?${query}`),
        );
        expect(response.status).toBe(400);
      }
      expect(app.calls).toEqual([]);
    } finally {
      await app.dispose();
    }
    const unscoped = fixture([]);
    try {
      const response = await unscoped.handler(
        new Request(
          `http://fixture/api/jones-updates/source-relation?base=${base}&head=${"b".repeat(40)}`,
        ),
      );
      expect(response.status).toBe(403);
      expect(unscoped.calls).toEqual([]);
    } finally {
      await unscoped.dispose();
    }
  });
});
