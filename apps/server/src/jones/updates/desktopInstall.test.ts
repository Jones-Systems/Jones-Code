import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ThreadId,
  type DesktopUpdateStatusReport,
  type JonesUpdateState,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { vi } from "vite-plus/test";
import * as ServerConfig from "../../config.ts";
import * as SelfUpdate from "../../cloud/selfUpdate.ts";
import * as Launcher from "../../cloud/serviceLauncherClient.ts";
import * as Startup from "../../serverRuntimeStartup.ts";
import * as DesktopReceiver from "../../resourceTelemetry/DesktopTelemetryReceiver.ts";
import * as JonesUpdates from "./service.ts";
import * as UpdateAdmissionFence from "./UpdateAdmissionFence.ts";
import { prepareNativeContinuationReceipt } from "./nativePreparation.ts";
import { isJonesRuntime } from "./qualification.ts";

// Runtime qualification is controlled independently of quit readiness in these fixtures.
vi.mock("./qualification.ts", () => ({
  isJonesRuntime: vi.fn(() => true),
  isPreviewRuntime: () => false,
}));
vi.mock(import("./nativePreparation.ts"), async (importOriginal) => ({
  ...(await importOriginal()),
  prepareNativeContinuationReceipt: vi.fn(async (input: { prepare: () => Promise<unknown> }) => {
    await input.prepare();
  }),
}));

it.effect("hands desktop installation to its controller before any continuation preparation", () =>
  Effect.gen(function* () {
    vi.mocked(prepareNativeContinuationReceipt).mockClear();
    const fs = yield* FileSystem.FileSystem;
    const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "jones-desktop-install-" });
    const config = yield* ServerConfig.ServerConfig.pipe(
      Effect.provide(ServerConfig.layerTest(process.cwd(), baseDir)),
    );
    const state: JonesUpdateState = {
      source: "jones-actions",
      channel: "jones-main",
      phase: "staged",
      environmentId: EnvironmentId.make("synthetic-environment"),
      currentVersion: "0.0.0-preview.20261010.1.1",
      stagedHandle: "a".repeat(64),
      capability: { check: true, download: true, install: true },
      provenance: {
        repository: "Jones-Systems/Jones-Code",
        sourceSha: "b".repeat(40),
        sourceTree: "c".repeat(40),
        workflow: ".github/workflows/artifact-desktop-mac.yml",
        runId: 1,
        runAttempt: 1,
        artifactId: 3,
        platform: "darwin",
        architecture: "arm64",
        artifactDigest: `sha256:${"d".repeat(64)}`,
      },
    };
    const report: DesktopUpdateStatusReport = {
      version: 1,
      type: "desktopUpdateStatus",
      state: {
        enabled: true,
        status: "downloaded",
        channel: "latest",
        currentVersion: state.currentVersion!,
        hostArch: "arm64",
        appArch: "arm64",
        runningUnderArm64Translation: false,
        availableVersion: null,
        downloadedVersion: null,
        releaseNotes: [],
        omittedReleaseCount: 0,
        downloadPercent: null,
        checkedAt: null,
        message: null,
        errorContext: null,
        canRetry: false,
        jones: state,
      },
    };
    let requestId = "";
    const committed: string[] = [];
    let preparations = 0;
    const dependencies = Layer.mergeAll(
      ServerConfig.layer({ ...config, mode: "desktop" }),
      UpdateAdmissionFence.layer,
      Layer.succeed(HostProcess.Platform, "darwin"),
      Layer.succeed(HostProcess.Architecture, "arm64"),
      Layer.mock(SelfUpdate.ServerSelfUpdate)({
        update: () => Effect.die("Unexpected server update"),
        commitDesktopUpdate: () => Effect.die("Unexpected legacy desktop update"),
      }),
      Layer.mock(Launcher.ServiceLauncherClient)({
        managed: false,
        requiresQualifiedTrialGate: false,
      }),
      Layer.mock(Startup.ServerRuntimeStartup)({
        awaitCommandReady: Effect.void,
        enqueueCommand: (effect) => effect,
        markRunningProviderSessionsForContinuation: Effect.die("Unexpected server preparation"),
        markOptedInProviderSessionsForContinuation: Effect.sync(() => {
          preparations += 1;
          return [];
        }),
        clearProviderSessionContinuationMarkers: () => Effect.void,
      }),
      Layer.mock(DesktopReceiver.DesktopTelemetryReceiver)({
        desktopUpdates: Effect.sync(() => ({
          latest: Option.some(report),
          changes: Stream.suspend(() =>
            requestId === ""
              ? Stream.empty
              : Stream.make({
                  ...report,
                  requestId,
                  outcome: "ready-to-install" as const,
                }),
          ),
        })),
        requestDesktopUpdate: (id) =>
          Effect.sync(() => {
            requestId = id;
          }),
        commitDesktopUpdate: (id) =>
          Effect.sync(() => {
            committed.push(id);
          }),
        cancelDesktopUpdate: () => Effect.void,
      }),
    );
    yield* Effect.gen(function* () {
      const updates = yield* JonesUpdates.JonesUpdates;
      const result = yield* updates.install({
        environmentId: state.environmentId!,
        currentVersion: state.currentVersion!,
        stagedHandle: state.stagedHandle!,
        continueRunningThreads: true,
      });
      expect(result.phase).toBe("installing");
      expect(committed).toEqual([requestId]);
      expect(preparations).toBe(0);
      expect(prepareNativeContinuationReceipt).not.toHaveBeenCalled();
      const preparation = {
        environmentId: state.environmentId!,
        currentVersion: state.currentVersion!,
        stagedHandle: state.stagedHandle!,
        transactionId: "e".repeat(64),
      };
      expect(
        (yield* updates.prepareNative({ ...preparation, transactionId: "invalid" })).phase,
      ).toBe("blocked");
      expect(prepareNativeContinuationReceipt).not.toHaveBeenCalled();
      yield* updates.prepareNative(preparation);
      expect(prepareNativeContinuationReceipt).toHaveBeenCalledWith(
        expect.objectContaining({
          handle: state.stagedHandle,
          transactionId: preparation.transactionId,
          environmentId: state.environmentId,
          version: state.currentVersion,
        }),
      );
      expect(preparations).toBe(1);
      // The desktop updater owns its own check and download, so exact selection is refused
      // without issuing another desktop request.
      const requested = requestId;
      const exact = yield* updates.downloadExact({ targetSource: "a".repeat(40) });
      expect(exact.phase).toBe("blocked");
      expect(exact.capability).toMatchObject({ download: false });
      expect(exact.capability.exactSource).toBeUndefined();
      expect(requestId).toBe(requested);
    }).pipe(Effect.provide(JonesUpdates.layer.pipe(Layer.provide(dependencies))));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect.each([
  { gate: "ready", mode: "desktop", qualified: true },
  { gate: "ready", mode: "desktop", qualified: false },
  { gate: "pending", mode: "desktop", qualified: true },
  { gate: "failed", mode: "desktop", qualified: true },
  { gate: "shutdown", mode: "desktop", qualified: true },
  { gate: "mark-failed", mode: "desktop", qualified: true },
  { gate: "ready", mode: "web", qualified: false },
] as const)("prepares quit with $gate gate in $mode mode (qualified $qualified)", (scenario) =>
  Effect.gen(function* () {
    vi.mocked(isJonesRuntime).mockReturnValue(scenario.qualified);
    const fs = yield* FileSystem.FileSystem;
    const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "jones-quit-prepare-" });
    const config = yield* ServerConfig.ServerConfig.pipe(
      Effect.provide(ServerConfig.layerTest(process.cwd(), baseDir)),
    );
    const gate = yield* Startup.makeCommandGate;
    const error = new Startup.ServerRuntimeStartupError({
      mode: scenario.mode,
      host: null,
      port: 0,
      cause: "synthetic command gate failure",
    });
    if (scenario.gate !== "pending" && scenario.gate !== "failed") {
      yield* gate.signalCommandReady;
    }
    if (scenario.gate === "failed" || scenario.gate === "shutdown") {
      yield* gate.failCommandReady(error);
    }
    let waits = 0;
    let enqueues = 0;
    let marks = 0;
    const dependencies = Layer.mergeAll(
      ServerConfig.layer({ ...config, mode: scenario.mode }),
      UpdateAdmissionFence.layer,
      Layer.succeed(HostProcess.Platform, "darwin"),
      Layer.succeed(HostProcess.Architecture, "arm64"),
      Layer.mock(SelfUpdate.ServerSelfUpdate)({}),
      Layer.mock(Launcher.ServiceLauncherClient)({
        managed: false,
        requiresQualifiedTrialGate: false,
      }),
      Layer.mock(Startup.ServerRuntimeStartup)({
        awaitCommandReady: Effect.sync(() => {
          waits += 1;
        }).pipe(Effect.andThen(gate.awaitCommandReady)),
        enqueueCommand: (effect) =>
          Effect.sync(() => {
            enqueues += 1;
          }).pipe(Effect.andThen(gate.enqueueCommand(effect))),
        markOptedInProviderSessionsForContinuation: Effect.gen(function* () {
          marks += 1;
          if (scenario.gate === "mark-failed") return yield* error;
          return [ThreadId.make("quit-thread-one"), ThreadId.make("quit-thread-two")];
        }),
      }),
      Layer.mock(DesktopReceiver.DesktopTelemetryReceiver)({
        desktopUpdates: Effect.succeed({ latest: Option.none(), changes: Stream.empty }),
      }),
    );
    yield* Effect.gen(function* () {
      const updates = yield* JonesUpdates.JonesUpdates;
      const result =
        scenario.gate === "pending"
          ? yield* Effect.gen(function* () {
              const fiber = yield* updates.prepareQuit.pipe(Effect.forkChild);
              yield* TestClock.adjust("250 millis");
              return yield* Fiber.join(fiber);
            })
          : yield* updates.prepareQuit;
      const prepared = scenario.mode === "desktop" && scenario.gate === "ready";
      expect(result).toEqual({ prepared, threadCount: prepared ? 2 : 0 });
      expect(waits).toBe(scenario.mode === "desktop" ? 1 : 0);
      expect(enqueues).toBe(
        scenario.mode === "desktop" && ["ready", "shutdown", "mark-failed"].includes(scenario.gate)
          ? 1
          : 0,
      );
      expect(marks).toBe(prepared || scenario.gate === "mark-failed" ? 1 : 0);
      if (scenario.gate === "pending") {
        yield* gate.signalCommandReady;
        yield* Effect.yieldNow;
        expect(marks).toBe(0);
      }
    }).pipe(Effect.provide(JonesUpdates.layer.pipe(Layer.provide(dependencies))));
  }).pipe(
    Effect.ensuring(
      Effect.sync(() => {
        vi.mocked(isJonesRuntime).mockReturnValue(true);
      }),
    ),
    Effect.scoped,
    Effect.provide(NodeServices.layer),
  ),
);
