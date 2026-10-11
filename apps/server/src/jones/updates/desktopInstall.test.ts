import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  type DesktopUpdateStatusReport,
  type JonesUpdateState,
} from "@t3tools/contracts";
import { HostProcessArchitecture, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { vi } from "vite-plus/test";
import * as ServerConfig from "../../config.ts";
import * as SelfUpdate from "../../cloud/selfUpdate.ts";
import * as Launcher from "../../cloud/serviceLauncherClient.ts";
import * as Startup from "../../serverRuntimeStartup.ts";
import * as DesktopReceiver from "../../resourceTelemetry/DesktopTelemetryReceiver.ts";
import * as JonesUpdates from "./service.ts";

// Qualification has its own contract tests; this fixture exercises the qualified desktop branch.
vi.mock("./qualification.ts", () => ({
  isJonesRuntime: () => true,
  isPreviewRuntime: () => false,
}));

it.effect("hands desktop installation to its controller before any continuation preparation", () =>
  Effect.gen(function* () {
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
      Layer.succeed(HostProcessPlatform, "darwin"),
      Layer.succeed(HostProcessArchitecture, "arm64"),
      Layer.mock(SelfUpdate.ServerSelfUpdate)({
        update: () => Effect.die("Unexpected server update"),
        commitDesktopUpdate: () => Effect.die("Unexpected legacy desktop update"),
      }),
      Layer.mock(Launcher.ServiceLauncherClient)({ managed: false }),
      Layer.mock(Startup.ServerRuntimeStartup)({
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
    const result = yield* Effect.gen(function* () {
      const updates = yield* JonesUpdates.JonesUpdates;
      return yield* updates.install({
        environmentId: state.environmentId!,
        currentVersion: state.currentVersion!,
        stagedHandle: state.stagedHandle!,
        continueRunningThreads: true,
      });
    }).pipe(Effect.provide(JonesUpdates.layer.pipe(Layer.provide(dependencies))));
    expect(result.phase).toBe("installing");
    expect(committed).toEqual([requestId]);
    expect(preparations).toBe(0);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
