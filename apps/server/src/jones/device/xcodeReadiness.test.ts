import { expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NetService from "@t3tools/shared/Net";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { HttpClient, HttpClientResponse } from "effect/http";

import * as ServerConfig from "../../config.ts";
import * as DeviceService from "../../device/DeviceService.ts";
import * as LocalDeviceHost from "../../device/LocalDeviceHost.ts";
import { DEVICE_HUB_VERSION } from "../../device/DeviceToolchain.ts";
import * as ProcessRunner from "../../processRunner.ts";
import * as ServerSettings from "../../serverSettings.ts";

const fixture = Effect.fn("xcodeFixture")(function* (android = false, xcodebuild = true) {
  const fs = yield* FileSystem.FileSystem;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "jones-xcode-ready-" });
  yield* fs.makeDirectory(`${root}/bin`);
  for (const command of ["xcrun", ...(xcodebuild ? ["xcodebuild"] : [])]) {
    yield* fs.writeFileString(`${root}/bin/${command}`, "#!/bin/sh\nexit 0\n");
    yield* fs.chmod(`${root}/bin/${command}`, 0o755);
  }
  if (android) {
    for (const tool of [
      "platform-tools/adb",
      "emulator/emulator",
      "cmdline-tools/latest/bin/avdmanager",
    ]) {
      const parent = tool.slice(0, tool.lastIndexOf("/"));
      yield* fs.makeDirectory(`${root}/sdk/${parent}`, { recursive: true });
      yield* fs.writeFileString(`${root}/sdk/${tool}`, "");
    }
  }
  const install = `${root}/tools/expo-device-hub/${DEVICE_HUB_VERSION}`;
  yield* fs.makeDirectory(`${install}/node_modules/expo-device-hub/dist/server`, {
    recursive: true,
  });
  yield* fs.writeFileString(`${install}/node_modules/expo-device-hub/dist/server/cli.mjs`, "");
  yield* fs.writeFileString(`${install}/.install-complete`, DEVICE_HUB_VERSION);
  const state = { code: 0, timedOut: false, stopped: 0, lists: 0 };
  const starts: string[][] = [];
  const checks: ProcessRunner.ProcessRunInput[] = [];
  const provideHost = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.provideService(HostProcess.Environment, {
        HOME: root,
        PATH: `${root}/bin`,
        DEVELOPER_DIR: "/selected/Xcode.app/Contents/Developer",
        ...(android ? { ANDROID_HOME: `${root}/sdk` } : {}),
      }),
      Effect.provideService(HostProcess.Platform, "darwin"),
      Effect.provideService(HostProcess.IsExecutable, false),
    );
  const host = yield* LocalDeviceHost.make().pipe(
    Effect.provide(Layer.mergeAll(ServerConfig.layerTest(root, root), NetService.layer)),
    Effect.provideService(ProcessRunner.ProcessRunner, {
      run: (input) =>
        Effect.sync(() => {
          if (input.command === "xcodebuild") {
            checks.push(input);
            expect(input.args).toEqual(["-checkFirstLaunchStatus"]);
          }
          return {
            code: ChildProcessSpawner.ExitCode(input.command === "xcodebuild" ? state.code : 0),
            timedOut: input.command === "xcodebuild" && state.timedOut,
            stdout: "",
            stderr: "",
            stdoutTruncated: false,
            stderrTruncated: false,
            stdoutInvalidUtf8: false,
            stderrInvalidUtf8: false,
          };
        }),
    }),
    Effect.provideService(
      ChildProcessSpawner.ChildProcessSpawner,
      ChildProcessSpawner.make((command) => {
        expect(ChildProcess.isStandardCommand(command)).toBe(true);
        if (!ChildProcess.isStandardCommand(command)) return Effect.die("Unexpected pipeline");
        starts.push([...command.args]);
        return Effect.acquireRelease(
          Effect.succeed(
            ChildProcessSpawner.makeHandle({
              pid: ChildProcessSpawner.ProcessId(2147480000 + starts.length),
              stdout: Stream.empty,
              stderr: Stream.empty,
              all: Stream.empty,
              exitCode: Effect.never,
              isRunning: Effect.succeed(true),
              kill: () => Effect.void,
              stdin: Sink.drain,
              getInputFd: () => Sink.drain,
              getOutputFd: () => Stream.empty,
              unref: Effect.succeed(Effect.void),
            }),
          ),
          () =>
            Effect.sync(() => {
              state.stopped++;
            }),
        );
      }),
    ),
    Effect.provideService(
      HttpClient.HttpClient,
      HttpClient.make((request) =>
        Effect.sync(() => {
          if (!request.url.endsWith("/api/devices"))
            return HttpClientResponse.fromWeb(request, new Response("ok"));
          state.lists++;
          return HttpClientResponse.fromWeb(
            request,
            Response.json({
              simulators: [
                {
                  id: "retained-iphone",
                  name: "iPhone 17",
                  platform: "ios",
                  version: "27",
                  physical: false,
                  booted: true,
                },
              ],
              emulators: [],
            }),
          );
        }),
      ),
    ),
    provideHost,
  );
  return { host, state, starts, checks, provideHost };
});

it.effect(
  "blocks incomplete, timed-out and missing Xcode without starting discovery, then recovers",
  () =>
    Effect.gen(function* () {
      for (const scenario of ["incomplete", "timeout", "missing"] as const) {
        const f = yield* fixture(false, scenario !== "missing");
        f.state.code = scenario === "incomplete" ? 69 : 0;
        f.state.timedOut = scenario === "timeout";
        for (let refresh = 0; refresh < 2; refresh++) {
          const summary = yield* f.provideHost(f.host.summary);
          const ios = summary.platforms.find((platform) => platform.platform === "ios");
          expect(ios?.available).toBe(false);
          expect(ios?.reason).toMatch(
            scenario === "incomplete"
              ? /setup is incomplete/
              : scenario === "timeout"
                ? /timed out/
                : /tools were not found/,
          );
          yield* f.provideHost(f.host.ensureReady(() => Effect.void)).pipe(Effect.flip);
        }
        expect(f.starts).toEqual([]);
        expect(f.state.lists).toBe(0);
        if (scenario === "missing") continue;
        f.state.code = 0;
        f.state.timedOut = false;
        const ready = yield* f.host.ensureReady(() => Effect.void);
        expect((yield* f.provideHost(ready.devices.list)).devices[0]?.id).toBe("retained-iphone");
        expect((yield* f.provideHost(f.host.summary)).agentDeviceInstalled).toBe(false);
        expect(
          f.checks.every(
            (check) => check.env?.DEVELOPER_DIR === "/selected/Xcode.app/Contents/Developer",
          ),
        ).toBe(true);
      }
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect(
  "rechecks reused hubs, preserves healthy Android and restores dual-platform discovery",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture(true);
      yield* f.provideHost(f.host.ensureReady(() => Effect.void));
      expect(f.starts[0]).not.toContain("--platform");
      f.state.code = 69;
      yield* f.provideHost(f.host.ensureReady(() => Effect.void));
      expect(f.starts[1]?.slice(-2)).toEqual(["--platform", "android"]);
      expect(f.state.stopped).toBe(1);
      yield* f.provideHost(f.host.ensureReady(() => Effect.void));
      expect(f.starts).toHaveLength(2);
      f.state.code = 0;
      yield* f.provideHost(f.host.ensureReady(() => Effect.void));
      expect(f.starts[2]).not.toContain("--platform");
      expect(f.state.stopped).toBe(2);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("replaces stale ready status with the setup reason and recovers after setup", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    const service = yield* f
      .provideHost(DeviceService.makeWithHosts(new Map([[f.host.id, f.host]])))
      .pipe(
        Effect.provide(
          Layer.mergeAll(NodeCrypto.layer, ServerSettings.layerTest({ enableDeviceSupport: true })),
        ),
      );
    expect((yield* f.provideHost(service.list)).hostStatuses[f.host.id]?.status).toBe("ready");
    f.state.code = 69;
    const blocked = yield* f.provideHost(service.list);
    expect(blocked.hostStatuses[f.host.id]).toMatchObject({
      status: "failed",
      detail: expect.stringContaining("setup is incomplete"),
    });
    expect(f.state.lists).toBe(1);
    f.state.code = 0;
    expect((yield* f.provideHost(service.list)).hostStatuses[f.host.id]?.status).toBe("ready");
    expect(f.state.lists).toBe(2);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
