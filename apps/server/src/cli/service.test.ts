import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NetService from "@t3tools/shared/Net";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Terminal from "effect/Terminal";
import { Command } from "effect/cli";
import { HttpClient } from "effect/http";
import { afterEach, vi } from "vite-plus/test";

import packageJson from "../../package.json" with { type: "json" };
import * as BootService from "../cloud/bootService.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as HostServiceConfig from "../jones/hostService/HostServiceConfig.ts";
import { JONES_BOOT_SERVICE_IDENTITY } from "../jones/hostService/identity.ts";
import {
  offerServiceDuringOnboarding,
  recoverServiceOnboardingOffer,
  serviceCommand,
} from "./service.ts";
import { jonesBootServiceLayer } from "./service.ts";

afterEach(() => vi.restoreAllMocks());

const status = {
  supported: true,
  installed: true,
  current: true,
  unitPath: "/home/me/.config/systemd/user/t3code.service",
  logPath: "/home/me/.t3/userdata/logs/boot-service.log",
} as const;

const newerServiceStatus = { ...status, current: false, installedVersion: "999.0.0" };

function makeTestService(
  serviceStatus: BootService.BootServiceStatus,
  restartOutcome: "restarted" | "queued" = "restarted",
) {
  const installOptions: Array<Parameters<BootService.BootService["Service"]["install"]>[0]> = [];
  const restarts: Array<true> = [];
  const service = BootService.BootService.of({
    status: Effect.succeed(serviceStatus),
    withStopped: (operation) => operation,
    restart: Effect.sync(() => {
      restarts.push(true);
      return serviceStatus.installed ? restartOutcome : "skipped";
    }),
    install: (options) =>
      Effect.sync(() => {
        installOptions.push(options);
        return {
          program: ["/test/t3/runtime/versions/1.0.0/t3", "__service-launcher"],
          baseDir: "/test/t3",
          unitPath: serviceStatus.unitPath,
          logPath: serviceStatus.logPath,
        };
      }),
    uninstall: Effect.succeed(false),
  });
  return { service, installOptions, restarts };
}

it.layer(Layer.mergeAll(NodeServices.layer, NetService.layer))("service commands", (it) => {
  it.effect.each(["install", "update"] as const)(
    "%s refuses a downgrade before changing the service",
    (command) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-service-cli-test-" });
        const { service, installOptions } = makeTestService(newerServiceStatus);
        vi.spyOn(BootService, "layer").mockReturnValue(
          Layer.succeed(BootService.BootService, service),
        );

        const error = yield* Command.runWith(serviceCommand, { version: packageJson.version })([
          command,
          "--base-dir",
          baseDir,
        ]).pipe(
          Effect.provideService(HostProcess.Environment, {}),
          Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env: {} }))),
          Effect.flip,
        );

        expect(error).toMatchObject({
          _tag: "BootServiceDowngradeRefusedError",
          installedVersion: "999.0.0",
          targetVersion: packageJson.version,
        });
        expect(installOptions).toEqual([]);
      }),
  );
});

it.effect("leaves a newer service unchanged during onboarding without prompting", () =>
  Effect.gen(function* () {
    const { service, installOptions } = makeTestService(newerServiceStatus);
    const terminal = Terminal.make({
      columns: Effect.succeed(80),
      rows: Effect.succeed(24),
      readInput: Effect.die("Onboarding must not prompt to replace a newer service."),
      readLine: Effect.die("Onboarding must not prompt to replace a newer service."),
      display: () => Effect.die("Onboarding must not prompt to replace a newer service."),
    });

    const ready = yield* offerServiceDuringOnboarding.pipe(
      Effect.provideService(BootService.BootService, service),
      Effect.provideService(Terminal.Terminal, terminal),
      Effect.provide(NodeServices.layer),
    );

    expect(ready).toBe("skipped");
    expect(installOptions).toEqual([]);
  }),
);

// regression: #9602 diagnose incomplete headless server setup
it.effect("keeps the manual-server fallback when background prerequisites fail", () =>
  Effect.gen(function* () {
    const ready = yield* recoverServiceOnboardingOffer(
      Effect.fail(new BootService.BootServicePrerequisiteError({ problem: "linger-disabled" })),
    );
    expect(ready).toBe("skipped");
  }),
);

const jonesConfigurationTestLayer = Layer.mergeAll(
  NodeServices.layer,
  Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make(() => Effect.die("no HTTP expected")),
  ),
  Layer.succeed(
    ProcessRunner.ProcessRunner,
    ProcessRunner.ProcessRunner.of({
      run: () => Effect.die("no process expected"),
    }),
  ),
);

it.layer(jonesConfigurationTestLayer)("Jones CLI service configuration", (it) => {
  it.effect("uses Jones identity without inventing a port when config is missing", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "jones-cli-config-" });
      const { service } = makeTestService(status);
      const layerSpy = vi
        .spyOn(BootService, "layer")
        .mockReturnValue(Layer.succeed(BootService.BootService, service));
      yield* BootService.BootService.pipe(
        Effect.provide(
          jonesBootServiceLayer({
            baseDir,
            logsDir: `${baseDir}/logs`,
            cliVersion: "1.2.3",
          }),
        ),
      );
      expect(layerSpy).toHaveBeenCalledWith({
        baseDir,
        logsDir: `${baseDir}/logs`,
        cliVersion: "1.2.3",
        identity: JONES_BOOT_SERVICE_IDENTITY,
        allowEnableLinger: false,
      });
    }),
  );

  it.effect("loads persisted loopback environment for updates and explicit linger setup", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "jones-cli-config-" });
      yield* HostServiceConfig.HostServiceConfig.pipe(
        Effect.flatMap((config) =>
          config.write(baseDir, { schema: 1, port: 4321, tailscaleServePort: 8443 }),
        ),
        Effect.provide(HostServiceConfig.layer),
      );
      const { service } = makeTestService(status);
      const layerSpy = vi
        .spyOn(BootService, "layer")
        .mockReturnValue(Layer.succeed(BootService.BootService, service));
      yield* BootService.BootService.pipe(
        Effect.provide(
          jonesBootServiceLayer({
            baseDir,
            logsDir: `${baseDir}/logs`,
            cliVersion: "1.2.4",
            allowEnableLinger: true,
          }),
        ),
      );
      expect(layerSpy).toHaveBeenCalledWith({
        baseDir,
        logsDir: `${baseDir}/logs`,
        cliVersion: "1.2.4",
        allowEnableLinger: true,
        identity: JONES_BOOT_SERVICE_IDENTITY,
        environment: {
          T3CODE_HOST: "127.0.0.1",
          T3CODE_PORT: "4321",
          T3CODE_TAILSCALE_SERVE: "false",
        },
      });
    }),
  );
});
