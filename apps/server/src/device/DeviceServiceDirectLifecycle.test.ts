import { expect, it } from "@effect/vitest";
import { afterEach, vi } from "vite-plus/test";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { DEFAULT_SERVER_SETTINGS, DeviceHostId, type ServerSettings } from "@t3tools/contracts";
import * as Net from "@t3tools/shared/Net";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/unstable/http/HttpClient";
import { ServerSettingsService } from "../serverSettings.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as ServerConfig from "../config.ts";
import * as DeviceHost from "./DeviceHost.ts";
import * as DeviceService from "./DeviceService.ts";
import * as SshDeviceHost from "./SshDeviceHost.ts";
import * as LocalSshDeviceHost from "./localSshDeviceHost.ts";
import { DeviceDirectGrants } from "./DeviceDirectGrants.ts";

afterEach(() => vi.restoreAllMocks());

it.effect(
  "retires a host when only its direct target changes and supplies lazy registered readiness",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const home = yield* fs.makeTempDirectoryScoped();
      const changes = yield* PubSub.unbounded<ServerSettings>();
      const hostId = DeviceHostId.make("mini");
      let settings: ServerSettings = {
        ...DEFAULT_SERVER_SETTINGS,
        deviceHosts: [
          { id: hostId, label: "Mini", target: "vps-mini", directSshTarget: "laptop-mini-a" },
        ],
      };
      const instances: Array<{
        retired: boolean;
        currentEndpoint: Effect.Effect<DeviceHost.DeviceDirectMediaEndpoint | null>;
      }> = [];
      let ensureCalls = 0;
      vi.spyOn(LocalSshDeviceHost, "remoteSshDeviceHosts").mockImplementation((hosts) =>
        Effect.succeed([...hosts]),
      );
      vi.spyOn(SshDeviceHost, "make").mockImplementation(
        (host, _onReady, _onStatus, currentEndpoint) =>
          Effect.gen(function* () {
            expect(currentEndpoint).toBeDefined();
            const captured = { retired: false, currentEndpoint: currentEndpoint! };
            instances.push(captured);
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                captured.retired = true;
              }),
            );
            const ready = {
              nodePath: "/node",
              hub: { origin: "http://hub.test" },
              directMedia: {
                target: host.directSshTarget!,
                owner: "owner",
                generation: String(instances.length),
                gatewayPort: 1234,
              },
              helpers: { serveSimAxSettings: null, serveSimCli: null },
              run: () => Effect.die("Unexpected remote command"),
            };
            return {
              id: host.id,
              summary: Effect.succeed({
                id: host.id,
                label: host.label,
                kind: "ssh",
                hubInstalled: true,
                agentDeviceInstalled: false,
                platforms: [],
              }),
              current: Effect.succeed(ready),
              ensureReady: () => {
                ensureCalls++;
                return Effect.succeed(ready);
              },
            } as unknown as DeviceHost.DeviceHost["Service"];
          }),
      );
      const local = {
        id: DeviceHostId.make("local"),
        summary: Effect.succeed({
          id: DeviceHostId.make("local"),
          label: "Local",
          kind: "local",
          hubInstalled: false,
          agentDeviceInstalled: false,
          platforms: [],
        }),
        current: Effect.succeed(null),
      } as unknown as DeviceHost.DeviceHost["Service"];
      const service = yield* DeviceService.make.pipe(
        Effect.provideService(DeviceHost.DeviceHost, local),
        Effect.provideService(DeviceDirectGrants, {
          issue: () => Effect.succeed(null),
          admit: () => Effect.succeed({ _tag: "Denied" as const }),
        }),
        Effect.provideService(ServerSettingsService, {
          getSettings: Effect.sync(() => settings),
          subscribeChanges: PubSub.subscribe(changes).pipe(Effect.map(Stream.fromSubscription)),
        } as unknown as ServerSettingsService["Service"]),
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make(() => Effect.die("Unexpected HTTP request")),
        ),
        Effect.provide(
          Layer.mergeAll(ServerConfig.layerTest(home, home), Net.layer, ProcessRunner.layer),
        ),
      );
      expect(instances).toHaveLength(1);
      expect((yield* instances[0]!.currentEndpoint)?.target).toBe("laptop-mini-a");
      const events = yield* service.subscribe;
      const nextHosts = Stream.fromSubscription(events).pipe(
        Stream.filter((state) => state.hosts.length === 2),
        Stream.take(1),
        Stream.runCollect,
      );
      settings = {
        ...settings,
        deviceHosts: [{ ...settings.deviceHosts[0]!, directSshTarget: "laptop-mini-b" }],
      };
      yield* PubSub.publish(changes, settings);
      yield* nextHosts;
      expect(instances).toHaveLength(2);
      expect(instances[0]!.retired).toBe(true);
      expect(instances[1]!.retired).toBe(false);
      expect((yield* instances[0]!.currentEndpoint)?.target).toBe("laptop-mini-b");
      expect((yield* instances[1]!.currentEndpoint)?.target).toBe("laptop-mini-b");
      expect(ensureCalls).toBe(0);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
