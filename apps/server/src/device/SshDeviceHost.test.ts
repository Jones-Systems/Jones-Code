// @effect-diagnostics preferSchemaOverJson:off - the external process fixture emits raw JSON over SSH stdout.
import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Net from "@t3tools/shared/Net";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as PlatformError from "effect/PlatformError";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as ServerConfig from "../config.ts";
import * as DeviceHost from "./DeviceHost.ts";
import { DeviceDirectGrants } from "../jones/device/DeviceDirectGrants.ts";
import * as SshDeviceHost from "./SshDeviceHost.ts";

it.effect("preserves installed status after probes and cleans failed agent activation", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const home = yield* fs.makeTempDirectoryScoped();
    const modes: string[] = [];
    const owners: string[] = [];
    let forwards = 0;
    let failForward = true;
    let rejectConfig = true;
    const spawner = ChildProcessSpawner.make((command) =>
      Effect.gen(function* () {
        if (command._tag !== "StandardCommand") return yield* Effect.die("Unexpected command");
        const forwarding = command.args.includes("-N");
        let output = "";
        if (forwarding) {
          expect(command.args).not.toContain("-R");
          if (failForward) {
            failForward = false;
            return yield* PlatformError.systemError({
              _tag: "AlreadyExists",
              module: "ChildProcess",
              method: "spawn",
              description: "Port already bound",
            });
          }
          forwards++;
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              forwards--;
            }),
          );
        } else {
          const stdin = command.options.stdin;
          if (
            !stdin ||
            typeof stdin !== "object" ||
            !("stream" in stdin) ||
            !Stream.isStream(stdin.stream)
          )
            return yield* Effect.die("Missing script");
          const script = yield* stdin.stream.pipe(
            Stream.decodeText(),
            Stream.runFold(
              () => "",
              (a, b) => a + b,
            ),
          );
          const mode = /const mode = "([^"]+)"/.exec(script)?.[1] ?? "";
          modes.push(mode);
          owners.push(/const owner = "([^"]+)"/.exec(script)?.[1] ?? "");
          output = JSON.stringify({
            nodePath: "/node",
            platforms: [{ platform: "ios", available: true }],
            hubPort: 1234,
            helpers: { serveSimAxSettings: null, serveSimCli: null },
            ...(mode === "agent-start"
              ? { daemonPort: 1235, token: "fixture", entryPath: "/agent.mjs" }
              : {}),
          });
        }
        return ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(123),
          stdout: Stream.make(new TextEncoder().encode(output)),
          stderr: Stream.empty,
          all: Stream.empty,
          exitCode: forwarding ? Effect.never : Effect.succeed(ChildProcessSpawner.ExitCode(0)),
          isRunning: Effect.succeed(forwarding),
          kill: () => Effect.void,
          stdin: Sink.drain,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
          unref: Effect.succeed(Effect.void),
        });
      }),
    );
    const host = yield* SshDeviceHost.make(
      { id: "test", label: "Test", target: "test.example" },
      () =>
        rejectConfig
          ? Effect.fail(
              new DeviceHost.DeviceHostError({
                hostId: "test",
                step: "configuring agent access",
                cause: new Error("fixture failure"),
              }),
            )
          : Effect.void,
    ).pipe(
      Effect.provideService(DeviceDirectGrants, {
        issue: () => Effect.succeed(null),
        admit: () => Effect.succeed({ _tag: "Denied" as const }),
      }),
      Effect.provide(Layer.mergeAll(ServerConfig.layerTest(home, home), Net.layer)),
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Effect.provideService(
        HttpClient.HttpClient,
        HttpClient.make((request) =>
          Effect.succeed(HttpClientResponse.fromWeb(request, new Response("ok"))),
        ),
      ),
    );
    yield* host.ensureReady(() => Effect.void);
    yield* SshDeviceHost.probe({ id: "test", label: "Test", target: "test.example" }).pipe(
      Effect.provide(ServerConfig.layerTest(home, home)),
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    );
    expect(new Set(owners).size).toBe(1);
    expect(owners[0]).toMatch(/^[a-f0-9]{24}$/);
    expect(forwards).toBe(1);
    expect(modes.filter((mode) => mode === "start")).toHaveLength(2);
    yield* host.platformAvailability("ios");
    expect((yield* host.summary).hubInstalled).toBe(true);
    const failed = yield* host.ensureAgentReady(() => Effect.void).pipe(Effect.result);
    expect(failed._tag).toBe("Failure");
    expect(forwards).toBe(0);
    expect(modes.at(-1)).toBe("stop-agent");
    expect(yield* host.current).toBeNull();
    rejectConfig = false;
    yield* host.ensureAgentReady(() => Effect.void);
    yield* host.platformAvailability("ios");
    expect((yield* host.summary).agentDeviceInstalled).toBe(true);
    yield* host.stopAgent;
    expect(forwards).toBe(1);
    yield* host.stop;
    expect(forwards).toBe(0);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect(
  "forwards only the restricted direct gateway and callback, rotates generations, and stops failed starts",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const home = yield* fs.makeTempDirectoryScoped();
      const commands: string[][] = [];
      const modes: string[] = [];
      const generations: string[] = [];
      let failForward = true;
      let forwards = 0;
      let owner = "";
      let healthGeneration = "";
      let rejectHealth = false;
      let rejectDescriptor = false;
      let failCleanup = false;
      let failBootstrap = false;
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.gen(function* () {
          if (command._tag !== "StandardCommand") return yield* Effect.die("Unexpected command");
          const forwarding = command.args.includes("-N");
          let output = "";
          let bootstrapExit = 0;
          if (forwarding) {
            commands.push([...command.args]);
            if (failForward) {
              failForward = false;
              return yield* PlatformError.systemError({
                _tag: "AlreadyExists",
                module: "ChildProcess",
                method: "spawn",
                description: "Port occupied",
              });
            }
            forwards++;
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                forwards--;
              }),
            );
          } else {
            const stdin = command.options.stdin;
            if (
              !stdin ||
              typeof stdin !== "object" ||
              !("stream" in stdin) ||
              !Stream.isStream(stdin.stream)
            )
              return yield* Effect.die("Missing script");
            const script = yield* stdin.stream.pipe(
              Stream.decodeText(),
              Stream.runFold(
                () => "",
                (a, b) => a + b,
              ),
            );
            const mode = /const mode = "([^"]+)"/.exec(script)?.[1] ?? "";
            modes.push(mode);
            if (mode === "stop-direct" && failCleanup) bootstrapExit = 1;
            const direct = /const direct = (.+);/.exec(script)?.[1];
            const generation =
              direct && direct !== "null" ? /"generation":"([^"]+)"/.exec(direct)?.[1] : undefined;
            if (generation && (mode === "start" || mode === "agent-start")) {
              generations.push(generation);
              if (failBootstrap) bootstrapExit = 1;
            }
            owner = /const owner = "([^"]+)"/.exec(script)?.[1] ?? "";
            if (generation) healthGeneration = generation;
            output = JSON.stringify({
              nodePath: "/node",
              platforms: [{ platform: "ios", available: true }],
              hubPort: 1234,
              helpers: { serveSimAxSettings: null, serveSimCli: null },
              ...(mode === "agent-start"
                ? { daemonPort: 1235, token: "fixture", entryPath: "/agent.mjs" }
                : {}),
              ...(generation && !rejectDescriptor
                ? { directMedia: { gatewayPort: 2234, admissionPort: 2235, generation } }
                : {}),
            });
          }
          return ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(123),
            stdout: Stream.make(new TextEncoder().encode(output)),
            stderr: Stream.empty,
            all: Stream.empty,
            exitCode: forwarding
              ? Effect.never
              : Effect.succeed(ChildProcessSpawner.ExitCode(bootstrapExit)),
            isRunning: Effect.succeed(forwarding),
            kill: () => Effect.void,
            stdin: Sink.drain,
            getInputFd: () => Sink.drain,
            getOutputFd: () => Stream.empty,
            unref: Effect.succeed(Effect.void),
          });
        }),
      );
      const server = yield* ServerConfig.ServerConfig.pipe(
        Effect.provide(ServerConfig.layerTest(home, home)),
      );
      const host = yield* SshDeviceHost.make(
        { id: "test", label: "Test", target: "vps-mini", directSshTarget: "laptop-mini" },
        undefined,
        undefined,
        Effect.succeed(null),
      ).pipe(
        Effect.provideService(DeviceDirectGrants, {
          issue: () => Effect.succeed(null),
          admit: () => Effect.succeed({ _tag: "Denied" as const }),
        }),
        Effect.provide(Layer.mergeAll(ServerConfig.layerTest(home, home), Net.layer)),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make((request) =>
            Effect.succeed(
              HttpClientResponse.fromWeb(
                request,
                Response.json({ owner, generation: rejectHealth ? "stale" : healthGeneration }),
              ),
            ),
          ),
        ),
      );
      const first = yield* host.ensureReady(() => Effect.void);
      expect(first.directMedia).toMatchObject({
        target: "laptop-mini",
        gatewayPort: 2234,
        generation: generations.at(-1),
      });
      expect(new Set(generations).size).toBe(2);
      expect(modes).toContain("stop-direct");
      expect(forwards).toBe(1);
      for (const args of commands) {
        const reverse = args.indexOf("-R");
        expect(args[reverse + 1]).toMatch(/^127\.0\.0\.1:2235:127\.0\.0\.1:\d+$/);
        expect(args[reverse + 1]).not.toBe(`127.0.0.1:2235:127.0.0.1:${server.port}`);
        expect(args).toContain("ControlMaster=no");
        expect(args).toContain("ControlPath=none");
        expect(args).toContain("ControlPersist=no");
        expect(args).not.toContain("ClearAllForwardings=yes");
        expect(args.some((arg) => arg.endsWith(":127.0.0.1:2234"))).toBe(true);
        expect(args).not.toContain("laptop-mini");
        expect(args.at(-1)).toBe("vps-mini");
      }
      yield* host.stop;
      expect(forwards).toBe(0);
      expect(modes.at(-1)).toBe("stop");
      const next = yield* host.ensureReady(() => Effect.void);
      expect(next.directMedia?.generation).not.toBe(first.directMedia?.generation);
      yield* host.stop;
      rejectHealth = true;
      const fallback = yield* host.ensureReady(() => Effect.void);
      expect(fallback.directMedia).toBeUndefined();
      expect(forwards).toBe(1);
      expect(commands.at(-1)).not.toContain("-R");
      const fallbackAgent = yield* host.ensureAgentReady(() => Effect.void);
      expect(fallbackAgent.directMedia).toBeUndefined();
      expect(fallbackAgent.agentDevice).toMatchObject({
        token: "fixture",
        entryPath: "/agent.mjs",
      });
      expect(commands.at(-1)).not.toContain("-R");
      expect(commands.at(-1)?.some((arg) => arg.endsWith(":127.0.0.1:1235"))).toBe(true);
      yield* host.stop;
      rejectHealth = false;
      rejectDescriptor = true;
      const noDescriptor = yield* host.ensureReady(() => Effect.void);
      expect(noDescriptor.directMedia).toBeUndefined();
      expect(forwards).toBe(1);
      yield* host.stop;
      rejectDescriptor = false;
      failBootstrap = true;
      const bootstrapFallback = yield* host.ensureReady(() => Effect.void);
      expect(bootstrapFallback.directMedia).toBeUndefined();
      expect(forwards).toBe(1);
      yield* host.stop;
      failBootstrap = false;
      rejectDescriptor = true;
      failCleanup = true;
      const unknown = yield* host.ensureReady(() => Effect.void).pipe(Effect.result);
      expect(unknown._tag).toBe("Failure");
      const modesBeforeRetry = modes.length;
      expect((yield* host.ensureReady(() => Effect.void).pipe(Effect.result))._tag).toBe("Failure");
      expect(modes).toHaveLength(modesBeforeRetry + 1); // readiness probe only; no direct bootstrap retry
      expect(forwards).toBe(0);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
