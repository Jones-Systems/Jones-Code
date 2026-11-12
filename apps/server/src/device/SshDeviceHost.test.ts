import * as Redacted from "effect/Redacted";
import { expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Net from "@t3tools/shared/Net";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as PlatformError from "effect/PlatformError";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as TestClock from "effect/testing/TestClock";
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
    const tunnelArgs: string[][] = [];
    const tunnelStdins: unknown[] = [];
    const commandArgs: string[][] = [];
    let forwards = 0;
    let failForward = true;
    let rejectConfig = true;
    const releases: Array<string | undefined> = [];
    const spawner = ChildProcessSpawner.make((command) =>
      Effect.gen(function* () {
        if (command._tag !== "StandardCommand") return yield* Effect.die("Unexpected command");
        const forwarding = command.args.includes("-L");
        (forwarding ? tunnelArgs : commandArgs).push(command.args.map(String));
        if (forwarding) tunnelStdins.push(command.options.stdin);
        let output = "";
        let exitCode = 0;
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
          if (mode === "") {
            output = `${"verbose-prefix ".repeat(400)}PANIC: emulator SDK is broken\n`;
            exitCode = 1;
          }
        }
        return ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(123),
          stdout: Stream.make(new TextEncoder().encode(output)),
          stderr: Stream.empty,
          all: Stream.empty,
          exitCode: forwarding
            ? Effect.never
            : Effect.succeed(ChildProcessSpawner.ExitCode(exitCode)),
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
      { kind: "ssh", id: "test", label: "Test", target: "test.example" },
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
          Effect.sync(() => {
            if (!request.url.endsWith("/rpc"))
              return HttpClientResponse.fromWeb(request, new Response("ok"));
            releases.push(request.headers.authorization);
            return HttpClientResponse.fromWeb(request, Response.json({ result: { ok: true } }));
          }),
        ),
      ),
    );
    yield* host.ensureReady(() => Effect.void);
    yield* SshDeviceHost.probe({
      kind: "ssh",
      id: "test",
      label: "Test",
      target: "test.example",
    }).pipe(
      Effect.provide(ServerConfig.layerTest(home, home)),
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    );
    expect(new Set(owners).size).toBe(1);
    expect(owners[0]).toMatch(/^[a-f0-9]{24}$/);
    expect(forwards).toBe(1);
    expect(modes.filter((mode) => mode === "start")).toHaveLength(2);
    const ready = yield* host.current;
    expect(ready).not.toBeNull();
    if (ready) {
      const failed = yield* ready.run("emulator", ["-list-avds"]);
      expect(failed.code).toBe(1);
      expect(failed.stdout).toMatch(/^\[truncated\]/);
      expect(failed.stderr).toBe(failed.stdout.trim());
      expect(failed.stderr).toMatch(/PANIC: emulator SDK is broken$/);
    }
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
    // Close releases agent sessions through the forwarded daemon.
    const agentReady = yield* host.current;
    yield* agentReady!.releaseAgentSession!("t3-session", "device");
    expect(releases).toEqual(["Bearer fixture"]);
    yield* host.stopAgent;
    expect(forwards).toBe(1);
    const multiplexingOff = ["ControlMaster=no", "ControlPath=none", "ControlPersist=no"];
    expect(tunnelArgs.length).toBeGreaterThan(0);
    for (const args of tunnelArgs) {
      for (const option of multiplexingOff) {
        const index = args.indexOf(option);
        expect(index).toBeGreaterThan(0);
        expect(args[index - 1]).toBe("-o");
      }
      // The tunnel lives only as long as the server holds the write end of its stdin.
      expect(args).not.toContain("-N");
      expect(args.slice(args.indexOf("test.example") + 1)).toEqual([
        "sh",
        "-c",
        "'exec cat >/dev/null'",
      ]);
    }
    expect(tunnelStdins).toEqual(tunnelArgs.map(() => "pipe"));
    expect(commandArgs.length).toBeGreaterThan(0);
    for (const args of commandArgs) {
      for (const option of multiplexingOff) expect(args).not.toContain(option);
    }
    yield* host.stop;
    expect(forwards).toBe(0);
  }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);

it.effect("lists Android devices without probing the host again once it reports Android", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const home = yield* fs.makeTempDirectoryScoped();
    const modes: string[] = [];
    let androidInstalled = false;
    const spawner = ChildProcessSpawner.make((command) =>
      Effect.gen(function* () {
        if (command._tag !== "StandardCommand") return yield* Effect.die("Unexpected command");
        const forwarding = command.args.includes("-L");
        let output = "";
        if (!forwarding) {
          const stdin = command.options.stdin;
          const script =
            stdin && typeof stdin === "object" && "stream" in stdin && Stream.isStream(stdin.stream)
              ? yield* stdin.stream.pipe(
                  Stream.decodeText(),
                  Stream.runFold(
                    () => "",
                    (a, b) => a + b,
                  ),
                )
              : "";
          const mode = /const mode = "([^"]+)"/.exec(script)?.[1] ?? "";
          modes.push(mode);
          output =
            mode === ""
              ? "Pixel_8\n"
              : JSON.stringify({
                  nodePath: "/node",
                  platforms: [
                    { platform: "ios", available: true },
                    { platform: "android", available: androidInstalled },
                  ],
                  hubPort: 1234,
                  helpers: { serveSimAxSettings: null, serveSimCli: null },
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
    const host = yield* SshDeviceHost.make({
      kind: "ssh",
      id: "test",
      label: "Test",
      target: "test.example",
    }).pipe(
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
              new Response(
                request.url.endsWith("/api/devices")
                  ? JSON.stringify({ simulators: [], emulators: [] })
                  : "ok",
              ),
            ),
          ),
        ),
      ),
    );
    const ready = yield* host.ensureReady(() => Effect.void);
    const androidNames = ready.devices.list.pipe(
      Effect.map((list) =>
        list.devices.filter((device) => device.platform === "android").map((device) => device.name),
      ),
    );

    modes.splice(0);

    expect(yield* androidNames).toEqual([]);
    expect(modes.splice(0)).toEqual(["probe"]);
    expect(yield* androidNames).toEqual([]);
    expect(modes.splice(0)).toEqual(["probe"]);

    // The SDK appears after connecting, so the next listing still probes the host.
    androidInstalled = true;
    expect(yield* androidNames).toEqual(["Pixel_8"]);
    expect(modes.splice(0)).toEqual(["probe", ""]);

    // Android is now known to be available, so listing only runs `emulator -list-avds`.
    expect(yield* androidNames).toEqual(["Pixel_8"]);
    expect(modes.splice(0)).toEqual([""]);

    androidInstalled = false;
    expect((yield* host.platformAvailability("android")).available).toBe(false);
    expect(modes.splice(0)).toEqual(["probe"]);
    expect(yield* androidNames).toEqual([]);
    expect(modes.splice(0)).toEqual(["probe"]);

    androidInstalled = true;
    if (host.inspect === undefined) return yield* Effect.die("SSH fixture must support inspect");
    yield* host.inspect;
    expect(modes.splice(0)).toEqual(["probe"]);
    expect(yield* androidNames).toEqual(["Pixel_8"]);
    expect(modes.splice(0)).toEqual([""]);
    yield* host.stop;
  }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
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
          const forwarding = command.args.includes("-L");
          let output = "";
          let bootstrapExit = 0;
          if (forwarding) {
            expect(command.options.stdin).toBe("pipe");
            commands.push(
              command.args.map((word) => (Redacted.isRedacted(word) ? Redacted.value(word) : word)),
            );
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
        {
          kind: "ssh",
          id: "test",
          label: "Test",
          target: "vps-mini",
          directSshTarget: "laptop-mini",
        },
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
        expect(args).toContain("-T");
        expect(args).not.toContain("-N");
        expect(args.slice(args.indexOf("vps-mini") + 1)).toEqual([
          "sh",
          "-c",
          "'exec cat >/dev/null'",
        ]);
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
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);

it.effect(
  "keeps the SSH forward through slow health probes and reconnects once the hub stays down",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const home = yield* fs.makeTempDirectoryScoped();
      let forwardSpawns = 0;
      let hubAnswers = true;
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.gen(function* () {
          if (command._tag !== "StandardCommand") return yield* Effect.die("Unexpected command");
          const forwarding = command.args.includes("-L");
          // A fresh forward reaches a working hub again.
          if (forwarding && ++forwardSpawns > 1) hubAnswers = true;
          const output = JSON.stringify({
            nodePath: "/node",
            platforms: [{ platform: "ios", available: true }],
            hubPort: 1234,
            helpers: { serveSimAxSettings: null, serveSimCli: null },
          });
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
      const host = yield* SshDeviceHost.make({
        kind: "ssh",
        id: "test",
        label: "Test",
        target: "test.example",
      }).pipe(
        Effect.provideService(DeviceDirectGrants, {
          issue: () => Effect.succeed(null),
          admit: () => Effect.succeed({ _tag: "Denied" as const }),
        }),
        Effect.provide(Layer.mergeAll(ServerConfig.layerTest(home, home), Net.layer)),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make((request) =>
            hubAnswers
              ? Effect.succeed(HttpClientResponse.fromWeb(request, new Response("ok")))
              : Effect.never,
          ),
        ),
      );
      yield* host.ensureReady(() => Effect.void);
      expect(forwardSpawns).toBe(1);

      // A loaded host can miss a probe or two without being gone; the forward stays up.
      hubAnswers = false;
      yield* TestClock.adjust("30 seconds");
      hubAnswers = true;
      yield* TestClock.adjust("2 minutes");
      expect(forwardSpawns).toBe(1);

      // A hub that keeps missing probes is still replaced.
      hubAnswers = false;
      // Recovery reset the miss count, so two new misses do not reconnect yet.
      yield* TestClock.adjust("30 seconds");
      expect(forwardSpawns).toBe(1);
      yield* TestClock.adjust("30 seconds");
      expect(forwardSpawns).toBe(2);
      yield* host.stop;
    }).pipe(
      Effect.scoped,
      Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer, TestClock.layer())),
    ),
);

it.effect(
  "counts a missed direct gateway probe with the others and reconnects only after consecutive misses",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const home = yield* fs.makeTempDirectoryScoped();
      let forwardSpawns = 0;
      let owner = "";
      let generation = "";
      let gatewayAnswers = true;
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.gen(function* () {
          if (command._tag !== "StandardCommand") return yield* Effect.die("Unexpected command");
          const forwarding = command.args.includes("-L");
          let output = "";
          if (forwarding) {
            // A fresh forward reaches a healthy gateway again.
            if (++forwardSpawns > 1) gatewayAnswers = true;
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
            const direct = /const direct = (.+);/.exec(script)?.[1];
            const bootGeneration =
              direct && direct !== "null" ? /"generation":"([^"]+)"/.exec(direct)?.[1] : undefined;
            owner = /const owner = "([^"]+)"/.exec(script)?.[1] ?? owner;
            if (bootGeneration) generation = bootGeneration;
            output = JSON.stringify({
              nodePath: "/node",
              platforms: [{ platform: "ios", available: true }],
              hubPort: 1234,
              helpers: { serveSimAxSettings: null, serveSimCli: null },
              ...(bootGeneration
                ? {
                    directMedia: {
                      gatewayPort: 2234,
                      admissionPort: 2235,
                      generation: bootGeneration,
                    },
                  }
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
        {
          kind: "ssh",
          id: "test",
          label: "Test",
          target: "vps-mini",
          directSshTarget: "laptop-mini",
        },
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
                Response.json({ owner, generation: gatewayAnswers ? generation : "stale" }),
              ),
            ),
          ),
        ),
      );
      const ready = yield* host.ensureReady(() => Effect.void);
      expect(ready.directMedia).toBeDefined();
      expect(forwardSpawns).toBe(1);

      // The hub answers throughout, so only the gateway probe can fail here.
      gatewayAnswers = false;
      yield* TestClock.adjust("20 seconds");
      expect(forwardSpawns).toBe(1);
      gatewayAnswers = true;
      yield* TestClock.adjust("10 seconds");
      // Recovery reset the miss count, so two new misses do not reconnect yet.
      gatewayAnswers = false;
      yield* TestClock.adjust("20 seconds");
      expect(forwardSpawns).toBe(1);
      // The third consecutive miss retires the forward, and the reconnect follows after its delay.
      yield* TestClock.adjust("10 seconds");
      yield* TestClock.adjust("2 seconds");
      expect(forwardSpawns).toBe(2);
      yield* host.stop;
    }).pipe(
      Effect.scoped,
      Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer, TestClock.layer())),
    ),
);

it.effect.each([
  {
    name: "a Windows PowerShell login shell",
    exitCode: 1,
    stderr: "sh : The term 'sh' is not recognized as the name of a cmdlet\r\n",
    shellEcho: "%OS%\r\nWindows_NT\r\n",
    nodePlatform: null,
    windows: true,
  },
  {
    name: "a Windows cmd login shell",
    exitCode: 1,
    stderr: "'sh' is not recognized as an internal or external command,\r\n",
    shellEcho: "Windows_NT $env:OS\r\n",
    nodePlatform: null,
    windows: true,
  },
  {
    name: "a Windows cmd login shell returning 9009",
    exitCode: 9009,
    stderr: "'sh' is not recognized as an internal or external command,\r\n",
    shellEcho: "Windows_NT $env:OS\r\n",
    nodePlatform: null,
    windows: true,
  },
  {
    name: "a Windows Git Bash login shell without a spawnable npm",
    exitCode: 1,
    stderr: "npm is missing from the non-interactive SSH PATH\n",
    shellEcho: "%OS% :OS\n",
    nodePlatform: "win32",
    windows: true,
  },
  {
    name: "a Linux host without npm",
    exitCode: 1,
    stderr: "npm is missing from the non-interactive SSH PATH\n",
    shellEcho: "%OS% :OS\n",
    nodePlatform: "linux",
    windows: false,
  },
  {
    name: "a POSIX host without Node",
    exitCode: 1,
    stderr: "Node is missing from the non-interactive SSH PATH\n",
    shellEcho: "%OS% :OS\n",
    nodePlatform: null,
    windows: false,
  },
  {
    name: "a connection failure",
    exitCode: 255,
    stderr: "Connection refused\n",
    shellEcho: "Windows_NT\n",
    nodePlatform: "win32",
    windows: false,
  },
])(
  "probe failure on $name keeps the SSH cause",
  ({ exitCode, stderr, shellEcho, nodePlatform, windows }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const home = yield* fs.makeTempDirectoryScoped();
      const shellChecks: string[] = [];
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.gen(function* () {
          if (command._tag !== "StandardCommand") return yield* Effect.die("Unexpected command");
          const remote = command.args.slice(command.args.indexOf("test.example") + 1).join(" ");
          const shellCheck = remote === "echo %OS% $env:OS";
          const platformCheck = remote === "node -p process.platform";
          if (shellCheck || platformCheck) shellChecks.push(remote);
          return ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(123),
            stdout: Stream.make(
              new TextEncoder().encode(
                shellCheck ? shellEcho : platformCheck ? `${nodePlatform ?? ""}\n` : "",
              ),
            ),
            stderr: Stream.make(
              new TextEncoder().encode(shellCheck || platformCheck ? "" : stderr),
            ),
            all: Stream.empty,
            exitCode: Effect.succeed(
              ChildProcessSpawner.ExitCode(
                shellCheck ? 0 : platformCheck ? (nodePlatform === null ? 1 : 0) : exitCode,
              ),
            ),
            isRunning: Effect.succeed(false),
            kill: () => Effect.void,
            stdin: Sink.drain,
            getInputFd: () => Sink.drain,
            getOutputFd: () => Stream.empty,
            unref: Effect.succeed(Effect.void),
          });
        }),
      );
      const failure = yield* SshDeviceHost.probe({
        kind: "ssh",
        id: "test",
        label: "Test",
        target: "test.example",
      }).pipe(
        Effect.provide(ServerConfig.layerTest(home, home)),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.flip,
      );
      expect(failure).toMatchObject({ _tag: "DeviceHostError", step: "probe" });
      const sshFailure = { _tag: "SshCommandError", exitCode, stderr };
      expect(failure.cause).toMatchObject(
        windows ? { _tag: "SshWindowsHostError", cause: sshFailure } : sshFailure,
      );
      // A transport failure has no remote shell to ask.
      if (exitCode === 255) expect(shellChecks).toEqual([]);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);
