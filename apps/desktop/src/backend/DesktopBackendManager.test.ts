import {
  SERVER_EXIT_CODE_STATE_DIR_OWNED,
  DesktopBackendBootstrap,
  type DesktopBackendBootstrap as DesktopBackendBootstrapValue,
  DesktopTelemetryControlMessage,
} from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Creating layer owns the compiled custody fixture and its scratch tree.
import * as NodeFSP from "node:fs/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Fixture source and output paths are fixed beneath its owned root.
import * as NodePath from "node:path";
// The fixture uses the selected supported test interpreter.
import * as NodeProcess from "node:process";
// Resolves only the two canonical modules compiled by this fixture.
import * as NodeURL from "node:url";
import { build } from "vite-plus/pack";
// @ts-expect-error -- Existing owned native fixture helper has no declarations.
import * as IdentityFixture from "../../../../scripts/jones/updates/native-fixture-support.mjs";

import * as DesktopBackendManager from "./DesktopBackendManager.ts";
import type { JonesColdBackendPermit } from "../jones/updates/jonesColdBackendCustody.ts";
import * as DesktopApp from "../app/DesktopApp.ts";
import * as DesktopBackendPool from "./DesktopBackendPool.ts";
import * as DesktopObservability from "../app/DesktopObservability.ts";
import * as DesktopTelemetryPublisher from "../telemetry/DesktopTelemetryPublisher.ts";
import * as DesktopBrowserHost from "../preview/DesktopBrowserHost.ts";
import * as DesktopWslEnvironment from "../wsl/DesktopWslEnvironment.ts";

const decodeDesktopBackendBootstrap = Schema.decodeEffect(
  Schema.fromJsonString(DesktopBackendBootstrap),
);
const isBackendProcessError = Schema.is(DesktopBackendManager.BackendProcessError);
const encodeDesktopTelemetryControl = Schema.encodeSync(
  Schema.fromJsonString(DesktopTelemetryControlMessage),
);

const baseConfig: DesktopBackendManager.DesktopBackendStartConfig = {
  executablePath: "/electron",
  args: ["/server/bin.mjs", "--bootstrap-fd", "3"],
  entryPath: "/server/bin.mjs",
  cwd: "/server",
  env: { ELECTRON_RUN_AS_NODE: "1" },
  bootstrap: {
    mode: "desktop",
    noBrowser: true,
    port: 3773,
    t3Home: "/tmp/t3",
    host: "127.0.0.1",
    desktopBootstrapToken: "token",
    tailscaleServeEnabled: false,
    tailscaleServePort: 443,
    desktopTelemetryFd: 4,
    desktopTelemetryControlFd: 5,
  },
  bootstrapDelivery: "fd3",
  extendEnv: true,
  httpBaseUrl: new URL("http://127.0.0.1:3773"),
  captureOutput: true,
  preflightFailure: Option.none(),
};

const configWithObservability: DesktopBackendBootstrapValue = {
  ...baseConfig.bootstrap,
  tailscaleServeEnabled: true,
  desktopTelemetryFd: 4,
  otlpTracesUrl: "http://127.0.0.1:4318/v1/traces",
};

function makeProcess(options?: {
  readonly stdout?: Stream.Stream<Uint8Array, PlatformError.PlatformError>;
  readonly stderr?: Stream.Stream<Uint8Array, PlatformError.PlatformError>;
  readonly exitCode?: Effect.Effect<ChildProcessSpawner.ExitCode, PlatformError.PlatformError>;
  readonly kill?: ChildProcessSpawner.ChildProcessHandle["kill"];
  readonly getOutputFd?: ChildProcessSpawner.ChildProcessHandle["getOutputFd"];
}): ChildProcessSpawner.ChildProcessHandle {
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(123),
    stdout: options?.stdout ?? Stream.empty,
    stderr: options?.stderr ?? Stream.empty,
    all: Stream.merge(options?.stdout ?? Stream.empty, options?.stderr ?? Stream.empty),
    exitCode: options?.exitCode ?? Effect.succeed(ChildProcessSpawner.ExitCode(0)),
    isRunning: Effect.succeed(false),
    kill: options?.kill ?? (() => Effect.void),
    stdin: Sink.drain,
    getInputFd: () => Sink.drain,
    getOutputFd: options?.getOutputFd ?? (() => Stream.empty),
    unref: Effect.succeed(Effect.void),
  });
}

function responseForRequest(
  request: HttpClientRequest.HttpClientRequest,
  status: number,
): HttpClientResponse.HttpClientResponse {
  return HttpClientResponse.fromWeb(request, new Response(null, { status }));
}

function layerHttpClient(
  handler: (
    request: HttpClientRequest.HttpClientRequest,
  ) => Effect.Effect<HttpClientResponse.HttpClientResponse>,
) {
  return Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request) => handler(request)),
  );
}

const layerHealthyHttpClient = layerHttpClient((request) =>
  Effect.succeed(responseForRequest(request, 200)),
);

function decodeBootstrap(raw: string) {
  return decodeDesktopBackendBootstrap(raw);
}

interface MakeInstanceInput {
  readonly id?: DesktopBackendManager.BackendInstanceId;
  readonly spawnerLayer: Layer.Layer<ChildProcessSpawner.ChildProcessSpawner>;
  readonly httpClientLayer?: Layer.Layer<HttpClient.HttpClient>;
  readonly backendOutputLog?: Partial<DesktopObservability.DesktopBackendOutputLogShape>;
  readonly onReady?: Effect.Effect<void>;
  readonly onShutdown?: Effect.Effect<void>;
  readonly onStateDirOwned?: Effect.Effect<void>;
  readonly onPreflightFailed?: (
    failure: DesktopBackendManager.PreflightFailure,
  ) => Effect.Effect<boolean>;
  readonly config?: DesktopBackendManager.DesktopBackendStartConfig;
  readonly configResolve?: Effect.Effect<
    DesktopBackendManager.DesktopBackendStartConfig,
    PlatformError.PlatformError
  >;
  readonly desktopTelemetryStream?: Stream.Stream<Uint8Array>;
  readonly desktopTelemetryPublisher?: Partial<
    DesktopTelemetryPublisher.DesktopTelemetryPublisher["Service"]
  >;
  readonly pruneRuntimes?: (distro: string | null, runtimeId: string) => Effect.Effect<void>;
}

function makeTestInstance(input: MakeInstanceInput) {
  const stubLog: DesktopObservability.DesktopBackendOutputLogShape = {
    beginSession: () => Effect.void,
    writeOutputChunk: () => Effect.void,
    persistFailureSnapshot: () => Effect.void,
    persistFailure: () => Effect.void,
    discardSession: Effect.void,
    ...input.backendOutputLog,
  };
  const layerServices = Layer.mergeAll(
    FileSystem.layerNoop({
      exists: () => Effect.succeed(true),
    }),
    input.spawnerLayer,
    input.httpClientLayer ?? layerHealthyHttpClient,
    Layer.succeed(DesktopObservability.DesktopBackendOutputLogFactory, {
      forInstance: () => Effect.succeed(stubLog),
    } satisfies DesktopObservability.DesktopBackendOutputLogFactory["Service"]),
    Layer.succeed(DesktopTelemetryPublisher.DesktopTelemetryPublisher, {
      latest: Effect.succeedNone,
      changes: Stream.empty,
      encoded: input.desktopTelemetryStream ?? Stream.empty,
      handleControlForSource: () => Effect.void,
      removeControlSource: () => Effect.void,
      publishUpdateReport: () => Effect.void,
      updateRequests: Stream.empty,
      updateCommits: Stream.empty,
      updateCancellations: Stream.empty,
      ...input.desktopTelemetryPublisher,
    }),
    DesktopBrowserHost.layer,
    DesktopWslEnvironment.layerTest(
      input.pruneRuntimes === undefined ? {} : { pruneRuntimes: input.pruneRuntimes },
    ),
  );

  const instance = DesktopBackendManager.makeBackendInstance({
    id: input.id ?? DesktopBackendManager.PRIMARY_INSTANCE_ID,
    label: Effect.succeed("Windows"),
    configResolve: input.configResolve ?? Effect.succeed(input.config ?? baseConfig),
    ...(input.onReady ? { onReady: () => input.onReady! } : {}),
    ...(input.onShutdown ? { onShutdown: () => input.onShutdown! } : {}),
    ...(input.onStateDirOwned ? { onStateDirOwned: () => input.onStateDirOwned! } : {}),
    ...(input.onPreflightFailed ? { onPreflightFailed: input.onPreflightFailed } : {}),
  });

  return instance.pipe(Effect.provide(layerServices));
}

// The driver captures the keeper directly. A finite child transfers only fixture
// pipe descriptors; it exits before the backend proceeds, leaving no orphan helper.
const coldPipeKeeper = String.raw`
import array, json, os, socket, sys
root = sys.argv[1]
server = socket.socket(socket.AF_UNIX)
server.bind(root + '/keeper.sock')
server.listen(1)
print('ready', flush=True)
connection, _ = server.accept()
fds = array.array('i')
try:
    _, ancillary, _, _ = connection.recvmsg(1, socket.CMSG_SPACE(2 * fds.itemsize))
    for level, kind, raw in ancillary:
        assert level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS
        fds.frombytes(raw[:len(raw) - len(raw) % fds.itemsize])
    assert len(fds) == 2
    connection.sendall(b'held')
    print('held', flush=True)
    sys.stdin.buffer.read(1)
finally:
    for fd in fds: os.close(fd)
    connection.close()
    server.close()
`;

const coldAdapterChild = String.raw`
import * as fs from 'node:fs';
import {spawn} from 'node:child_process';
const sender = spawn('python3', ['-c',
 'import array,socket,sys; s=socket.socket(socket.AF_UNIX); s.connect(sys.argv[1]); s.sendmsg([b"x"],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,array.array("i",[1,8]))]); assert s.recv(4)==b"held"; s.close()',
 process.env.ROOT + '/keeper.sock'], {stdio:['ignore',1,'pipe','ignore','ignore','ignore','ignore','ignore',8]});
sender.stderr.resume();
await new Promise((resolve,reject) => {sender.on('error',reject); sender.on('close',code => code===0 ? resolve() : reject(new Error('Descriptor sender failed')));});
const binding = JSON.parse(process.env.BINDING);
if(process.env.SCENARIO === 'early-failure') binding.transferId='9'.repeat(64);
fs.writeSync(8,JSON.stringify({protocol:1,binding,sequence:1,type:'READY',identity:{pid:process.pid,parentPid:process.ppid,uniqueId:'1',parentUniqueId:'2',pidVersion:1,uid:process.getuid()}})+'\n');
setInterval(()=>{},1000);
`;

// Appended only to the canonical module in this owned build. Production exports
// and the production private permit issuer remain unchanged and closed.
const coldFixturePermit = String.raw`
export const fixturePermit = Object.freeze({});
const fixtureRoot = NodeProcess.argv[3];
const fixtureBinding = {transactionId:'1'.repeat(64),transferId:'2'.repeat(64),handoffSha256:'3'.repeat(64),sourceSha:'4'.repeat(40),sourceTree:'5'.repeat(40),artifactSha256:'6'.repeat(64),bootSessionUuid:'00000000-0000-4000-8000-000000000001'};
coldPermits.set(fixturePermit, {executable:NodeProcess.execPath,args:[fixtureRoot+'/child.mjs'],cwd:fixtureRoot,
 env:{PATH:NodeProcess.env.PATH,HOME:fixtureRoot,TMPDIR:fixtureRoot,ROOT:fixtureRoot,SCENARIO:NodeProcess.argv[2],BINDING:JSON.stringify(fixtureBinding)},binding:fixtureBinding});
`;

const coldManagerHarness = String.raw`
import * as FixtureAssert from 'node:assert/strict';
import * as FixtureProcess from 'node:child_process';
import * as FixtureFS from 'node:fs';
import * as FixtureTimers from 'node:timers/promises';
import * as FixtureLayer from 'effect/Layer';
import * as FixtureDeferred from 'effect/Deferred';
import {fixturePermit} from '../jones/updates/jonesColdBackendCustody.ts';
const root=process.argv[3], scenario=process.argv[2];
const keeper=FixtureProcess.spawn('python3',[root+'/keeper.py',root],{stdio:['pipe','pipe','pipe']});
let keeperOutput='', keeperClosed=false;
keeper.stdout.on('data',chunk=>{keeperOutput+=chunk;}); keeper.stderr.resume();
const keeperDone=new Promise((resolve,reject)=>{keeper.once('error',reject);keeper.once('close',()=>{keeperClosed=true;resolve();});});
let instance, retained, program, cancellation=false, cleanupRunning;
const until=async(test)=>{const deadline=Date.now()+10000;while(!test()){if(Date.now()>deadline)throw new Error('Fixture deadline');await FixtureTimers.setTimeout(5);}};
const cleanup=()=>cleanupRunning ??= (async()=>{
 retained ??= retainedColdRuns.get(PRIMARY_INSTANCE_ID);
 keeper.stdin.end('x');
 await Promise.race([keeperDone,FixtureTimers.setTimeout(1000)]);
 if(!keeperClosed){keeper.kill('SIGKILL');await keeperDone;}
 if(retained?.owner){await retained.stop();await until(()=>retained.owner.observe().settled);retained.scopeFinalized=true;retained.reconcile();}
 FixtureFS.writeFileSync(root+'/settled.json',JSON.stringify({settled:keeperClosed && (!retained?.owner || retained.result?.settled===true)}));
})();
const cancel=()=>{if(cancellation)return;cancellation=true;void (async()=>{if(program)await Effect.runPromise(Fiber.interrupt(program));await cleanup();})().finally(()=>{process.exitCode=143;});};
process.on('SIGTERM',cancel);process.on('SIGINT',cancel);
try {
 await until(()=>keeperOutput.includes('ready'));
 if(cancellation)throw new Error('Cancelled before backend launch');
 let resolves=0, warmSpawns=0;
 const config={executablePath:process.execPath,args:[],entryPath:root+'/child.mjs',cwd:root,env:{},
  bootstrap:{mode:'desktop',noBrowser:true,port:3773,t3Home:root,host:'127.0.0.1',desktopBootstrapToken:'fixture',tailscaleServeEnabled:false,tailscaleServePort:443,desktopTelemetryFd:4,desktopTelemetryControlFd:5,desktopBrowserFd:6,desktopBrowserControlFd:7},
  bootstrapDelivery:'fd3',extendEnv:false,httpBaseUrl:new URL('http://127.0.0.1:3773'),captureOutput:true,preflightFailure:Option.none(),jonesColdBackendPermit:fixturePermit};
 const spec={id:PRIMARY_INSTANCE_ID,label:Effect.succeed('fixture'),configResolve:Effect.sync(()=>{resolves++;return config;})};
 const log={beginSession:()=>Effect.void,writeOutputChunk:()=>Effect.void,persistFailureSnapshot:()=>Effect.void,persistFailure:()=>Effect.void,discardSession:Effect.void};
 const services=FixtureLayer.mergeAll(
  FileSystem.layerNoop({exists:()=>Effect.succeed(true)}),
  FixtureLayer.succeed(ChildProcessSpawner.ChildProcessSpawner,ChildProcessSpawner.make(()=>Effect.sync(()=>{warmSpawns++;throw new Error('Unexpected warm spawn');}))),
  FixtureLayer.succeed(HttpClient.HttpClient,HttpClient.make(()=>Effect.never)),
  FixtureLayer.succeed(DesktopObservability.DesktopBackendOutputLogFactory,{forInstance:()=>Effect.succeed(log)}),
  FixtureLayer.succeed(DesktopTelemetryPublisher.DesktopTelemetryPublisher,{latest:Effect.succeedNone,changes:Stream.empty,encoded:Stream.empty,handleControlForSource:()=>Effect.void,removeControlSource:()=>Effect.void,publishUpdateReport:()=>Effect.void,updateRequests:Stream.empty,updateCommits:Stream.empty,updateCancellations:Stream.empty}),
  DesktopBrowserHost.layer,DesktopWslEnvironment.layerTest({}));
 program=Effect.runFork(Effect.scoped(Effect.gen(function*(){
  const racing=scenario==='race-cold' || scenario==='race-warm';
  const entered=yield* FixtureDeferred.make(), resume=yield* FixtureDeferred.make();
  let contenderStart;
  if(racing){
   const contender=yield* makeBackendInstance({...spec,configResolve:Effect.gen(function*(){
    resolves++;yield* FixtureDeferred.succeed(entered,undefined);yield* FixtureDeferred.await(resume);
    return scenario==='race-warm' ? {...config,jonesColdBackendPermit:undefined} : config;
   })});
   contenderStart=yield* contender.start.pipe(Effect.forkChild);
   yield* FixtureDeferred.await(entered);
  }
  instance=yield* makeBackendInstance(spec);yield* instance.start;
  yield* Effect.promise(()=>until(()=>keeperOutput.includes('held')));
  retained=retainedColdRuns.get(PRIMARY_INSTANCE_ID);FixtureAssert.ok(retained?.owner);
  if(scenario!=='early-failure')yield* instance.stop();
  yield* Effect.promise(()=>until(()=>retained.scopeFinalized));
  const snapshot=yield* instance.snapshot;
  FixtureAssert.equal(snapshot.coldCustody.result.settled,false);
  FixtureAssert.equal(snapshot.coldCustody.result.exitObserved,true);
  FixtureAssert.equal(snapshot.coldCustody.result.pipesClosed,false);
  FixtureAssert.equal(snapshot.coldCustody.scopeFinalized,true);
  FixtureAssert.equal(retainedColdRuns.get(PRIMARY_INSTANCE_ID),retained);
  if(racing){
   const owner=retained.owner;
   yield* FixtureDeferred.succeed(resume,undefined);yield* Fiber.join(contenderStart);
   FixtureAssert.equal(retainedColdRuns.get(PRIMARY_INSTANCE_ID),retained);
   FixtureAssert.equal(backendRunAdmissions.get(PRIMARY_INSTANCE_ID),retained);
   FixtureAssert.equal(retained.owner,owner);FixtureAssert.equal(retained.result.settled,false);
  }
  yield* instance.start;
  const replacement=yield* makeBackendInstance(spec);yield* replacement.start;
  FixtureAssert.equal(resolves,racing ? 2 : 1);FixtureAssert.equal(warmSpawns,0);
  yield* instance.stop();
  FixtureAssert.equal(retainedColdRuns.get(PRIMARY_INSTANCE_ID),retained);
  const stops=retained.result.events.filter(e=>e==='STOP_REQUEST').length;
  yield* Effect.promise(cleanup);
  yield* instance.stop();
  FixtureAssert.equal(retained.result.events.filter(e=>e==='STOP_REQUEST').length,stops+1);
  FixtureAssert.equal(retainedColdRuns.has(PRIMARY_INSTANCE_ID),false);
  FixtureAssert.equal(backendRunAdmissions.has(PRIMARY_INSTANCE_ID),false);
  FixtureAssert.equal(retained.result.settled,true);
  FixtureAssert.equal(retained.result.activationAllowed,false);
  FixtureAssert.equal(Option.isNone((yield* instance.snapshot).activePid),true);
 }).pipe(Effect.provide(services))));
 await Effect.runPromise(Fiber.join(program));
} finally {await cleanup();}
`;

describe("DesktopBackendManager", () => {
  it.for(["early-failure", "cancel", "race-cold", "race-warm"] as const)(
    "retains actual cold custody after %s while captured pipes remain open",
    async (scenario, { signal }) => {
      await IdentityFixture.withNativeFixtureScratch(async (root: string) => {
        const managerPath = NodeURL.fileURLToPath(import.meta.url).replace(/\.test\.ts$/, ".ts");
        const custodyPath = NodePath.resolve(
          NodePath.dirname(managerPath),
          "../jones/updates/jonesColdBackendCustody.ts",
        );
        const marker = NodePath.join(root, "settled.json");
        await NodeFSP.writeFile(marker, JSON.stringify({ settled: false }));
        await NodeFSP.writeFile(NodePath.join(root, "keeper.py"), coldPipeKeeper);
        await NodeFSP.writeFile(NodePath.join(root, "child.mjs"), coldAdapterChild);
        const sources = new Map([
          [managerPath, (await NodeFSP.readFile(managerPath, "utf8")) + coldManagerHarness],
          [custodyPath, (await NodeFSP.readFile(custodyPath, "utf8")) + coldFixturePermit],
        ]);
        let launched = false;
        try {
          await build({
            config: false,
            cwd: root,
            entry: { fixture: managerPath },
            outDir: root,
            clean: false,
            format: "esm",
            dts: false,
            sourcemap: false,
            tsconfig: false,
            outExtensions: () => ({ js: ".mjs" }),
            outputOptions: { codeSplitting: false },
            deps: { alwaysBundle: (id) => !id.startsWith("node:"), onlyBundle: false },
            plugins: [
              {
                name: "owned-lexical-cold-custody-fixture",
                resolveId(id) {
                  if (id === "electron") return "\0cold-fixture-electron.cjs";
                },
                load(id) {
                  if (id === "\0cold-fixture-electron.cjs") return "module.exports = {};";
                  return sources.get(id);
                },
              },
            ],
            logLevel: "silent",
          });
          launched = true;
          const result = await IdentityFixture.runNativeFixtureCommand(
            NodeProcess.execPath,
            [NodePath.join(root, "fixture.mjs"), scenario, root],
            root,
            { signal },
          );
          assert.equal(result.code, 0, result.stderr);
          assert.equal(JSON.parse(await NodeFSP.readFile(marker, "utf8")).settled, true);
        } catch (cause) {
          const settled = await NodeFSP.readFile(marker, "utf8")
            .then((raw) => JSON.parse(raw).settled)
            .catch(() => false);
          if (launched && !settled) Object.assign(cause as object, { preserveScratch: true });
          throw cause;
        }
      });
    },
  );
  it.effect("refuses forged cold custody without spawning or scheduling a warm restart", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const finalized = yield* Deferred.make<void>();
        let starts = 0;
        const instance = yield* makeTestInstance({
          config: {
            ...baseConfig,
            bootstrap: { ...baseConfig.bootstrap, desktopBrowserFd: 6, desktopBrowserControlFd: 7 },
            jonesColdBackendPermit: {} as JonesColdBackendPermit,
          },
          spawnerLayer: Layer.succeed(
            ChildProcessSpawner.ChildProcessSpawner,
            ChildProcessSpawner.make(() =>
              Effect.sync(() => {
                starts++;
                return makeProcess();
              }),
            ),
          ),
          desktopTelemetryPublisher: {
            removeControlSource: () => Deferred.succeed(finalized, undefined).pipe(Effect.asVoid),
          },
        });
        yield* instance.start;
        yield* Deferred.await(finalized);
        yield* TestClock.adjust(Duration.seconds(1));
        const snapshot = yield* instance.snapshot;
        assert.equal(starts, 0);
        assert.isFalse(snapshot.desiredRunning);
        assert.isFalse(snapshot.restartScheduled);
        assert.isFalse(snapshot.ready);
      }).pipe(Effect.provide(TestClock.layer())),
    ),
  );
  it.effect("spawns the backend with fd3 bootstrap and fd4 telemetry", () =>
    Effect.scoped(
      Effect.gen(function* () {
        let spawnedCommand: ChildProcess.Command | undefined;
        let bootstrapJson = "";
        let telemetryJson = "";
        let readyCount = 0;
        const ready = yield* Deferred.make<void>();
        const exited = yield* Queue.unbounded<void>();

        const layerSpawner = Layer.succeed(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make((command) =>
            Effect.gen(function* () {
              spawnedCommand = command;
              if (command._tag === "StandardCommand") {
                const fd3 = command.options.additionalFds?.fd3;
                if (fd3?.type === "input" && fd3.stream) {
                  bootstrapJson = yield* fd3.stream.pipe(Stream.decodeText(), Stream.mkString);
                }
                const fd4 = command.options.additionalFds?.fd4;
                if (fd4?.type === "input" && fd4.stream) {
                  telemetryJson = yield* fd4.stream.pipe(Stream.decodeText(), Stream.mkString);
                }
              }

              return makeProcess({
                exitCode: Deferred.await(ready).pipe(Effect.as(ChildProcessSpawner.ExitCode(0))),
              });
            }),
          ),
        );

        const instance = yield* makeTestInstance({
          config: {
            ...baseConfig,
            bootstrap: configWithObservability,
          },
          spawnerLayer: layerSpawner,
          desktopTelemetryStream: Stream.encodeText(
            Stream.make('{"version":1,"type":"desktopTelemetryHello","electronPid":123}\n'),
          ),
          onReady: Effect.sync(() => {
            readyCount += 1;
          }).pipe(Effect.andThen(Deferred.succeed(ready, void 0)), Effect.asVoid),
          backendOutputLog: {
            persistFailure: () => Queue.offer(exited, void 0).pipe(Effect.asVoid),
          },
        });

        yield* instance.start;
        yield* Queue.take(exited);

        assert.equal(readyCount, 1);
        assert.isDefined(spawnedCommand);
        if (spawnedCommand._tag !== "StandardCommand") {
          throw new Error("Expected backend to spawn a standard command.");
        }

        assert.equal(spawnedCommand.command, "/electron");
        assert.deepEqual(spawnedCommand.args, ["/server/bin.mjs", "--bootstrap-fd", "3"]);
        assert.equal(spawnedCommand.options.cwd, "/server");
        assert.equal(spawnedCommand.options.extendEnv, true);
        assert.equal(spawnedCommand.options.stdout, "pipe");
        assert.equal(spawnedCommand.options.stderr, "pipe");
        assert.equal(spawnedCommand.options.killSignal, "SIGTERM");
        assert.isDefined(spawnedCommand.options.forceKillAfter);
        assert.equal(spawnedCommand.options.additionalFds?.fd4?.type, "input");
        assert.equal(spawnedCommand.options.additionalFds?.fd5?.type, "output");
        assert.equal(
          Duration.toMillis(Duration.fromInputUnsafe(spawnedCommand.options.forceKillAfter)),
          5_000,
        );

        assert.deepEqual(yield* decodeBootstrap(bootstrapJson), configWithObservability);
        assert.equal(
          telemetryJson,
          '{"version":1,"type":"desktopTelemetryHello","electronPid":123}\n',
        );
      }),
    ),
  );

  it.effect("preserves the readiness timeout cause and process context", () =>
    Effect.gen(function* () {
      const requested = yield* Deferred.make<HttpClientRequest.HttpClientRequest>();
      const layer = Layer.merge(
        TestClock.layer(),
        layerHttpClient((request) =>
          Deferred.succeed(requested, request).pipe(Effect.andThen(Effect.never)),
        ),
      );

      yield* Effect.gen(function* () {
        const readiness = yield* DesktopBackendManager.waitForHttpReady({
          executablePath: baseConfig.executablePath,
          entryPath: baseConfig.entryPath,
          cwd: baseConfig.cwd,
          httpBaseUrl: baseConfig.httpBaseUrl,
          timeout: Duration.millis(50),
        }).pipe(Effect.flip, Effect.forkChild);

        const request = yield* Deferred.await(requested);
        assert.equal(request.url, "http://127.0.0.1:3773/.well-known/t3/environment");

        yield* TestClock.adjust(Duration.millis(50));
        const error = yield* Fiber.join(readiness);

        assert.instanceOf(error, DesktopBackendManager.BackendReadinessTimeoutError);
        assert.equal(error.executablePath, "/electron");
        assert.equal(error.entryPath, "/server/bin.mjs");
        assert.equal(error.cwd, "/server");
        assert.equal(error.httpBaseUrl.href, "http://127.0.0.1:3773/");
        assert.equal(error.readinessUrl.href, "http://127.0.0.1:3773/.well-known/t3/environment");
        assert.equal(error.timeoutMs, 50);
        assert.isDefined(error.cause);
        assert.equal(
          error.message,
          "Timed out after 50ms waiting for desktop backend readiness at http://127.0.0.1:3773/.well-known/t3/environment.",
        );
      }).pipe(Effect.provide(layer));
    }),
  );

  it.effect("reports bootstrap encoding failures with stable process context", () =>
    Effect.gen(function* () {
      const layerSpawner = Layer.succeed(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("unexpected backend spawn")),
      );
      const error = yield* DesktopBackendManager.runBackendProcess({
        ...baseConfig,
        desktopTelemetryStream: Stream.empty,
        bootstrap: {
          ...baseConfig.bootstrap,
          port: 0,
        },
      }).pipe(
        Effect.flip,
        Effect.scoped,
        Effect.provide(Layer.merge(layerSpawner, layerHealthyHttpClient)),
      );

      if (error._tag !== "BackendProcessBootstrapEncodeError") {
        return assert.fail(`Expected bootstrap encode error, received ${error._tag}`);
      }
      assert.equal(error.executablePath, "/electron");
      assert.equal(error.entryPath, "/server/bin.mjs");
      assert.equal(error.cwd, "/server");
      assert.equal(error.httpBaseUrl.href, "http://127.0.0.1:3773/");
      assert.isDefined(error.cause);
      assert.equal(
        error.message,
        "Failed to encode the desktop backend bootstrap payload for /server/bin.mjs.",
      );
      assert.isTrue(isBackendProcessError(error));
    }),
  );

  it.effect("preserves spawn failures without deriving their message from the cause", () =>
    Effect.gen(function* () {
      const spawnCause = PlatformError.systemError({
        _tag: "PermissionDenied",
        module: "ChildProcessSpawner",
        method: "spawn",
        pathOrDescriptor: baseConfig.executablePath,
        description: "low-level detail that must not become the public message",
      });
      const layerSpawner = Layer.succeed(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.fail(spawnCause)),
      );
      const error = yield* DesktopBackendManager.runBackendProcess({
        ...baseConfig,
        desktopTelemetryStream: Stream.empty,
      }).pipe(
        Effect.flip,
        Effect.scoped,
        Effect.provide(Layer.merge(layerSpawner, layerHealthyHttpClient)),
      );

      if (error._tag !== "BackendProcessSpawnError") {
        return assert.fail(`Expected backend spawn error, received ${error._tag}`);
      }
      assert.equal(error.executablePath, "/electron");
      assert.equal(error.entryPath, "/server/bin.mjs");
      assert.equal(error.cwd, "/server");
      assert.equal(error.httpBaseUrl.href, "http://127.0.0.1:3773/");
      assert.strictEqual(error.cause, spawnCause);
      assert.equal(
        error.message,
        "Failed to spawn desktop backend entry /server/bin.mjs with /electron.",
      );
      assert.notInclude(error.message, spawnCause.message);
      assert.isTrue(isBackendProcessError(error));
    }),
  );

  it.effect("preserves exit-status failures without copying their detail into the message", () =>
    Effect.gen(function* () {
      const exitCause = PlatformError.systemError({
        _tag: "PermissionDenied",
        module: "ChildProcess",
        method: "exitCode",
        description: "exit-status-secret-sentinel",
      });
      const layerSpawner = Layer.succeed(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() =>
          Effect.succeed(
            makeProcess({
              exitCode: Effect.fail(exitCause),
            }),
          ),
        ),
      );
      const error = yield* DesktopBackendManager.runBackendProcess({
        ...baseConfig,
        desktopTelemetryStream: Stream.empty,
      }).pipe(
        Effect.flip,
        Effect.scoped,
        Effect.provide(Layer.merge(layerSpawner, layerHealthyHttpClient)),
      );

      if (error._tag !== "BackendProcessExitStatusError") {
        return assert.fail(`Expected backend exit-status error, received ${error._tag}`);
      }
      assert.equal(error.pid, 123);
      assert.equal(error.executablePath, "/electron");
      assert.equal(error.entryPath, "/server/bin.mjs");
      assert.equal(error.cwd, "/server");
      assert.equal(error.httpBaseUrl.href, "http://127.0.0.1:3773/");
      assert.strictEqual(error.cause, exitCause);
      assert.equal(error.message, "Failed to read the exit status of desktop backend process 123.");
      assert.notInclude(error.message, "exit-status-secret-sentinel");
      assert.isTrue(isBackendProcessError(error));
    }),
  );

  it.effect("reports output stream failures with process and stream context", () =>
    Effect.gen(function* () {
      const outputCause = PlatformError.systemError({
        _tag: "BadResource",
        module: "ChildProcess",
        method: "stdout",
        description: "output-stream-secret-sentinel",
      });
      const reported = yield* Deferred.make<DesktopBackendManager.BackendProcessOutputError>();
      const layerSpawner = Layer.succeed(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() =>
          Effect.succeed(
            makeProcess({
              stdout: Stream.fail(outputCause),
              exitCode: Deferred.await(reported).pipe(Effect.as(ChildProcessSpawner.ExitCode(0))),
            }),
          ),
        ),
      );

      const exit = yield* DesktopBackendManager.runBackendProcess({
        ...baseConfig,
        desktopTelemetryStream: Stream.empty,
        onOutputFailure: (error) => Deferred.succeed(reported, error).pipe(Effect.asVoid),
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(layerSpawner, layerHealthyHttpClient)));
      const error = yield* Deferred.await(reported);

      assert.equal(exit.code.pipe(Option.getOrUndefined), 0);
      if (error._tag !== "BackendProcessOutputReadError") {
        return assert.fail(`Expected output read error, received ${error._tag}`);
      }
      assert.equal(error.executablePath, "/electron");
      assert.equal(error.entryPath, "/server/bin.mjs");
      assert.equal(error.cwd, "/server");
      assert.equal(error.httpBaseUrl.href, "http://127.0.0.1:3773/");
      assert.equal(error.pid, 123);
      assert.equal(error.streamName, "stdout");
      assert.strictEqual(error.cause, outputCause);
      assert.equal(error.message, "Failed to read stdout from desktop backend process 123.");
      assert.notInclude(error.message, "output-stream-secret-sentinel");
    }),
  );

  it.effect("reports output handler failures separately from stream read failures", () =>
    Effect.gen(function* () {
      const chunk = new TextEncoder().encode("backend output");
      const nextChunk = new TextEncoder().encode("still draining");
      const outputCause = new Error("output-handler-secret-sentinel");
      const reported = yield* Deferred.make<DesktopBackendManager.BackendProcessOutputError>();
      const drained = yield* Deferred.make<void>();
      let outputCount = 0;
      const layerSpawner = Layer.succeed(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() =>
          Effect.succeed(
            makeProcess({
              stdout: Stream.make(chunk, nextChunk),
              exitCode: Deferred.await(drained).pipe(Effect.as(ChildProcessSpawner.ExitCode(0))),
            }),
          ),
        ),
      );

      const exit = yield* DesktopBackendManager.runBackendProcess({
        ...baseConfig,
        desktopTelemetryStream: Stream.empty,
        onOutput: () => {
          outputCount += 1;
          return outputCount === 1
            ? Effect.fail(outputCause)
            : Deferred.succeed(drained, void 0).pipe(Effect.asVoid);
        },
        onOutputFailure: (error) => Deferred.succeed(reported, error).pipe(Effect.asVoid),
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(layerSpawner, layerHealthyHttpClient)));
      const error = yield* Deferred.await(reported);

      assert.equal(exit.code.pipe(Option.getOrUndefined), 0);
      if (error._tag !== "BackendProcessOutputHandlingError") {
        return assert.fail(`Expected output handling error, received ${error._tag}`);
      }
      assert.equal(error.executablePath, "/electron");
      assert.equal(error.entryPath, "/server/bin.mjs");
      assert.equal(error.cwd, "/server");
      assert.equal(error.httpBaseUrl.href, "http://127.0.0.1:3773/");
      assert.equal(error.pid, 123);
      assert.equal(error.streamName, "stdout");
      assert.equal(error.chunkByteLength, chunk.byteLength);
      assert.strictEqual(error.cause, outputCause);
      assert.equal(
        error.message,
        `Failed to handle ${chunk.byteLength} bytes from stdout of desktop backend process 123.`,
      );
      assert.notInclude(error.message, "output-handler-secret-sentinel");
      assert.equal(outputCount, 2);
    }),
  );

  it.effect("reports child exit before waiting for trailing output to drain", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const exitObserved = yield* Deferred.make<void>();
        const finishOutputDrain = yield* Deferred.make<void>();
        const layerSpawner = Layer.succeed(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() =>
            Effect.succeed(
              makeProcess({
                stdout: Stream.fromEffect(
                  Deferred.await(finishOutputDrain).pipe(
                    Effect.as(new TextEncoder().encode("trailing output\n")),
                  ),
                ),
                exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
              }),
            ),
          ),
        );

        const runFiber = yield* DesktopBackendManager.runBackendProcess({
          ...baseConfig,
          desktopTelemetryStream: Stream.empty,
          onExitObserved: () => Deferred.succeed(exitObserved, void 0).pipe(Effect.asVoid),
        }).pipe(
          Effect.provide(Layer.merge(layerSpawner, layerHealthyHttpClient)),
          Effect.forkChild,
        );

        yield* Deferred.await(exitObserved);
        assert.isUndefined(runFiber.pollUnsafe());

        yield* Deferred.succeed(finishOutputDrain, void 0);
        assert.equal((yield* Fiber.join(runFiber)).code.pipe(Option.getOrUndefined), 1);
      }),
    ),
  );

  it.effect("continues routing desktop telemetry control messages after an invalid line", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const handled = yield* Deferred.make<boolean>();
        const controlMessage = encodeDesktopTelemetryControl({
          version: 1,
          type: "setDiagnosticsDemand",
          enabled: true,
        });
        const layerSpawner = Layer.succeed(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() =>
            Effect.succeed(
              makeProcess({
                getOutputFd: (fd) =>
                  fd === 5
                    ? Stream.encodeText(Stream.make(`not-json\n${controlMessage}\n`))
                    : Stream.empty,
                exitCode: Deferred.await(handled).pipe(Effect.as(ChildProcessSpawner.ExitCode(0))),
              }),
            ),
          ),
        );
        const instance = yield* makeTestInstance({
          spawnerLayer: layerSpawner,
          desktopTelemetryPublisher: {
            handleControlForSource: (_sourceId, message) =>
              message.type === "setDiagnosticsDemand"
                ? Deferred.succeed(handled, message.enabled).pipe(Effect.asVoid)
                : Effect.void,
          },
        });

        yield* instance.start;
        assert.isTrue(yield* Deferred.await(handled));
      }),
    ),
  );

  it.effect("drains trailing child output before persisting an unexpected exit", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const persistedOutput = yield* Deferred.make<ReadonlyArray<string>>();
        const outputDrainStarted = yield* Deferred.make<void>();
        const outputChunks = yield* Ref.make<Array<string>>([]);
        const layerSpawner = Layer.succeed(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() =>
            Effect.succeed(
              makeProcess({
                stdout: Stream.fromEffect(
                  Deferred.succeed(outputDrainStarted, void 0).pipe(
                    Effect.andThen(Effect.sleep(Duration.seconds(1))),
                    Effect.as(new TextEncoder().encode("trailing output\n")),
                  ),
                ),
                exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
              }),
            ),
          ),
        );
        const instance = yield* makeTestInstance({
          spawnerLayer: layerSpawner,
          httpClientLayer: layerHttpClient(() => Effect.never),
          backendOutputLog: {
            writeOutputChunk: (_streamName, chunk) =>
              Ref.update(outputChunks, (current) => [...current, new TextDecoder().decode(chunk)]),
            persistFailure: () =>
              Ref.get(outputChunks).pipe(
                Effect.flatMap((chunks) => Deferred.succeed(persistedOutput, chunks)),
                Effect.asVoid,
              ),
          },
        });

        yield* instance.start;
        yield* Deferred.await(outputDrainStarted);
        yield* TestClock.adjust(Duration.seconds(1));

        assert.deepEqual(yield* Deferred.await(persistedOutput), ["trailing output\n"]);
      }).pipe(Effect.provide(TestClock.layer())),
    ),
  );

  it.effect("retries HTTP readiness before reporting the backend ready", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const requestUrls: Array<string> = [];
        const prunedRuntimes: Array<[string | null, string]> = [];
        const statuses = [503, 200];
        let readyCount = 0;
        const firstRequest = yield* Deferred.make<void>();
        const backendReady = yield* Deferred.make<void>();
        const processExit = yield* Deferred.make<void>();
        const pruneComplete = yield* Deferred.make<void>();
        const exited = yield* Queue.unbounded<void>();

        const layerSpawner = Layer.succeed(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() =>
            Effect.succeed(
              makeProcess({
                exitCode: Deferred.await(processExit).pipe(
                  Effect.as(ChildProcessSpawner.ExitCode(0)),
                ),
              }),
            ),
          ),
        );

        const instance = yield* makeTestInstance({
          spawnerLayer: layerSpawner,
          config: {
            ...baseConfig,
            runningDistro: "Ubuntu",
            wslRuntimeId: "1.2.3-x64",
          },
          pruneRuntimes: (distro, runtimeId) =>
            Effect.sync(() => {
              prunedRuntimes.push([distro, runtimeId]);
            }).pipe(Effect.andThen(Deferred.succeed(pruneComplete, void 0)), Effect.asVoid),
          httpClientLayer: layerHttpClient((request) =>
            Effect.gen(function* () {
              const status = statuses.shift();
              assert.isDefined(status);
              requestUrls.push(request.url);
              yield* Deferred.succeed(firstRequest, void 0);
              return responseForRequest(request, status);
            }),
          ),
          onReady: Effect.sync(() => {
            readyCount += 1;
          }).pipe(Effect.andThen(Deferred.succeed(backendReady, void 0)), Effect.asVoid),
          backendOutputLog: {
            persistFailure: () => Queue.offer(exited, void 0).pipe(Effect.asVoid),
          },
        });

        yield* instance.start;
        yield* Deferred.await(firstRequest);

        assert.equal(readyCount, 0);
        assert.deepEqual(prunedRuntimes, []);
        assert.deepEqual(requestUrls, ["http://127.0.0.1:3773/.well-known/t3/environment"]);

        yield* TestClock.adjust(Duration.millis(100));
        yield* Deferred.await(backendReady);
        yield* Deferred.await(pruneComplete);
        yield* Deferred.succeed(processExit, void 0);
        yield* Queue.take(exited);

        assert.equal(readyCount, 1);
        assert.deepEqual(prunedRuntimes, [["Ubuntu", "1.2.3-x64"]]);
        assert.deepEqual(requestUrls, [
          "http://127.0.0.1:3773/.well-known/t3/environment",
          "http://127.0.0.1:3773/.well-known/t3/environment",
        ]);
      }).pipe(Effect.provide(TestClock.layer())),
    ),
  );

  it.effect(
    "re-probes readiness after the first budget expires while the backend is still alive",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const requestUrls: Array<string> = [];
          let requestCount = 0;
          let readyCount = 0;
          let readinessTimeoutCount = 0;
          const firstProbe = yield* Deferred.make<void>();
          const childExit = yield* Deferred.make<void>();

          const layerSpawner = Layer.succeed(
            ChildProcessSpawner.ChildProcessSpawner,
            ChildProcessSpawner.make(() =>
              Effect.succeed(
                makeProcess({
                  exitCode: Deferred.await(childExit).pipe(
                    Effect.as(ChildProcessSpawner.ExitCode(0)),
                  ),
                }),
              ),
            ),
          );

          const layerHttp = layerHttpClient((request) =>
            Effect.gen(function* () {
              requestCount += 1;
              requestUrls.push(request.url);
              yield* Deferred.succeed(firstProbe, void 0);
              return responseForRequest(request, requestCount <= 2 ? 503 : 200);
            }),
          );

          const runFiber = yield* DesktopBackendManager.runBackendProcess({
            ...baseConfig,
            desktopTelemetryStream: Stream.empty,
            readinessTimeout: Duration.millis(50),
            onReady: () =>
              Effect.sync(() => {
                readyCount += 1;
              }),
            onReadinessFailure: () =>
              Effect.sync(() => {
                readinessTimeoutCount += 1;
              }),
          }).pipe(Effect.provide(Layer.merge(layerSpawner, layerHttp)), Effect.forkChild);

          yield* Deferred.await(firstProbe);
          assert.equal(readyCount, 0);
          assert.equal(readinessTimeoutCount, 0);

          yield* TestClock.adjust(Duration.millis(50));
          assert.equal(readinessTimeoutCount, 1);
          assert.equal(readyCount, 0);

          yield* TestClock.adjust(Duration.millis(100));
          assert.equal(readinessTimeoutCount, 2);
          assert.equal(readyCount, 1);
          assert.equal(requestUrls.length, 3);

          yield* Deferred.succeed(childExit, void 0);
          assert.equal((yield* Fiber.join(runFiber)).code.pipe(Option.getOrUndefined), 0);
        }).pipe(Effect.provide(TestClock.layer())),
      ),
  );

  it.effect("starts the configured backend and closes the scoped process on stop", () =>
    Effect.scoped(
      Effect.gen(function* () {
        let startCount = 0;
        let closedCount = 0;
        const closed = yield* Deferred.make<void>();
        const teardownStarted = yield* Deferred.make<void>();
        const finishTeardown = yield* Deferred.make<void>();
        const startedPids = yield* Queue.unbounded<number>();
        const ready = yield* Deferred.make<void>();
        const backendReadyFlag = yield* Ref.make(false);
        let shutdownCount = 0;
        let persistedFailureCount = 0;
        let discardedSessionCount = 0;
        let removedTelemetrySources = 0;

        const layerSpawner = Layer.succeed(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() =>
            Effect.gen(function* () {
              const scope = yield* Scope.Scope;
              startCount += 1;
              yield* Queue.offer(startedPids, 123);
              const close = Deferred.succeed(teardownStarted, undefined).pipe(
                Effect.andThen(Deferred.await(finishTeardown)),
                Effect.andThen(
                  Effect.sync(() => {
                    closedCount += 1;
                  }),
                ),
                Effect.andThen(Deferred.succeed(closed, void 0)),
                Effect.asVoid,
              );

              yield* Scope.addFinalizer(scope, close);

              return makeProcess({
                exitCode: Deferred.await(closed).pipe(Effect.as(ChildProcessSpawner.ExitCode(0))),
                kill: () => close,
              });
            }),
          ),
        );

        const instance = yield* makeTestInstance({
          spawnerLayer: layerSpawner,
          onReady: Ref.set(backendReadyFlag, true).pipe(
            Effect.andThen(Deferred.succeed(ready, void 0)),
            Effect.asVoid,
          ),
          onShutdown: Ref.set(backendReadyFlag, false).pipe(
            Effect.andThen(
              Effect.sync(() => {
                shutdownCount += 1;
              }),
            ),
          ),
          backendOutputLog: {
            persistFailure: () =>
              Effect.sync(() => {
                persistedFailureCount += 1;
              }),
            discardSession: Effect.sync(() => {
              discardedSessionCount += 1;
            }),
          },
          desktopTelemetryPublisher: {
            removeControlSource: () =>
              Effect.sync(() => {
                removedTelemetrySources += 1;
              }),
          },
        });
        assert.isTrue(Option.isNone(yield* instance.currentConfig));

        yield* instance.start;
        assert.equal(yield* Queue.take(startedPids), 123);
        yield* Deferred.await(ready);
        assert.isTrue(yield* Ref.get(backendReadyFlag));
        assert.deepEqual(yield* instance.currentConfig, Option.some(baseConfig));

        const runningSnapshot = yield* instance.snapshot;
        assert.equal(runningSnapshot.ready, true);
        assert.deepEqual(runningSnapshot.activePid, Option.some(123));

        const stopFiber = yield* instance.stop().pipe(Effect.forkChild);
        yield* Deferred.await(teardownStarted).pipe(Effect.timeout("1 second"));
        assert.isFalse(yield* Ref.get(backendReadyFlag));
        assert.equal(shutdownCount, 1);
        yield* Deferred.succeed(finishTeardown, undefined);
        yield* Fiber.join(stopFiber).pipe(Effect.timeout("1 second"));
        assert.equal(startCount, 1);
        assert.equal(closedCount, 1);
        assert.equal(persistedFailureCount, 0);
        assert.equal(discardedSessionCount, 1);
        assert.equal(removedTelemetrySources, 1);

        const stoppedSnapshot = yield* instance.snapshot;
        assert.isFalse(yield* Ref.get(backendReadyFlag));
        assert.equal(shutdownCount, 1);
        assert.equal(stoppedSnapshot.desiredRunning, false);
        assert.equal(stoppedSnapshot.ready, false);
        assert.equal(Option.isNone(stoppedSnapshot.activePid), true);
      }),
    ),
  );

  it.effect("restarts when start is requested during stop teardown", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const starts = yield* Queue.unbounded<number>();
        const teardownStarted = yield* Deferred.make<void>();
        const finishTeardown = yield* Deferred.make<void>();
        let startCount = 0;

        const layerSpawner = Layer.succeed(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() =>
            Effect.gen(function* () {
              const scope = yield* Scope.Scope;
              const closed = yield* Deferred.make<void>();
              startCount += 1;
              yield* Queue.offer(starts, startCount);
              if (startCount === 1) {
                yield* Scope.addFinalizer(
                  scope,
                  Deferred.succeed(teardownStarted, undefined).pipe(
                    Effect.andThen(Deferred.await(finishTeardown)),
                    Effect.andThen(Deferred.succeed(closed, undefined)),
                    Effect.asVoid,
                  ),
                );
              } else {
                yield* Scope.addFinalizer(
                  scope,
                  Deferred.succeed(closed, undefined).pipe(Effect.asVoid),
                );
              }
              return makeProcess({
                exitCode: Deferred.await(closed).pipe(Effect.as(ChildProcessSpawner.ExitCode(0))),
              });
            }),
          ),
        );

        const instance = yield* makeTestInstance({
          spawnerLayer: layerSpawner,
          httpClientLayer: layerHttpClient(() => Effect.never),
        });

        yield* instance.start;
        assert.equal(yield* Queue.take(starts), 1);

        const stopFiber = yield* instance.stop().pipe(Effect.forkChild);
        yield* Deferred.await(teardownStarted).pipe(Effect.timeout("1 second"));
        yield* instance.start;
        assert.equal((yield* instance.snapshot).desiredRunning, true);

        yield* Deferred.succeed(finishTeardown, undefined);
        yield* Fiber.join(stopFiber).pipe(Effect.timeout("1 second"));
        yield* TestClock.adjust(Duration.millis(500));

        assert.equal(yield* Queue.take(starts).pipe(Effect.timeout("1 second")), 2);
        const restarted = yield* instance.snapshot;
        assert.equal(restarted.desiredRunning, true);
        assert.deepEqual(restarted.activePid, Option.some(123));
      }).pipe(Effect.provide(TestClock.layer())),
    ),
  );

  it.effect("retries config resolution after a start request during stop teardown", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const starts = yield* Queue.unbounded<number>();
        const teardownStarted = yield* Deferred.make<void>();
        const finishTeardown = yield* Deferred.make<void>();
        const configAttempts = yield* Ref.make(0);
        let startCount = 0;

        const configFailure = PlatformError.systemError({
          _tag: "Unknown",
          module: "DesktopBackendManager",
          method: "configResolve",
          description: "transient configuration failure",
        });
        const layerSpawner = Layer.succeed(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() =>
            Effect.gen(function* () {
              const scope = yield* Scope.Scope;
              const closed = yield* Deferred.make<void>();
              startCount += 1;
              yield* Queue.offer(starts, startCount);
              if (startCount === 1) {
                yield* Scope.addFinalizer(
                  scope,
                  Deferred.succeed(teardownStarted, undefined).pipe(
                    Effect.andThen(Deferred.await(finishTeardown)),
                    Effect.andThen(Deferred.succeed(closed, undefined)),
                    Effect.asVoid,
                  ),
                );
              } else {
                yield* Scope.addFinalizer(
                  scope,
                  Deferred.succeed(closed, undefined).pipe(Effect.asVoid),
                );
              }
              return makeProcess({
                exitCode: Deferred.await(closed).pipe(Effect.as(ChildProcessSpawner.ExitCode(0))),
              });
            }),
          ),
        );
        const configResolve = Ref.updateAndGet(configAttempts, (attempt) => attempt + 1).pipe(
          Effect.flatMap((attempt) =>
            attempt === 2 ? Effect.fail(configFailure) : Effect.succeed(baseConfig),
          ),
        );
        const instance = yield* makeTestInstance({
          spawnerLayer: layerSpawner,
          configResolve,
          httpClientLayer: layerHttpClient(() => Effect.never),
        });

        yield* instance.start;
        assert.equal(yield* Queue.take(starts), 1);

        const stopFiber = yield* instance.stop().pipe(Effect.forkChild);
        yield* Deferred.await(teardownStarted).pipe(Effect.timeout("1 second"));
        yield* instance.start;
        yield* Deferred.succeed(finishTeardown, undefined);
        yield* Fiber.join(stopFiber).pipe(Effect.timeout("1 second"));

        const pendingRestart = yield* instance.snapshot;
        assert.equal(pendingRestart.desiredRunning, true);
        assert.equal(pendingRestart.restartScheduled, true);

        yield* TestClock.adjust(Duration.seconds(2));

        assert.equal(yield* Queue.take(starts).pipe(Effect.timeout("1 second")), 2);
        assert.equal(yield* Ref.get(configAttempts), 3);
        assert.equal((yield* instance.snapshot).desiredRunning, true);
      }).pipe(Effect.provide(TestClock.layer())),
    ),
  );

  it.effect("keeps a timed-out run active until its process exits", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const starts = yield* Queue.unbounded<number>();
        const teardownStarted = yield* Deferred.make<void>();
        const finishTeardown = yield* Deferred.make<void>();
        let startCount = 0;

        const layerSpawner = Layer.succeed(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() =>
            Effect.gen(function* () {
              const scope = yield* Scope.Scope;
              const closed = yield* Deferred.make<void>();
              startCount += 1;
              yield* Queue.offer(starts, startCount);
              if (startCount === 1) {
                yield* Scope.addFinalizer(
                  scope,
                  Deferred.succeed(teardownStarted, undefined).pipe(
                    Effect.andThen(Deferred.await(finishTeardown)),
                    Effect.andThen(Deferred.succeed(closed, undefined)),
                    Effect.asVoid,
                  ),
                );
              } else {
                yield* Scope.addFinalizer(
                  scope,
                  Deferred.succeed(closed, undefined).pipe(Effect.asVoid),
                );
              }
              return makeProcess({
                exitCode: Deferred.await(closed).pipe(Effect.as(ChildProcessSpawner.ExitCode(0))),
              });
            }),
          ),
        );

        const instance = yield* makeTestInstance({
          spawnerLayer: layerSpawner,
          httpClientLayer: layerHttpClient(() => Effect.never),
        });

        yield* instance.start;
        assert.equal(yield* Queue.take(starts), 1);

        const stopFiber = yield* instance
          .stop({ timeout: Duration.millis(100) })
          .pipe(Effect.forkChild);
        yield* Deferred.await(teardownStarted).pipe(Effect.timeout("1 second"));
        yield* instance.start;
        yield* TestClock.adjust(Duration.millis(100));
        yield* Fiber.join(stopFiber).pipe(Effect.timeout("1 second"));

        assert.equal(startCount, 1);
        const timedOut = yield* instance.snapshot;
        assert.equal(timedOut.desiredRunning, true);
        assert.deepEqual(timedOut.activePid, Option.some(123));

        yield* Deferred.succeed(finishTeardown, undefined);
        yield* TestClock.adjust(Duration.millis(500));

        assert.equal(yield* Queue.take(starts).pipe(Effect.timeout("1 second")), 2);
      }).pipe(Effect.provide(TestClock.layer())),
    ),
  );

  it.effect("does not notify shutdown before the first start has prior state", () =>
    Effect.scoped(
      Effect.gen(function* () {
        let shutdownCount = 0;
        const closed = yield* Deferred.make<void>();
        const startedPids = yield* Queue.unbounded<number>();

        const layerSpawner = Layer.succeed(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() =>
            Effect.gen(function* () {
              yield* Queue.offer(startedPids, 123);
              const close = Deferred.succeed(closed, void 0).pipe(Effect.asVoid);
              return makeProcess({
                exitCode: Deferred.await(closed).pipe(Effect.as(ChildProcessSpawner.ExitCode(0))),
                kill: () => close,
              });
            }),
          ),
        );

        const instance = yield* makeTestInstance({
          spawnerLayer: layerSpawner,
          httpClientLayer: layerHttpClient(() => Effect.never),
          onShutdown: Effect.sync(() => {
            shutdownCount += 1;
          }),
        });

        yield* instance.start;
        assert.equal(yield* Queue.take(startedPids), 123);
        assert.equal(shutdownCount, 0);
      }),
    ),
  );

  it.effect("restarts an unexpectedly exited backend with the Effect clock", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const starts = yield* Queue.unbounded<number>();
        const failures = yield* Queue.unbounded<string>();
        let startCount = 0;

        const layerSpawner = Layer.succeed(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() =>
            Effect.sync(() => {
              startCount += 1;
              return makeProcess({
                exitCode: Queue.offer(starts, startCount).pipe(
                  Effect.as(ChildProcessSpawner.ExitCode(1)),
                ),
              });
            }),
          ),
        );

        const instance = yield* makeTestInstance({
          spawnerLayer: layerSpawner,
          httpClientLayer: layerHttpClient(() => Effect.never),
          backendOutputLog: {
            persistFailure: ({ details }) => Queue.offer(failures, details).pipe(Effect.asVoid),
          },
        });

        yield* instance.start;

        assert.equal(yield* Queue.take(starts), 1);
        assert.equal(yield* Queue.take(failures), "pid=123 code=1");

        yield* TestClock.adjust(Duration.millis(499));
        assert.equal(yield* Queue.size(starts), 0);
        yield* TestClock.adjust(Duration.millis(1));
        assert.equal(yield* Queue.take(starts), 2);

        yield* TestClock.adjust(Duration.millis(999));
        assert.equal(yield* Queue.size(starts), 0);
        yield* TestClock.adjust(Duration.millis(1));
        assert.equal(yield* Queue.take(starts), 3);
      }).pipe(Effect.provide(TestClock.layer())),
    ),
  );

  it.effect("reports state-directory ownership once without scheduling another backend", () =>
    Effect.scoped(
      Effect.gen(function* () {
        let startCount = 0;
        const refused = yield* Deferred.make<void>();
        const instance = yield* makeTestInstance({
          spawnerLayer: Layer.succeed(
            ChildProcessSpawner.ChildProcessSpawner,
            ChildProcessSpawner.make(() =>
              Effect.sync(() => {
                startCount += 1;
                return makeProcess({
                  exitCode: Effect.succeed(
                    ChildProcessSpawner.ExitCode(SERVER_EXIT_CODE_STATE_DIR_OWNED),
                  ),
                });
              }),
            ),
          ),
          httpClientLayer: layerHttpClient(() => Effect.never),
          onStateDirOwned: Deferred.succeed(refused, undefined).pipe(Effect.asVoid),
        });
        yield* instance.start;
        yield* Deferred.await(refused);
        const snapshot = yield* instance.snapshot;
        assert.isFalse(snapshot.desiredRunning);
        assert.isFalse(snapshot.restartScheduled);
        yield* TestClock.adjust(Duration.minutes(1));
        assert.equal(startCount, 1);
      }).pipe(Effect.provide(TestClock.layer())),
    ),
  );

  it.effect("does not notify shutdown when a scheduled restart starts from non-ready state", () =>
    Effect.scoped(
      Effect.gen(function* () {
        let shutdownCount = 0;

        const layerSpawner = Layer.succeed(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() => Effect.die("unexpected backend spawn")),
        );

        const instance = yield* makeTestInstance({
          spawnerLayer: layerSpawner,
          config: {
            ...baseConfig,
            preflightFailure: Option.some({ reason: "preflight failed", fatal: false }),
          },
          onShutdown: Effect.sync(() => {
            shutdownCount += 1;
          }),
        });

        yield* instance.start;
        assert.equal(shutdownCount, 0);

        yield* TestClock.adjust(Duration.millis(500));
        assert.equal(shutdownCount, 0);
      }).pipe(Effect.provide(TestClock.layer())),
    ),
  );

  it.effect("surfaces a fatal preflight failure once and stops looping after the cap", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const failures: string[] = [];
        const layerSpawner = Layer.succeed(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() => Effect.die("unexpected backend spawn")),
        );

        const instance = yield* makeTestInstance({
          spawnerLayer: layerSpawner,
          config: {
            ...baseConfig,
            preflightFailure: Option.some({ reason: "Node.js not found", fatal: true }),
          },
          onPreflightFailed: (failure) =>
            Effect.sync(() => {
              failures.push(failure.reason);
            }).pipe(Effect.as(false)),
        });

        yield* instance.start;
        assert.deepEqual(failures, []);

        yield* TestClock.adjust(Duration.millis(500));
        yield* TestClock.adjust(Duration.seconds(1));
        yield* TestClock.adjust(Duration.seconds(2));
        yield* TestClock.adjust(Duration.seconds(4));
        assert.deepEqual(failures, ["Node.js not found"]);

        yield* TestClock.adjust(Duration.seconds(8));
        yield* TestClock.adjust(Duration.seconds(30));
        assert.deepEqual(failures, ["Node.js not found"]);
      }).pipe(Effect.provide(TestClock.layer())),
    ),
  );

  it.effect("can be started again after a fatal preflight cap once config recovers", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const failing = yield* Ref.make(true);
        const starts = yield* Queue.unbounded<number>();
        const layerSpawner = Layer.succeed(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() =>
            Queue.offer(starts, 123).pipe(
              Effect.as(
                makeProcess({
                  exitCode: Effect.never,
                }),
              ),
            ),
          ),
        );

        const instance = yield* makeTestInstance({
          spawnerLayer: layerSpawner,
          configResolve: Ref.get(failing).pipe(
            Effect.map((isFailing) =>
              isFailing
                ? {
                    ...baseConfig,
                    preflightFailure: Option.some({
                      reason: "Node.js not found",
                      fatal: true,
                    }),
                  }
                : baseConfig,
            ),
          ),
        });

        yield* instance.start;
        yield* TestClock.adjust(Duration.millis(500));
        yield* TestClock.adjust(Duration.seconds(1));
        yield* TestClock.adjust(Duration.seconds(2));
        yield* TestClock.adjust(Duration.seconds(4));
        yield* TestClock.adjust(Duration.seconds(8));

        const parked = yield* instance.snapshot;
        assert.equal(parked.desiredRunning, false);
        assert.equal(parked.ready, false);
        assert.isTrue(Option.isNone(parked.activePid));
        assert.equal(parked.restartScheduled, false);
        assert.equal(yield* Queue.size(starts), 0);

        yield* Ref.set(failing, false);
        yield* instance.start;

        assert.equal(yield* Queue.take(starts), 123);
        const running = yield* instance.snapshot;
        assert.equal(running.desiredRunning, true);
        assert.deepEqual(running.activePid, Option.some(123));
      }).pipe(Effect.provide(TestClock.layer())),
    ),
  );

  it.effect("keeps retrying a transient (non-fatal) preflight failure without surfacing", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const failures: string[] = [];
        const layerSpawner = Layer.succeed(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() => Effect.die("unexpected backend spawn")),
        );

        const instance = yield* makeTestInstance({
          spawnerLayer: layerSpawner,
          config: {
            ...baseConfig,
            preflightFailure: Option.some({ reason: "wslpath conversion failed", fatal: false }),
          },
          onPreflightFailed: (failure) =>
            Effect.sync(() => {
              failures.push(failure.reason);
            }).pipe(Effect.as(false)),
        });

        yield* instance.start;

        yield* TestClock.adjust(Duration.minutes(2));
        assert.deepEqual(failures, []);
      }).pipe(Effect.provide(TestClock.layer())),
    ),
  );

  it.effect("surfaces a bounded transient preflight failure after its retry limit", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const failures: string[] = [];
        const layerSpawner = Layer.succeed(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() => Effect.die("unexpected backend spawn")),
        );

        const instance = yield* makeTestInstance({
          spawnerLayer: layerSpawner,
          config: {
            ...baseConfig,
            preflightFailure: Option.some({
              reason: "WSL toolchain probe timed out",
              fatal: false,
              retryLimit: 3,
            }),
          },
          onPreflightFailed: (failure) =>
            Effect.sync(() => {
              failures.push(failure.reason);
            }).pipe(Effect.as(false)),
        });

        yield* instance.start;
        yield* TestClock.adjust(Duration.millis(500));
        assert.deepEqual(failures, []);

        yield* TestClock.adjust(Duration.seconds(1));
        assert.deepEqual(failures, ["WSL toolchain probe timed out"]);
      }).pipe(Effect.provide(TestClock.layer())),
    ),
  );

  it.effect("cancels a scheduled restart when start is requested manually", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const starts = yield* Queue.unbounded<number>();
        const secondClosed = yield* Deferred.make<void>();
        let startCount = 0;

        const layerSpawner = Layer.succeed(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() =>
            Effect.gen(function* () {
              startCount += 1;
              yield* Queue.offer(starts, startCount);

              if (startCount === 1) {
                return makeProcess({
                  exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
                });
              }

              const scope = yield* Scope.Scope;
              const close = Deferred.succeed(secondClosed, void 0).pipe(Effect.asVoid);
              yield* Scope.addFinalizer(scope, close);
              return makeProcess({
                exitCode: Deferred.await(secondClosed).pipe(
                  Effect.as(ChildProcessSpawner.ExitCode(0)),
                ),
                kill: () => close,
              });
            }),
          ),
        );

        const instance = yield* makeTestInstance({
          spawnerLayer: layerSpawner,
          httpClientLayer: layerHttpClient(() => Effect.never),
        });

        yield* instance.start;

        assert.equal(yield* Queue.take(starts), 1);
        let restartScheduled = false;
        while (!restartScheduled) {
          restartScheduled = (yield* instance.snapshot).restartScheduled;
          if (!restartScheduled) {
            yield* Effect.yieldNow;
          }
        }

        yield* instance.start;
        assert.equal(yield* Queue.take(starts), 2);

        yield* instance.stop();
        yield* TestClock.adjust(Duration.millis(500));

        assert.equal(yield* Queue.size(starts), 0);
      }).pipe(Effect.provide(TestClock.layer())),
    ),
  );

  it.effect("does not restart after stop cancels a scheduled restart", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const starts = yield* Queue.unbounded<number>();
        let startCount = 0;

        const layerSpawner = Layer.succeed(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() =>
            Effect.sync(() => {
              startCount += 1;
              return makeProcess({
                exitCode: Queue.offer(starts, startCount).pipe(
                  Effect.as(ChildProcessSpawner.ExitCode(1)),
                ),
              });
            }),
          ),
        );

        const instance = yield* makeTestInstance({
          spawnerLayer: layerSpawner,
          httpClientLayer: layerHttpClient(() => Effect.never),
        });

        yield* instance.start;
        assert.equal(yield* Queue.take(starts), 1);

        let restartScheduled = false;
        while (!restartScheduled) {
          restartScheduled = (yield* instance.snapshot).restartScheduled;
          if (!restartScheduled) {
            yield* Effect.yieldNow;
          }
        }

        yield* instance.stop();
        yield* TestClock.adjust(Duration.millis(500));

        assert.equal(yield* Queue.size(starts), 0);
        assert.equal((yield* instance.snapshot).desiredRunning, false);
      }).pipe(Effect.provide(TestClock.layer())),
    ),
  );

  it.effect("stopAllPoolInstances bounds the quit finalizer when backends hang", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const teardownStarted = yield* Queue.unbounded<string>();
        const teardownFinished = yield* Queue.unbounded<string>();
        const allowTeardown = yield* Deferred.make<void>();

        const makeInstance = (name: string) =>
          makeTestInstance({
            id: DesktopBackendManager.BackendInstanceId(name),
            spawnerLayer: Layer.succeed(
              ChildProcessSpawner.ChildProcessSpawner,
              ChildProcessSpawner.make(() =>
                Effect.gen(function* () {
                  const scope = yield* Scope.Scope;
                  yield* Scope.addFinalizer(
                    scope,
                    Queue.offer(teardownStarted, name).pipe(
                      Effect.andThen(Deferred.await(allowTeardown)),
                      Effect.andThen(Queue.offer(teardownFinished, name)),
                      Effect.asVoid,
                    ),
                  );
                  return makeProcess({ exitCode: Effect.never });
                }),
              ),
            ),
            httpClientLayer: layerHttpClient(() => Effect.never),
          });

        const instance1 = yield* makeInstance("instance1");
        const instance2 = yield* makeInstance("instance2");

        yield* instance1.start;
        yield* instance2.start;

        const layerMockPool = Layer.succeed(DesktopBackendPool.DesktopBackendPool, {
          list: Effect.succeed([instance1, instance2]),
          get: () => Effect.succeedNone,
          primary: Effect.die(new Error("primary not implemented")),
          register: () => Effect.die(new Error("register not implemented")),
          unregister: () => Effect.die(new Error("unregister not implemented")),
        });

        const quitFiber = yield* Effect.scoped(
          Effect.addFinalizer(() => DesktopApp.stopAllPoolInstances()),
        ).pipe(Effect.provide(layerMockPool), Effect.forkChild);

        const started = yield* Queue.takeN(teardownStarted, 2);
        assert.deepEqual(started.toSorted(), ["instance1", "instance2"]);

        yield* TestClock.adjust(Duration.seconds(5));
        yield* Fiber.join(quitFiber);
        assert.equal(yield* Queue.size(teardownFinished), 0);

        yield* Deferred.succeed(allowTeardown, undefined);
        const finished = yield* Queue.takeN(teardownFinished, 2);
        assert.deepEqual(finished.toSorted(), ["instance1", "instance2"]);
      }).pipe(Effect.provide(TestClock.layer())),
    ),
  );
});

it.for(["ordinary", "trial", "live-enrollment"] as const)(
  "joins the production Manager and bundled backend owner channel after %s admission",
  async (scenario, { signal }) => {
    await IdentityFixture.withNativeFixtureScratch(async (root: string) => {
      const managerPath = NodeURL.fileURLToPath(import.meta.url).replace(/\.test\.ts$/, ".ts");
      const backendPath = NodePath.resolve(
        NodePath.dirname(managerPath),
        "../../../server/src/jones/workstreams/DesktopNativeOwner.ts",
      );
      const home = await NodeFSP.realpath(root);
      const profile = NodePath.join(home, "profile");
      const databasePath = NodePath.join(home, "userdata/statev2.sqlite");
      await NodeFSP.mkdir(profile);
      await NodeFSP.mkdir(NodePath.dirname(databasePath), { mode: 0o700 });
      await NodeFSP.mkdir(NodePath.join(home, "runtime"));
      await NodeFSP.writeFile(databasePath, scenario === "live-enrollment" ? "" : "opaque state", {
        mode: 0o600,
      });
      const metadata = {
        name: "t3code",
        jonesSource: {
          repository: "Jones-Systems/Jones-Code",
          sha: "a".repeat(40),
          tree: "b".repeat(40),
        },
      };
      const active = {
        protocol: 1,
        owner: "desktop",
        home,
        databasePath,
        profile,
        environmentId: "fixture",
        version: "fixture",
        sourceSha: metadata.jonesSource.sha,
        sourceTree: metadata.jonesSource.tree,
      };
      const manifest = NodePath.join(home, "runtime/jones-active-install.json");
      const tx = NodePath.join(home, "runtime/jones-updates/transactions", "f".repeat(64));
      const descriptorPath =
        scenario === "trial" ? NodePath.join(tx, "trial-descriptor.json") : undefined;
      await NodeFSP.writeFile(
        manifest,
        JSON.stringify(
          scenario === "trial"
            ? {
                ...active,
                version: "previous",
                sourceSha: "c".repeat(40),
                sourceTree: "d".repeat(40),
              }
            : active,
        ),
        { mode: 0o600 },
      );
      if (descriptorPath !== undefined) {
        await NodeFSP.mkdir(tx, { recursive: true });
        await NodeFSP.writeFile(
          descriptorPath,
          JSON.stringify({
            ...active,
            startupGateProtocol: 1,
            transactionId: "f".repeat(64),
            stagedHandle: "e".repeat(64),
          }),
        );
        await NodeFSP.writeFile(
          NodePath.join(tx, "journal.json"),
          JSON.stringify({
            phase: "trial",
            intent: {
              protocol: 1,
              transactionId: "f".repeat(64),
              staged: { ...active, handle: "e".repeat(64) },
            },
          }),
        );
      }
      const fixture = {
        scenario,
        home,
        databasePath,
        profile,
        metadata,
        active,
        manifest,
        tx,
        descriptorPath,
      };
      await NodeFSP.writeFile(NodePath.join(root, "input.json"), JSON.stringify(fixture));
      const parentHarness = [
        "import * as OwnerFixtureFS from 'node:fs';",
        "import * as OwnerFixtureAssert from 'node:assert/strict';",
        "import * as OwnerFixtureServices from '@effect/platform-node/NodeServices';",
        "import * as OwnerFixtureStartup from '../jones/updates/jonesNativeStartup.ts';",
        "const fixture = JSON.parse(OwnerFixtureFS.readFileSync(process.argv[2] + '/input.json', 'utf8'));",
        "OwnerFixtureStartup.holdJonesDesktopNativeWriterFence({ platform:'darwin', env:{ T3CODE_HOME:fixture.home, T3CODE_DESKTOP_USER_DATA_DIR:fixture.profile, T3CODE_JONES_TRIAL_DESCRIPTOR:fixture.descriptorPath }, homeDirectory:fixture.home, appDataDirectory:fixture.home, version:'fixture', metadata:fixture.metadata });",
        "const ownerResolver = OwnerFixtureStartup.currentJonesDesktopOwner();",
        "let output = ''; let committed = false;",
        "const outcome = await Effect.runPromise(Effect.scoped(runBackendProcess({",
        "executablePath:process.execPath,args:[fixture.home+'/backend.mjs',fixture.home],entryPath:fixture.home+'/backend.mjs',cwd:fixture.home,env:{},extendEnv:true,",
        "bootstrap:{mode:'desktop',noBrowser:true,port:3773,t3Home:fixture.home,host:'127.0.0.1',desktopBootstrapToken:'fixture',tailscaleServeEnabled:false,tailscaleServePort:443,desktopNativeOwnerInputFd:8,desktopNativeOwnerOutputFd:9},",
        "bootstrapDelivery:'fd3',httpBaseUrl:new URL('http://127.0.0.1:3773'),captureOutput:true,preflightFailure:Option.none(),jonesDesktopOwner:ownerResolver,desktopTelemetryStream:Stream.empty,",
        "onOutput:(kind,bytes)=>Effect.sync(()=>{ output += new TextDecoder().decode(bytes); if(fixture.descriptorPath && output.includes('pending') && !committed){ committed=true; OwnerFixtureFS.writeFileSync(fixture.manifest,JSON.stringify(fixture.active)); const journal=JSON.parse(OwnerFixtureFS.readFileSync(fixture.tx+'/journal.json','utf8')); journal.phase='resumed'; OwnerFixtureFS.writeFileSync(fixture.tx+'/journal.json',JSON.stringify(journal)); OwnerFixtureFS.writeFileSync(fixture.home+'/commit-ready','ready'); } })",
        "}).pipe(Effect.provide(OwnerFixtureServices.layer), Effect.provideService(HttpClient.HttpClient, HttpClient.make(()=>Effect.never)))));",
        "OwnerFixtureFS.writeFileSync(fixture.home+'/settled.json',JSON.stringify({settled:true}));",
        "OwnerFixtureAssert.equal(Option.getOrUndefined(outcome.code),0,output);",
        "OwnerFixtureAssert.ok(output.includes('qualified'),output);",
        "if(fixture.descriptorPath) OwnerFixtureAssert.ok(output.includes('pending'),output);",
        "OwnerFixtureFS.writeFileSync(fixture.home+'/settled.json',JSON.stringify({settled:true}));",
      ].join("\n");
      const childHarness = [
        "import * as LiveFixtureSqlite from 'node:sqlite';",
        "import * as LiveFixtureProcess from 'node:child_process';",
        "import * as LiveFixtureModule from 'node:module';",
        "import * as LiveFixtureAssert from 'node:assert/strict';",
        "import * as LiveFixtureSqlClient from '@t3tools/shared/nodeSqliteClient';",
        "import { runDesktopNativeOwnerOperation as liveOwnerOperation } from './enrollment/desktopOwner.ts';",
        "const fixture = JSON.parse(NodeFS.readFileSync(process.argv[2] + '/input.json','utf8'));",
        "const owner = openDesktopNativeOwnerSession({ home:fixture.home,databasePath:fixture.databasePath,authorityDir:fixture.home+'/native-store-authority',version:'fixture',buildMetadata:fixture.metadata,descriptorPath:fixture.descriptorPath,inputFd:8,outputFd:9 });",
        "try {",
        "if(fixture.descriptorPath){ let refused=false; try{ await owner.refresh(); }catch{refused=true;} if(!refused||owner.available)throw new Error('Trial prematurely qualified'); process.stdout.write('pending\\n');",
        "await new Promise((resolve,reject)=>{ const deadline=Date.now()+10000; const check=()=>{if(NodeFS.existsSync(fixture.home+'/commit-ready'))resolve();else if(Date.now()>deadline)reject(new Error('Commit not observed'));else NodeTimers.setTimeout(check,10);};check();}); }",
        "await owner.refresh(); owner.assertCurrent(); if(owner.binding?.version!=='fixture')throw new Error('Wrong generation'); process.stdout.write('qualified\\n');",
        "if(fixture.scenario==='live-enrollment'){",
        "NodeFS.writeFileSync(fixture.home+'/userdata/environment-id','fixture',{mode:0o600});",
        "const writer=new LiveFixtureSqlite.DatabaseSync(fixture.databasePath);writer.exec('PRAGMA journal_mode=DELETE; CREATE TABLE fixture_writes(value INTEGER);');",
        "const reader=new LiveFixtureSqlite.DatabaseSync(fixture.databasePath,{readOnly:true});",
        "const nativeFs=LiveFixtureModule.createRequire(import.meta.url)('node:fs');const originalOpen=nativeFs.openSync;let rawOpens=0;nativeFs.openSync=function(path,...args){if(path===fixture.databasePath)rawOpens++;return originalOpen.call(this,path,...args);};LiveFixtureModule.syncBuiltinESMExports();",
        "let timer;let writes=0;const probe=()=>{const result=LiveFixtureProcess.spawnSync('python3',['-c',\"import sqlite3,sys\\nc=sqlite3.connect(sys.argv[1],timeout=0,isolation_level=None)\\ntry:\\n c.execute('BEGIN IMMEDIATE');c.execute('ROLLBACK');sys.exit(1)\\nexcept sqlite3.OperationalError as e:\\n sys.exit(0 if 'locked' in str(e) else 2)\\nfinally:c.close()\",fixture.databasePath],{encoding:'utf8',timeout:5000});LiveFixtureAssert.equal(result.status,0,'Competing writer entered; raw main-file opens='+rawOpens+' '+result.stderr);};",
        "try{await Effect.runPromise(Effect.scoped(Effect.gen(function*(){",
        "writer.exec('BEGIN IMMEDIATE; INSERT INTO fixture_writes VALUES(1)');probe();",
        "timer=NodeTimers.setInterval(()=>{writer.prepare('INSERT INTO fixture_writes VALUES(?)').run(++writes);},5);",
        "const currentOwner={binding:owner.binding,assertCurrent:owner.assertCurrent,refresh:Effect.tryPromise({try:owner.refresh,catch:()=>unavailable()})};const location={baseDir:fixture.home,authorityDir:fixture.home+'/native-store-authority',environmentId:'fixture'};",
        "yield* liveOwnerOperation({operation:'authority-enroll'},currentOwner,location);probe();",
        "yield* Effect.promise(()=>new Promise(resolve=>NodeTimers.setTimeout(resolve,25)));",
        "const prepared=yield* Effect.result(liveOwnerOperation({operation:'provider-prepare',enrollmentId:'live',ownerId:'owner',principalId:'principal',registryOrigin:'https://registry.example',lifetimeDays:1},currentOwner,location));probe();LiveFixtureAssert.equal(prepared._tag,'Success','prepare failed; raw main-file opens='+rawOpens);",
        "LiveFixtureAssert.equal(rawOpens,0,'Raw main-file inspection is forbidden in the running backend');LiveFixtureAssert.ok(writes>0);",
        "clearInterval(timer);timer=undefined;writer.exec('COMMIT');LiveFixtureAssert.ok(reader.prepare('SELECT count(*) AS count FROM fixture_writes').get().count>1);",
        "}).pipe(Effect.provide(LiveFixtureSqlClient.layer({filename:fixture.databasePath})))));",
        "}finally{if(timer!==undefined)clearInterval(timer);nativeFs.openSync=originalOpen;LiveFixtureModule.syncBuiltinESMExports();try{writer.exec('ROLLBACK');}catch{}reader.close();writer.close();}}",
        "}finally{owner.close();}",
      ].join("\n");
      const sources = new Map([
        [managerPath, (await NodeFSP.readFile(managerPath, "utf8")) + "\n" + parentHarness],
        [backendPath, (await NodeFSP.readFile(backendPath, "utf8")) + "\n" + childHarness],
      ]);
      await NodeFSP.writeFile(
        NodePath.join(root, "settled.json"),
        JSON.stringify({ settled: false }),
      );
      let launched = false;
      try {
        for (const [name, entry] of Object.entries({ fixture: managerPath, backend: backendPath }))
          await build({
            config: false,
            cwd: root,
            entry: { [name]: entry },
            outDir: root,
            clean: false,
            format: "esm",
            dts: false,
            sourcemap: false,
            tsconfig: false,
            outExtensions: () => ({ js: ".mjs" }),
            outputOptions: { codeSplitting: false },
            deps: { alwaysBundle: (id) => !id.startsWith("node:"), onlyBundle: false },
            plugins: [
              {
                name: "owned-desktop-owner-integration",
                resolveId(id) {
                  if (id === "electron") return "\0owner-fixture-electron.cjs";
                },
                load(id) {
                  if (id === "\0owner-fixture-electron.cjs") return "module.exports = {};";
                  if (
                    id === NodePath.resolve(NodePath.dirname(backendPath), "../../../package.json")
                  )
                    return JSON.stringify({
                      version: "fixture",
                      jonesSource: metadata.jonesSource,
                    });
                  return sources.get(id);
                },
              },
            ],
            logLevel: "silent",
          });
        launched = true;
        const result = await IdentityFixture.runNativeFixtureCommand(
          NodeProcess.execPath,
          [NodePath.join(root, "fixture.mjs"), root],
          root,
          { signal },
        );
        assert.equal(result.code, 0, result.stderr);
        assert.equal(
          JSON.parse(await NodeFSP.readFile(NodePath.join(root, "settled.json"), "utf8")).settled,
          true,
        );
      } catch (cause) {
        const settled = await NodeFSP.readFile(NodePath.join(root, "settled.json"), "utf8")
          .then((raw) => JSON.parse(raw).settled)
          .catch(() => false);
        if (launched && !settled) Object.assign(cause as object, { preserveScratch: true });
        throw cause;
      }
    });
  },
);
