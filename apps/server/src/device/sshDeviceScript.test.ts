// @effect-diagnostics nodeBuiltinImport:off globalFetchInEffect:off - verifies generated remote scripts using real shell and Node processes.
import * as Effect from "effect/Effect";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { describe, expect, it } from "@effect/vitest";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import { quoteRemoteArg, remoteDeviceEnvironment, remoteDeviceScript } from "./sshDeviceScript.ts";
import { AGENT_DEVICE_VERSION, DEVICE_HUB_VERSION } from "./DeviceToolchain.ts";

const exec = NodeUtil.promisify(NodeChildProcess.execFile);

it.effect("finds Android Studio Java for a non-interactive SSH session", () =>
  Effect.gen(function* () {
    if ((yield* HostProcessPlatform) === "win32") return;
    yield* Effect.promise(async () => {
      const home = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-ssh-java-"));
      try {
        const javaHome = NodePath.join(home, ".local/opt/android-studio/jbr");
        await NodeFSP.mkdir(NodePath.join(javaHome, "bin"), { recursive: true });
        await NodeFSP.writeFile(
          NodePath.join(javaHome, "bin/java"),
          "#!/bin/sh\necho test-java\n",
          { mode: 0o755 },
        );
        const result = await exec("/bin/sh", ["-c", `${remoteDeviceEnvironment}\njava`], {
          env: { HOME: home, PATH: "/nonexistent", JAVA_HOME: "" },
        });
        expect(result.stdout.trim()).toBe("test-java");
      } finally {
        await NodeFSP.rm(home, { recursive: true, force: true });
      }
    });
  }),
);

it.effect("preserves shell metacharacters and newlines in remote arguments", () =>
  Effect.gen(function* () {
    if ((yield* HostProcessPlatform) === "win32") return;
    yield* Effect.promise(async () => {
      const value = "quotes ' \" ; $(echo expanded) $HOME\nnext line";
      const result = await exec("sh", ["-c", `printf %s ${quoteRemoteArg(value)}`]);
      expect(result.stdout).toBe(value);
    });
  }),
);

describe("remote helper lifecycle", () => {
  it.effect("reuses its own healthy helpers and stops only its own runtime", () =>
    Effect.gen(function* () {
      if ((yield* HostProcessPlatform) === "win32") return;
      yield* Effect.promise(async () => {
        const home = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-remote-script-"));
        try {
          const bin = NodePath.join(home, "bin");
          await NodeFSP.mkdir(bin);
          await NodeFSP.writeFile(NodePath.join(bin, "adb"), "#!/bin/sh\nexit 0\n", {
            mode: 0o755,
          });
          const root = NodePath.join(home, ".t3/device");
          const hubDir = NodePath.join(root, `tools/expo-device-hub@${DEVICE_HUB_VERSION}`);
          const agentDir = NodePath.join(root, `tools/agent-device@${AGENT_DEVICE_VERSION}`);
          const hub = NodePath.join(hubDir, "node_modules/expo-device-hub/dist/server/cli.mjs");
          const agent = NodePath.join(agentDir, "node_modules/agent-device/bin/agent-device.mjs");
          await NodeFSP.mkdir(NodePath.join(hubDir, "node_modules/expo-device-hub/dist/server"), {
            recursive: true,
          });
          await NodeFSP.mkdir(NodePath.join(agentDir, "node_modules/agent-device/bin"), {
            recursive: true,
          });
          await NodeFSP.writeFile(NodePath.join(hubDir, ".install-complete"), DEVICE_HUB_VERSION);
          await NodeFSP.writeFile(
            NodePath.join(agentDir, ".install-complete"),
            AGENT_DEVICE_VERSION,
          );
          await NodeFSP.writeFile(
            hub,
            `import http from 'node:http'; import fs from 'node:fs';
  if(fs.existsSync('fail-start-once')) {fs.unlinkSync('fail-start-once');process.exit(1);}
  const args=process.argv.slice(2); http.createServer((req,res)=>{res.statusCode=fs.existsSync('unhealthy-'+process.pid)?503:200;res.end('ok');}).listen(Number(args[args.indexOf('--port')+1]),'127.0.0.1');`,
          );
          await NodeFSP.writeFile(
            agent,
            `import fs from 'node:fs'; import path from 'node:path'; import http from 'node:http'; import {spawn} from 'node:child_process';
  const args=process.argv.slice(2);
  const state=process.env.AGENT_DEVICE_STATE_DIR || args[args.indexOf('--state-dir')+1];
  const file=path.join(state,'daemon.json');
  if(args[0]==='daemon') { const data=JSON.parse(fs.readFileSync(file,'utf8')); fs.writeFileSync(path.join(state,'stopped-agent'),String(data.pid)); try {process.kill(data.pid,'SIGTERM')} catch {} }
  else if(args[0]==='serve') { const server=http.createServer((req,res)=>{res.statusCode=fs.existsSync(path.join(state,'unhealthy-agent-'+process.pid))?503:200;res.end('ok');}); server.listen(0,'127.0.0.1',()=>{fs.writeFileSync(file,JSON.stringify({httpPort:server.address().port,pid:process.pid,token:'test'}));process.send?.('ready');process.disconnect?.();}); }
  else { const child=spawn(process.execPath,[path.join(path.dirname(process.argv[1]),'daemon.mjs'),'serve'],{detached:true,stdio:['ignore','ignore','ignore','ipc'],env:process.env});await new Promise((resolve,reject)=>{child.once('message',resolve);child.once('error',reject);});child.unref(); }
  `,
          );
          await NodeFSP.copyFile(agent, NodePath.join(NodePath.dirname(agent), "daemon.mjs"));
          const fakeWs = NodePath.join(hubDir, "node_modules/ws");
          await NodeFSP.mkdir(fakeWs, { recursive: true });
          await NodeFSP.writeFile(
            NodePath.join(fakeWs, "index.js"),
            "const {EventEmitter}=require('node:events');module.exports={WebSocket:class {},WebSocketServer:class extends EventEmitter {close(){}}};",
          );
          const nextHubVersion = DEVICE_HUB_VERSION + "-upgrade";
          const nextAgentVersion = AGENT_DEVICE_VERSION + "-upgrade";
          let invocation = 0;
          const directGenerations = new Map<string, string>();
          const invoke = async (
            owner: string,
            mode: "probe" | "start" | "agent-start" | "stop-agent" | "stop" | "stop-direct",
            upgraded = false,
            directGeneration?: string,
          ) => {
            const file = NodePath.join(home, `${owner}-${mode}-${invocation++}.cjs`);
            await NodeFSP.writeFile(
              file,
              `const originalKill = process.kill; process.kill = (pid, signal) => { if (signal === 'SIGTERM') require('node:fs').appendFileSync(${JSON.stringify(NodePath.join(home, "stops"))}, pid+'\\n'); return originalKill(pid, signal); };\n` +
                remoteDeviceScript(
                  owner,
                  mode,
                  directGeneration ? { hostId: "mini", generation: directGeneration } : undefined,
                )
                  .replace(DEVICE_HUB_VERSION, upgraded ? nextHubVersion : DEVICE_HUB_VERSION)
                  .replace(
                    AGENT_DEVICE_VERSION,
                    upgraded ? nextAgentVersion : AGENT_DEVICE_VERSION,
                  ),
            );
            const result = await exec(process.execPath, [file], {
              env: { ...process.env, HOME: home, PATH: `${bin}:${process.env.PATH}` },
            });
            const value = result.stdout ? JSON.parse(result.stdout) : null;
            if (value?.directMedia) directGenerations.set(owner, value.directMedia.generation);
            return value;
          };
          const inventory = await invoke("one", "probe");
          expect(inventory.tools.hub.installedVersions).toEqual([DEVICE_HUB_VERSION]);
          expect(inventory.tools.hub.runningVersion).toBeNull();
          expect(inventory.tools.agent.installedVersions).toEqual([AGENT_DEVICE_VERSION]);
          await expect(NodeFSP.stat(NodePath.join(root, "hosts/one/hub.json"))).rejects.toThrow();
          const template = NodePath.join(home, "hub-template");
          await NodeFSP.cp(hubDir, template, { recursive: true });
          await NodeFSP.rm(NodePath.join(hubDir, ".install-complete"));
          const installLock = hubDir + ".lock";
          await NodeFSP.symlink("2147483647:exited-installer", installLock);
          await NodeFSP.writeFile(
            NodePath.join(bin, "npm"),
            `#!${process.execPath}\nconst fs=require('node:fs');const args=process.argv.slice(2);if(args[0]==='--version'){console.log('10.0.0');process.exit(0);}fs.cpSync(${JSON.stringify(template)},args[args.indexOf('--prefix')+1],{recursive:true});`,
            { mode: 0o755 },
          );
          await NodeFSP.mkdir(NodePath.join(root, "hosts/one"), { recursive: true });
          await NodeFSP.writeFile(NodePath.join(root, "hosts/one/fail-start-once"), "");
          // Unavailable advisory bookkeeping must not prevent either helper from starting.
          await NodeFSP.writeFile(NodePath.join(root, "tools/.maintenance-lock"), "blocked");
          await NodeFSP.writeFile(
            NodePath.join(root, "tools/.users"),
            "unwritable lease directory",
          );
          try {
            const [manual, concurrent] = await Promise.all([
              invoke("one", "start"),
              invoke("one", "start"),
            ]);
            expect(concurrent.hubPort).toBe(manual.hubPort);
            expect(manual.daemonPort).toBeUndefined();
            await expect(
              NodeFSP.stat(NodePath.join(root, "hosts/one/daemon.json")),
            ).rejects.toThrow();
            const [first, concurrentAgent] = await Promise.all([
              invoke("one", "agent-start"),
              invoke("one", "agent-start"),
            ]);
            expect(concurrentAgent.hubPort).toBe(first.hubPort);
            expect(concurrentAgent.daemonPort).toBe(first.daemonPort);
            const running = await invoke("one", "probe");
            expect(running.tools.hub.runningVersion).toBe(DEVICE_HUB_VERSION);
            expect(running.tools.agent.runningVersion).toBe(AGENT_DEVICE_VERSION);
            const second = await invoke("two", "agent-start");
            const reused = await invoke("one", "agent-start");
            expect(reused.hubPort).toBe(first.hubPort);
            expect(reused.daemonPort).toBe(first.daemonPort);
            expect(second.hubPort).not.toBe(first.hubPort);
            expect(second.daemonPort).not.toBe(first.daemonPort);
            const firstHub = JSON.parse(
              await NodeFSP.readFile(NodePath.join(root, "hosts/one/hub.json"), "utf8"),
            );
            const secondHub = JSON.parse(
              await NodeFSP.readFile(NodePath.join(root, "hosts/two/hub.json"), "utf8"),
            );
            await NodeFSP.writeFile(NodePath.join(root, `hosts/one/unhealthy-${firstHub.pid}`), "");
            let repaired = await invoke("one", "agent-start");
            expect(repaired.hubPort).not.toBe(first.hubPort);
            const stopped = (await NodeFSP.readFile(NodePath.join(home, "stops"), "utf8"))
              .trim()
              .split("\n");
            expect(stopped).toContain(String(firstHub.pid));
            expect(stopped).not.toContain(String(secondHub.pid));
            const previousDaemon = JSON.parse(
              await NodeFSP.readFile(NodePath.join(root, "hosts/one/daemon.json"), "utf8"),
            );
            for (const [source, name, version] of [
              [hubDir, "expo-device-hub", nextHubVersion],
              [agentDir, "agent-device", nextAgentVersion],
            ]) {
              const destination = NodePath.join(root, `tools/${name}@${version}`);
              await NodeFSP.cp(source!, destination, { recursive: true });
              await NodeFSP.writeFile(NodePath.join(destination, ".install-complete"), version!);
            }
            const upgraded = await invoke("one", "agent-start", true);
            expect(upgraded.entryPath).toContain(nextAgentVersion);
            const upgradedHub = JSON.parse(
              await NodeFSP.readFile(NodePath.join(root, "hosts/one/hub.json"), "utf8"),
            );
            expect(upgradedHub.entryPath).toContain(nextHubVersion);
            const upgradedDaemon = JSON.parse(
              await NodeFSP.readFile(NodePath.join(root, "hosts/one/daemon.json"), "utf8"),
            );
            expect(upgradedDaemon.pid).not.toBe(previousDaemon.pid);
            expect(await invoke("one", "agent-start", true)).toEqual(upgraded);
            await NodeFSP.writeFile(
              NodePath.join(root, `hosts/one/unhealthy-agent-${upgradedDaemon.pid}`),
              "",
            );
            repaired = await invoke("one", "agent-start", true);
            expect(
              await NodeFSP.readFile(NodePath.join(root, "hosts/one/stopped-agent"), "utf8"),
            ).toBe(String(upgradedDaemon.pid));
            expect(repaired.daemonPort).not.toBe(upgraded.daemonPort);
            // Stop still uses the recorded entry when a future pinned package is not installed yet.
            const originalScript = remoteDeviceScript("one", "stop-agent");
            const upgradedStop = NodePath.join(home, "upgraded-stop.cjs");
            await NodeFSP.writeFile(
              upgradedStop,
              originalScript.replace(AGENT_DEVICE_VERSION, "999.0.0"),
            );
            await exec(process.execPath, [upgradedStop], {
              env: { ...process.env, HOME: home, PATH: `${bin}:${process.env.PATH}` },
            });
            const daemon = JSON.parse(
              await NodeFSP.readFile(NodePath.join(root, "hosts/one/daemon.json"), "utf8"),
            );
            expect(
              await NodeFSP.readFile(NodePath.join(root, "hosts/one/stopped-agent"), "utf8"),
            ).toBe(String(daemon.pid));
            expect((await fetch(`http://127.0.0.1:${repaired.hubPort}/readyz`)).ok).toBe(true);
            const firstDirect = await invoke("one", "start", true, "generation-one");
            expect(firstDirect.hubPort).toBe(repaired.hubPort);
            const directRecord = JSON.parse(
              await NodeFSP.readFile(NodePath.join(root, "hosts/one/direct-gateway.json"), "utf8"),
            );
            expect(directRecord.owner).toBe("one");
            expect(directRecord.generation).toBe("generation-one");
            expect(firstDirect.directMedia.admissionPort).toBe(directRecord.admissionPort);
            expect(
              await (await fetch(`http://127.0.0.1:${directRecord.port}/readyz`)).json(),
            ).toEqual({ owner: "one", generation: "generation-one" });
            expect(
              (await fetch(`http://127.0.0.1:${directRecord.port}/api/device-hub/readyz`)).status,
            ).toBe(403);
            const secondDirect = await invoke("two", "start", false, "generation-two");
            const rotated = await invoke("one", "start", true, "generation-next");
            expect(rotated.hubPort).toBe(firstDirect.hubPort);
            expect(rotated.directMedia.generation).toBe("generation-next");
            expect(rotated.directMedia.gatewayPort).not.toBe(firstDirect.directMedia.gatewayPort);
            await expect(NodeFSP.stat(directRecord.entryPath)).rejects.toThrow();
            await invoke("one", "stop-direct", true, "generation-one");
            const replacement = JSON.parse(
              await NodeFSP.readFile(NodePath.join(root, "hosts/one/direct-gateway.json"), "utf8"),
            );
            expect(replacement.generation).toBe("generation-next");
            const gatewayFile = NodePath.join(root, "hosts/one/direct-gateway.json");
            const otherHub = JSON.parse(
              await NodeFSP.readFile(NodePath.join(root, "hosts/two/hub.json"), "utf8"),
            );
            await NodeFSP.writeFile(
              gatewayFile,
              JSON.stringify({ ...replacement, pid: otherHub.pid }),
            );
            await expect(invoke("one", "stop-direct", true, "generation-next")).rejects.toThrow();
            expect((await fetch(`http://127.0.0.1:${otherHub.port}/readyz`)).ok).toBe(true);
            await NodeFSP.writeFile(gatewayFile, "{unconfirmed-record");
            await expect(invoke("one", "stop-direct", true, "generation-next")).rejects.toThrow();
            await NodeFSP.writeFile(gatewayFile, JSON.stringify(replacement));
            expect((await fetch(`http://127.0.0.1:${replacement.port}/readyz`)).ok).toBe(true);
            // Proxy-only bootstrap must leave a different captured direct generation alone.
            await invoke("one", "start", true);
            expect(
              JSON.parse(
                await NodeFSP.readFile(
                  NodePath.join(root, "hosts/one/direct-gateway.json"),
                  "utf8",
                ),
              ),
            ).toEqual(replacement);
            expect(
              (await NodeFSP.readFile(NodePath.join(home, "stops"), "utf8")).split("\n"),
            ).toContain(String(directRecord.pid));
            // A gateway startup failure must preserve the Hub and the other owner's gateway.
            await NodeFSP.writeFile(
              NodePath.join(
                root,
                `tools/expo-device-hub@${nextHubVersion}/node_modules/ws/index.js`,
              ),
              "throw Error('gateway fixture failure');",
            );
            await expect(invoke("one", "start", true, "generation-failed")).rejects.toThrow();
            await expect(
              NodeFSP.stat(NodePath.join(root, "hosts/one/direct-gateway.json")),
            ).rejects.toThrow();
            await expect(
              NodeFSP.stat(NodePath.join(root, "hosts/one/direct-gateway-generation-failed.cjs")),
            ).rejects.toThrow();
            expect((await fetch(`http://127.0.0.1:${firstDirect.hubPort}/readyz`)).ok).toBe(true);
            expect(
              (await fetch(`http://127.0.0.1:${secondDirect.directMedia.gatewayPort}/readyz`)).ok,
            ).toBe(true);
            await invoke("one", "stop");
            expect((await fetch(`http://127.0.0.1:${second.hubPort}/readyz`)).ok).toBe(true);
            expect(
              JSON.parse(await NodeFSP.readFile(NodePath.join(root, "hosts/two/hub.json"), "utf8"))
                .owner,
            ).toBe("two");
          } finally {
            for (const [owner, generation] of directGenerations)
              await invoke(owner, "stop-direct", false, generation).catch(() => {});
            await invoke("one", "stop").catch(() => {});
            await invoke("two", "stop").catch(() => {});
          }
        } finally {
          await NodeFSP.rm(home, { recursive: true, force: true });
        }
      });
    }),
  );
});
