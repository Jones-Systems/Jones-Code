// oxlint-disable t3code/no-wall-clock-in-tests -- Real OS consumer deadlines and cancellation bounds require the monotonic host clock; fake clocks cannot bound these children.
// oxlint-disable t3code/no-test-timeout-or-unreasoned-skip -- Six real Linux lifecycle cases own finite child lifetimes and a 24s cleanup deadline; the 27s runner ceiling permits that cleanup. Unsupported hosts are excluded before allocation.
// @effect-diagnostics nodeBuiltinImport:off -- Bounded real Linux consumer-family harness qualification.
import * as NodeFS from "node:fs";
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeURL from "node:url";
import * as NodePath from "node:path";
import * as NodeTimers from "node:timers";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { beforeAll, describe, expect, it } from "vite-plus/test";
import {
  acquireFamilyFixture,
  makeFamilyFixtureReceiver,
  retainFamilyFixtureFailure,
  assertNaturalFamilyStreams,
  completeFamilyFixture,
  disposeFamilyObserver,
  observeFamilyStreams,
  readFamilyReceipt,
  type FamilyFixtureManifest,
  makeArtifactCustody,
  probeConsumerFamily,
  superviseConsumer,
} from "./sourceRenewalArtifacts.ts";

const fixtureSource = `
import * as fs from 'node:fs';
const deadline = setTimeout(() => process.exit(19), 18000);
deadline.unref();
process.on('message', ({id, command, args}) => {
  if (command === 'identity') {
    const fds = fs.readdirSync('/proc/self/fd').flatMap(fd => {
      try { const s = fs.fstatSync(Number(fd)); return [[s.dev, s.ino]]; } catch { return []; }
    });
    process.send({id, value: {pid: process.pid, fds}});
  } else if (command === 'echo') process.send({id, value: args});
  else if (command === 'close') {
    process.send({id, value: {closed: true}}, () => process.disconnect());
  }
});
`;

type Mode = "survivors" | "abort" | "supervisor-loss" | "overflow";
function adversarialSource(mode: Mode) {
  const descendant =
    mode === "survivors"
      ? `
import os,signal,time
signal.alarm(18)
if os.fork(): os._exit(0)
os.setsid()
if os.fork(): os._exit(0)
signal.alarm(18)
print("ready", flush=True)
time.sleep(18)
`
      : `
import os,signal,time
signal.alarm(18)
for _ in range(9):
    if not os.fork():
        signal.alarm(2)
        time.sleep(1)
        os._exit(0)
print("ready", flush=True)
os._exit(0)
`;
  return `
import * as cp from 'node:child_process';
const mode = ${JSON.stringify(mode)};
const deadline = setTimeout(() => process.exit(0), mode === 'supervisor-loss' ? 2000 : 18000);
// Keep the TERM-ignoring worker alive after IPC disconnect so parent-loss cleanup,
// rather than Node event-loop exhaustion, owns its exit. The timer still bounds it.
if (mode !== 'abort') deadline.unref();
if (mode === 'abort') process.on('SIGTERM', () => {});
let started;
const descendants = () => started ??= new Promise((resolve, reject) => {
  const child = cp.spawn('/usr/bin/python3', ['-I', '-B', '-c', ${JSON.stringify(descendant)}], {stdio:['ignore','pipe','inherit']});
  child.on('error', reject);
  child.stdout.once('data', () => { child.stdout.destroy(); child.unref(); resolve(); });
});
process.on('message', async ({id, command}) => {
  if (command === 'identity') {
    if (mode === 'survivors') await descendants();
    process.send({id, value:{pid:process.pid}}, () => {
      if (mode === 'supervisor-loss') process.kill(process.ppid, 'SIGKILL');
    });
  } else if (command === 'close' && mode !== 'abort' && mode !== 'supervisor-loss') {
    if (mode === 'overflow') await descendants();
    process.send({id, value:{closed:true}}, () => { process.disconnect(); process.exit(0); });
  }
});
`;
}
const fixtureBudgetMs = 24_000;
const manifestLimit = 8_192;
const namespace = NodePath.resolve(
  import.meta.dirname,
  "../../../../../../node_modules/.cache/source-renewal-family-repair-v1",
);
function birthIdentity(pid: number) {
  const raw = NodeFS.readFileSync(`/proc/${pid}/stat`, "utf8");
  const fields = raw.slice(raw.lastIndexOf(")") + 2).split(" ");
  const result = [pid, Number(fields[19])];
  if (!result.every((value) => Number.isSafeInteger(value) && value > 0))
    throw new Error("Birth identity unavailable");
  return result;
}
function allocateFixture() {
  const deadline = performance.now() + fixtureBudgetMs;
  NodeFS.mkdirSync(namespace, { recursive: true, mode: 0o700 });
  const run = NodeFS.mkdtempSync(NodePath.join(namespace, "run-"));
  const root = NodePath.join(run, "scratch");
  NodeFS.mkdirSync(root, { mode: 0o700 });
  const stat = NodeFS.lstatSync(root);
  const binding = { custodyRoot: root, custodyRootIdentity: [stat.dev, stat.ino, stat.uid] };
  const persist = (name: string, value: unknown) => {
    const fd = NodeFS.openSync(NodePath.join(run, name), "wx", 0o600);
    try {
      NodeFS.writeFileSync(fd, JSON.stringify(value));
      NodeFS.fsyncSync(fd);
    } finally {
      NodeFS.closeSync(fd);
    }
    const directory = NodeFS.openSync(
      run,
      NodeFS.constants.O_RDONLY | NodeFS.constants.O_DIRECTORY,
    );
    try {
      NodeFS.fsyncSync(directory);
    } finally {
      NodeFS.closeSync(directory);
    }
  };
  persist("allocation.json", {
    fixtureId: run,
    ...binding,
    controllerIdentity: birthIdentity(process.pid),
    deadlineMs: fixtureBudgetMs,
  });
  const journal = (value: unknown) => {
    const fd = NodeFS.openSync(
      NodePath.join(run, "completion.jsonl"),
      NodeFS.constants.O_WRONLY |
        NodeFS.constants.O_APPEND |
        NodeFS.constants.O_CREAT |
        NodeFS.constants.O_NOFOLLOW,
      0o600,
    );
    try {
      const line = JSON.stringify(value) + "\n";
      if (NodeFS.fstatSync(fd).size + Buffer.byteLength(line) > 4 * 1024 * 1024)
        throw new Error("Fixture evidence journal bound exceeded");
      NodeFS.writeFileSync(fd, line);
      NodeFS.fsyncSync(fd);
    } finally {
      NodeFS.closeSync(fd);
    }
    const directory = NodeFS.openSync(
      run,
      NodeFS.constants.O_RDONLY | NodeFS.constants.O_DIRECTORY,
    );
    try {
      NodeFS.fsyncSync(directory);
    } finally {
      NodeFS.closeSync(directory);
    }
  };
  return {
    run,
    root,
    binding,
    persist,
    journal,
    remaining: () => Math.max(0, deadline - performance.now()),
  };
}
async function withinDeadline<A>(work: Promise<A>, millis: number): Promise<A> {
  let timer: ReturnType<typeof NodeTimers.setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        // @effect-diagnostics-next-line globalTimers:off -- Native observer deadline is always cleared in finally.
        timer = NodeTimers.setTimeout(
          () => reject(new Error("Fixture overall deadline exceeded")),
          millis,
        );
      }),
    ]);
  } finally {
    if (timer !== undefined) NodeTimers.clearTimeout(timer);
  }
}
function makeFixture(source: string) {
  const allocation = allocateFixture();
  const { root, binding, persist, run } = allocation;
  const custody = makeArtifactCustody(binding);
  const worker = NodePath.join(root, "consumer.mjs");
  NodeFS.writeFileSync(worker, source);
  let child!: ReturnType<typeof superviseConsumer>;
  let disposed = false;
  let handlesClosed = false;
  let finishFamily!: () => void;
  let finishDisconnect!: () => void;
  const ipcDisconnected = new Promise<void>((resolve) => {
    finishDisconnect = resolve;
  });
  const familyCompletion = new Promise<void>((resolve) => {
    finishFamily = resolve;
  });
  const records: Array<{ event: string; data: unknown }> = [];
  const order: string[] = [];
  const fixture = acquireFamilyFixture({
    fixtureId: run,
    birthIdentity,
    controllerIdentity: birthIdentity(process.pid),
    persist: allocation.journal,
    lifecycle: {
      custody,
      requested: {
        root,
        artifactDigest: "synthetic",
        build: { repository: "Jones-Systems/Jones-Code", sha: "synthetic", tree: "synthetic" },
        worker,
        workerSha256: "synthetic",
        runtime: { executable: process.execPath, version: process.version, sha256: "synthetic" },
      },
      record: (event, data) => {
        records.push({ event, data });
      },
      fork: (lease) => {
        child = superviseConsumer(custody, lease, {
          executable: NodeFS.realpathSync(process.execPath),
          args: [worker],
          cwd: root,
          env: { ...process.env },
        });
        return child;
      },
      acquired: (owned) => {
        child = owned;
        observeFamilyStreams(child);
        child.on("close", () => {
          handlesClosed = true;
          finishFamily();
        });
        child.on("message", (message: unknown) => {
          if ((message as { value?: { closed?: boolean } }).value?.closed) order.push("reply");
        });
        child.on("disconnect", () => {
          order.push("disconnect");
          finishDisconnect();
        });
      },
    },
  });
  const { lifecycle, ready } = fixture;
  const proof = () => {
    const manifest = fixture.manifest();
    if (!manifest || !handlesClosed) return undefined;
    assertNaturalFamilyStreams(observeFamilyStreams(child));
    try {
      return readFamilyReceipt(manifest, manifest.receiptPath);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
      throw error;
    }
  };
  const dispose = async (primaryFailure?: unknown) => {
    if (disposed) return;
    const manifest = fixture.manifest();
    if (!manifest) {
      disposed = true;
      retainFamilyFixtureFailure({
        diagnostics: fixture.diagnostics(),
        primaryFailure: primaryFailure ?? new Error("Exact worker manifest unavailable"),
        persist: (value) => persist("retained.json", value),
        disposeObserver: () => {
          const owned = fixture.child();
          if (owned) disposeFamilyObserver(owned);
        },
      });
    }
    const closing =
      allocation.remaining() > 0 ? lifecycle.close().catch(() => undefined) : Promise.resolve();
    await completeFamilyFixture({
      manifest,
      familyCompletion: Promise.all([closing, familyCompletion, ipcDisconnected]).then(
        () => undefined,
      ),
      streams: observeFamilyStreams(child),
      deadlineMs: allocation.remaining(),
      readReceipt: proof,
      allowInheritedStreamClosure: true,
      persist: (result) => allocation.journal({ result, records }),
      remove: () => {
        NodeFS.rmSync(root, { recursive: true });
      },
      disposeObserver: () => disposeFamilyObserver(child),
      primaryFailure: primaryFailure === undefined ? undefined : String(primaryFailure),
    });
    disposed = true;
  };
  return {
    ...allocation,
    custody,
    lifecycle,
    ready,
    proof,
    dispose,
    records,
    order,
    child: () => child,
  };
}
async function withFixture(
  source: string,
  body: (fixture: ReturnType<typeof makeFixture>) => Promise<void>,
) {
  const fixture = makeFixture(source);
  let primary: unknown;
  try {
    await withinDeadline(body(fixture), fixture.remaining());
  } catch (error) {
    primary = error;
  }
  try {
    await fixture.dispose(primary);
  } catch (cleanup) {
    if (primary !== undefined)
      throw new AggregateError([primary, cleanup], "Fixture assertion and cleanup failed", {
        cause: cleanup,
      });
    throw cleanup;
  }
  if (primary !== undefined) throw primary;
}

// Exclude unsupported hosts before hooks allocate files or start a process.
describe.skipIf(HostProcess.Platform.defaultValue() !== "linux")(
  "source renewal Linux family",
  () => {
    beforeAll(async () => {
      await probeConsumerFamily();
    });

    it("Gate 1: real factory lifecycle custody preserves JSON IPC and proves complete closure", async () => {
      await withFixture(fixtureSource, async (f) => {
        const identity = await f.ready<{ pid: number; fds: number[][] }>((identity) => {
          expect(identity.pid).toBeGreaterThan(0);
        });
        const payload = "x".repeat(256 * 1024);
        expect(await f.lifecycle.call("echo", payload, 0)).toBe(payload);
        await f.lifecycle.close();
        expect(f.order).toEqual(["reply", "disconnect"]);
        const proof = f.records.find((item) => item.event === "source-renewal family closed")!
          .data as {
          supervisorIdentity: number[];
          consumerIdentity: number[];
          controlIdentity: number[];
          ipcFdReleased: boolean;
        };
        expect(proof.supervisorIdentity[0]).toBe(f.child().pid);
        expect(proof.consumerIdentity[0]).toBe(identity.pid);
        expect(identity.pid).not.toBe(f.child().pid);
        expect(identity.fds).not.toContainEqual(proof.controlIdentity);
        expect(proof.ipcFdReleased).toBe(true);
        assertNaturalFamilyStreams(observeFamilyStreams(f.child()));
        expect(f.child().control!.destroyed).toBe(true);
        expect(f.child().connected).toBe(false);
        f.custody.assertRemoval();
        await f.custody.remove(async () => {
          // The shared disposer persists the independent diagnostic completion before deletion.
          await f.dispose();
        });
        expect(NodeFS.existsSync(f.root)).toBe(false);
      });
    }, 27_000);

    it("fences a double-forked setsid survivor while the private supervisor closes its family", async () => {
      await withFixture(adversarialSource("survivors"), async (f) => {
        await f.ready<{ pid: number }>((value) => {
          expect(value.pid).toBeGreaterThan(0);
        });
        await expect(f.lifecycle.close()).rejects.toThrow();
        expect(f.proof()).toMatchObject({ outcome: "survivors", familyClosed: true });
        expect(f.proof()!.signalsSent.length).toBeGreaterThan(0);
        expect(() => f.custody.assertRemoval()).toThrow("fenced");
      });
    }, 27_000);

    it("bounds cancellation of an unanswered call against a TERM-ignoring consumer", async () => {
      await withFixture(adversarialSource("abort"), async (f) => {
        await f.ready(() => undefined);
        const controller = new AbortController();
        const pending = f.lifecycle.call("unanswered", null, 0, controller.signal);
        const result = expect(pending).rejects.toThrow("fixture abort");
        controller.abort(new Error("fixture abort"));
        await result;
        const started = performance.now();
        await expect(f.lifecycle.close()).rejects.toThrow("fixture abort");
        expect(performance.now() - started).toBeLessThan(16_500);
        expect(f.proof()).toMatchObject({ familyClosed: true, outcome: "cancel" });
        expect(() => f.custody.assertRemoval()).toThrow("fenced");
      });
    }, 27_000);

    it("fences supervisor loss and waits for the finite outer-owned worker handles", async () => {
      await withFixture(adversarialSource("supervisor-loss"), async (f) => {
        await f.ready(() => undefined);
        await expect(f.lifecycle.close()).rejects.toThrow();
        expect(f.proof()).toBeUndefined();
        expect(() => f.custody.assertRemoval()).toThrow("fenced");
      });
    }, 27_000);

    it("fences tracking overflow even when all finite descendants are subsequently reaped", async () => {
      await withFixture(adversarialSource("overflow"), async (f) => {
        await f.ready(() => undefined);
        await expect(f.lifecycle.close()).rejects.toThrow();
        expect(f.proof()).toMatchObject({ processOverflow: true, familyClosed: true });
        expect(() => f.custody.assertRemoval()).toThrow("fenced");
      });
    }, 27_000);

    it("writes a parent-loss receipt that fresh custody cannot adopt", async () => {
      const allocation = allocateFixture();
      const { root, run, binding, persist } = allocation;
      const worker = NodePath.join(root, "consumer.mjs");
      NodeFS.writeFileSync(worker, adversarialSource("abort"));
      const module = NodeURL.pathToFileURL(
        NodePath.join(import.meta.dirname, "sourceRenewalArtifacts.ts"),
      ).href;
      const source = `
import * as NodeFS from 'node:fs';
import * as Net from 'node:net';
import {makeArtifactCustody, acquireFamilyFixture, runFamilyFixtureController, superviseConsumer} from ${JSON.stringify(module)};
const root = ${JSON.stringify(root)};
const controllerDeadline = performance.now()+8000;
const identity = pid => { const raw = NodeFS.readFileSync('/proc/'+pid+'/stat','utf8'); const id = [pid, Number(raw.slice(raw.lastIndexOf(')')+2).split(' ')[19])]; if (!id.every(v => Number.isSafeInteger(v) && v > 0)) throw Error('Birth identity unavailable'); return id; };
const persist = value => {
  const path = ${JSON.stringify(NodePath.join(run, "controller-acquisition.jsonl"))};
  const fd = NodeFS.openSync(path, NodeFS.constants.O_WRONLY|NodeFS.constants.O_APPEND|NodeFS.constants.O_CREAT|NodeFS.constants.O_NOFOLLOW, 0o600);
  try { const line = JSON.stringify(value)+'\\n'; if(NodeFS.fstatSync(fd).size + Buffer.byteLength(line) > 65536) throw Error('Controller evidence bound exceeded'); NodeFS.writeFileSync(fd,line); NodeFS.fsyncSync(fd); } finally { NodeFS.closeSync(fd); }
  const directory = NodeFS.openSync(${JSON.stringify(run)}, NodeFS.constants.O_RDONLY|NodeFS.constants.O_DIRECTORY);
  try { NodeFS.fsyncSync(directory); } finally { NodeFS.closeSync(directory); }
};
const custody = makeArtifactCustody(${JSON.stringify(binding)});
const fixture = acquireFamilyFixture({fixtureId:${JSON.stringify(run)}, birthIdentity:identity, controllerIdentity:identity(process.pid), persist, lifecycle:{custody, requested:{root,artifactDigest:'synthetic',build:{repository:'Jones-Systems/Jones-Code',sha:'synthetic',tree:'synthetic'},worker:${JSON.stringify(worker)},workerSha256:'synthetic',runtime:{executable:process.execPath,version:process.version,sha256:'synthetic'}}, record:()=>{}, fork: lease=>superviseConsumer(custody,lease,{executable:process.execPath,args:[${JSON.stringify(worker)}],cwd:root,env:{...process.env},fixtureInheritedOutput:{stdout:1,stderr:2}})}});
try {
  await runFamilyFixtureController({fixture, deadlineMs:Math.max(0,controllerDeadline-performance.now()), persist, sendManifest:manifest => {
    const encoded = JSON.stringify({manifest})+'\\n';
    if (Buffer.byteLength(encoded) > ${manifestLimit}) throw Error('Manifest bound exceeded');
    if (NodeFS.writeSync(3, encoded) !== Buffer.byteLength(encoded)) throw Error('Incomplete manifest delivery');
  }, acknowledgement:() => new Promise((resolve,reject) => {
    const stream = new Net.Socket({fd:4,readable:true,writable:false});
    let data = '';
    stream.on('data', chunk => { data += chunk; if (data.length > 128) reject(Error('Ack bound exceeded')); else if (data.endsWith('\\n')) resolve(data); });
    stream.on('error',reject); stream.on('end',()=>reject(Error('Ack missing')));
  })});
  process.exit(0);
} catch (error) {
  try { NodeFS.writeSync(3, JSON.stringify({failure:String(error),diagnostics:fixture.diagnostics()})+'\\n'); } finally { process.exit(21); }
}
`;
      const child = NodeChildProcess.spawn(
        process.execPath,
        ["--input-type=module", "-e", source],
        {
          cwd: root,
          stdio: ["ignore", "pipe", "pipe", "pipe", "pipe"],
        },
      );
      let scratchRemoved = false;
      let observerDisposed = false;
      const disposeObserver = () => {
        if (observerDisposed) return;
        observerDisposed = true;
        disposeFamilyObserver(child, [child.stdio[3], child.stdio[4]]);
      };
      const streams = observeFamilyStreams(child);
      let manifest: FamilyFixtureManifest | undefined;
      let boundProof: ReturnType<typeof readFamilyReceipt> | undefined;
      let primary: unknown;
      let receivedDiagnostics: unknown = null;
      let output = "";
      let outputBytes = 0;
      let controllerExited = false;
      const controllerExit = new Promise<void>((resolve, reject) => {
        child.on("error", reject);
        child.on("exit", (code, signal) => {
          controllerExited = true;
          if (code === 0 && signal === null) resolve();
          else reject(new Error(`Controller failed ${code}/${signal}; ${output}`));
        });
      });
      // Reject immediately into a retained promise so a pre-manifest exit cannot go unhandled.
      void controllerExit.catch(() => undefined);
      const familyCompletion = new Promise<void>((resolve, reject) => {
        let ends = 0;
        for (const stream of [child.stdout!, child.stderr!]) {
          stream.on("end", () => {
            if (++ends === 2) resolve();
          });
          stream.on("close", () => {
            if (!stream.readableEnded)
              reject(new Error("Outer family output closed before natural EOF"));
          });
          stream.on("error", reject);
          stream.on("data", (data) => {
            outputBytes += Buffer.byteLength(data);
            output = (output + String(data)).slice(-4096);
            if (outputBytes > 256 * 1024) reject(new Error("Outer output bound exceeded"));
          });
        }
      });
      void familyCompletion.catch(() => undefined);
      try {
        const controllerIdentity = birthIdentity(child.pid!);
        persist("controller.json", { ...binding, controllerIdentity });
        const receiver = makeFamilyFixtureReceiver({
          persist: (value) => {
            if ((value as { trust: string }).trust === "UNTRUSTED") receivedDiagnostics = value;
            allocation.journal(value);
          },
          validate: (candidate) => {
            expect(candidate.fixtureId).toBe(run);
            expect(candidate.custodyRoot).toBe(root);
            expect(candidate.custodyRootIdentity).toEqual(binding.custodyRootIdentity);
            expect(candidate.controllerIdentity).toEqual(controllerIdentity);
            expect(candidate.attempt).toBe(1);
            expect(candidate.nonce).toMatch(/^[0-9a-f-]{36}$/);
            const stem = NodePath.join(root, "closure", `${candidate.attempt}-${candidate.nonce}`);
            expect(candidate.receiptPath).toBe(`${stem}.receipt.json`);
            const raw = NodeFS.readFileSync(`${stem}.request.json`);
            expect(candidate.requestSha256).toBe(
              NodeCrypto.createHash("sha256").update(raw).digest("hex"),
            );
            for (const id of [candidate.supervisorIdentity, candidate.consumerIdentity]) {
              expect(id).toHaveLength(2);
              expect(birthIdentity(id[0]!)).toEqual(id);
            }
          },
        });
        const delivered = await withinDeadline(
          new Promise<FamilyFixtureManifest>((resolve, reject) => {
            let data = "";
            let accepting = true;
            const channel = child.stdio[3]!;
            child.stdio[4]?.on("error", reject);
            channel.on("error", reject);
            channel.on("data", (chunk) => {
              if (!accepting) return;
              if (Buffer.byteLength(data) + Buffer.byteLength(chunk) > manifestLimit) {
                accepting = false;
                reject(new Error("Manifest channel bound exceeded"));
                return;
              }
              data += String(chunk);
              try {
                receiver.capture(data);
              } catch (error) {
                accepting = false;
                reject(error);
                return;
              }
              if (!data.endsWith("\n")) return;
              accepting = false;
              try {
                const candidate = receiver.receive(data);
                persist("manifest.json", candidate);
                manifest = candidate;
                if (controllerExited)
                  throw new Error("Controller exited before manifest acknowledgement");
                const acknowledgement = child.stdio[4] as import("node:stream").Duplex;
                acknowledgement.write(candidate.nonce + "\n", (error) => {
                  if (error) reject(error);
                  else resolve(candidate);
                });
              } catch (error) {
                reject(error);
              }
            });
            channel.on("end", () => {
              if (!manifest) reject(new Error("Manifest channel ended without custody"));
            });
          }),
          allocation.remaining(),
        );
        await completeFamilyFixture({
          manifest: delivered,
          controllerExit,
          familyCompletion,
          streams,
          deadlineMs: allocation.remaining(),
          readReceipt: () => {
            boundProof = readFamilyReceipt(delivered, delivered.receiptPath);
            return boundProof;
          },
          expectedOutcome: "parent-loss",
          allowInheritedStreamClosure: false,
          persist: allocation.journal,
          remove: () => {
            NodeFS.rmSync(root, { recursive: true });
            scratchRemoved = true;
          },
          disposeObserver,
        });
        const proof = boundProof!;
        expect(proof.consumerExit).toEqual({ code: null, signal: "SIGKILL" });
        expect(proof.signalsSent).toEqual(
          expect.arrayContaining([
            { identity: proof.consumerIdentity, signal: 15 },
            { identity: proof.consumerIdentity, signal: 9 },
          ]),
        );
        const fresh = makeArtifactCustody(binding);
        expect(() =>
          fresh.settle(proof.attempt, proof, {
            supervisor: {
              pid: proof.supervisorIdentity[0]!,
              exit: { code: 0, signal: null },
              handlesClosed: true,
            },
            streams,
            ipc: { disconnected: true },
            worker: { returnedPid: proof.consumerIdentity[0]! },
            control: "none",
            outputBytes: 0,
          }),
        ).toThrow("Unknown or duplicate");
      } catch (error) {
        primary = error;
        retainFamilyFixtureFailure({
          diagnostics: {
            manifest: manifest ?? null,
            received: receivedDiagnostics,
            ...binding,
            output,
          },
          primaryFailure: error,
          retained: !scratchRemoved,
          persist: (value) => persist("retained.json", value),
          disposeObserver,
        });
      } finally {
        // These dedicated observer channels never authorize receipt reads or scratch deletion.
        disposeObserver();
        if (primary)
          process.stderr.write(`Retained parent-loss fixture ${root}; exact evidence ${run}\n`);
      }
    }, 27_000);
  },
);
