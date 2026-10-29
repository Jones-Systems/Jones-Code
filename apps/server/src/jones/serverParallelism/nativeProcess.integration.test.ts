// oxlint-disable t3code/no-real-time-wait-in-tests -- Bounded OS absence probes after captured parent loss are cleanup evidence; application milestones use typed receipts.
// @effect-diagnostics nodeBuiltinImport:off globalTimers:off -- Real captured process/FD3 qualification, only with an explicit admitted command manifest.
import * as NodeChildProcess from "node:child_process";
import * as NodeEvents from "node:events";
import * as NodeStream from "node:stream";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeReadline from "node:readline";
import * as NodeTimersPromises from "node:timers/promises";
import { expect, it } from "vite-plus/test";
import { runNativeQualification } from "./nativeQualification.ts";
import { minimalWorkerEnvironment } from "../serverCompute/workerCommand.ts";
import { proveAbsent, GenerationJournal } from "../serverCompute/journal.ts";
import { ComputeSupervisor } from "../serverCompute/supervisor.ts";
import { ProcessSlot } from "../serverCompute/processSlot.ts";

const manifestPath = process.env.JONES_SP_COMMAND_MANIFEST;
const parentLossCount = process.env.JONES_SP_PROFILE === "vps" ? 3 : 2;
// Missing packaging inputs are an explicit skip/unknown, never passing evidence.
const native = it.skipIf(!manifestPath);
for (const count of [2, 3] as const)
  native(
    `real native ${count}-way combined load and whole-cohort retirement`,
    async () => {
      const manifest = JSON.parse(await NodeFSP.readFile(manifestPath!, "utf8"));
      const scratch = await NodeFSP.realpath(
        await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-sp-native-test-")),
      );
      let retired = false;
      try {
        const report = await runNativeQualification({
          count,
          onShutdown: () => {
            retired = true;
          },
          directory: NodePath.join(scratch, "fixture"),
          command: {
            ...manifest,
            env: minimalWorkerEnvironment(
              process.env,
              manifest.runtimeForm === "sea",
              manifest.runtimeForm === "electron",
            ),
          },
        });
        retired = report.shutdownProved;
        expect(report.productiveOverlapSamples).toBeGreaterThan(0);
        expect(report.predicates.normalTargets).toBe(true);
        expect(report.predicates.resourcesComplete).toBe(true);
        expect(report.predicates.childRssWithinBudget).toBe(true);
        expect(report.predicates.serializedNegativeControl).toBe(true);
        expect(report.predicates.noSteadyChurn).toBe(true);
        expect(report.rotations).toBeGreaterThan(0);
        expect(
          report.churnWindows.every((window) => Object.keys(window.failures).length === 0),
        ).toBe(true);
      } finally {
        if (retired) await NodeFSP.rm(scratch, { recursive: true });
      }
    },
    600_000,
  );

native(
  "guardian retires productive native children after captured parent SIGKILL",
  async () => {
    const scratch = await NodeFSP.realpath(
      await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-sp-parent-loss-")),
    );
    const fixture = NodeURL.fileURLToPath(
      new URL(
        "../../../../../scripts/jones/serverParallelism/parent-loss-fixture.mjs",
        import.meta.url,
      ),
    );
    const parent = NodeChildProcess.spawn(
      process.execPath,
      [fixture, manifestPath!, scratch, String(parentLossCount)],
      {
        stdio: ["ignore", "pipe", "pipe"],
        env: minimalWorkerEnvironment(process.env, false, false),
      },
    );
    const exited = new Promise<void>((resolve, reject) => {
      parent.once("exit", () => resolve());
      parent.once("error", reject);
    });
    const lines = NodeReadline.createInterface({ input: parent.stdout! });
    let children: number[] = [],
      proved = false;
    parent.stderr!.resume();
    let timer: NodeJS.Timeout | undefined;
    try {
      children = await Promise.race([
        (async () => {
          for await (const line of lines) {
            const receipt = JSON.parse(line);
            if (receipt.type === "productive-overlap") return receipt.pids as number[];
          }
          throw new Error("Parent finished without productive receipt");
        })(),
        new Promise<never>((_resolve, reject) => {
          timer = setTimeout(() => reject(new Error("No productive native receipt")), 120_000);
        }),
      ]);
      expect(children).toHaveLength(parentLossCount);
      parent.kill("SIGKILL"); // Only this test's captured parent handle.
      await exited;
      // Restart admission uses the same bounded OS absence predicate, never kills
      // journal-selected PIDs. Observation cadence supplies no cleanup evidence.
      // oxlint-disable-next-line t3code/no-wall-clock-in-tests -- Real captured-process absence has an OS deadline; TestClock cannot advance child retirement.
      const deadline = performance.now() + 5_000;
      // oxlint-disable-next-line t3code/no-wall-clock-in-tests -- This bounded cleanup probe measures OS process retirement, not application latency.
      while (performance.now() < deadline && !children.every(proveAbsent))
        await NodeTimersPromises.setTimeout(20);
      proved = children.every(proveAbsent);
      expect(proved).toBe(true);
      const manifest = JSON.parse(await NodeFSP.readFile(manifestPath!, "utf8"));
      const command = {
        ...manifest,
        env: minimalWorkerEnvironment(
          process.env,
          manifest.runtimeForm === "sea",
          manifest.runtimeForm === "electron",
        ),
      };
      proved = false;
      const journal = await GenerationJournal.acquire(
        NodePath.join(scratch, "parent-loss", "journal"),
        parentLossCount,
      );
      const replacement = new ComputeSupervisor({
        count: parentLossCount,
        build: command.build,
        journal,
        start: (boot, register, onExit) => ProcessSlot.start(command, boot, register, onExit),
      });
      proved = false;
      try {
        const handle = replacement.register({
          cwd: NodePath.join(scratch, "parent-loss", "workspace-0"),
          variant: "paths",
        });
        await replacement.ensure(handle);
        const result = await replacement.run(handle, "search", { query: "file", limit: 32 });
        expect(result.entries.length).toBeGreaterThan(0);
      } finally {
        await replacement.shutdown();
        proved = true;
      }
    } finally {
      clearTimeout(timer);
      lines.close();
      if (parent.exitCode === null && parent.signalCode === null) {
        parent.kill("SIGKILL");
        await exited;
      }
      if (proved) await NodeFSP.rm(scratch, { recursive: true });
    }
  },
  180_000,
);

function observeWrongBuild(
  child: NodeEvents.EventEmitter,
  streams: {
    input: NodeStream.Writable | null;
    output: NodeStream.Readable | null;
    diagnostics: NodeStream.Readable | null;
    control: NodeStream.Readable | null | undefined;
  },
) {
  let failure: Error | undefined;
  let exit: { code: number | null; signal: string | null } | undefined;
  let closed: { code: number | null; signal: string | null } | undefined;
  let diagnostics = Buffer.alloc(0);
  let drainComplete = false;
  const disposers: (() => void)[] = [];
  let rejectFailure!: (cause: Error) => void;
  const failed = new Promise<never>((_resolve, reject) => {
    rejectFailure = reject;
  });
  // A failure may arrive while cleanup is draining, after the race has settled.
  void failed.catch(() => {});
  const fail = (message: string, cause?: unknown) => {
    if (failure) return;
    failure = new Error(message, { cause });
    rejectFailure(failure);
  };
  const onError = (cause: Error) => fail("Wrong-build spawn/observer error", cause);
  child.once("error", onError);
  disposers.push(() => child.off("error", onError));
  if (streams.input) {
    const input = streams.input;
    input.on("error", onError);
    disposers.push(() => input.off("error", onError));
  } else fail("Wrong-build missing stdin endpoint");
  const onExit = (code: number | null, signal: string | null) => {
    exit = { code, signal };
  };
  child.once("exit", onExit);
  disposers.push(() => child.off("exit", onExit));
  const childClosed = new Promise<void>((resolve) => {
    const onClose = (code: number | null, signal: string | null) => {
      closed = { code, signal };
      resolve();
    };
    child.once("close", onClose);
    disposers.push(() => child.off("close", onClose));
  });
  const drain = (
    stream: NodeStream.Readable | null | undefined,
    channel: string,
    forbidden: boolean,
  ) => {
    if (!stream) {
      fail(`Wrong-build missing ${channel} endpoint`);
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => {
      const onData = (chunk: Buffer) => {
        // Retain only a diagnostic tail; even one byte is forbidden on stdout/FD3.
        diagnostics = Buffer.concat([diagnostics, chunk.subarray(-4096)]).subarray(-4096);
        if (forbidden && chunk.length > 0) fail(`Wrong-build emitted ${channel} data`);
      };
      const onEnd = () => resolve();
      const onClose = () => {
        if (!stream.readableEnded) fail(`Wrong-build ${channel} closed before drain`);
        resolve();
      };
      const onStreamError = (cause: Error) => {
        fail(`Wrong-build ${channel} observer error`, cause);
        resolve();
      };
      stream.on("data", onData);
      stream.once("end", onEnd);
      stream.once("close", onClose);
      stream.on("error", onStreamError);
      disposers.push(() => {
        stream.off("data", onData);
        stream.off("end", onEnd);
        stream.off("close", onClose);
        stream.off("error", onStreamError);
      });
    });
  };
  // Install flowing readers synchronously, before waiting for exit or sending anything.
  const drained = Promise.all([
    childClosed,
    drain(streams.output, "stdout", true),
    drain(streams.control, "FD3", true),
    drain(streams.diagnostics, "stderr", false),
  ]).then(() => {
    drainComplete = true;
  });
  return {
    failed,
    drained,
    fail,
    assertRejected() {
      if (failure) throw failure;
      if (
        !drainComplete ||
        !closed ||
        !exit ||
        exit.code === null ||
        exit.code === 0 ||
        exit.signal !== null ||
        closed.code !== exit.code ||
        closed.signal !== exit.signal
      )
        throw new Error("Wrong-build did not reject with a natural unsignaled nonzero exit");
    },
    diagnostics: () => diagnostics.toString("utf8"),
    dispose() {
      streams.input?.destroy();
      streams.output?.destroy();
      streams.control?.destroy();
      streams.diagnostics?.destroy();
      for (const dispose of disposers) dispose();
    },
  };
}

async function expectWrongBuildRejection(child: NodeChildProcess.ChildProcess) {
  const observer = observeWrongBuild(child, {
    input: child.stdin,
    output: child.stdout,
    diagnostics: child.stderr,
    control: child.stdio[3] as NodeStream.Duplex | null | undefined,
  });
  let timer: NodeJS.Timeout | undefined;
  let cleanupTimer: NodeJS.Timeout | undefined;
  let failure: unknown;
  let cleanupFailure: unknown;
  try {
    await Promise.race([
      observer.drained.then(() => observer.assertRejected()),
      observer.failed,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error("Wrong-build rejection timed out")), 15_000);
      }),
    ]);
  } catch (cause) {
    failure = cause;
  } finally {
    clearTimeout(timer);
    try {
      if (child.pid && child.exitCode === null && child.signalCode === null) {
        observer.fail("Wrong-build required test cleanup SIGKILL");
        child.kill("SIGKILL"); // Only the captured child; never a journal or process lookup.
      }
      await Promise.race([
        observer.drained,
        new Promise<never>((_resolve, reject) => {
          cleanupTimer = setTimeout(
            () => reject(new Error("Captured wrong-build child cleanup/drain unproved")),
            5_000,
          );
        }),
      ]);
    } catch (cause) {
      cleanupFailure = cause;
    } finally {
      clearTimeout(cleanupTimer);
      observer.dispose();
    }
  }
  if (failure === undefined && cleanupFailure === undefined) observer.assertRejected();
  if (failure !== undefined || cleanupFailure !== undefined)
    throw new AggregateError(
      [failure, cleanupFailure].filter((cause) => cause !== undefined),
      `Wrong-build rejection failed: ${String(failure ?? cleanupFailure)}; diagnostics: ${observer.diagnostics()}`,
      { cause: failure ?? cleanupFailure },
    );
}

for (const order of ["buffered-before-exit", "ready-then-nonzero"] as const)
  it(`private dispatch observer rejects ${order} through stream drain`, async () => {
    const child = new NodeEvents.EventEmitter();
    const streams = {
      input: new NodeStream.PassThrough(),
      output: new NodeStream.PassThrough(),
      diagnostics: new NodeStream.PassThrough(),
      control: new NodeStream.PassThrough(),
    };
    const ready = Buffer.from('{"type":"ready"}\n');
    if (order === "buffered-before-exit") streams.control.write(ready);
    const observer = observeWrongBuild(child, streams);
    try {
      if (order === "ready-then-nonzero") streams.control.write(ready);
      child.emit("exit", 1, null);
      child.emit("close", 1, null);
      streams.output.end();
      streams.control.end();
      streams.diagnostics.end();
      await observer.drained;
      expect(() => observer.assertRejected()).toThrow("Wrong-build emitted FD3 data");
    } finally {
      observer.dispose();
    }
  });

for (const invalid of [
  "stdout",
  "malformed",
  "truncated",
  "signal",
  "zero",
  "error",
  "observer-error",
  "premature-close",
  "cleanup",
] as const)
  it(`private dispatch observer rejects ${invalid}`, async () => {
    const child = new NodeEvents.EventEmitter();
    const streams = {
      input: new NodeStream.PassThrough(),
      output: new NodeStream.PassThrough(),
      diagnostics: new NodeStream.PassThrough(),
      control: new NodeStream.PassThrough(),
    };
    const observer = observeWrongBuild(child, streams);
    try {
      if (invalid === "stdout") streams.output.write('{"type":"result"}\n');
      if (invalid === "malformed") streams.control.write("not-json\n");
      if (invalid === "truncated") streams.control.write('{"type":"rea');
      if (invalid === "error") child.emit("error", new Error("spawn failed"));
      if (invalid === "observer-error") streams.control.emit("error", new Error("read failed"));
      if (invalid === "cleanup") observer.fail("Wrong-build required test cleanup SIGKILL");
      if (invalid === "premature-close") streams.control.destroy();
      const code = invalid === "signal" ? null : invalid === "zero" ? 0 : 1;
      const signal = invalid === "signal" ? "SIGKILL" : null;
      child.emit("exit", code, signal);
      child.emit("close", code, signal);
      streams.output.end();
      if (invalid !== "premature-close") streams.control.end();
      streams.diagnostics.end();
      await observer.drained;
      expect(() => observer.assertRejected()).toThrow();
    } finally {
      observer.dispose();
    }
  });

it("private dispatch observer accepts only a drained natural rejection", async () => {
  const child = new NodeEvents.EventEmitter();
  const streams = {
    input: new NodeStream.PassThrough(),
    output: new NodeStream.PassThrough(),
    diagnostics: new NodeStream.PassThrough(),
    control: new NodeStream.PassThrough(),
  };
  const observer = observeWrongBuild(child, streams);
  try {
    streams.diagnostics.write("expected bootstrap rejection");
    child.emit("exit", 1, null);
    expect(() => observer.assertRejected()).toThrow();
    child.emit("close", 1, null);
    expect(() => observer.assertRejected()).toThrow();
    streams.output.end();
    streams.control.end();
    streams.diagnostics.end();
    await observer.drained;
    expect(() => observer.assertRejected()).not.toThrow();
  } finally {
    observer.dispose();
  }
});

native(
  "private dispatch admits matching build and useful work",
  async () => {
    const manifest = JSON.parse(await NodeFSP.readFile(manifestPath!, "utf8"));
    const scratch = await NodeFSP.realpath(
      await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-sp-matching-build-")),
    );
    let retired = true;
    let supervisor: ComputeSupervisor | undefined;
    let failure: unknown;
    const cleanupFailures: unknown[] = [];
    try {
      const workspace = NodePath.join(scratch, "workspace");
      await NodeFSP.mkdir(workspace);
      await NodeFSP.writeFile(NodePath.join(workspace, "matching-build.txt"), "matching smoke\n");
      const command = {
        ...manifest,
        env: minimalWorkerEnvironment(
          process.env,
          manifest.runtimeForm === "sea",
          manifest.runtimeForm === "electron",
        ),
      };
      const journal = await GenerationJournal.acquire(
        NodePath.join(scratch, "journal"),
        parentLossCount,
      );
      supervisor = new ComputeSupervisor({
        count: parentLossCount,
        build: command.build,
        journal,
        start: (boot, register, onExit) => ProcessSlot.start(command, boot, register, onExit),
      });
      retired = false;
      const handle = supervisor.register({ cwd: workspace, variant: "paths" });
      await supervisor.ensure(handle);
      const result = await supervisor.run(handle, "search", { query: "matching-build", limit: 32 });
      expect(result.entries.length).toBeGreaterThan(0);
    } catch (cause) {
      failure = cause;
    } finally {
      if (supervisor) {
        try {
          await supervisor.shutdown();
          retired = true;
        } catch (cause) {
          cleanupFailures.push(cause);
        }
      }
      if (retired) {
        try {
          await NodeFSP.rm(scratch, { recursive: true });
        } catch (cause) {
          cleanupFailures.push(cause);
        }
      }
    }
    if (failure !== undefined || cleanupFailures.length > 0)
      throw new AggregateError(
        [failure, ...cleanupFailures].filter((cause) => cause !== undefined),
        "Matching-build smoke failed",
        { cause: failure ?? cleanupFailures[0] },
      );
  },
  60_000,
);

native(
  "private dispatch fails closed for wrong-build",
  async () => {
    const manifest = JSON.parse(await NodeFSP.readFile(manifestPath!, "utf8"));
    const boot = {
      version: 1,
      build: (manifest.build === "0".repeat(64) ? "1" : "0").repeat(64),
      authorityId: "negative-smoke",
      nonce: "negative-smoke",
      slotId: 0,
      slotGeneration: 1,
    };
    const child = NodeChildProcess.spawn(
      manifest.file,
      [...manifest.args, JSON.stringify(boot)],
      {
        stdio: ["pipe", "pipe", "pipe", "pipe"],
        env: minimalWorkerEnvironment(
          process.env,
          manifest.runtimeForm === "sea",
          manifest.runtimeForm === "electron",
        ),
      },
    );
    // Keep stdin and FD3 valid and send no GO: any admission already defeats rejection.
    await expectWrongBuildRejection(child);
  },
  30_000,
);

for (const invalid of ["bootstrap", "missing-fd3"] as const)
  native(
    `private dispatch fails closed for ${invalid}`,
    async () => {
      const manifest = JSON.parse(await NodeFSP.readFile(manifestPath!, "utf8"));
      const boot =
        invalid === "bootstrap"
          ? {}
          : {
              version: 1,
              build: manifest.build,
              authorityId: "negative-smoke",
              nonce: "negative-smoke",
              slotId: 0,
              slotGeneration: 1,
            };
      const child = NodeChildProcess.spawn(
        manifest.file,
        [...manifest.args, JSON.stringify(boot)],
        {
          stdio: ["pipe", "pipe", "pipe"],
          env: minimalWorkerEnvironment(
            process.env,
            manifest.runtimeForm === "sea",
            manifest.runtimeForm === "electron",
          ),
        },
      );
      child.stdin!.end();
      child.stdout!.resume();
      child.stderr!.resume();
      const exited = new Promise<{ code: number | null; signal: string | null }>(
        (resolve, reject) => {
          child.once("exit", (code, signal) => resolve({ code, signal }));
          child.once("error", reject);
        },
      );
      let timer: NodeJS.Timeout | undefined;
      try {
        const result = await Promise.race([
          exited,
          new Promise<never>((_resolve, reject) => {
            timer = setTimeout(() => reject(new Error("Private dispatch failed to close")), 15_000);
          }),
        ]);
        expect(result.code !== 0 || result.signal !== null).toBe(true);
      } finally {
        clearTimeout(timer);
        if (child.exitCode === null && child.signalCode === null) {
          child.kill("SIGKILL");
          await exited;
        }
      }
    },
    30_000,
  );

const blockReceiptPath = process.env.JONES_SP_NATIVE_BLOCK_RECEIPT;
const nativeReceipt = it.skipIf(!manifestPath || !blockReceiptPath);
nativeReceipt(
  "guardian native barrier receipts retain the OS witness and same-journal replacement",
  async () => {
    const receipt = JSON.parse(await NodeFSP.readFile(blockReceiptPath!, "utf8"));
    const manifest = JSON.parse(await NodeFSP.readFile(manifestPath!, "utf8"));
    // The separate CI barrier command produces this receipt; a successful test subprocess is insufficient.
    expect(receipt.proof.phase).toBe("fff-create/log-init/fifo-open");
    expect(receipt.proof.subject.build).toBe(manifest.build);
    expect(receipt.proof.profile).toBe(parentLossCount === 3 ? "vps" : "laptop");
    expect(receipt.proof.form).toBe(manifest.runtimeForm);
    expect(receipt.proof.blockerHeldThroughKill).toBe(true);
    expect(receipt.proof.parentCapturedSigkill).toBe(true);
    expect(receipt.proof.childrenAbsent).toBe(true);
    expect(receipt.proof.sameJournalRecovered).toBe(true);
    expect(receipt.proof.replacementUseful).toBe(true);
    expect(receipt.proof.negativeControl).toEqual({
      witnessed: true,
      exactFifoReleased: true,
      normalResult: true,
      cleanRetirement: true,
    });
    expect(receipt.proof.noWitnessRejected).toBe(true);
    expect(receipt.predicate).toBe(true);
  },
);
