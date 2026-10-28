// oxlint-disable t3code/no-real-time-wait-in-tests -- Bounded OS absence probes after captured parent loss are cleanup evidence; application milestones use typed receipts.
// @effect-diagnostics nodeBuiltinImport:off globalTimers:off -- Real captured process/FD3 qualification, only with an explicit admitted command manifest.
import { spawn } from "node:child_process";
import { mkdtemp, realpath, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { createInterface } from "node:readline";
import { setTimeout as pause } from "node:timers/promises";
import { expect, it } from "vite-plus/test";
import { runNativeQualification } from "./nativeQualification.ts";
import { minimalWorkerEnvironment } from "../serverCompute/workerCommand.ts";
import { proveAbsent } from "../serverCompute/journal.ts";

const manifestPath = process.env.JONES_SP_COMMAND_MANIFEST;
// Missing packaging inputs are an explicit skip/unknown, never passing evidence.
const native = manifestPath ? it : it.skip;
for (const count of [2, 3] as const)
  native(
    `real native ${count}-way combined load and whole-cohort retirement`,
    async () => {
      const manifest = JSON.parse(await readFile(manifestPath!, "utf8"));
      const scratch = await realpath(await mkdtemp(join(tmpdir(), "jones-sp-native-test-")));
      let retired = false;
      try {
        const report = await runNativeQualification({
          count,
          directory: join(scratch, "fixture"),
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
        expect(report.rotations).toBeGreaterThan(0);
        expect(
          report.churnWindows.every((window) => Object.keys(window.failures).length === 0),
        ).toBe(true);
      } finally {
        if (retired) await rm(scratch, { recursive: true });
      }
    },
    600_000,
  );

native(
  "guardian retires productive native children after captured parent SIGKILL",
  async () => {
    const scratch = await realpath(await mkdtemp(join(tmpdir(), "jones-sp-parent-loss-")));
    const fixture = fileURLToPath(
      new URL(
        "../../../../../scripts/jones/serverParallelism/parent-loss-fixture.mjs",
        import.meta.url,
      ),
    );
    const parent = spawn(process.execPath, [fixture, manifestPath!, scratch], {
      stdio: ["ignore", "pipe", "pipe"],
      env: minimalWorkerEnvironment(process.env, false, false),
    });
    const exited = new Promise<void>((resolve, reject) => {
      parent.once("exit", () => resolve());
      parent.once("error", reject);
    });
    const lines = createInterface({ input: parent.stdout! });
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
      expect(children).toHaveLength(2);
      parent.kill("SIGKILL"); // Only this test's captured parent handle.
      await exited;
      // Restart admission uses the same bounded OS absence predicate, never kills
      // journal-selected PIDs. Observation cadence supplies no cleanup evidence.
      const deadline = performance.now() + 5_000;
      while (performance.now() < deadline && !children.every(proveAbsent)) await pause(20);
      proved = children.every(proveAbsent);
      expect(proved).toBe(true);
    } finally {
      clearTimeout(timer);
      lines.close();
      if (parent.exitCode === null && parent.signalCode === null) {
        parent.kill("SIGKILL");
        await exited;
      }
      if (proved) await rm(scratch, { recursive: true });
    }
  },
  180_000,
);

for (const invalid of ["bootstrap", "missing-fd3"] as const)
  native(
    `private dispatch fails closed for ${invalid}`,
    async () => {
      const manifest = JSON.parse(await readFile(manifestPath!, "utf8"));
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
      const child = spawn(manifest.file, [...manifest.args, JSON.stringify(boot)], {
        stdio: ["pipe", "pipe", "pipe"], // FD3 deliberately absent; never a provider/server child.
        env: minimalWorkerEnvironment(
          process.env,
          manifest.runtimeForm === "sea",
          manifest.runtimeForm === "electron",
        ),
      });
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
