// @effect-diagnostics nodeBuiltinImport:off globalTimers:off
// Serving-loop cost of one heavy preparation job, off-process versus the in-process path it
// replaced. The in-process run is the negative control: it must visibly load the loop.
import * as NodePerfHooks from "node:perf_hooks";
import { expect, it } from "vite-plus/test";
import { verifyStagedQualifiedRuntime } from "../../cloud/qualifiedRuntime.ts";
import { makePreparationSupervisor } from "./supervisor.ts";
import {
  BASELINE,
  fixtureHost,
  makePreparationHome,
  sourceWorkerCommand,
  stageFixtureRuntime,
} from "./Preparation.testkit.ts";

const PAYLOAD_BYTES = 128 * 1024 * 1024;
const TICK_MS = 5;

interface LoopCost {
  readonly elapsedMs: number;
  /** Time this thread's event loop spent running callbacks rather than waiting. */
  readonly activeMs: number;
  readonly utilization: number;
  readonly maxDelayMs: number;
  /** A 5 ms heartbeat timer's firings, against the firings an idle loop would deliver. */
  readonly ticks: number;
  readonly expectedTicks: number;
}

async function loopCost(work: () => Promise<unknown>): Promise<LoopCost> {
  const delay = NodePerfHooks.monitorEventLoopDelay({ resolution: 1 });
  let ticks = 0;
  const ticker = setInterval(() => {
    ticks += 1;
  }, TICK_MS);
  delay.enable();
  const before = NodePerfHooks.performance.eventLoopUtilization();
  const started = NodePerfHooks.performance.now();
  try {
    await work();
  } finally {
    clearInterval(ticker);
    delay.disable();
  }
  const elapsedMs = NodePerfHooks.performance.now() - started;
  const loop = NodePerfHooks.performance.eventLoopUtilization(before);
  return {
    elapsedMs,
    activeMs: loop.active,
    utilization: loop.utilization,
    maxDelayMs: delay.max / 1e6,
    ticks,
    expectedTicks: Math.floor(elapsedMs / TICK_MS),
  };
}

it("hashes a heavy staged runtime without loading the serving event loop", async () => {
  const home = await makePreparationHome();
  try {
    const staged = await stageFixtureRuntime(home, { bytes: PAYLOAD_BYTES });
    const supervisor = makePreparationSupervisor({
      baseDir: home.base,
      cacheRoot: home.cacheRoot,
      command: sourceWorkerCommand,
    });
    const job = {
      kind: "verify",
      stagedHandle: staged.stagedHandle,
      activeVersion: BASELINE,
      host: fixtureHost,
    } as const;
    const worker = await loopCost(() => supervisor.run(job));
    const inProcess = await loopCost(() =>
      verifyStagedQualifiedRuntime(home.base, BASELINE, staged.stagedHandle, fixtureHost),
    );
    // @effect-diagnostics-next-line globalConsole:off - The measurement prints its raw loop costs as reviewable evidence.
    console.info(
      `[preparation responsiveness] ${JSON.stringify({ payloadBytes: PAYLOAD_BYTES, worker, inProcess })}`,
    );
    // Negative control: the same verification on the serving loop keeps it busy.
    expect(inProcess.utilization).toBeGreaterThan(0.3);
    expect(worker.activeMs * 4).toBeLessThan(inProcess.activeMs);
    expect(worker.utilization * 4).toBeLessThan(inProcess.utilization);
    expect(worker.ticks).toBeGreaterThanOrEqual(worker.expectedTicks * 0.5);
  } finally {
    await home.remove();
  }
});
