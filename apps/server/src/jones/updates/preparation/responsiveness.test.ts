// One synthetic preparation operation in a fresh source Node parent, with the original controls.
import { expect, it, onTestFinished } from "vite-plus/test";
import { startResponsivenessSource } from "./responsivenessSource.ts";

it("hashes a heavy staged runtime without loading the serving event loop", async () => {
  const helper = await startResponsivenessSource();
  onTestFinished(() => helper.close());
  const measurement = await helper.result;
  const { worker, inProcess } = measurement;
  // @effect-diagnostics-next-line globalConsole:off - Preserve the original single measurement log exemption.
  console.info(
    `[preparation responsiveness] ${JSON.stringify({ ...measurement, helper: helper.readback() })}`,
  );
  // The worker validates retained receipts; the unchanged in-process control hashes the payload.
  expect(inProcess.utilization).toBeGreaterThan(0.3);
  expect(worker.activeMs * 4).toBeLessThan(inProcess.activeMs);
  expect(worker.utilization * 4).toBeLessThan(inProcess.utilization);
  expect(worker.ticks).toBeGreaterThanOrEqual(worker.expectedTicks * 0.5);
});
