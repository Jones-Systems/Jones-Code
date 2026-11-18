import { expect, it } from "vite-plus/test";
import { CapturedSampling, PopulationSampling, untilSamplingAbort } from "./capturedSampling.ts";

it("closes only the captured generation and drains every prior read before retirement", async () => {
  const old = new CapturedSampling();
  const replacement = new CapturedSampling();
  const first = old.acquire()!;
  const second = old.acquire()!;
  let drained = false;
  const retirement = old.closeAndDrain().then(() => {
    drained = true;
  });
  expect(old.acquire()).toBeUndefined();
  const later = replacement.acquire();
  expect(later).toBeTypeOf("function");
  first();
  await Promise.resolve();
  expect(drained).toBe(false);
  first(); // Duplicate release cannot discharge the second admitted read.
  await Promise.resolve();
  expect(drained).toBe(false);
  second();
  await retirement;
  expect(drained).toBe(true);
  later!();
  await replacement.closeAndDrain();
});

it("abort remains failed and closed while late genuine reads drain without retrospective success", async () => {
  const capture = new CapturedSampling();
  const finish = capture.acquire()!;
  const controller = new AbortController();
  const drain = capture.closeAndDrain(controller.signal);
  controller.abort();
  await expect(drain).rejects.toThrow("Sampling drain aborted");
  expect(capture.acquire()).toBeUndefined();
  finish();
  await capture.closeAndDrain();
  await expect(drain).rejects.toThrow("Sampling drain aborted");
});

it("preserves an external failed read instead of interpreting disappearance as exit", async () => {
  const external = Promise.withResolvers<void>();
  const observed = untilSamplingAbort(external.promise);
  external.reject(new Error("captured PID disappeared"));
  await expect(observed).rejects.toThrow("captured PID disappeared");
});

it("serializes sampling and population changes through failure and duplicate release", async () => {
  const gate = new PopulationSampling();
  const releaseSample = await gate.acquire();
  const order: string[] = [];
  const transition = gate.acquire().then((release) => {
    try {
      order.push("transition");
      throw new Error("transition failed");
    } finally {
      release();
    }
  });
  const sample = gate.acquire().then((release) => {
    order.push("sample");
    release();
  });
  await Promise.resolve();
  expect(order).toEqual([]);
  releaseSample();
  releaseSample();
  await expect(transition).rejects.toThrow("transition failed");
  await sample;
  expect(order).toEqual(["transition", "sample"]);
});

it("an aborted population waiter never acquires and does not block later samples", async () => {
  const gate = new PopulationSampling();
  const release = await gate.acquire();
  const controller = new AbortController();
  const abandoned = gate.acquire(controller.signal);
  const later = gate.acquire();
  controller.abort();
  await expect(abandoned).rejects.toThrow("Sampling drain aborted");
  release();
  (await later)();
});
