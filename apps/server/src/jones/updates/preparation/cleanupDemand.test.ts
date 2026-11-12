import { expect, it, vi } from "vite-plus/test";
import { preparationCleanupDemand } from "./cleanupDemand.ts";

const retained = { removed: 0, retained: 2, failed: 0 };

it("runs once at startup and quiesces despite retained downloads or standing holds", async () => {
  const sweep = vi.fn(async () => retained);
  const cleanup = preparationCleanupDemand(sweep);
  for (let tick = 0; tick < 100; tick++) await cleanup.flush();
  expect(sweep).toHaveBeenCalledTimes(1);
});

it("coalesces demand, shares an in-flight pass and preserves a later transition", async () => {
  let finish!: (result: typeof retained) => void;
  const sweep = vi.fn(() => new Promise<typeof retained>((resolve) => { finish = resolve; }));
  const cleanup = preparationCleanupDemand(sweep);
  for (let i = 0; i < 100; i++) cleanup.request();
  const first = cleanup.flush();
  expect(cleanup.flush()).toBe(first);
  await Promise.resolve();
  cleanup.request(); // A worker settled after the current pass took its snapshot.
  expect(cleanup.flush()).toBe(first);
  finish(retained);
  await first;
  expect(sweep).toHaveBeenCalledTimes(1);
  const next = cleanup.flush();
  await Promise.resolve();
  finish(retained);
  await next;
  await cleanup.flush();
  expect(sweep).toHaveBeenCalledTimes(2);
});

it.each(["result", "throw"])("bounds failed %s retries until a new transition", async (failure) => {
  const sweep = vi.fn(async () => {
    if (failure === "throw") throw new Error("Synthetic sweep failure.");
    return { ...retained, failed: 1 };
  });
  const cleanup = preparationCleanupDemand(sweep);
  for (let tick = 0; tick < 100; tick++) await cleanup.flush().catch(() => undefined);
  expect(sweep).toHaveBeenCalledTimes(3);
  cleanup.request();
  for (let tick = 0; tick < 100; tick++) await cleanup.flush().catch(() => undefined);
  expect(sweep).toHaveBeenCalledTimes(6);
});

it("does not retry a successful sweep's own completion", async () => {
  const sweep = vi.fn(async () => ({ removed: 3, retained: 0, failed: 0 }));
  const cleanup = preparationCleanupDemand(sweep);
  await cleanup.flush();
  await cleanup.flush();
  cleanup.request();
  await cleanup.flush();
  await cleanup.flush();
  expect(sweep).toHaveBeenCalledTimes(2);
});
