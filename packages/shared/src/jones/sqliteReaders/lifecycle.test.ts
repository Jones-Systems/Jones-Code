import { assert, it } from "@effect/vitest";
import type { Worker } from "node:worker_threads";
import { retireReaderWorker } from "./lifecycle.ts";

it("accepts only completed termination as retirement proof", async () => {
  let finish: (code: number) => void = () => {};
  const pending = new Promise<number>((resolve) => { finish = resolve; });
  let proved = false;
  const retiring = retireReaderWorker({ terminate: () => pending } as Worker).then((value) => { proved = value; });
  await Promise.resolve();
  assert.isFalse(proved);
  finish(1);
  await retiring;
  assert.isTrue(proved);
  assert.isFalse(await retireReaderWorker({ terminate: () => Promise.reject(new Error("unknown")) } as Worker));
});


it("bounds unknown termination without treating its timeout as exit", async () => {
  const proof = await retireReaderWorker(
    { terminate: () => new Promise<number>(() => {}) } as Worker,
    1,
  );
  assert.isFalse(proof);
});
