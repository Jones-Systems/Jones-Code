import { describe, expect, it, vi } from "vite-plus/test";

import { createProjectThreadStartLatch } from "./projectThreadStartLatch";

describe("project thread start latch", () => {
  it("accepts exactly one same-tick submission before minting identifiers and releases after success", async () => {
    const latch = createProjectThreadStartLatch();
    const delivery = Promise.withResolvers<void>();
    let nextId = 0;
    const deliveredIds: number[] = [];
    const submit = vi.fn(async () => {
      const id = ++nextId;
      await delivery.promise;
      deliveredIds.push(id);
    });

    const first = latch.run(submit);
    const duplicate = latch.run(submit);

    expect(nextId).toBe(1);
    expect(submit).toHaveBeenCalledTimes(1);
    delivery.resolve();
    await Promise.all([first, duplicate]);
    expect(deliveredIds).toEqual([1]);

    await latch.run(submit);
    expect(deliveredIds).toEqual([1, 2]);
  });

  it("releases after a failed submission so the same draft can retry", async () => {
    const latch = createProjectThreadStartLatch();
    const delivery = Promise.withResolvers<void>();
    const submit = vi.fn(() => delivery.promise);
    const first = latch.run(submit);
    const duplicate = latch.run(submit);
    const failed = expect(first).rejects.toThrow("connection lost");

    expect(submit).toHaveBeenCalledTimes(1);
    delivery.reject(new Error("connection lost"));
    await failed;
    await duplicate;

    const retry = vi.fn(async () => undefined);
    await latch.run(retry);
    expect(retry).toHaveBeenCalledTimes(1);
  });
});
