import { expect, it } from "@effect/vitest";
import { observeSafely } from "./observations.ts";

it("keeps resource lifecycle callbacks running when an observation sink fails", () => {
  const delivered: number[] = [];
  const observe = observeSafely<number>((event) => {
    delivered.push(event);
    throw new Error("telemetry unavailable");
  });
  expect(() => {
    observe(1);
    observe(2);
  }).not.toThrow();
  expect(delivered).toEqual([1, 2]);
  expect(() => observeSafely(undefined)(1)).not.toThrow();
});
