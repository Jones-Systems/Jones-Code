import { expect, it } from "vite-plus/test";
import * as Cause from "effect/Cause";
import * as Option from "effect/Option";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import { publishInBatches } from "./batchedPublication.ts";

it("forwards synchronous startup and nested publications without stale replay", () => {
  const registry = AtomRegistry.make();
  const startup = Atom.make((get) => {
    get.setSelf(1);
    return 0;
  });
  const startupWrapper = publishInBatches(startup);
  expect(registry.get(startupWrapper)).toBe(1);
  const source = Atom.make(0);
  const wrapped = publishInBatches(source);
  const seen: number[] = [];
  const stop = registry.subscribe(
    wrapped,
    (value) => {
      seen.push(value);
      if (value === 1) registry.set(source, 2);
    },
    { immediate: true },
  );
  registry.set(source, 1);
  expect(seen).toEqual([0, 1, 2]);
  Atom.batch(() => {
    registry.set(source, 3);
    registry.set(source, 4);
  });
  expect(registry.get(wrapped)).toBe(4);
  expect(seen.at(-1)).toBe(4);
  stop();
  registry.dispose();
});

it("forwards refresh and hydrates the source through initialValueTarget", () => {
  let reads = 0;
  const source = Atom.make(() => ++reads);
  const wrapped = publishInBatches(source);
  const registry = AtomRegistry.make();
  const stop = registry.mount(wrapped);
  expect(registry.get(wrapped)).toBe(1);
  registry.refresh(wrapped);
  expect(registry.get(wrapped)).toBe(2);
  stop();
  registry.dispose();
  const writable = Atom.make(0);
  const hydrated = publishInBatches(writable);
  const restored = AtomRegistry.make({ initialValues: [[hydrated, 7]] });
  const dispose = restored.mount(hydrated);
  expect(restored.get(writable)).toBe(7);
  expect(restored.get(hydrated)).toBe(7);
  restored.set(writable, 8);
  expect(restored.get(hydrated)).toBe(8);
  dispose();
  restored.dispose();
});

it("preserves AsyncResult identities and previous-success failure behavior", () => {
  const registry = AtomRegistry.make();
  const success = AsyncResult.success<number, string>(42);
  const source = Atom.make<AsyncResult.AsyncResult<number, string>>(success);
  const wrapped = publishInBatches(source);
  const stop = registry.mount(wrapped);
  const waiting = AsyncResult.waiting(success);
  registry.set(source, waiting);
  expect(registry.get(wrapped)).toBe(waiting);
  const failed = AsyncResult.failureWithPrevious(Cause.fail("unavailable"), {
    previous: Option.some(success),
  });
  registry.set(source, failed);
  expect(registry.get(wrapped)).toBe(failed);
  expect(Option.getOrElse(AsyncResult.value(registry.get(wrapped)), () => -1)).toBe(42);
  const emptyFailure = AsyncResult.failure<number, string>(Cause.fail("no previous value"));
  registry.set(source, emptyFailure);
  expect(registry.get(wrapped)).toBe(emptyFailure);
  expect(Option.getOrElse(AsyncResult.value(registry.get(wrapped)), () => -1)).toBe(-1);
  stop();
  registry.dispose();
});

it("surfaces a listener error, drains other notifications and accepts the next update", () => {
  const registry = AtomRegistry.make();
  const source = Atom.make(0);
  const wrapped = publishInBatches(source);
  const seen: number[] = [];
  const stop = registry.mount(wrapped);
  const bad = registry.subscribe(wrapped, () => {
    throw new Error("synthetic listener");
  });
  const good = registry.subscribe(wrapped, (value) => seen.push(value));
  expect(() => registry.set(source, 1)).toThrow("synthetic listener");
  expect(seen).toEqual([1]);
  bad();
  registry.set(source, 2);
  expect(seen).toEqual([1, 2]);
  good();
  stop();
  registry.dispose();
});

it("unsubscribes on removal and ignores an obsolete callback after disposal", async () => {
  const registry = AtomRegistry.make();
  let captured = (_value: number) => {};
  let release = () => {};
  const finalized = new Promise<void>((resolve) => {
    release = resolve;
  });
  const source = Atom.make((get) => {
    captured = (value) => get.setSelf(value);
    get.addFinalizer(release);
    return 0;
  }).pipe(Atom.setIdleTTL(0));
  const wrapped = publishInBatches(source);
  const seen: number[] = [];
  const stop = registry.subscribe(wrapped, (value) => seen.push(value), { immediate: true });
  captured(1);
  expect(seen).toEqual([0, 1]);
  stop();
  await finalized;
  registry.dispose();
  captured(2);
  expect(seen).toEqual([0, 1]);
  expect(registry.getNodes().size).toBe(0);
});
