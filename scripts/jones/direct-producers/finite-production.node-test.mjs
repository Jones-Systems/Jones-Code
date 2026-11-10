// Supplemental sequential interpreter only: real Git/SQLite and production
// generators, not configured Effect scheduling, interruption or scope proof.
import * as NodeModule from "node:module";
import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import { installEffectSeam } from "../../../apps/server/src/jones/workspaceRetirement/EffectSeam.node-test-support.mjs";

installEffectSeam();
const modules = {
  "effect/Cause": `export const hasInterrupts = cause => cause?._tag === 'FixtureInterrupt'; export const squash = cause => cause;`,
  "effect/Exit": `export const isSuccess = exit => exit._tag === 'Success'; export const isFailure = exit => exit._tag === 'Failure';`,
};
NodeModule.registerHooks({
  resolve(specifier, context, next) {
    return modules[specifier]
      ? { url: "data:text/javascript," + encodeURIComponent(modules[specifier]), shortCircuit: true }
      : next(specifier, context);
  },
  load(url, context, next) {
    const loaded = next(url, context);
    const source =
      typeof loaded.source === "string"
        ? loaded.source
        : loaded.source
          ? new TextDecoder().decode(loaded.source)
          : undefined;
    if (!url.startsWith("data:") || !source?.includes("export const effect = run")) return loaded;
    return {
      ...loaded,
      source:
        source +
        `
export const exit = self => effect(async context => {
  try { return {_tag:'Success', value:await self.run(context)}; }
  catch(cause) { return {_tag:'Failure', cause}; }
});
export const interrupt = effect(async () => { throw {_tag:'FixtureInterrupt'}; });
export const scoped = self => self;
export const acquireUseRelease = (acquire, use, release) => effect(async context => {
  const resource = await acquire.run(context);
  const result = await exit(suspend(() => use(resource))).run(context);
  await release(resource, result).run(context);
  if (result._tag === 'Failure') throw result.cause;
  return result.value;
});
`,
    };
  },
});
const { finiteProductionCases, finiteScratchReceipts } = await import("./finite-production.testkit.ts");
for (const [name, run] of Object.entries(finiteProductionCases)) NodeTest.test(name, run);
NodeTest.after(() => {
  NodeAssert.ok(finiteScratchReceipts.every((receipt) => receipt.cleaned));
  console.log("exact scratch cleanup", JSON.stringify(finiteScratchReceipts));
});
