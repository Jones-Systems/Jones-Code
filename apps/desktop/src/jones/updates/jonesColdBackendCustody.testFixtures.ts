// @effect-diagnostics nodeBuiltinImport:off -- Owns the synthetic driver, cancellation and closure receipt under the supplied scratch root.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeProcess from "node:process";
import * as NodePerfHooks from "node:perf_hooks";
import * as NodeTimersPromises from "node:timers/promises";
// @ts-expect-error -- The existing owned fixture has no declarations.
import * as IdentityFixture from "../../../../../scripts/jones/updates/native-fixture-support.mjs";

export async function observeOwnedFixture(root: string, scenario: string, signal: AbortSignal) {
  const marker = NodePath.join(root, "custody.json");
  // Only the creating layer decides cleanup. A killed driver cannot erase uncertainty.
  await NodeFSP.writeFile(marker, JSON.stringify({ state: "held" }));
  const cancel = new AbortController();
  let failure: unknown;
  let result;
  try {
    const command = IdentityFixture.runNativeFixtureCommand(
      NodeProcess.execPath,
      [NodePath.join(root, "custody.mjs"), scenario, root],
      root,
      { signal: AbortSignal.any([signal, cancel.signal]) },
    ).then(
      (value: { code: number; stdout: string; stderr: string }) => ({ value }),
      (cause: unknown) => ({ cause }),
    );
    try {
      if (scenario.startsWith("external-cancel")) {
        const deadline = NodePerfHooks.performance.now() + 5000;
        while (JSON.parse(await NodeFSP.readFile(marker, "utf8")).state !== "ready") {
          if (NodePerfHooks.performance.now() >= deadline)
            throw new Error("Captured fixture did not reach READY.");
          await NodeTimersPromises.setTimeout(5);
        }
        cancel.abort(new Error("Owned external cancellation after READY."));
      }
    } finally {
      if (scenario.startsWith("external-cancel")) cancel.abort();
      result = await command;
    }
  } catch (cause) {
    failure = cause;
  }
  const custody = await NodeFSP.readFile(marker, "utf8")
    .then((value) => JSON.parse(value))
    .catch(() => null);
  if (custody?.state !== "settled") {
    const uncertain = new Error("Captured fixture descendant closure is unproved.", {
      cause: failure,
    });
    Object.assign(uncertain, { preserveScratch: true });
    throw uncertain;
  }
  if (failure) throw failure;
  return { result, custody };
}
