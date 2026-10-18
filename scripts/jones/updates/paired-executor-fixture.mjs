import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";

function literal(source) {
  const marker = "String.raw`";
  const start = source.indexOf(marker);
  NodeAssert.ok(start >= 0 && source.trimEnd().endsWith("`;"));
  return source.slice(start + marker.length, source.lastIndexOf("`;"));
}

function pairedFragments(pairedSource) {
  const declaration = "export const jonesPairedStateSource = Object.freeze({";
  NodeAssert.equal(pairedSource.split(declaration).length, 2);
  let remaining = pairedSource
    .slice(pairedSource.indexOf(declaration) + declaration.length)
    .trimStart();
  const fragments = {};
  for (const name of ["syncTree", "copyAndTopology", "recoveryAndPair", "syncParent"]) {
    const prefix = name + ": String.raw`";
    NodeAssert.ok(remaining.startsWith(prefix), "Unexpected paired source fragment.");
    const end = remaining.indexOf("`", prefix.length);
    NodeAssert.ok(end >= prefix.length, "Unterminated paired source fragment.");
    const fragment = remaining.slice(prefix.length, end);
    NodeAssert.ok(!fragment.includes("${"), "Paired source fragments must be literal.");
    remaining = remaining.slice(end + 1).trimStart();
    NodeAssert.ok(remaining.startsWith(","), "Unexpected paired source expression.");
    remaining = remaining.slice(1).trimStart();
    fragments[name] = fragment;
  }
  NodeAssert.equal(remaining.trimEnd(), "} as const);");
  return fragments;
}

export function materializePairedExecutor(executorSource, pairedSource) {
  NodeAssert.ok(
    executorSource.includes(
      'import { jonesPairedStateSource } from "./jonesPairedStateSource.ts";',
    ),
  );
  let executor = literal(executorSource);
  for (const [name, fragment] of Object.entries(pairedFragments(pairedSource))) {
    const reference = "${jonesPairedStateSource." + name + "}";
    NodeAssert.equal(executor.split(reference).length, 2);
    executor = executor.replace(reference, () => fragment);
  }
  NodeAssert.ok(
    !executor.includes("${") && !executor.includes("`"),
    "Unresolved executor substitution.",
  );
  return executor;
}

export async function readPairedExecutorSource() {
  const [executorSource, pairedSource] = await Promise.all([
    NodeFSP.readFile(
      new URL(
        "../../../apps/desktop/src/jones/updates/jonesNativeHelperSource.ts",
        import.meta.url,
      ),
      "utf8",
    ),
    NodeFSP.readFile(
      new URL("../../../apps/desktop/src/jones/updates/jonesPairedStateSource.ts", import.meta.url),
      "utf8",
    ),
  ]);
  return materializePairedExecutor(executorSource, pairedSource);
}
