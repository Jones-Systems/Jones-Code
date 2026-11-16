// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeUtil from "node:util";
import * as NodeURL from "node:url";
import {
  recoverStagedSelection,
  type RecoverStagedSelectionInput,
} from "../../../apps/server/src/jones/updates/recoverStagedSelection.ts";

export function parseRecoveryArguments(args: string[]): RecoverStagedSelectionInput {
  const names = [
    "base-dir",
    "active-version",
    "expected-active-source",
    "environment-id",
    "target-source",
    "target-version",
    "staged-handle",
  ] as const;
  const options = Object.fromEntries(
    names.map((name) => [name, { type: "string" as const }]),
  ) as Record<(typeof names)[number], { type: "string" }>;
  const { values, tokens } = NodeUtil.parseArgs({
    args,
    options: { ...options, apply: { type: "boolean" } },
    strict: true,
    allowPositionals: false,
    tokens: true,
  });
  const seen = new Set<string>();
  for (const token of tokens) {
    if (token.kind !== "option" || seen.has(token.name))
      throw new Error("Duplicate or invalid option.");
    seen.add(token.name);
  }
  const required = (name: (typeof names)[number]): string => {
    const value = values[name];
    if (typeof value !== "string" || value.trim() === "")
      throw new Error("Missing required option.");
    return value;
  };
  return {
    baseDir: required("base-dir"),
    activeVersion: required("active-version"),
    expectedActiveSource: required("expected-active-source"),
    environmentId: required("environment-id"),
    targetSource: required("target-source"),
    targetVersion: required("target-version"),
    stagedHandle: required("staged-handle"),
    apply: values.apply === true,
  };
}

if (
  process.argv[1] !== undefined &&
  NodeFS.realpathSync(NodeURL.fileURLToPath(import.meta.url)) ===
    NodeFS.realpathSync(process.argv[1])
) {
  let result;
  try {
    result = await recoverStagedSelection(parseRecoveryArguments(process.argv.slice(2)));
  } catch {
    result = { status: "refused", reason: "invalid-arguments" } as const;
  }
  process.stdout.write(`${JSON.stringify(result)}\n`);
  process.exitCode = result.status === "unknown" ? 3 : result.status === "refused" ? 2 : 0;
}
