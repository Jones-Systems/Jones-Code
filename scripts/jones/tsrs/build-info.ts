import * as Identity from "./identity.ts";
import { configurationSnapshots } from "./config-inputs.ts";
import {
  object,
  fileInfo,
  admitNamespace,
  RAW_LIMIT,
  ENVELOPE_LIMIT,
  NAMESPACE_LIMIT,
} from "./seed-files.ts";

export interface CheckArguments {
  kind: "check" | "information";
  project?: string;
  incremental?: boolean;
  buildInfo?: string;
  noEmit?: boolean;
}

function refuse(arg: string): never {
  throw new Error(
    `Unqualified one-shot typecheck argument: ${arg}. Build/watch, response files, positional files and unknown options are not supported; use pnpm typecheck:incremental for repeated checks.`,
  );
}

// The pinned native parser uses case-insensitive names, one or two dashes,
// optional lowercase boolean values and last-option precedence. Equals syntax
// and null values are deliberately outside this qualified one-shot interface.
export function classifyArguments(args: string[]): CheckArguments {
  if (args.length === 1 && /^(?:--?version|--?v)$/i.test(args[0]!)) return { kind: "information" };
  const result: CheckArguments = { kind: "check" };
  const booleans = new Set([
    "noemit",
    "pretty",
    "incremental",
    "diagnostics",
    "extendeddiagnostics",
    "listfiles",
    "explainfiles",
    "traceresolution",
  ]);
  for (let index = 0; index < args.length; index++) {
    const arg = args[index]!;
    if (/^--?(?:checkerassignment|checkers)(?:=|$)/i.test(arg))
      throw new Error("Checker assignment/count are pinned by the repository launcher.");
    if (!/^--?[^-=]+$/.test(arg)) refuse(arg);
    const name = arg.replace(/^--?/, "").toLowerCase();
    if (name === "p" || name === "project" || name === "tsbuildinfofile") {
      const value = args[++index];
      if (!value || value.startsWith("-") || value.startsWith("@") || value === "null") refuse(arg);
      if (name === "tsbuildinfofile") result.buildInfo = value;
      else result.project = value;
    } else if (booleans.has(name)) {
      let value = true;
      if (args[index + 1] === "true" || args[index + 1] === "false")
        value = args[++index] === "true";
      if (name === "incremental") result.incremental = value;
      if (name === "noemit") {
        if (!value) refuse(`${arg} false`);
        result.noEmit = value;
      }
    } else refuse(arg);
  }
  return result;
}

export async function projectConfiguration(root: string, cwd: string, args: CheckArguments) {
  const canonicalRoot = await Identity.files((fs) => fs.realPath(root));
  const canonicalCwd = await Identity.files((fs) => fs.realPath(cwd));
  if (canonicalCwd !== cwd || (cwd !== canonicalRoot && !Identity.contains(canonicalRoot, cwd)))
    throw new Error("Typecheck cwd must be a canonical directory in this worktree.");
  let config: string;
  if (args.project !== undefined) {
    const selected = Identity.path.resolve(cwd, args.project);
    const stat = await Identity.regularInfo(selected);
    config = stat?.type === "Directory" ? Identity.path.join(selected, "tsconfig.json") : selected;
  } else {
    let directory = cwd;
    for (;;) {
      const selected = Identity.path.join(directory, "tsconfig.json");
      if (await Identity.regularInfo(selected)) {
        config = selected;
        break;
      }
      if (directory === canonicalRoot)
        throw new Error("No project tsconfig.json inside this worktree.");
      directory = Identity.path.dirname(directory);
    }
  }
  const directory = Identity.path.dirname(config);
  const snapshot = (
    await configurationSnapshots(canonicalRoot, [{ directory, configFile: config }])
  ).get(directory)!;
  if (!snapshot.validOptions || snapshot.hasReferences)
    throw new Error("Invalid compilerOptions or unqualified project-reference topology.");
  const { options, origins } = snapshot;
  for (const name of ["incremental", "composite", "noEmit"]) {
    if (options[name] !== undefined && typeof options[name] !== "boolean")
      throw new Error(`Unqualified ${name} configuration.`);
  }
  if (!(args.noEmit ?? options.noEmit))
    throw new Error(
      "The one-shot launcher requires --noEmit or noEmit:true in the project configuration.",
    );
  const incremental = args.incremental ?? options.incremental ?? options.composite ?? false;
  let output: string | undefined;
  if (incremental) {
    // None of the admitted repository projects uses output relocation. Avoid
    // inventing TypeScript's rootDir/outDir/outFile default-path algorithm.
    if (
      options.outDir !== undefined ||
      options.outFile !== undefined ||
      options.rootDir !== undefined
    )
      throw new Error(
        "Incremental output relocation is not qualified; use the retained typecheck command.",
      );
    if (args.buildInfo !== undefined) output = Identity.path.resolve(cwd, args.buildInfo);
    else if (options.tsBuildInfoFile !== undefined) {
      if (typeof options.tsBuildInfoFile !== "string" || !options.tsBuildInfoFile)
        throw new Error("Invalid tsBuildInfoFile configuration.");
      output = Identity.path.resolve(
        Identity.path.dirname(origins.tsBuildInfoFile!),
        options.tsBuildInfoFile,
      );
    } else output = config.replace(/(?:\.json)?$/, ".tsbuildinfo");
    if (
      Identity.path.dirname(output) !== directory ||
      !Identity.path.basename(output).endsWith(".tsbuildinfo")
    )
      throw new Error(
        "Custom build-info output must be a .tsbuildinfo file in the project config directory; other cache/output locations are not qualified.",
      );
  }
  if (!incremental && (args.buildInfo !== undefined || options.tsBuildInfoFile !== undefined))
    throw new Error("tsBuildInfoFile requires an incremental/composite one-shot check.");
  if (!object(options)) throw new Error("Invalid project options.");
  return { root: canonicalRoot, cwd: canonicalCwd, config, directory, snapshot, output };
}

export const STATE_SCHEMA = "jones-tsrs-build-info/v1";
export const STATE_ENTRY_LIMIT = 4096;

// Count all entries (including crashed invocations), not just valid envelopes.
// This is admission, not an eviction policy or concurrent native-write quota.
export async function admitState(
  root: string,
  namespace: string,
  additional: number,
  entries = 4,
): Promise<void> {
  await admitNamespace(root, namespace, additional);
  let count = entries;
  const visit = async (directory: string): Promise<void> => {
    for (const name of await Identity.files((fs) => fs.readDirectory(directory))) {
      if (++count > STATE_ENTRY_LIMIT)
        throw new Error(
          `Native state admission exceeds ${STATE_ENTRY_LIMIT} entries (including crash residue); no automatic cleanup.`,
        );
      const filename = Identity.path.join(directory, name);
      try {
        if ((await fileInfo(filename)).type === "Directory") await visit(filename);
      } catch (error) {
        if (Identity.errorCode(error) !== "ENOENT") throw error;
      }
    }
  };
  await visit(namespace);
}

export const STATE_STORAGE = {
  payload: RAW_LIMIT,
  envelope: ENVELOPE_LIMIT,
  aggregate: NAMESPACE_LIMIT,
  entries: STATE_ENTRY_LIMIT,
};
