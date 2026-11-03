import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Crypto from "effect/Crypto";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Identity from "./identity.ts";
import { processIdentity, CommandCancelled, cancellationExitCode } from "./patch.ts";
import { configurationSnapshots } from "./config-inputs.ts";
import {
  canonical,
  digest,
  safeDirectory,
  readBounded,
  validNativePayload,
  encodeEnvelope,
  readSeed,
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
  const incremental =
    (args.incremental ?? options.incremental) === true || options.composite === true;
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
      Identity.path.basename(output).startsWith(".jones-tsrs-") ||
      Identity.path.dirname(output) !== directory ||
      !Identity.path.basename(output).endsWith(".tsbuildinfo")
    )
      throw new Error(
        "Custom build-info output must be a .tsbuildinfo file in the project config directory; other cache/output locations are not qualified.",
      );
  }

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

const RESERVATION = RAW_LIMIT + ENVELOPE_LIMIT + 16384;
export const STATE_STORAGE = {
  reserve: RESERVATION,
  payload: RAW_LIMIT,
  envelope: ENVELOPE_LIMIT,
  aggregate: NAMESPACE_LIMIT,
  entries: STATE_ENTRY_LIMIT,
};

const CONVERSION = "same-parent/self-reference-only/v1";

function metadata(
  info: Awaited<ReturnType<typeof fileInfo>>,
  filename: string,
  directory = false,
): string {
  const uid = HostProcess.UserId.defaultValue();
  const inode = Option.getOrUndefined(info.ino);
  if (
    uid === undefined ||
    Option.getOrUndefined(info.uid) !== uid ||
    inode === undefined ||
    info.type !== (directory ? "Directory" : "File") ||
    (!directory && Option.getOrUndefined(info.nlink) !== 1n)
  )
    throw new Error(`Unsafe or foreign-owned build-info path: ${filename}`);
  return canonical({ dev: String(info.dev), inode: String(inode), uid });
}

async function parentIdentity(root: string, directory: string): Promise<string> {
  await safeDirectory(root, directory, false);
  if ((await Identity.files((fs) => fs.realPath(directory))) !== directory)
    throw new Error(`Noncanonical output parent: ${directory}`);
  const parents: string[] = [];
  let current = root;
  parents.push(metadata(await fileInfo(current), current, true));
  for (const component of Identity.path
    .relative(root, directory)
    .split(Identity.path.sep)
    .filter(Boolean)) {
    current = Identity.path.join(current, component);
    parents.push(metadata(await fileInfo(current), current, true));
  }
  return canonical(parents);
}

async function outputStamp(filename: string): Promise<string | undefined> {
  const info = await Identity.regularInfo(filename);
  if (!info) return;
  const owner = metadata(info, filename);
  const bytes = await readBounded(filename, RAW_LIMIT);
  if (!bytes) throw new Error(`Unsafe or oversized logical build-info output: ${filename}`);
  return canonical({ owner, checksum: digest(bytes), size: bytes.length });
}

export function convertSelfReference(payload: Buffer, from: string, to: string): Buffer {
  if (!validNativePayload(payload) || Identity.path.dirname(from) !== Identity.path.dirname(to))
    throw new Error("Malformed native build-info payload or changed relative-path base.");
  const value: unknown = JSON.parse(payload.toString("utf8"));
  if (
    !object(value) ||
    !object(value.options) ||
    typeof value.options.tsBuildInfoFile !== "string" ||
    Identity.path.resolve(Identity.path.dirname(from), value.options.tsBuildInfoFile) !== from
  )
    throw new Error("Missing or unexpected native build-info self-reference; refusing retention.");
  value.options.tsBuildInfoFile = `./${Identity.path.basename(to)}`;
  const converted = Buffer.from(JSON.stringify(value));
  if (!validNativePayload(converted))
    throw new Error("Converted native build info exceeds its bound or is invalid.");
  return converted;
}

export function statePaths(root: string, directory: string, output: string) {
  const namespace = Identity.path.join(root, "node_modules/.cache/jones/tsrs/build-info/v1");
  const slot = digest(canonical([root, directory, output]));
  return {
    namespace,
    entry: Identity.path.join(namespace, `${slot}.entry.json`),
    raw: Identity.path.join(namespace, `${slot}.raw.json`),
  };
}

async function liveOwner(value: unknown): Promise<boolean> {
  if (
    !object(value) ||
    !Number.isSafeInteger(value.pid) ||
    Number(value.pid) <= 0 ||
    (value.start !== undefined && typeof value.start !== "string")
  )
    throw new Error("Malformed build-info process ownership record.");
  try {
    process.kill(Number(value.pid), 0);
  } catch (error) {
    if (Identity.errorCode(error) === "ESRCH") return false;
    throw error;
  }
  if (HostProcess.Platform.defaultValue() !== "linux") return true;
  if (typeof value.start !== "string")
    throw new Error("Build-info record lacks Linux process start identity.");
  try {
    return (await processIdentity(Number(value.pid))).start === value.start;
  } catch (error) {
    if (Identity.errorCode(error) === "ENOENT") return false;
    throw error;
  }
}

async function jsonRecord(filename: string) {
  const bytes = await readBounded(filename, 8192);
  if (!bytes) throw new Error(`Missing or oversized build-info accounting record: ${filename}`);
  const value: unknown = JSON.parse(bytes.toString("utf8"));
  if (!object(value)) throw new Error(`Malformed build-info accounting record: ${filename}`);
  return value;
}

async function writeOwned(
  filename: string,
  bytes: Buffer,
  owned: Map<string, string>,
  uncertain: (value: boolean) => void = () => {},
) {
  await Identity.files((fs) =>
    Effect.gen(function* () {
      const file = yield* fs.open(filename, { flag: "wx", mode: 0o600 });
      uncertain(true);
      owned.set(filename, metadata(yield* file.stat, filename));
      uncertain(false);
      yield* file.writeAll(bytes);
    }),
  );
}

// This lock covers only accounting/atomic publication, never compiler execution.
async function coordinated<A>(
  root: string,
  namespace: string,
  signal: AbortSignal,
  operation: () => Promise<A>,
): Promise<A> {
  await safeDirectory(root, namespace, true);
  const lock = Identity.path.join(namespace, "admission.lock");
  const deadline = (await Effect.runPromise(Clock.currentTimeMillis)) + 10000;
  for (;;) {
    if (signal.aborted) throw new CommandCancelled(signal);
    const attempt = new Map<string, string>();
    let uncertain = false;
    try {
      await writeOwned(
        lock,
        Buffer.from(JSON.stringify({ owner: await processIdentity(process.pid) })),
        attempt,
        (value) => {
          uncertain = value;
        },
      );
      break;
    } catch (error) {
      if (uncertain)
        throw new Error(`Admission lock ownership unconfirmed; preserve ${lock}`, { cause: error });
      if (attempt.has(lock)) {
        if (metadata(await fileInfo(lock), lock) !== attempt.get(lock))
          throw new Error(`Admission lock ownership changed; preserve ${lock}`, { cause: error });
        await Identity.files((fs) => fs.remove(lock));
      }
      if (Identity.errorCode(error) !== "EEXIST") throw error;
      let old: Record<string, unknown>;
      try {
        old = await jsonRecord(lock);
      } catch (readError) {
        if ((await Effect.runPromise(Clock.currentTimeMillis)) >= deadline) throw readError;
        await Effect.runPromise(Effect.sleep(25));
        continue;
      }
      if (!(await liveOwner(old.owner)))
        throw new Error(`Unresolved build-info admission residue: ${lock}`, { cause: error });
      if ((await Effect.runPromise(Clock.currentTimeMillis)) >= deadline)
        throw new Error(`Build-info admission busy: ${lock}`, { cause: error });
      await Effect.runPromise(Effect.sleep(25));
    }
  }
  const owned = metadata(await fileInfo(lock), lock);
  let outcome: { value: A } | { failure: unknown };
  try {
    outcome = { value: await operation() };
  } catch (error) {
    outcome = { failure: error };
  }
  if (metadata(await fileInfo(lock), lock) !== owned)
    throw new Error(`Admission lock ownership changed: ${lock}`, {
      cause: "failure" in outcome ? outcome.failure : undefined,
    });
  await Identity.files((fs) => fs.remove(lock));
  if ("failure" in outcome) throw outcome.failure;
  return outcome.value;
}

async function storageAdmission(
  root: string,
  namespace: string,
  additional: number,
): Promise<void> {
  let external = 0;
  let externalEntries = 0;
  for (const name of await Identity.files((fs) => fs.readDirectory(namespace))) {
    if (!name.endsWith(".run.json") && !name.endsWith(".raw.json")) continue;
    const filename = Identity.path.join(namespace, name);
    const record = await jsonRecord(filename);
    if (
      record.schema !== STATE_SCHEMA ||
      typeof record.output !== "string" ||
      !Identity.contains(root, record.output) ||
      !record.output.endsWith(".tsbuildinfo") ||
      Identity.path.basename(record.output).startsWith(".jones-tsrs-")
    )
      throw new Error(`Unsafe build-info accounting record: ${filename}`);
    if (name.endsWith(".run.json")) {
      if (
        record.phase !== "active" ||
        !(await liveOwner(record.owner)) ||
        record.reserve !== RESERVATION
      )
        throw new Error(
          `Unresolved build-info invocation residue: ${filename}; ${String(record.working)}`,
        );
      if (
        typeof record.working !== "string" ||
        Identity.path.dirname(record.working) !== Identity.path.dirname(record.output) ||
        !/^\.jones-tsrs-[a-f0-9-]+\.tsbuildinfo$/.test(Identity.path.basename(record.working))
      )
        throw new Error(`Unsafe invocation working-file accounting: ${filename}`);
      const info = await Identity.regularInfo(record.working);
      if (info) {
        metadata(info, record.working);
        if (Number(info.size) > RAW_LIMIT)
          throw new Error(`Working file exceeds 16 MiB: ${record.working}`);
      }
      external += RESERVATION;
      externalEntries++;
    } else {
      const info = await Identity.regularInfo(record.output);
      if (info) {
        metadata(info, record.output);
        if (Number(info.size) > RAW_LIMIT)
          throw new Error(`Retained raw output exceeds 16 MiB: ${record.output}`);
        external += Number(info.size);
        externalEntries++;
      }
    }
  }
  await admitState(root, namespace, additional + external, 4 + externalEntries);
}

export interface StatefulCheck {
  root: string;
  cwd: string;
  args: string[];
  compiler: Identity.Installed;
  signal: AbortSignal;
  run: (args: string[], spawned: () => void) => Promise<number>;
  log?: (line: string) => void;
}

export async function runWithBuildInfo(options: StatefulCheck): Promise<number> {
  const { root, cwd, args, compiler, signal, run } = options;
  const classified = classifyArguments(args);
  if (classified.kind === "information") return run(args, () => {});
  await Identity.provePrivate(compiler);
  const configuration = await projectConfiguration(root, cwd, classified);
  const { output, directory, config, snapshot } = configuration;
  const unchanged = async () =>
    (await projectConfiguration(root, cwd, classified)).snapshot.identity === snapshot.identity;
  if (!output) {
    if (!(await unchanged())) throw new Error("Project configuration changed before checking.");
    const result = await run(args, () => {});
    if (!(await unchanged())) throw new Error("Project configuration changed during checking.");
    return result;
  }
  const parent = await parentIdentity(root, directory);
  const before = await outputStamp(output);
  const paths = statePaths(root, directory, output);
  const identity = digest(
    canonical({
      schema: STATE_SCHEMA,
      conversion: CONVERSION,
      root,
      cwd,
      config,
      parent,
      output,
      args,
      options: snapshot.options,
      configuration: snapshot.identity,
      compiler: {
        target: compiler.target,
        executable: compiler.executable,
        wrapper: compiler.wrapper,
        nativeManifest: compiler.nativeManifest,
        hash: Identity.TARGETS[compiler.target].patched,
        tsrs: Identity.TSRS_VERSION,
        effect: Identity.EFFECT_VERSION,
        version: Identity.BUILD_VERSION,
        checkerOptions: Identity.CHECKER_OPTIONS,
      },
    }),
  );
  const nonce = Effect.runSync(
    Effect.flatMap(Crypto.Crypto, (crypto) => crypto.randomUUIDv4()).pipe(
      Effect.provide(NodeCrypto.layer),
    ),
  );
  const working = Identity.path.join(directory, `.jones-tsrs-${nonce}.tsbuildinfo`);
  const account = Identity.path.join(paths.namespace, `${nonce}.run.json`);
  const staging = Identity.path.join(paths.namespace, `${nonce}.entry.tmp`);
  const markerStaging = Identity.path.join(paths.namespace, `${nonce}.raw.tmp`);
  const owned = new Map<string, string>();
  let uncertainOwnership: string | undefined;
  const create = (filename: string, bytes: Buffer) =>
    writeOwned(filename, bytes, owned, (uncertain) => {
      uncertainOwnership = uncertain ? filename : undefined;
    });
  const record = {
    schema: STATE_SCHEMA,
    owner: await processIdentity(process.pid),
    phase: "active",
    output,
    working,
    reserve: RESERVATION,
  };
  const markUnknown = async () => {
    if (!owned.has(account)) return;
    await safeDirectory(root, paths.namespace, false);
    const bytes = Buffer.from(JSON.stringify({ ...record, phase: "unknown" }));
    await Identity.files((fs) =>
      Effect.gen(function* () {
        const file = yield* fs.open(account, { flag: "r+", noFollow: true });
        if (metadata(yield* file.stat, account) !== owned.get(account))
          throw new Error(`Accounting ownership changed: ${account}`);
        yield* file.writeAll(bytes);
        yield* file.truncate(bytes.length);
      }),
    );
  };
  let transferred = false;
  let outputAccounted = false;
  let spawned = false;
  let exitConfirmed = true;
  let result = 1;
  let failure: unknown;
  let failed = false;
  try {
    await coordinated(root, paths.namespace, signal, async () => {
      await storageAdmission(root, paths.namespace, RESERVATION);
      await create(account, Buffer.from(JSON.stringify(record)));
    });
    if (parent !== (await parentIdentity(root, directory)) || !(await unchanged()))
      throw new Error("Configuration/output parent changed before seed use.");
    const seed = await readSeed(paths.entry, identity, STATE_SCHEMA);
    await create(working, seed ? convertSelfReference(seed, output, working) : Buffer.alloc(0));
    options.log?.(`[tsrs] ${seed ? "seed validated" : "cache miss"}: ${output}; ${working}`);
    if (signal.aborted) throw new CommandCancelled(signal);
    if (!(await unchanged())) throw new Error("Changed configuration before checking.");
    exitConfirmed = false;
    try {
      result = await run([...args, "--tsBuildInfoFile", working], () => {
        spawned = true;
      });
      exitConfirmed = true;
      if (signal.aborted) result = cancellationExitCode(signal);
    } catch (error) {
      exitConfirmed = !spawned;
      throw error;
    }
    const publishNativeResult = async () => {
      if (signal.aborted) return;
      if (!(await unchanged()) || parent !== (await parentIdentity(root, directory)))
        throw new Error(
          "Configuration/output parent changed during checking; no success or seed publication.",
        );
      if (metadata(await fileInfo(working), working) !== owned.get(working))
        throw new Error(`Native working-file ownership changed: ${working}`);
      const native = await readBounded(working, RAW_LIMIT);
      if (result !== 0 && (!native?.length || !validNativePayload(native))) return;
      if (!native) throw new Error(`Native build info missing or oversized: ${working}`);
      const logical = convertSelfReference(native, working, output);
      await coordinated(root, paths.namespace, signal, async () => {
        await storageAdmission(root, paths.namespace, 0);
        if (!(await unchanged()) || parent !== (await parentIdentity(root, directory)))
          throw new Error("Configuration/output parent changed before publication.");
        const current = await outputStamp(output);
        let admittedConcurrent = false;
        if (current !== before && (await Identity.regularInfo(paths.raw))) {
          const marker = await jsonRecord(paths.raw);
          admittedConcurrent =
            marker.schema === STATE_SCHEMA &&
            marker.output === output &&
            marker.stamp === current &&
            marker.parent === parent;
        }
        if (current !== before && !admittedConcurrent)
          throw new Error(`Foreign logical output replacement; preserving it: ${output}`);
        if (metadata(await fileInfo(working), working) !== owned.get(working))
          throw new Error(`Working-file ownership changed: ${working}`);
        await Identity.files((fs) =>
          Effect.gen(function* () {
            const file = yield* fs.open(working, { flag: "r+", noFollow: true });
            if (metadata(yield* file.stat, working) !== owned.get(working))
              throw new Error("Working-file inode changed before conversion.");
            yield* file.writeAll(logical);
            yield* file.truncate(logical.length);
          }),
        );
        if (result === 0) await create(staging, encodeEnvelope(identity, logical, STATE_SCHEMA));
        if (
          !(await unchanged()) ||
          parent !== (await parentIdentity(root, directory)) ||
          (await outputStamp(output)) !== current
        )
          throw new Error(
            "Configuration, parent or logical destination changed before atomic publication.",
          );
        await Identity.files((fs) => fs.rename(working, output));
        transferred = true;
        owned.delete(working);
        const stamp = await outputStamp(output);
        await create(
          markerStaging,
          Buffer.from(JSON.stringify({ schema: STATE_SCHEMA, output, parent, stamp })),
        );
        await Identity.files((fs) => fs.rename(markerStaging, paths.raw));
        owned.delete(markerStaging);
        outputAccounted = true;
        if (result === 0) {
          await Identity.files((fs) => fs.rename(staging, paths.entry));
          owned.delete(staging);
        }
      });
    };
    await publishNativeResult();
  } catch (error) {
    failed = true;
    failure = error;
  }
  {
    if (!exitConfirmed || uncertainOwnership || (transferred && !outputAccounted)) {
      await markUnknown();
      throw new Error(
        `Unconfirmed native exit/ownership/output accounting; preserve ${[...owned.keys(), uncertainOwnership].filter(Boolean).join(", ")}`,
        { cause: failure },
      );
    }
    const cleanupFailures: string[] = [];
    // Accounting outlives all external scratch; a failed cleanup must retain it.
    for (const [filename, owner] of [...owned].filter(([filename]) => filename !== account)) {
      try {
        await safeDirectory(root, Identity.path.dirname(filename), false);
        if (metadata(await fileInfo(filename), filename) !== owner)
          throw new Error("ownership changed");
        await Identity.files((fs) => fs.remove(filename));
        owned.delete(filename);
      } catch (error) {
        cleanupFailures.push(`${filename}: ${String(error)}`);
      }
    }
    if (cleanupFailures.length) {
      await markUnknown();
      throw new Error(
        `Build-info cleanup refused; preserve account ${account}; ${cleanupFailures.join("; ")}`,
        { cause: failure },
      );
    }
    if (owned.has(account)) {
      await coordinated(root, paths.namespace, new AbortController().signal, async () => {
        if (metadata(await fileInfo(account), account) !== owned.get(account))
          throw new Error(`Accounting ownership changed: ${account}`);
        await Identity.files((fs) => fs.remove(account));
      });
    }
  }
  if (failed) throw failure;
  return result;
}
