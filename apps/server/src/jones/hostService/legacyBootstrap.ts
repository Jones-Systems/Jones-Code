// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeZlib from "node:zlib";
import { parseServiceState, type ServiceState } from "../../cloud/serviceProtocol.ts";
import { decodeMigrationPlan, type MigrationPlan } from "../updates/migrationPlan.ts";

export interface LegacyBootstrapBinding {
  readonly legacyDirectServe?: boolean;
  readonly taskOperationId?: string;
  readonly taskHandoffSha256?: string;
  readonly taskDropin?: string;
  readonly preserveDropin?: ReadonlyArray<string>;
  readonly serviceUnit?: "jones-code.service" | "t3code.service";
  readonly serviceUnitSha256?: string;
  readonly acceptUnattestedChildCapability?: boolean;
}
const purpose = "retain existing managed tunnel during operator-owned replacement";
const digestPattern = /^[a-f0-9]{64}$/;
const object = (value: unknown): Record<string, unknown> => {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new Error("Invalid task handoff document.");
  return value as Record<string, unknown>;
};
const exactKeys = (value: Record<string, unknown>, keys: readonly string[]) =>
  JSON.stringify(Object.keys(value).sort()) === JSON.stringify([...keys].sort());
export const bytesSha256 = (bytes: string | Uint8Array) =>
  NodeCrypto.createHash("sha256").update(bytes).digest("hex");
export type AdoptionState =
  | { readonly kind: "native"; readonly state: ServiceState }
  | {
      readonly kind: "task-handoff";
      readonly activeVersion: string;
      readonly operationId: string;
      readonly preimageSha256: string;
    };
/** A bound task marker's pending status is separate from every native update phase. */
export function classifyAdoptionServiceState(
  text: string,
  binding: LegacyBootstrapBinding,
): AdoptionState {
  const native = parseServiceState(text);
  if (native !== undefined) {
    if (native.update?.status === "pending")
      throw new Error("Service state is pending or unknown.");
    if (binding.legacyDirectServe === true)
      throw new Error("Legacy mode requires the exact task handoff preimage.");
    return { kind: "native", state: native };
  }
  if (binding.legacyDirectServe !== true) throw new Error("Service state is pending or unknown.");
  if (
    !binding.taskOperationId ||
    !digestPattern.test(binding.taskHandoffSha256 ?? "") ||
    bytesSha256(text) !== binding.taskHandoffSha256 ||
    binding.acceptUnattestedChildCapability !== true
  )
    throw new Error(
      "Legacy bootstrap requires matching operation/hash bindings and explicit unattested-child acceptance.",
    );
  const document = object(JSON.parse(text) as unknown);
  const update = object(document.update);
  const from = object(document.source);
  const target = object(document.target);
  if (
    !exactKeys(document, [
      "nativeProtocol3State",
      "operationId",
      "purpose",
      "schema",
      "source",
      "target",
      "update",
    ]) ||
    document.schema !== "jones.task-tunnel-handoff/1" ||
    document.purpose !== purpose ||
    document.operationId !== binding.taskOperationId ||
    document.nativeProtocol3State !== false ||
    !exactKeys(update, ["operationId", "status"]) ||
    update.operationId !== document.operationId ||
    update.status !== "pending" ||
    !exactKeys(from, ["binarySha256", "pid", "startTicks", "uid", "version"]) ||
    !digestPattern.test(String(from.binarySha256)) ||
    !Number.isSafeInteger(from.pid) ||
    Number(from.pid) < 1 ||
    !Number.isSafeInteger(from.uid) ||
    Number(from.uid) < 1 ||
    !(
      (typeof from.startTicks === "string" && /^\d+$/.test(from.startTicks)) ||
      (typeof from.startTicks === "number" &&
        Number.isSafeInteger(from.startTicks) &&
        from.startTicks >= 0)
    ) ||
    typeof from.version !== "string" ||
    !/^\d+\.\d+\.\d+-nightly\.\d{8}\.\d+$/.test(from.version) ||
    !exactKeys(target, ["version"]) ||
    typeof target.version !== "string" ||
    !/^\d+\.\d+\.\d+-preview\.\d{8}\.\d+(?:\.\d+)?$/.test(target.version)
  )
    throw new Error(
      "Task handoff shape, purpose or pending-marker identity is not the bound completed-installation handoff.",
    );
  return {
    kind: "task-handoff",
    activeVersion: target.version,
    operationId: binding.taskOperationId,
    preimageSha256: binding.taskHandoffSha256!,
  };
}
export function taskDropinBinding(value: string | undefined) {
  const match = /^(50-[A-Za-z0-9_.-]+\.conf)=([a-f0-9]{64})$/.exec(value ?? "");
  if (!match)
    throw new Error("Legacy bootstrap requires a bound task drop-in 50 name and SHA-256.");
  return { name: match[1]!, sha256: match[2]! };
}
/** Parse only the public task-owned direct-serve directive set; no credential directives are opened. */
export function parseTaskDropin(text: string, baseDir: string) {
  let section = false;
  let executable: string | undefined;
  let reset = false;
  const environment: Record<string, string> = {};
  let argv: string[] = [];
  for (const raw of text.split("\n")) {
    const line = raw.trim();
    if (line === "" || line.startsWith("#") || line.startsWith(";")) continue;
    if (line === "[Service]") {
      if (section) throw new Error("Duplicate task service section.");
      section = true;
      continue;
    }
    if (!section) throw new Error("Unknown task drop-in section or directive.");
    if (line === "ExecStart=") {
      if (reset || executable !== undefined) throw new Error("Ambiguous task ExecStart reset.");
      reset = true;
      continue;
    }
    if (line.startsWith("ExecStart=")) {
      if (!reset || executable !== undefined)
        throw new Error("Task direct-serve entry requires one reset and one command.");
      argv = line.slice("ExecStart=".length).split(/\s+/);
      executable = argv.shift();
      if (
        !executable ||
        !NodePath.isAbsolute(executable) ||
        /["'\\$`;\r\n]/.test(executable) ||
        argv.shift() !== "serve"
      )
        throw new Error("Unsupported task executable or command.");
      continue;
    }
    if (line.startsWith("WorkingDirectory=")) {
      if (
        line.slice("WorkingDirectory=".length) !== baseDir &&
        line.slice("WorkingDirectory=".length) !== "%h"
      )
        throw new Error("Unknown task working directory.");
      continue;
    }
    const env = /^Environment=(T3CODE_(?:HOME|PORT|HOST|MODE))=([^\s"'\\$`;]+)$/.exec(line);
    if (env && environment[env[1]!] === undefined) {
      environment[env[1]!] = env[2]!;
      continue;
    }
    throw new Error(
      "Unknown task drop-in directive; no credential or additional service effects are allowed.",
    );
  }
  if (!executable || !reset || argv.length % 2 !== 0)
    throw new Error("Missing or ambiguous task direct-serve argv.");
  const seen = new Set<string>();
  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index]!;
    const value = argv[index + 1]!;
    if (
      !["--host", "--port", "--base-dir", "--mode"].includes(flag) ||
      seen.has(flag) ||
      /["'\\$`;\r\n]/.test(value)
    )
      throw new Error("Unmapped or repeated task serve flag.");
    seen.add(flag);
    const key = {
      "--host": "T3CODE_HOST",
      "--port": "T3CODE_PORT",
      "--base-dir": "T3CODE_HOME",
      "--mode": "T3CODE_MODE",
    }[flag]!;
    if (environment[key] !== undefined && environment[key] !== value)
      throw new Error("Conflicting task environment and serve argv.");
    environment[key] = value;
  }
  if (
    environment.T3CODE_HOME !== baseDir ||
    environment.T3CODE_HOST !== "127.0.0.1" ||
    !/^\d+$/.test(environment.T3CODE_PORT ?? "") ||
    Number(environment.T3CODE_PORT) < 1 ||
    Number(environment.T3CODE_PORT) > 65535 ||
    (environment.T3CODE_MODE !== undefined && !["web", "desktop"].includes(environment.T3CODE_MODE))
  )
    throw new Error("Task home, loopback host, port or mode is not bound.");
  return {
    executable,
    environment,
    port: Number(environment.T3CODE_PORT),
    execStart: `ExecStart=${executable} serve ${argv.join(" ")}`,
  };
}
export async function fileMetadata(file: string) {
  const stat = await NodeFSP.lstat(file, { bigint: true });
  if (!stat.isFile() || stat.isSymbolicLink())
    throw new Error("Preserved drop-in is not a regular file.");
  return JSON.stringify({
    dev: String(stat.dev),
    ino: String(stat.ino),
    size: String(stat.size),
    mtimeNs: String(stat.mtimeNs),
    ctimeNs: String(stat.ctimeNs),
    mode: String(stat.mode),
    uid: String(stat.uid),
  });
}
export async function assertNoBootstrapHazards(
  base: string,
  uid: number,
): Promise<readonly string[]> {
  const exists = async (file: string) =>
    NodeFSP.lstat(file).then(
      () => true,
      (e: NodeJS.ErrnoException) => {
        if (e.code !== "ENOENT") throw e;
        return false;
      },
    );
  for (const relative of [
    "runtime/.restart-pending",
    "runtime/.service-stopping",
    "runtime/native-store-authority",
  ])
    if (await exists(NodePath.join(base, relative)))
      throw new Error(
        "Pending or native-store authority state requires reconciliation before bootstrap.",
      );
  const authorityDirectory = NodePath.join(base, "native-store-authority");
  const authority = await NodeFSP.lstat(authorityDirectory).catch((e: NodeJS.ErrnoException) => {
    if (e.code !== "ENOENT") throw e;
    return undefined;
  });
  if (authority !== undefined) {
    // Server startup creates this private directory even before authority state exists.
    if (
      !authority.isDirectory() ||
      authority.isSymbolicLink() ||
      authority.uid !== uid ||
      (authority.mode & 0o777) !== 0o700 ||
      (await NodeFSP.readdir(authorityDirectory)).length !== 0
    )
      throw new Error(
        "Pending or native-store authority state requires reconciliation before bootstrap.",
      );
    const observed = await NodeFSP.lstat(authorityDirectory);
    if (
      observed.dev !== authority.dev ||
      observed.ino !== authority.ino ||
      observed.uid !== authority.uid ||
      observed.mode !== authority.mode
    )
      throw new Error("Native-store authority directory changed during bootstrap inspection.");
  }
  for (const relative of ["runtime/jones-updates/selections", "runtime/staged-updates"])
    if (
      (await exists(NodePath.join(base, relative))) &&
      (await NodeFSP.readdir(NodePath.join(base, relative))).length > 0
    )
      throw new Error("Staged native update state prevents bootstrap.");
  const backups = NodePath.join(base, "runtime/db-backup");
  const retained = await NodeFSP.readdir(backups).catch((e: NodeJS.ErrnoException) => {
    if (e.code !== "ENOENT") throw e;
    return [] as string[];
  });
  for (const name of retained) {
    const directory = NodePath.join(backups, name);
    const stat = await NodeFSP.lstat(directory);
    if (
      !stat.isDirectory() ||
      stat.isSymbolicLink() ||
      (await exists(NodePath.join(directory, ".restore-pending")))
    )
      throw new Error("Interrupted or unknown retained backup prevents bootstrap.");
  }
  return retained;
}
export function readPreflightMigrationPlan(text: string, version: string): MigrationPlan {
  const result = object(JSON.parse(text) as unknown);
  const plan = decodeMigrationPlan(result.migrationPlan);
  if (
    result.status !== "ready" ||
    result.version !== version ||
    result.launcherProtocol !== 4 ||
    result.startupGateProtocol !== 1 ||
    plan === undefined
  )
    throw new Error("Candidate read-only migration preflight is unavailable.");
  return plan;
}
/** Hash the archive's single ordinary t3 entry without extraction or native-state access. */
export async function archiveExecutableSha256(archive: string): Promise<string> {
  const input = NodeFS.createReadStream(archive);
  const gzip = input.pipe(NodeZlib.createGunzip());
  input.on("error", (error) => gzip.destroy(error));
  let buffer = Buffer.alloc(0);
  let remaining = 0;
  let padding = 0;
  let selected = false;
  let found = false;
  let total = 0;
  const digest = NodeCrypto.createHash("sha256");
  const text = (bytes: Buffer) => bytes.toString("utf8").replace(/\0.*$/s, "");
  try {
    for await (const bytes of gzip) {
      total += bytes.length;
      if (total > 2 * 1024 * 1024 * 1024)
        throw new Error("Archive exceeds the expanded byte bound.");
      buffer = Buffer.concat([buffer, bytes]);
      while (buffer.length > 0) {
        if (remaining > 0) {
          const body = buffer.subarray(0, Math.min(remaining, buffer.length));
          if (selected) digest.update(body);
          remaining -= body.length;
          buffer = buffer.subarray(body.length);
          continue;
        }
        if (padding > 0) {
          const count = Math.min(padding, buffer.length);
          padding -= count;
          buffer = buffer.subarray(count);
          continue;
        }
        if (buffer.length < 512) break;
        const header = buffer.subarray(0, 512);
        buffer = buffer.subarray(512);
        if (header.every((byte) => byte === 0)) continue;
        const size = text(header.subarray(124, 136)).trim();
        if (!/^[0-7]+$/.test(size)) throw new Error("Unsupported archive header.");
        remaining = Number.parseInt(size, 8);
        padding = (512 - (remaining % 512)) % 512;
        const prefix = text(header.subarray(345, 500));
        const name = `${prefix ? `${prefix}/` : ""}${text(header.subarray(0, 100))}`;
        selected = /^[^/]+\/t3$/.test(name);
        if (selected) {
          if (found || ![0, 48].includes(header[156] ?? 0) || remaining < 1)
            throw new Error("Archive t3 entry is ambiguous.");
          found = true;
        }
      }
    }
    if (!found || remaining !== 0 || padding !== 0)
      throw new Error("Archive executable is missing or truncated.");
    return digest.digest("hex");
  } finally {
    input.destroy();
    gzip.destroy();
  }
}
