// @effect-diagnostics nodeBuiltinImport:off - explicitly bound external bootstrap artifacts.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { PROCESS_PLACEMENT_BOOTSTRAP_ENV, ProcessPlacementError } from "./processPlacement.ts";

export interface ProcessPlacementBootstrap {
  readonly version: 1;
  readonly executablePath: string;
  readonly executableSha256: string;
  readonly scriptPath: string;
  readonly scriptSha256: string;
  readonly policyPath: string;
  readonly policySha256: string;
  readonly helperPath: string;
  readonly helperSha256: string;
}
const fields = ["executable", "script", "policy", "helper"] as const;
export function readProcessPlacementBootstrap(
  environment: NodeJS.ProcessEnv = process.env,
): ProcessPlacementBootstrap | undefined {
  const text = environment[PROCESS_PLACEMENT_BOOTSTRAP_ENV];
  if (text === undefined) return undefined;
  if (Buffer.byteLength(text, "utf8") > 16 * 1024)
    throw new ProcessPlacementError("bootstrap binding exceeds 16 KiB");
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    throw new ProcessPlacementError("invalid bootstrap JSON");
  }
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new ProcessPlacementError("invalid bootstrap binding");
  const record = value as Record<string, unknown>;
  const keys = ["version", ...fields.flatMap((field) => [`${field}Path`, `${field}Sha256`])];
  if (record.version !== 1 || Object.keys(record).some((key) => !keys.includes(key)))
    throw new ProcessPlacementError("unsupported bootstrap binding");
  for (const field of fields) {
    const path = record[`${field}Path`];
    const sha = record[`${field}Sha256`];
    if (
      typeof path !== "string" ||
      !NodePath.isAbsolute(path) ||
      NodePath.normalize(path) !== path ||
      /[\u0000-\u001f\u007f]/.test(path) ||
      typeof sha !== "string" ||
      !/^[0-9a-f]{64}$/.test(sha)
    )
      throw new ProcessPlacementError("invalid bootstrap artifact identity");
  }
  return record as unknown as ProcessPlacementBootstrap;
}
export function validateProcessPlacementBootstrap(
  binding: ProcessPlacementBootstrap,
  platform = HostProcessPlatform.defaultValue(),
): void {
  if (platform !== "linux") throw new ProcessPlacementError("bootstrap requires Linux cgroup v2");
  try {
    for (const field of fields) {
      const path = binding[`${field}Path`];
      const stat = NodeFS.lstatSync(path);
      if (
        NodeFS.realpathSync(path) !== path ||
        !stat.isFile() ||
        (stat.mode & 0o022) !== 0 ||
        (stat.uid !== 0 && stat.uid !== process.getuid?.())
      )
        throw new ProcessPlacementError("unsafe bootstrap artifact");
      if ((field === "executable" || field === "helper") && (stat.mode & 0o111) === 0)
        throw new ProcessPlacementError("bootstrap executable unavailable");
      const sha = NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(path)).digest("hex");
      if (sha !== binding[`${field}Sha256`])
        throw new ProcessPlacementError("bootstrap artifact identity changed");
    }
  } catch (cause) {
    if (cause instanceof ProcessPlacementError) throw cause;
    throw new ProcessPlacementError("bootstrap artifact unavailable");
  }
}
export function placementBootstrapArguments(
  binding: ProcessPlacementBootstrap,
): ReadonlyArray<string> {
  return [
    binding.scriptPath,
    "--script-sha256",
    binding.scriptSha256,
    "--policy",
    binding.policyPath,
    "--policy-sha256",
    binding.policySha256,
    "--helper",
    binding.helperPath,
    "--helper-sha256",
    binding.helperSha256,
    "--",
  ];
}
