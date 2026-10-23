// @effect-diagnostics nodeBuiltinImport:off - Build-only adapter over a task-owned staged CLI distribution.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import {
  JONES_MACOS_IDENTITY,
  JONES_MACOS_TEAM,
  productionSigning,
  signingRequirement,
} from "./policy.ts";

export const JONES_MACOS_CLI_ID = "com.jonessystems.jonescode.cli";
type Environment = Readonly<Record<string, string | undefined>>;
type CommandResult = { stdout: string; stderr: string };
export type CliSigningCommand = (command: string, args: string[]) => CommandResult;

function execute(command: string, args: string[]): CommandResult {
  const result = NodeChildProcess.spawnSync(command, args, {
    encoding: "utf8",
    stdio: ["pipe", "pipe", "pipe"],
    maxBuffer: 1024 * 1024,
    timeout: 30 * 60 * 1000,
  });
  if (result.error || result.status !== 0) {
    throw new Error(`Production CLI signing operation failed: ${command}.`);
  }
  return { stdout: result.stdout, stderr: result.stderr };
}

export function requireProductionCliBuild(
  env: Environment,
  target: { platform: string; arch: string },
): boolean {
  if (!productionSigning(env)) return false;
  if (target.platform !== "mac" || target.arch !== "arm64") {
    throw new Error("Production CLI signing requires the Apple Silicon macOS target.");
  }
  if (env.CSC_NAME !== JONES_MACOS_IDENTITY || env.T3CODE_APPLE_TEAM_ID !== JONES_MACOS_TEAM) {
    throw new Error("Production CLI signing requires the expected Developer ID identity and team.");
  }
  for (const key of [
    "CSC_KEYCHAIN",
    "JONES_NOTARY_KEY_FILE",
    "APPLE_NOTARY_KEY_ID",
    "APPLE_NOTARY_ISSUER_ID",
  ]) {
    if (!env[key]?.trim()) throw new Error(`Production CLI signing prerequisite missing: ${key}.`);
  }
  return true;
}

export function cliComponentIdentifier(contentDir: string, file: string): string {
  const name = NodePath.relative(NodePath.resolve(contentDir), NodePath.resolve(file))
    .split(NodePath.sep)
    .join("/");
  if (!name || name === ".." || name.startsWith("../") || NodePath.isAbsolute(name)) {
    throw new Error("CLI signing component escapes the staged distribution.");
  }
  if (name === "t3") return JONES_MACOS_CLI_ID;
  const stable = name.replace(/node_modules\/\.pnpm\/[^/]+\/node_modules\//g, "node_modules/");
  return `${JONES_MACOS_CLI_ID}.component.${NodeCrypto.createHash("sha256").update(stable).digest("hex")}`;
}

type PayloadFile = { path: string; name: string; digest: string; mode: number; machO: boolean };
const machOMagic = new Set([
  "feedface",
  "cefaedfe",
  "feedfacf",
  "cffaedfe",
  "cafebabe",
  "bebafeca",
  "cafebabf",
  "bfbafeca",
]);

function payloadFiles(contentDir: string): PayloadFile[] {
  const root = NodePath.resolve(contentDir);
  if (!NodeFS.lstatSync(root).isDirectory()) throw new Error("CLI staging root must be a directory.");
  const files: PayloadFile[] = [];
  function visit(directory: string): void {
    for (const name of NodeFS.readdirSync(directory).sort()) {
      const path = NodePath.join(directory, name);
      const stat = NodeFS.lstatSync(path);
      if (stat.isDirectory()) {
        visit(path);
      } else if (stat.isFile()) {
        const fd = NodeFS.openSync(path, "r");
        const hash = NodeCrypto.createHash("sha256");
        const chunk = Buffer.alloc(64 * 1024);
        let first = true;
        let machO = false;
        try {
          for (;;) {
            const size = NodeFS.readSync(fd, chunk, 0, chunk.length, null);
            if (!size) break;
            if (first) machO = size >= 4 && machOMagic.has(chunk.subarray(0, 4).toString("hex"));
            first = false;
            hash.update(chunk.subarray(0, size));
          }
        } finally {
          NodeFS.closeSync(fd);
        }
        files.push({
          path,
          name: NodePath.relative(root, path).split(NodePath.sep).join("/"),
          digest: hash.digest("hex"),
          mode: stat.mode & 0o777,
          machO,
        });
      } else {
        throw new Error("Production CLI payload must contain regular files and directories only.");
      }
    }
  }
  visit(root);
  return files;
}

function digestPayload(files: PayloadFile[]): string {
  return NodeCrypto.createHash("sha256")
    .update(JSON.stringify(files.map(({ name, digest, mode }) => ({ name, digest, mode }))))
    .digest("hex");
}

export function inventoryCliMachO(contentDir: string): string[] {
  const files = payloadFiles(contentDir).filter((file) => file.machO);
  if (!files.some((file) => file.name === "t3")) throw new Error("Staged CLI executable is not Mach-O.");
  for (const file of files) {
    if (/(^|\/)@openai\//.test(file.name) || /^codex(?:[-.]|$)/.test(NodePath.basename(file.name))) {
      throw new Error("OpenAI-owned binaries are outside Jones CLI signing scope.");
    }
  }
  return files
    .sort((a, b) => {
      if (a.name === "t3") return 1;
      if (b.name === "t3") return -1;
      return b.name.split("/").length - a.name.split("/").length || a.name.localeCompare(b.name);
    })
    .map((file) => file.path);
}

export type CliPayloadSeal = { digest: string; stem: string };

export function assertCliPayloadUnchanged(contentDir: string, seal: CliPayloadSeal): void {
  if (
    NodePath.basename(contentDir) !== seal.stem ||
    digestPayload(payloadFiles(contentDir)) !== seal.digest
  ) {
    throw new Error("CLI distribution changed after its notarization submission.");
  }
}

export function signProductionCli(
  input: { contentDir: string; entitlements: string },
  env: Environment,
  run: CliSigningCommand = execute,
): CliPayloadSeal {
  if (!requireProductionCliBuild(env, { platform: "mac", arch: "arm64" })) {
    throw new Error("Production CLI signing requires explicit production mode.");
  }
  const components = inventoryCliMachO(input.contentDir);
  if (
    new Set(components.map((file) => cliComponentIdentifier(input.contentDir, file))).size !==
    components.length
  ) {
    throw new Error("CLI components do not have distinct signing identifiers.");
  }
  for (const component of components) {
    const identifier = cliComponentIdentifier(input.contentDir, component);
    run("/usr/bin/codesign", [
      "--force",
      "--sign",
      JONES_MACOS_IDENTITY,
      "--keychain",
      env.CSC_KEYCHAIN!,
      "--options",
      "runtime",
      "--timestamp",
      "--identifier",
      identifier,
      "--requirements",
      `=designated => ${signingRequirement(identifier)}`,
      ...(identifier === JONES_MACOS_CLI_ID
        ? ["--entitlements", input.entitlements]
        : ["--preserve-metadata=entitlements"]),
      component,
    ]);
    run("/usr/bin/codesign", [
      "--verify",
      "--strict",
      "-R",
      `=${signingRequirement(identifier)}`,
      component,
    ]);
    const metadata = run("/usr/bin/codesign", ["--display", "--verbose=4", component]);
    const lines = `${metadata.stdout}\n${metadata.stderr}`.split(/\r?\n/);
    if (
      !lines.includes(`Identifier=${identifier}`) ||
      !lines.includes(`TeamIdentifier=${JONES_MACOS_TEAM}`) ||
      !lines.includes(`Authority=${JONES_MACOS_IDENTITY}`) ||
      !lines.some((line) => line.startsWith("Timestamp=")) ||
      !lines.some((line) => /flags=.*\bruntime\b/.test(line))
    ) {
      throw new Error("CLI component signing identity, timestamp or hardened runtime differs.");
    }
  }
  const seal = {
    digest: digestPayload(payloadFiles(input.contentDir)),
    stem: NodePath.basename(input.contentDir),
  };
  const scratch = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-cli-notary-"));
  try {
    const archive = NodePath.join(scratch, "distribution.zip");
    run("/usr/bin/ditto", ["-c", "-k", "--keepParent", input.contentDir, archive]);
    assertCliPayloadUnchanged(input.contentDir, seal);
    const result = run("/usr/bin/xcrun", [
      "notarytool",
      "submit",
      archive,
      "--key",
      env.JONES_NOTARY_KEY_FILE!,
      "--key-id",
      env.APPLE_NOTARY_KEY_ID!,
      "--issuer",
      env.APPLE_NOTARY_ISSUER_ID!,
      "--wait",
      "--output-format",
      "json",
    ]);
    let receipt: unknown;
    try {
      receipt = JSON.parse(result.stdout);
    } catch {
      throw new Error("CLI notarization returned an invalid acceptance receipt.");
    }
    if (
      typeof receipt !== "object" ||
      receipt === null ||
      !("status" in receipt) ||
      receipt.status !== "Accepted"
    ) {
      throw new Error("Complete CLI distribution was not accepted by notarization.");
    }
    assertCliPayloadUnchanged(input.contentDir, seal);
    return seal;
  } finally {
    NodeFS.rmSync(scratch, { recursive: true, force: true });
  }
}
