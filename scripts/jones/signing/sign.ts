// @effect-diagnostics nodeBuiltinImport:off - Build-only signing adapter; native credentials never enter application services.
import { execFile } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { sign as signApplication, type SignOptions } from "@electron/osx-sign";
import { componentIdentifier, JONES_MACOS_APP_ID, JONES_MACOS_IDENTITY, productionSigning, signingRequirement } from "./policy.ts";

const exec = promisify(execFile);
async function run(command: string, args: string[]): Promise<void> {
  try {
    await exec(command, args, { maxBuffer: 1024 * 1024, timeout: 30 * 60 * 1000 });
  } catch {
    // Tool failures can include request credentials; only the operation is public evidence.
    throw new Error(`Production signing operation failed: ${command}.`);
  }
}

export function productionSignOptions(options: SignOptions, env: NodeJS.ProcessEnv): SignOptions {
  const keychain = env.CSC_KEYCHAIN;
  if (!keychain) throw new Error("Production signing keychain missing.");
  return {
    ...options,
    identity: JONES_MACOS_IDENTITY,
    keychain,
    platform: "darwin",
    type: "distribution",
    strictVerify: true,
    optionsForFile: (file, context) => {
      const inherited = options.optionsForFile?.(file, context);
      const identifier = componentIdentifier(options.app, file);
      return {
        ...inherited,
        hardenedRuntime: true,
        timestamp: "http://timestamp.apple.com/ts01",
        requirements: `=designated => ${signingRequirement(identifier)}`,
        additionalArguments: [...(inherited?.additionalArguments ?? []), "--identifier", identifier],
      };
    },
    batchCodesignCalls: true,
  };
}

export async function signMac(options: SignOptions): Promise<void> {
  if (!productionSigning(process.env)) {
    await signApplication({ ...options, batchCodesignCalls: true });
    return;
  }
  const key = process.env.JONES_NOTARY_KEY_FILE;
  const keyId = process.env.APPLE_NOTARY_KEY_ID;
  const issuer = process.env.APPLE_NOTARY_ISSUER_ID;
  if (!key || !keyId || !issuer) throw new Error("Production notarization configuration missing.");
  await signApplication(productionSignOptions(options, process.env));
  await run("/usr/bin/codesign", ["--verify", "--deep", "--strict", "-R", signingRequirement(JONES_MACOS_APP_ID), options.app]);
  const scratch = await mkdtemp(join(tmpdir(), "jones-notarize-"));
  try {
    const archive = join(scratch, "application.zip");
    await run("/usr/bin/ditto", ["-c", "-k", "--keepParent", options.app, archive]);
    await run("/usr/bin/xcrun", ["notarytool", "submit", archive, "--key", key, "--key-id", keyId, "--issuer", issuer, "--wait", "--output-format", "json"]);
    await run("/usr/bin/xcrun", ["stapler", "staple", options.app]);
    await run("/usr/bin/xcrun", ["stapler", "validate", options.app]);
    await run("/usr/sbin/spctl", ["--assess", "--type", "execute", options.app]);
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}
