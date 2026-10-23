#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off - Build-only signing adapter; native credentials never enter application services.
import { execFileSync } from "node:child_process";
import {
  appendFileSync,
  chmodSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  JONES_MACOS_APP_ID,
  JONES_MACOS_IDENTITY,
  JONES_MACOS_TEAM,
  productionSigning,
} from "./policy.ts";

const sensitiveKeys = [
  "MACOS_SIGNING_CERTIFICATE_P12_BASE64",
  "MACOS_SIGNING_CERTIFICATE_PASSWORD",
  "APPLE_NOTARY_API_KEY_P8_BASE64",
  "MACOS_PROVISIONING_PROFILE_BASE64",
] as const;

export function buildEnvironment(env: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const clean = { ...env };
  for (const key of sensitiveKeys) delete clean[key];
  for (const key of [
    "CSC_LINK",
    "CSC_KEY_PASSWORD",
    "APPLE_API_KEY",
    "APPLE_API_KEY_ID",
    "APPLE_API_ISSUER",
    "APPLE_ID",
    "APPLE_APP_SPECIFIC_PASSWORD",
    "DEBUG",
  ])
    delete clean[key];
  return clean;
}

export function requireTrustedSigningContext(env: NodeJS.ProcessEnv): void {
  if (
    env.GITHUB_REPOSITORY !== "Jones-Systems/Jones-Code" ||
    env.GITHUB_REF !== "refs/heads/main" ||
    !["push", "workflow_dispatch"].includes(env.GITHUB_EVENT_NAME ?? "") ||
    !/^[a-f0-9]{40}$/.test(env.GITHUB_SHA ?? "") ||
    !productionSigning(env)
  ) {
    throw new Error("Production signing requires the canonical default-branch signing workflow.");
  }
  for (const key of [
    ...sensitiveKeys.filter((key) => key !== "MACOS_PROVISIONING_PROFILE_BASE64"),
    "APPLE_NOTARY_KEY_ID",
    "APPLE_NOTARY_ISSUER_ID",
  ] as const) {
    if (!env[key]?.trim()) throw new Error(`Production signing prerequisite missing: ${key}.`);
  }
}

function run(command: string, args: string[], env: NodeJS.ProcessEnv, input?: string): string {
  try {
    return execFileSync(command, args, {
      env,
      input,
      encoding: "utf8",
      stdio: ["pipe", "pipe", "pipe"],
      maxBuffer: 32 * 1024 * 1024,
    });
  } catch {
    throw new Error(`Production signing operation failed: ${command}.`);
  }
}

export function productionBuild(): void {
  requireTrustedSigningContext(process.env);
  if (process.platform !== "darwin")
    throw new Error("Production signing requires a disposable macOS runner.");
  const env = buildEnvironment(process.env);
  if (run("git", ["rev-parse", "HEAD"], env).trim() !== env.GITHUB_SHA)
    throw new Error("Signing source differs from workflow commit.");
  const searchList = run("/usr/bin/security", ["list-keychains", "-d", "user"], env)
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line.trim()) as string);
  const scratch = mkdtempSync(join(tmpdir(), "jones-signing-"));
  chmodSync(scratch, 0o700);
  const keychain = join(scratch, "job.keychain-db");
  let created = false;
  try {
    const importer = join(scratch, "import-identity");
    run(
      "/usr/bin/clang",
      [
        "-framework",
        "Foundation",
        "-framework",
        "Security",
        "-o",
        importer,
        "scripts/jones/signing/import-identity.m",
      ],
      env,
    );
    // This keychain is exclusively created for the job. Its password is intentionally empty.
    run("/usr/bin/security", ["create-keychain", "-p", "", keychain], env);
    created = true;
    run("/usr/bin/security", ["set-keychain-settings", "-lut", "21600", keychain], env);
    run("/usr/bin/security", ["unlock-keychain", "-p", "", keychain], env);
    run(
      importer,
      [keychain],
      env,
      JSON.stringify({
        certificate: process.env.MACOS_SIGNING_CERTIFICATE_P12_BASE64,
        password: process.env.MACOS_SIGNING_CERTIFICATE_PASSWORD,
      }),
    );
    run(
      "/usr/bin/security",
      ["set-key-partition-list", "-S", "apple-tool:,apple:,codesign:", "-s", "-k", "", keychain],
      env,
    );
    run("/usr/bin/security", ["list-keychains", "-d", "user", "-s", keychain, ...searchList], env);
    const key = join(scratch, "notary.p8");
    const profile = join(scratch, "desktop.provisionprofile");
    writeFileSync(key, Buffer.from(process.env.APPLE_NOTARY_API_KEY_P8_BASE64!, "base64"), {
      mode: 0o600,
      flag: "wx",
    });
    const nativePasskeys = process.env.JONES_MACOS_NATIVE_PASSKEYS === "enabled";
    if (nativePasskeys) {
      if (!process.env.MACOS_PROVISIONING_PROFILE_BASE64?.trim())
        throw new Error("Enabled native passkeys require a provisioning profile.");
      writeFileSync(profile, Buffer.from(process.env.MACOS_PROVISIONING_PROFILE_BASE64, "base64"), {
        mode: 0o600,
        flag: "wx",
      });
    } else if (process.env.MACOS_PROVISIONING_PROFILE_BASE64?.trim()) {
      throw new Error("Provisioning profile supplied without native passkey opt-in.");
    }
    const buildEnv = {
      ...env,
      CSC_KEYCHAIN: keychain,
      CSC_NAME: JONES_MACOS_IDENTITY,
      CSC_IDENTITY_AUTO_DISCOVERY: "true",
      T3CODE_DESKTOP_SIGNED: "true",
      T3CODE_APPLE_TEAM_ID: JONES_MACOS_TEAM,
      T3CODE_MACOS_PROVISIONING_PROFILE: nativePasskeys ? profile : "",
      JONES_NOTARY_KEY_FILE: key,
      JONES_MACOS_PRODUCTION_ID: JONES_MACOS_APP_ID,
    };
    run(
      process.execPath,
      [
        "scripts/build-desktop-artifact.ts",
        "--platform",
        "mac",
        "--target",
        "dmg",
        "--arch",
        "arm64",
        "--signed",
        "--output-dir",
        "artifacts/desktop-production",
      ],
      buildEnv,
    );
    const output = "artifacts/desktop-production";
    const artifacts = readdirSync(output).filter((name) => name.endsWith(".dmg"));
    if (artifacts.length !== 1) throw new Error("Expected one production DMG.");
    const artifact = artifacts[0]!;
    run("/usr/bin/hdiutil", ["verify", join(output, artifact)], env);
    const sha256 = run("/usr/bin/shasum", ["-a", "256", join(output, artifact)], env).split(
      /\s+/,
    )[0];
    const version: unknown = JSON.parse(readFileSync("apps/desktop/package.json", "utf8")).version;
    if (typeof version !== "string" || !/^[a-zA-Z0-9.+-]+$/.test(version))
      throw new Error("Invalid production version.");
    const tree = run("git", ["rev-parse", "HEAD^{tree}"], env).trim();
    writeFileSync(join(output, `${artifact}.sha256`), `${sha256}  ${artifact}\n`);
    writeFileSync(join(output, "SOURCE_COMMIT"), `${env.GITHUB_SHA}\n`);
    writeFileSync(
      join(output, "ARTIFACT.json"),
      JSON.stringify(
        {
          schema: 1,
          repository: "Jones-Systems/Jones-Code",
          source: env.GITHUB_SHA,
          tree,
          workflow: ".github/workflows/jones-desktop-signing.yml",
          event: env.GITHUB_EVENT_NAME,
          ref: env.GITHUB_REF,
          runId: env.GITHUB_RUN_ID,
          runAttempt: env.GITHUB_RUN_ATTEMPT,
          version,
          platform: "darwin",
          architecture: "arm64",
          artifact,
          sha256,
          signed: true,
          notarized: true,
          bundleIdentifier: JONES_MACOS_APP_ID,
          teamId: JONES_MACOS_TEAM,
        },
        null,
        2,
      ) + "\n",
    );
    if (env.GITHUB_ENV) appendFileSync(env.GITHUB_ENV, `JONES_PRODUCTION_VERSION=${version}\n`);
  } finally {
    // Restore the preimage even if keychain creation failed after changing its search list.
    try {
      run("/usr/bin/security", ["list-keychains", "-d", "user", "-s", ...searchList], env);
      const restored = run("/usr/bin/security", ["list-keychains", "-d", "user"], env)
        .trim()
        .split("\n")
        .filter(Boolean)
        .map((line) => JSON.parse(line.trim()));
      if (JSON.stringify(restored) !== JSON.stringify(searchList))
        throw new Error("Keychain search-list restoration readback differs.");
    } finally {
      try {
        if (created) run("/usr/bin/security", ["delete-keychain", keychain], env);
      } finally {
        rmSync(scratch, { recursive: true, force: true });
      }
    }
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    productionBuild();
  } catch (error) {
    process.stderr.write(
      `${error instanceof Error ? error.message : "Production signing failed."}\n`,
    );
    process.exitCode = 1;
  }
}
