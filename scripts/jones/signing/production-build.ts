#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off - Build-only signing adapter; native credentials never enter application services.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { JONES_MACOS_CLI_ID } from "./cli.ts";
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
    return NodeChildProcess.execFileSync(command, args, {
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

export function productionTarget(value = "desktop"): "desktop" | "cli" {
  if (value !== "desktop" && value !== "cli") throw new Error("Unknown production signing target.");
  return value;
}

export function productionBuild(target: "desktop" | "cli" = "desktop"): void {
  productionTarget(target);
  requireTrustedSigningContext(process.env);
  // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone signing entrypoint checks the actual runner before creating native resources without loading application services.
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
  const scratch = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-signing-"));
  NodeFS.chmodSync(scratch, 0o700);
  const keychain = NodePath.join(scratch, "job.keychain-db");
  let created = false;
  try {
    const importer = NodePath.join(scratch, "import-identity");
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
    const key = NodePath.join(scratch, "notary.p8");
    const profile = NodePath.join(scratch, "desktop.provisionprofile");
    NodeFS.writeFileSync(key, Buffer.from(process.env.APPLE_NOTARY_API_KEY_P8_BASE64!, "base64"), {
      mode: 0o600,
      flag: "wx",
    });
    const nativePasskeys = target === "desktop" && process.env.JONES_MACOS_NATIVE_PASSKEYS === "enabled";
    if (nativePasskeys) {
      if (!process.env.MACOS_PROVISIONING_PROFILE_BASE64?.trim())
        throw new Error("Enabled native passkeys require a provisioning profile.");
      NodeFS.writeFileSync(profile, Buffer.from(process.env.MACOS_PROVISIONING_PROFILE_BASE64, "base64"), {
        mode: 0o600,
        flag: "wx",
      });
    } else if (target === "desktop" && process.env.MACOS_PROVISIONING_PROFILE_BASE64?.trim()) {
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
    const version: unknown = JSON.parse(
      NodeFS.readFileSync(
        target === "desktop" ? "apps/desktop/package.json" : "apps/server/package.json",
        "utf8",
      ),
    ).version;
    if (typeof version !== "string" || !/^[a-zA-Z0-9.+-]+$/.test(version))
      throw new Error("Invalid production version.");
    const output = target === "desktop" ? "artifacts/desktop-production" : "artifacts/cli-production";
    const buildArgs =
      target === "desktop"
        ? [
            "scripts/build-desktop-artifact.ts",
            "--platform",
            "mac",
            "--target",
            "dmg",
            "--arch",
            "arm64",
            "--signed",
            "--output-dir",
            output,
          ]
        : [
            "scripts/build-cli-archive.ts",
            "--platform",
            "mac",
            "--arch",
            "arm64",
            "--version",
            version,
            "--resource-monitor-dir",
            "artifacts/cli-build-inputs/resource-monitor",
            "--output-dir",
            output,
          ];
    run(process.execPath, buildArgs, buildEnv);
    const extension = target === "desktop" ? ".dmg" : ".tar.gz";
    const artifacts = NodeFS.readdirSync(output).filter((name) => name.endsWith(extension));
    if (artifacts.length !== 1) throw new Error("Expected one production artifact.");
    const artifact = artifacts[0]!;
    if (target === "desktop") run("/usr/bin/hdiutil", ["verify", NodePath.join(output, artifact)], env);
    else
      run(
        process.execPath,
        [
          "scripts/smoke-cli-archive.ts",
          "--archive",
          NodePath.join(output, artifact),
          "--expect-version",
          version,
        ],
        env,
      );
    const sha256 = run("/usr/bin/shasum", ["-a", "256", NodePath.join(output, artifact)], env).split(
      /\s+/,
    )[0];
    const tree = run("git", ["rev-parse", "HEAD^{tree}"], env).trim();
    NodeFS.writeFileSync(
      NodePath.join(output, target === "desktop" ? `${artifact}.sha256` : "SHA256SUMS"),
      `${sha256}  ${artifact}\n`,
    );
    NodeFS.writeFileSync(NodePath.join(output, "SOURCE_COMMIT"), `${env.GITHUB_SHA}\n`);
    NodeFS.writeFileSync(
      NodePath.join(output, "ARTIFACT.json"),
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
          ...(target === "desktop"
            ? { bundleIdentifier: JONES_MACOS_APP_ID }
            : { signingIdentifier: JONES_MACOS_CLI_ID }),
          teamId: JONES_MACOS_TEAM,
        },
        null,
        2,
      ) + "\n",
    );
    if (env.GITHUB_ENV) NodeFS.appendFileSync(env.GITHUB_ENV, `JONES_PRODUCTION_VERSION=${version}\n`);
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
        NodeFS.rmSync(scratch, { recursive: true, force: true });
      }
    }
  }
}

if (process.argv[1] && NodePath.resolve(process.argv[1]) === NodeURL.fileURLToPath(import.meta.url)) {
  try {
    if (process.argv.length > 3) throw new Error("Production signing accepts one fixed target only.");
    productionBuild(productionTarget(process.argv[2]));
  } catch (error) {
    process.stderr.write(
      `${error instanceof Error ? error.message : "Production signing failed."}\n`,
    );
    process.exitCode = 1;
  }
}
