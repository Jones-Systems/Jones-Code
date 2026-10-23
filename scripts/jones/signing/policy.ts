// @effect-diagnostics nodeBuiltinImport:off - Build-only signing adapter; native credentials never enter application services.
import { createHash } from "node:crypto";
import * as path from "node:path";

export const JONES_MACOS_TEAM = "2CUPSQS986";
export const JONES_MACOS_APP_ID = "com.jonessystems.jonescode";
export const JONES_MACOS_IDENTITY = `Developer ID Application: Malcolm Jones (${JONES_MACOS_TEAM})`;

type Environment = Readonly<Record<string, string | undefined>>;

export function productionSigning(env: Environment): boolean {
  const mode = env.JONES_MACOS_RELEASE_MODE;
  if (mode === undefined || mode === "development") return false;
  if (mode !== "production") throw new Error("Unknown Jones macOS release mode.");
  if (env.JONES_MACOS_PRODUCTION_ID !== JONES_MACOS_APP_ID) {
    throw new Error("Production bundle identity requires explicit owner-approved configuration.");
  }
  return true;
}

export function desktopAppId(env: Environment, developmentId: string): string {
  return productionSigning(env) ? JONES_MACOS_APP_ID : developmentId;
}

export function requireProductionBuild(
  env: Environment,
  options: { platform: string; signed: boolean; variant?: string },
): void {
  if (!productionSigning(env)) return;
  if (options.platform !== "mac" || !options.signed || options.variant === "preview-companion") {
    throw new Error("Production signing requires the signed macOS desktop target.");
  }
  if (env.T3CODE_APPLE_TEAM_ID !== JONES_MACOS_TEAM) {
    throw new Error("Production signing requires the Jones Developer ID team.");
  }
  nativePasskeysEnabled(env);
  for (const key of ["CSC_KEYCHAIN"] as const) {
    if (!env[key]?.trim()) throw new Error(`Production signing prerequisite missing: ${key}.`);
  }
}

export function nativePasskeysEnabled(env: Environment): boolean {
  if (!productionSigning(env)) return true;
  const mode = env.JONES_MACOS_NATIVE_PASSKEYS ?? "disabled";
  if (mode !== "enabled" && mode !== "disabled") throw new Error("Unknown native passkey mode.");
  if (mode === "disabled") {
    if (env.T3CODE_MACOS_PROVISIONING_PROFILE?.trim() || env.T3CODE_CLERK_PASSKEY_RP_DOMAINS?.trim()) {
      throw new Error("Native passkey configuration requires explicit opt-in; refusing silent fallback.");
    }
    return false;
  }
  if (!env.T3CODE_MACOS_PROVISIONING_PROFILE?.trim() ||
      !(env.T3CODE_CLERK_PUBLISHABLE_KEY?.trim() || env.T3CODE_CLERK_PASSKEY_RP_DOMAINS?.trim())) {
    throw new Error("Enabled native passkeys require a provisioning profile and Clerk RP configuration.");
  }
  return true;
}

export function requireProductionPasskeys(
  env: Environment,
  capabilities: { touchIdKeychainAccessGroup: string | undefined; browserPasskeys: boolean },
): void {
  if (productionSigning(env) && nativePasskeysEnabled(env) && (capabilities.touchIdKeychainAccessGroup !== `${JONES_MACOS_TEAM}.${JONES_MACOS_APP_ID}.webauthn` || !capabilities.browserPasskeys)) {
    throw new Error("Production provisioning profile must preserve Touch ID and browser passkeys.");
  }
}

export function componentIdentifier(app: string, file: string): string {
  const target = path.resolve(file).replace(/\.app[\\/]Contents[\\/]MacOS[\\/][^\\/]+$/, ".app");
  let relative = path.relative(path.resolve(app), target);
  if (relative === "") return JONES_MACOS_APP_ID;
  if (relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) {
    throw new Error("Signing component escapes the application bundle.");
  }
  // Dependency versions and framework version directories must not rotate code identity.
  relative = relative.replaceAll(path.sep, "/")
    .replace(/node_modules\/\.pnpm\/[^/]+\/node_modules\//g, "node_modules/")
    .replace(/\.framework\/Versions\/[^/]+\//g, ".framework/");
  return `${JONES_MACOS_APP_ID}.component.${createHash("sha256").update(relative).digest("hex")}`;
}

export function signingRequirement(identifier: string): string {
  if (!/^[a-zA-Z0-9.-]+$/.test(identifier)) throw new Error("Invalid code identifier.");
  return `identifier "${identifier}" and anchor apple generic and certificate 1[field.1.2.840.113635.100.6.2.6] exists and certificate leaf[field.1.2.840.113635.100.6.1.13] exists and certificate leaf[subject.OU] = "${JONES_MACOS_TEAM}"`;
}
