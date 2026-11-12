import { COMPANION_PRODUCT } from "@t3tools/shared/jones/previewCompanionProduct";

export type DesktopPackageVariant = "preview-companion";

export function companionPackageMetadata(variant?: DesktopPackageVariant) {
  return variant === "preview-companion"
    ? {
        name: COMPANION_PRODUCT.packageName,
        productName: COMPANION_PRODUCT.name,
        jonesDesktopProduct: COMPANION_PRODUCT.marker,
      }
    : {};
}

function fields(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

export function applyCompanionPackageVariant(
  config: Record<string, unknown>,
  version: string,
  variant?: DesktopPackageVariant,
): Record<string, unknown> {
  if (variant !== "preview-companion") return config;
  const result: Record<string, unknown> = {
    ...config,
    appId: COMPANION_PRODUCT.appId,
    productName: COMPANION_PRODUCT.name,
    artifactName: `${COMPANION_PRODUCT.artifactPrefix}-\${version}-\${arch}.\${ext}`,
    // Explicit null also prevents electron-builder from inferring a GitHub feed from CI.
    publish: null,
    protocols: [],
  };
  for (const platform of ["mac", "linux", "win"] as const) {
    if (config[platform] === undefined) continue;
    const platformConfig: Record<string, unknown> = {
      ...fields(config[platform]),
      protocols: [],
      publish: null,
    };
    if (platform === "mac") {
      delete platformConfig["entitlements"];
      delete platformConfig["provisioningProfile"];
    }
    result[platform] = platformConfig;
  }
  if (config.linux !== undefined) {
    result.linux = {
      ...fields(result.linux),
      executableName: COMPANION_PRODUCT.packageName,
      desktop: { entry: { StartupWMClass: COMPANION_PRODUCT.packageName } },
    };
    // Ordinary package metadata installs files into t3code's paths; companion owns no such files.
    result.deb = { ...fields(config.deb), fpm: [] };
  }
  if (config.dmg !== undefined) {
    result.dmg = { ...fields(config.dmg), title: `${COMPANION_PRODUCT.name} ${version} Installer` };
  }
  return result;
}
