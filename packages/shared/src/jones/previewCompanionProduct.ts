export const COMPANION_PRODUCT = {
  marker: "preview-companion",
  name: "Jones Preview Companion",
  packageName: "jones-preview-companion",
  appId: "com.jonessystems.jonespreviewcompanion",
  artifactPrefix: "Jones-Preview-Companion",
  environmentKey: "JONES_PREVIEW_COMPANION_PRODUCT",
} as const;

export function isCompanionPackage(metadata: unknown): boolean {
  if (metadata === null || typeof metadata !== "object" || Array.isArray(metadata)) {
    throw new Error("Invalid desktop product metadata.");
  }
  const record = metadata as Record<string, unknown>;
  const marker = record.jonesDesktopProduct;
  if (marker === undefined && record.name !== COMPANION_PRODUCT.packageName) return false;
  if (marker !== COMPANION_PRODUCT.marker || record.name !== COMPANION_PRODUCT.packageName) {
    throw new Error("Invalid preview companion product marker.");
  }
  return true;
}
