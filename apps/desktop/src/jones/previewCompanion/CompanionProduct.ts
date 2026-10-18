import type { EarlyLinuxElectronOptions } from "../../app/DesktopEarlyElectronStartup.ts";

import {
  COMPANION_PRODUCT,
  isCompanionPackage,
} from "@t3tools/shared/jones/previewCompanionProduct";
export {
  COMPANION_PRODUCT,
  isCompanionPackage,
} from "@t3tools/shared/jones/previewCompanionProduct";

/** CLI file launches may have no app-root manifest; packaged metadata must always be readable. */
export function readDesktopProductMetadata(input: {
  readonly isPackaged: boolean;
  readonly readPackage: () => string;
}): unknown {
  let raw: string;
  try {
    raw = input.readPackage();
  } catch (error) {
    if (
      !input.isPackaged &&
      error !== null &&
      typeof error === "object" &&
      "code" in error &&
      (error.code === "ENOENT" || error.code === "ENOTDIR")
    )
      return {};
    throw error;
  }
  return JSON.parse(raw);
}

/** Bind product storage before loading main, so shell-less launches never inherit the ordinary profile. */
export function configureCompanionProduct(input: {
  readonly metadata: unknown;
  readonly appDataDirectory: string;
  readonly homeDirectory: string;
  readonly join: (...parts: string[]) => string;
  readonly env: Record<string, string | undefined>;
  readonly createDirectory: (directory: string) => void;
  readonly setPath: (name: "userData" | "sessionData", directory: string) => void;
}): void {
  const companion = isCompanionPackage(input.metadata);
  input.env[COMPANION_PRODUCT.environmentKey] = companion ? "true" : "false";
  if (!companion) return;
  const userData = input.join(input.appDataDirectory, COMPANION_PRODUCT.name);
  const sessionData = input.join(userData, "Session");
  const stateHome = input.join(input.homeDirectory, ".jones-preview-companion");
  for (const directory of [userData, sessionData, stateHome]) input.createDirectory(directory);
  input.env.T3CODE_DESKTOP_USER_DATA_DIR = userData;
  input.env.T3CODE_HOME = stateHome;
  input.env.T3CODE_DESKTOP_APP_USER_MODEL_ID = COMPANION_PRODUCT.appId;
  input.env.T3CODE_DISABLE_AUTO_UPDATE = "true";
  input.env.T3CODE_DESKTOP_MOCK_UPDATES = "false";
  input.setPath("userData", userData);
  input.setPath("sessionData", sessionData);
}

export function companionLinuxIdentity(
  options: EarlyLinuxElectronOptions | null,
  companion: boolean,
): EarlyLinuxElectronOptions | null {
  return options === null || !companion
    ? options
    : {
        ...options,
        linuxDesktopEntryName: `${COMPANION_PRODUCT.packageName}.desktop`,
        linuxWmClass: COMPANION_PRODUCT.packageName,
      };
}
