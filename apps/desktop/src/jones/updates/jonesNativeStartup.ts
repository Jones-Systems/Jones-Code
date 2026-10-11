// @effect-diagnostics nodeBuiltinImport:off -- Runs synchronously in boot before profile configuration.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import { holdJonesNativeWriterFence } from "@t3tools/shared/jones/nativeWriterFence";
import { isCompanionPackage } from "@t3tools/shared/jones/previewCompanionProduct";
import { resolveDesktopUserDataOverride } from "../../app/DesktopUserDataOverride.ts";

export function holdJonesDesktopNativeWriterFence(input: {
  readonly platform: string;
  readonly env: Record<string, string | undefined>;
  readonly homeDirectory: string;
  readonly appDataDirectory: string;
  readonly version: string;
  readonly metadata: unknown;
}): void {
  if (input.platform !== "darwin" || isCompanionPackage(input.metadata)) return;
  const explicitHome = input.env.T3CODE_HOME?.trim();
  const home = explicitHome || NodePath.join(input.homeDirectory, ".t3");
  const development = Boolean(input.env.VITE_DEV_SERVER_URL?.trim());
  const legacy = NodePath.join(input.appDataDirectory, "T3 Code (Dev)");
  const profile =
    resolveDesktopUserDataOverride(input.env.T3CODE_DESKTOP_USER_DATA_DIR, NodePath) ??
    (development && NodeFS.existsSync(legacy)
      ? legacy
      : NodePath.join(input.appDataDirectory, development ? "t3code-dev" : "t3code-v2"));
  holdJonesNativeWriterFence({
    home,
    databasePath: NodePath.join(home, development && !explicitHome ? "dev" : "userdata", "statev2.sqlite"),
    profile,
    descriptorPath: input.env.T3CODE_JONES_TRIAL_DESCRIPTOR,
    version: input.version,
    buildMetadata: input.metadata,
  });
}
