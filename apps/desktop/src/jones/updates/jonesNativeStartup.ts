// @effect-diagnostics nodeBuiltinImport:off -- Runs synchronously in boot before profile configuration.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import {
  holdJonesNativeWriterFence,
  holdJonesDesktopOwner,
  type JonesNativeWriterAdmission,
} from "@t3tools/shared/jones/nativeWriterFence";
import { isCompanionPackage } from "@t3tools/shared/jones/previewCompanionProduct";
import { holdJonesColdStartup } from "./jonesColdBackendCustody.ts";
import { resolveDesktopUserDataOverride } from "../../app/DesktopUserDataOverride.ts";

let resolveDesktopOwner: (() => JonesNativeWriterAdmission) | undefined;

/** Called only by primary backend configuration, after Clerk forwards secondary launches. */
export function currentJonesDesktopOwner(): (() => JonesNativeWriterAdmission) | undefined {
  return resolveDesktopOwner;
}

export function holdJonesDesktopNativeWriterFence(input: {
  readonly platform: string;
  readonly env: Record<string, string | undefined>;
  readonly homeDirectory: string;
  readonly appDataDirectory: string;
  readonly version: string;
  readonly metadata: unknown;
}): void {
  resolveDesktopOwner = undefined;
  holdJonesColdStartup(input.env);
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
  const writerInput = {
    home,
    databasePath: NodePath.join(
      home,
      development && !explicitHome ? "dev" : "userdata",
      "statev2.sqlite",
    ),
    profile,
    descriptorPath: input.env.T3CODE_JONES_TRIAL_DESCRIPTOR,
    version: input.version,
    buildMetadata: input.metadata,
  };
  const admission = holdJonesNativeWriterFence(writerInput);
  if (admission === undefined && writerInput.descriptorPath === undefined) return;
  let owner: JonesNativeWriterAdmission | undefined;
  resolveDesktopOwner = () => {
    if (owner === undefined) {
      const ordinary = holdJonesNativeWriterFence({ ...writerInput, descriptorPath: undefined });
      if (ordinary === undefined)
        throw new Error("Desktop ownership requires ordinary native admission.");
      owner = holdJonesDesktopOwner(ordinary);
    }
    owner.assertCurrent();
    return owner;
  };
}
