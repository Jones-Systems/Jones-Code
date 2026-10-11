// @effect-diagnostics nodeBuiltinImport:off -- Admission must precede creation or migration of native state.
import * as NodePath from "node:path";
import { holdJonesNativeWriterFence } from "@t3tools/shared/jones/nativeWriterFence";
import packageJson from "../../../package.json" with { type: "json" };

export function holdJonesServerNativeWriterFence(databasePath: string): void {
  holdJonesNativeWriterFence({
    home: NodePath.dirname(NodePath.dirname(databasePath)),
    databasePath,
    profile: process.env.T3CODE_DESKTOP_USER_DATA_DIR,
    descriptorPath: process.env.T3CODE_JONES_TRIAL_DESCRIPTOR,
    version: packageJson.version,
    buildMetadata: packageJson,
  });
}
