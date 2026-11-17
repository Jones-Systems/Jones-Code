import { cliReleaseChannelOf } from "@t3tools/shared/cliRelease";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { parseServiceState } from "../../cloud/serviceProtocol.ts";

/**
 * Whether a T3 home belongs to the Jones host updater; `update` and `uninstall` share this reading.
 * An unparseable `service-state.json` fails closed and outranks `managed`.
 */
export type JonesRuntimeManagement = "unmanaged" | "managed" | "unreadable-service-state";

const JONES_ACTIVE_INSTALL_RECORD = "jones-active-install.json";
const JONES_RUNTIME_RECEIPT = ".jones-runtime-receipt.json";

/** Read-only: inspects `<baseDir>/runtime` and never creates, repairs or removes anything. */
export const classifyJonesManagedRuntime = Effect.fn("jones.cli.classify_managed_runtime")(
  function* (baseDir: string, version: string) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const runtimeDir = path.join(baseDir, "runtime");
    const receiptPath = (receiptVersion: string) =>
      path.join(runtimeDir, "versions", receiptVersion, JONES_RUNTIME_RECEIPT);
    const admitted = yield* fs.exists(path.join(runtimeDir, JONES_ACTIVE_INSTALL_RECORD));
    const receipted = yield* fs.exists(receiptPath(version));
    const statePath = path.join(runtimeDir, "service-state.json");
    let serviceQualified = false;
    if (yield* fs.exists(statePath)) {
      const state = parseServiceState(yield* fs.readFileString(statePath));
      if (state === undefined) return "unreadable-service-state" as const;
      serviceQualified =
        cliReleaseChannelOf(state.activeVersion) === "preview" ||
        (yield* fs.exists(receiptPath(state.activeVersion)));
    }
    return admitted || receipted || serviceQualified || cliReleaseChannelOf(version) === "preview"
      ? ("managed" as const)
      : ("unmanaged" as const);
  },
);

/** Why `t3 uninstall` refuses a home that is not `unmanaged`. Nothing has been removed by then. */
export function jonesUninstallRefusal(
  path: Path.Path,
  baseDir: string,
  management: Exclude<JonesRuntimeManagement, "unmanaged">,
): string {
  const runtimeDir = path.join(baseDir, "runtime");
  const consequence = `Uninstalling would remove Jones install and recovery records under ${runtimeDir}, so nothing was removed.`;
  return management === "managed"
    ? `This is a Jones Code runtime managed by its host updater. ${consequence}`
    : `The service state under ${runtimeDir} could not be read, so t3 cannot rule out a Jones Code runtime managed by its host updater. ${consequence}`;
}
