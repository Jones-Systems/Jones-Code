import { cliReleaseChannelOf } from "@t3tools/shared/cliRelease";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { parseServiceState } from "../../cloud/serviceProtocol.ts";

/**
 * Whether a T3 home is a Jones-managed runtime. The CLI's `update` and `uninstall` commands share
 * this one reading so they cannot disagree about which homes belong to the Jones host updater.
 *
 * - `unmanaged`: no Jones evidence; ordinary CLI maintenance may proceed.
 * - `managed`: the home holds the admitted-install record, a Jones runtime receipt for the active
 *   or requested version, preview service state, or the caller names a preview version.
 * - `unreadable-service-state`: `service-state.json` exists but cannot be understood. This fails
 *   closed rather than guessing, and takes precedence over `managed`.
 */
export type JonesRuntimeManagement = "unmanaged" | "managed" | "unreadable-service-state";

export const JONES_ACTIVE_INSTALL_RECORD = "jones-active-install.json";
export const JONES_RUNTIME_RECEIPT = ".jones-runtime-receipt.json";

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
