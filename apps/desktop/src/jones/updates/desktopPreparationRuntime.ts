// @effect-diagnostics nodeBuiltinImport:off - Resolves the bundled worker and settings paths.
import * as NodeCrypto from "node:crypto";
import * as NodePath from "node:path";

import {
  JonesBackgroundPreparation,
  fileBackgroundPreparationStore,
  type BackgroundPreparationController,
} from "./backgroundPreparation.ts";
import type { JonesDesktopUpdateOptions } from "./JonesDesktopUpdates.ts";
import { createJonesStagingWorker, forkJonesStagingChild } from "./stagingWorkerClient.ts";

/** Built beside the main bundle from `jonesStagingWorker.ts`. */
const JONES_STAGING_WORKER_FILE = NodePath.join("jones", "updates", "jonesStagingWorker.cjs");
/** Jones-owned: the upstream desktop settings writer drops keys it does not know. */
const JONES_DESKTOP_UPDATES_SETTINGS_FILE = "jones-desktop-updates.json";

/**
 * Wires the desktop's single preparation process into its updater: every download, startup
 * artifact validation and replaced-payload removal runs there, one job at a time.
 */
export function createJonesDesktopPreparation(input: {
  readonly bundleDirectory: string;
  readonly stateDirectory: string;
  readonly platform: NodeJS.Platform;
}) {
  const worker = createJonesStagingWorker(() =>
    forkJonesStagingChild(NodePath.join(input.bundleDirectory, JONES_STAGING_WORKER_FILE)),
  );
  const controllerOptions = {
    prepareStage: (candidate, roots, onVerifying, { token, onStarted }) =>
      worker.run(
        { kind: "prepare", token, candidate, roots, platform: input.platform },
        onVerifying,
        onStarted === undefined ? {} : { onStarted },
      ),
    cancelPreparation: (token) => worker.cancel(token),
    preparationWorker: () => worker.heldWorker(),
    preparationExited: (token) => worker.reaped(token),
    validateArtifact: (directory) =>
      worker.run({ kind: "validate-artifact", token: NodeCrypto.randomUUID(), directory }),
  } satisfies Pick<
    JonesDesktopUpdateOptions,
    | "prepareStage"
    | "validateArtifact"
    | "cancelPreparation"
    | "preparationWorker"
    | "preparationExited"
  >;
  let background: JonesBackgroundPreparation | undefined;
  return {
    controllerOptions,
    attach: (
      controller: BackgroundPreparationController & { readonly updaterRoot: string },
      suppressed?: (source: string) => Promise<boolean>,
    ) =>
      (background = new JonesBackgroundPreparation({
        controller,
        ...(suppressed === undefined ? {} : { suppressed }),
        store: fileBackgroundPreparationStore(
          NodePath.join(input.stateDirectory, JONES_DESKTOP_UPDATES_SETTINGS_FILE),
        ),
        reclaim: (handle) =>
          worker.run({
            kind: "reclaim",
            token: NodeCrypto.randomUUID(),
            updatesRoot: controller.updaterRoot,
            handle,
          }),
      })),
    /** Stops scheduling and the running job's process group; owned by the updater's scope. */
    close: async () => {
      background?.close();
      await worker.close();
    },
  };
}
