import { useAtomValue } from "@effect/atom-react";
import { createSteadyShellAtoms } from "@t3tools/client-runtime/state/shell";
import { createEnvironmentThreadShellAtoms } from "@t3tools/client-runtime/state/threads";

import { environmentCatalog } from "../../connection/catalog";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { serverEnvironment } from "../../state/server";
import { environmentShell } from "../../state/shell";
import { threadEnvironment } from "../../state/threads";

const steadyShells = createSteadyShellAtoms({
  catalogValueAtom: environmentCatalog.catalogValueAtom,
  snapshotAtom: threadEnvironment.snapshotAtom,
  shellStateAtom: environmentShell.stateValueAtom,
  rowActionsAtom: threadEnvironment.rowActionsAtom,
  connectionStateAtom: environmentCatalog.stateAtom,
});
const sidebarShells = createEnvironmentThreadShellAtoms({
  catalogValueAtom: environmentCatalog.catalogValueAtom,
  snapshotAtom: steadyShells.snapshotAtom,
  hiddenThreadTagsAtom: serverEnvironment.hiddenThreadTagsAtom,
});

export function useSteadySidebarThreads() {
  return useAtomValue(sidebarShells.quietThreadShellsAtom);
}

export function sidebarRowsAreHeld(): boolean {
  return appAtomRegistry.get(steadyShells.holdingAtom);
}
