// @effect-diagnostics nodeBuiltinImport:off -- Owns the bounded native reservation-durability observation.
import * as NodeTimersPromises from "node:timers/promises";
import { reconcileUpdateOperation } from "../updates/launcherOperation.ts";

/** Holds acceptance after real reservation reconciliation, past the server's timeout. */
export async function observeSlowReservationDurability(baseDir: string, operationId: string) {
  const window = (await reconcileUpdateOperation(baseDir, operationId, undefined)).state;
  await NodeTimersPromises.setTimeout(200);
  return window;
}
