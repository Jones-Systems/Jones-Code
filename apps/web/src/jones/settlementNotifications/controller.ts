import { createSettlementNotificationController } from "@t3tools/client-runtime/jones/settlement-notifications";
import { createLocalStorageSettlementStore } from "./localStorageStore";

let controller: ReturnType<typeof createSettlementNotificationController> | undefined;

export function getSettlementNotificationController() {
  controller ??= createSettlementNotificationController({
    store: createLocalStorageSettlementStore(),
    log: (event) => console.warn(`[settlement-notifications] ${event}`),
  });
  return controller;
}
