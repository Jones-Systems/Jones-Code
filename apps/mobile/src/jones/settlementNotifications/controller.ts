import { createSettlementNotificationController } from "@t3tools/client-runtime/jones/settlement-notifications";
import { SQLiteStorage } from "expo-sqlite/kv-store";
import { AppState } from "react-native";

import { createSettlementKvStore, SETTLEMENT_DATABASE_NAME } from "./kvStore";

const log = (event: string, detail?: unknown) => {
  console.warn(`[settlement-notifications] ${event}`, detail);
};

export const settlementNotificationController = createSettlementNotificationController({
  store: createSettlementKvStore(new SQLiteStorage(SETTLEMENT_DATABASE_NAME)),
  log,
});

void settlementNotificationController.hydrate().catch((error: unknown) => {
  log("hydrate-failed", error);
});

AppState.addEventListener("change", (state) => {
  if (state !== "active") {
    void settlementNotificationController.flush().catch((error: unknown) => {
      log("background-flush-failed", error);
    });
  }
});
