import type { SQLiteStorage } from "expo-sqlite/kv-store";
import type { SettlementNotificationStore } from "@t3tools/client-runtime/jones/settlement-notifications";

export const SETTLEMENT_DATABASE_NAME = "jones-settlement-notifications";
const INBOX_KEY = "jones.settlementNotifications.v1.inbox";
const BASELINE_PREFIX = "jones.settlementNotifications.v1.baseline.";

type SettlementKeyValueStorage = Pick<
  SQLiteStorage,
  "getItem" | "setItem" | "removeItem" | "getAllKeys"
>;

export function createSettlementKvStore(
  storage: SettlementKeyValueStorage,
): SettlementNotificationStore {
  return {
    loadInbox: () => storage.getItem(INBOX_KEY),
    saveInbox: (json) => storage.setItem(INBOX_KEY, json),
    loadBaseline: (environmentId) => storage.getItem(BASELINE_PREFIX + environmentId),
    saveBaseline: (environmentId, json) => storage.setItem(BASELINE_PREFIX + environmentId, json),
    removeBaseline: (environmentId) => storage.removeItem(BASELINE_PREFIX + environmentId),
    listBaselineEnvironmentIds: async () =>
      (await storage.getAllKeys())
        .filter((key) => key.startsWith(BASELINE_PREFIX) && key.length > BASELINE_PREFIX.length)
        .map((key) => key.slice(BASELINE_PREFIX.length)),
  };
}
