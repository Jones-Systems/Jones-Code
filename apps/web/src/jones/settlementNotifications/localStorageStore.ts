import { SettlementProjectRemaining } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import type {
  SettlementEntry,
  SettlementInboxState,
  SettlementNotificationStore,
} from "@t3tools/client-runtime/jones/settlement-notifications";

export const SETTLEMENT_STORAGE_PREFIX = "jones.settlementNotifications.v1.";
const ENTRY_PREFIX = `${SETTLEMENT_STORAGE_PREFIX}entry.`;
const PROJECT_REMAINING_PREFIX = `${SETTLEMENT_STORAGE_PREFIX}projectRemaining.`;
const ACK_PREFIX = `${SETTLEMENT_STORAGE_PREFIX}ack.`;
const BASELINE_PREFIX = `${SETTLEMENT_STORAGE_PREFIX}baseline.`;

function parse(json: string | null): unknown {
  try {
    return json === null ? null : JSON.parse(json);
  } catch {
    return null;
  }
}

const isProjectRemaining = Schema.is(SettlementProjectRemaining);

function isEntry(value: unknown): value is SettlementEntry {
  if (typeof value !== "object" || value === null) return false;
  const row = value as Record<string, unknown>;
  return (
    typeof row.id === "string" &&
    typeof row.environmentId === "string" &&
    typeof row.threadId === "string" &&
    typeof row.title === "string" &&
    typeof row.settledAtMs === "number" &&
    Number.isFinite(row.settledAtMs) &&
    typeof row.detectedAtMs === "number" &&
    Number.isFinite(row.detectedAtMs) &&
    typeof row.threadGone === "boolean" &&
    (row.projectRemaining === undefined || isProjectRemaining(row.projectRemaining)) &&
    row.id === `${row.environmentId}:${row.threadId}:${row.settledAtMs}`
  );
}

function decodeInbox(value: unknown): SettlementInboxState | null {
  if (typeof value !== "object" || value === null) return null;
  const row = value as Record<string, unknown>;
  if (row.version !== 1 || !Array.isArray(row.entries) || !Array.isArray(row.acknowledged))
    return null;
  if (!row.entries.every(isEntry) || !row.acknowledged.every((id) => typeof id === "string"))
    return null;
  return { version: 1, entries: row.entries, acknowledged: row.acknowledged };
}

export function createLocalStorageSettlementStore(
  getStorage: () => Storage = () => window.localStorage,
  events: Pick<Window, "addEventListener" | "removeEventListener"> = window,
): SettlementNotificationStore {
  const keys = (storage: Storage) => {
    const result: string[] = [];
    for (let index = 0; index < storage.length; index += 1) {
      const key = storage.key(index);
      if (key !== null) result.push(key);
    }
    return result;
  };
  const loadInbox = () => {
    const storage = getStorage();
    const legacy = decodeInbox(parse(storage.getItem(`${SETTLEMENT_STORAGE_PREFIX}inbox`)));
    const entries = new Map(legacy?.entries.map((entry) => [entry.id, entry]));
    const acknowledged = new Set(legacy?.acknowledged);
    for (const key of keys(storage)) {
      if (key.startsWith(ENTRY_PREFIX)) {
        const record = parse(storage.getItem(key));
        if (typeof record !== "object" || record === null) continue;
        const row = record as Record<string, unknown>;
        if (
          row.version === 1 &&
          isEntry(row.entry) &&
          key === ENTRY_PREFIX + encodeURIComponent(row.entry.id)
        ) {
          entries.set(row.entry.id, row.entry);
        }
      } else if (key.startsWith(ACK_PREFIX)) {
        const record = parse(storage.getItem(key));
        if (typeof record !== "object" || record === null) continue;
        const row = record as Record<string, unknown>;
        if (
          row.version === 1 &&
          typeof row.id === "string" &&
          key === ACK_PREFIX + encodeURIComponent(row.id)
        )
          acknowledged.add(row.id);
      }
    }
    for (const [id, entry] of entries) {
      const record = parse(storage.getItem(PROJECT_REMAINING_PREFIX + encodeURIComponent(id)));
      if (typeof record !== "object" || record === null) continue;
      const row = record as Record<string, unknown>;
      if (row.version === 1 && row.id === id && isProjectRemaining(row.projectRemaining)) {
        entries.set(id, { ...entry, projectRemaining: row.projectRemaining });
      }
    }
    return JSON.stringify({
      version: 1,
      entries: [...entries.values()],
      acknowledged: [...acknowledged],
    });
  };
  return {
    loadInbox,
    saveInbox(json) {
      const inbox = decodeInbox(parse(json));
      if (!inbox) throw new Error("Invalid settlement inbox");
      const storage = getStorage();
      // Separate tombstones survive stale snapshots, including saves from a closing tab.
      for (const id of inbox.acknowledged) {
        const key = ACK_PREFIX + encodeURIComponent(id);
        const value = JSON.stringify({ version: 1, id });
        if (storage.getItem(key) !== value) storage.setItem(key, value);
      }
      for (const entry of inbox.entries) {
        // Older tabs replace whole v1 entries without these bullets; keep an additive copy.
        if (entry.projectRemaining?.length) {
          const key = PROJECT_REMAINING_PREFIX + encodeURIComponent(entry.id);
          const value = JSON.stringify({
            version: 1,
            id: entry.id,
            projectRemaining: entry.projectRemaining,
          });
          if (storage.getItem(key) !== value) storage.setItem(key, value);
        }
        const key = ENTRY_PREFIX + encodeURIComponent(entry.id);
        const value = JSON.stringify({ version: 1, entry });
        if (storage.getItem(key) !== value) storage.setItem(key, value);
      }
    },
    loadBaseline(environmentId) {
      return getStorage().getItem(BASELINE_PREFIX + encodeURIComponent(environmentId));
    },
    saveBaseline(environmentId, json) {
      const storage = getStorage();
      const key = BASELINE_PREFIX + encodeURIComponent(environmentId);
      const previous = parse(storage.getItem(key)) as {
        version?: unknown;
        snapshotSequence?: unknown;
        confirmedAtMs?: unknown;
      } | null;
      const next = parse(json) as { snapshotSequence?: unknown; confirmedAtMs?: unknown } | null;
      if (
        previous?.version === 1 &&
        typeof previous.snapshotSequence === "number" &&
        typeof next?.snapshotSequence === "number" &&
        previous.snapshotSequence > next.snapshotSequence &&
        typeof previous.confirmedAtMs === "number" &&
        typeof next.confirmedAtMs === "number" &&
        next.confirmedAtMs - previous.confirmedAtMs < 60_000
      )
        return;
      storage.setItem(key, json);
    },
    removeBaseline(environmentId) {
      getStorage().removeItem(BASELINE_PREFIX + encodeURIComponent(environmentId));
    },
    listBaselineEnvironmentIds() {
      return keys(getStorage())
        .filter((key) => key.startsWith(BASELINE_PREFIX))
        .flatMap((key) => {
          try {
            return [decodeURIComponent(key.slice(BASELINE_PREFIX.length))];
          } catch {
            return [];
          }
        });
    },
    subscribeExternalInbox(listener) {
      const onStorage = (event: StorageEvent) => {
        if (
          event.key !== null &&
          !event.key.startsWith(ENTRY_PREFIX) &&
          !event.key.startsWith(ACK_PREFIX) &&
          !event.key.startsWith(PROJECT_REMAINING_PREFIX) &&
          event.key !== `${SETTLEMENT_STORAGE_PREFIX}inbox`
        )
          return;
        try {
          if (event.storageArea !== null && event.storageArea !== getStorage()) return;
          listener(loadInbox());
        } catch {
          // A blocked storage area must not interrupt the in-memory notification.
        }
      };
      events.addEventListener("storage", onStorage as EventListener);
      return () => events.removeEventListener("storage", onStorage as EventListener);
    },
  };
}
