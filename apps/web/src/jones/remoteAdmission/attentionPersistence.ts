import type {
  AttentionPersistence,
  Item,
} from "@t3tools/client-runtime/jones/remoteAdmission/controller";

const prefix = "jones.remote-admission.attention.v1:";
const listeners = new Set<() => void>();
let revision = 0;
const memory = new Map<string, string>();
function read(key: string) {
  try {
    return localStorage.getItem(prefix + key) ?? memory.get(key) ?? null;
  } catch {
    return memory.get(key) ?? null;
  }
}
function write(key: string, value: string) {
  memory.set(key, value);
  try {
    localStorage.setItem(prefix + key, value);
  } catch {
    /* Keep attention usable when browser storage is unavailable. */
  }
  revision++;
  for (const listener of listeners) listener();
}
export const attentionRevision = () => revision;
export const subscribeAttention = (listener: () => void) => {
  listeners.add(listener);
  const changed = (event: StorageEvent) => {
    if (event.key?.startsWith(prefix)) {
      revision++;
      listener();
    }
  };
  window.addEventListener("storage", changed);
  return () => {
    listeners.delete(listener);
    window.removeEventListener("storage", changed);
  };
};
export function attentionDismissals(kind: "incoming" | "results") {
  return {
    has(key: string) {
      return read(JSON.stringify([kind, key])) === "dismissed";
    },
    add(key: string) {
      write(JSON.stringify([kind, key]), "dismissed");
    },
  };
}
export const dismissedIncoming = attentionDismissals("incoming");
export const dismissedResults = attentionDismissals("results");
export function receiverAttention(receiver: string): AttentionPersistence {
  const key = JSON.stringify(["feed", receiver]);
  return {
    read() {
      try {
        const value: unknown = JSON.parse(read(key) ?? "null");
        if (
          !value ||
          typeof value !== "object" ||
          !("cursor" in value) ||
          (value.cursor !== null && typeof value.cursor !== "string") ||
          !("results" in value) ||
          !Array.isArray(value.results)
        )
          return null;
        const results = value.results.filter(
          (item): item is Item =>
            item &&
            typeof item === "object" &&
            typeof item.attempt === "string" &&
            item.record &&
            typeof item.record.id === "string" &&
            typeof item.record.bindingDigest === "string",
        );
        const uncertainAccess =
          "uncertainAccess" in value && Array.isArray(value.uncertainAccess)
            ? value.uncertainAccess.filter(
                (
                  entry,
                ): entry is {
                  id: string;
                  action: string;
                  generation: number;
                  restoreCommandId?: string;
                } =>
                  entry &&
                  typeof entry.id === "string" &&
                  ["update", "revoke", "restore"].includes(entry.action) &&
                  Number.isInteger(entry.generation) &&
                  (entry.restoreCommandId === undefined ||
                    typeof entry.restoreCommandId === "string"),
              )
            : [];
        return { cursor: value.cursor, results, uncertainAccess };
      } catch {
        return null;
      }
    },
    write(value) {
      write(key, JSON.stringify(value));
    },
  };
}
