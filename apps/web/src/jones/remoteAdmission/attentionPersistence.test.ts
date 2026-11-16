import { expect, it, vi } from "vite-plus/test";
import { attentionDismissals, receiverAttention } from "./attentionPersistence";

it("persists dismissal for only the immutable client receiver/binding/attempt key", () => {
  const values = new Map<string, string>();
  vi.stubGlobal("localStorage", { getItem: (key: string) => values.get(key) ?? null, setItem: (key: string, value: string) => values.set(key, value) });
  try {
    const first = attentionDismissals("incoming");
    first.add(JSON.stringify(["receiver-a", "request", "digest", "original"]));
    const remounted = attentionDismissals("incoming");
    expect(remounted.has(JSON.stringify(["receiver-a", "request", "digest", "original"]))).toBe(true);
    expect(remounted.has(JSON.stringify(["receiver-b", "request", "digest", "original"]))).toBe(false);
    expect(remounted.has(JSON.stringify(["receiver-a", "request", "digest", "retry-1"]))).toBe(false);
    expect(attentionDismissals("results").has(JSON.stringify(["receiver-a", "request", "digest", "original"]))).toBe(false);
  } finally { vi.unstubAllGlobals(); }
});
it("restores the saved cursor and unresolved access fence per receiver", () => {
  const values = new Map<string, string>();
  vi.stubGlobal("localStorage", { getItem: (key: string) => values.get(key) ?? null, setItem: (key: string, value: string) => values.set(key, value) });
  try {
    const snapshot = { cursor: "saved-head", results: [], uncertainAccess: [{ id: "revoked-predecessor", action: "restore", generation: 3, restoreCommandId: "original-restore-command" }] };
    receiverAttention("feed-receiver-a").write(snapshot);
    expect(receiverAttention("feed-receiver-a").read()).toEqual(snapshot);
    expect(receiverAttention("feed-receiver-b").read()).toBeNull();
  } finally { vi.unstubAllGlobals(); }
});
