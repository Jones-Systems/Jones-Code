import { nativeWorkstreamThreadKey } from "@t3tools/client-runtime/state/workstreams";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";

export type MobileThreadOrderScope =
  | { readonly kind: "shelf"; readonly section: "pinned" | "active" }
  | { readonly kind: "workstream"; readonly groupKey: string | null };

export interface MobileThreadOrderSnapshot {
  readonly enabled: boolean;
  readonly revision: string;
  readonly primaryGroupByThreadKey: ReadonlyMap<string, string>;
}

export interface MobileThreadOrderSource {
  read(): MobileThreadOrderSnapshot | null;
  subscribe(listener: () => void): () => void;
}

export interface MobileThreadMoveContext {
  readonly scope: MobileThreadOrderScope;
  readonly source: MobileThreadOrderSource;
  readonly removePrimary: (thread: EnvironmentThreadShell) => Promise<void>;
}

export function mobilePrimaryGroupMap(
  groups: readonly {
    readonly key: string;
    readonly threadKeys: ReadonlySet<string>;
  }[],
): ReadonlyMap<string, string> {
  const primary = new Map<string, string>();
  const conflicts = new Set<string>();
  for (const group of groups) {
    for (const key of group.threadKeys) {
      if (conflicts.has(key)) continue;
      const prior = primary.get(key);
      if (prior !== undefined && prior !== group.key) {
        primary.delete(key);
        conflicts.add(key);
      } else primary.set(key, group.key);
    }
  }
  return primary;
}

export function mobileThreadOrderScope(
  thread: Pick<EnvironmentThreadShell, "environmentId" | "id" | "pinnedAt">,
  snapshot: MobileThreadOrderSnapshot,
): MobileThreadOrderScope {
  if (snapshot.enabled) {
    const groupKey = snapshot.primaryGroupByThreadKey.get(
      nativeWorkstreamThreadKey(thread.environmentId, thread.id),
    );
    if (groupKey !== undefined || thread.pinnedAt == null)
      return { kind: "workstream", groupKey: groupKey ?? null };
  }
  return { kind: "shelf", section: thread.pinnedAt != null ? "pinned" : "active" };
}

export function mobileThreadOrderScopes(
  snapshot: MobileThreadOrderSnapshot,
): MobileThreadOrderScope[] {
  return snapshot.enabled
    ? [
        { kind: "shelf", section: "pinned" },
        ...[...new Set(snapshot.primaryGroupByThreadKey.values())].map((groupKey) => ({
          kind: "workstream" as const,
          groupKey,
        })),
        { kind: "workstream", groupKey: null },
      ]
    : [
        { kind: "shelf", section: "pinned" },
        { kind: "shelf", section: "active" },
      ];
}

export function sameMobileThreadOrderScope(
  left: MobileThreadOrderScope,
  right: MobileThreadOrderScope,
): boolean {
  return left.kind === "shelf"
    ? right.kind === "shelf" && left.section === right.section
    : right.kind === "workstream" && left.groupKey === right.groupKey;
}

export function mobileThreadOrderSection(scope: MobileThreadOrderScope): "pinned" | "active" {
  return scope.kind === "shelf" ? scope.section : "active";
}
