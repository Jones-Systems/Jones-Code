import type { ContextMenuSource, ScopedThreadRef } from "@t3tools/contracts";

export interface HeaderThreadMenuActions {
  readonly onFind?: ((threadRef: ScopedThreadRef) => void) | undefined;
  readonly onStartRename: () => void;
  readonly source?: ContextMenuSource;
}

type ThreadMenuOwner = (
  threadRef: ScopedThreadRef,
  position: { x: number; y: number },
  headerActions: HeaderThreadMenuActions,
) => void;

let owner: ThreadMenuOwner | null = null;

// The mounted sidebar owns assignment, project filters and their current actions.
// The chat header uses that same menu instead of maintaining a second snapshot.
export function registerThreadMenuOwner(next: ThreadMenuOwner) {
  owner = next;
  return () => {
    if (owner === next) owner = null;
  };
}

export function openOwnedThreadMenu(...args: Parameters<ThreadMenuOwner>): boolean {
  if (owner === null) return false;
  owner(...args);
  return true;
}
