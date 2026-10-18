import type { OwnedChildReceipt, OwnedRoot } from "./guard.mjs";

export function runOwnedChild(options: {
  owner: OwnedRoot;
  executable: string;
  args: readonly string[];
  env: Readonly<Record<string, string>>;
  timeoutMs: number;
  terminateGraceMs: number;
  reapTimeoutMs: number;
  maxOutputBytes: number;
  signal?: AbortSignal;
}): Promise<OwnedChildReceipt>;
