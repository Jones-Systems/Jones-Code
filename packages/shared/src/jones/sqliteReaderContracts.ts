import type { SqliteReadPriority } from "./sqliteReadPriority.ts";

export interface SqliteReaderPoolOptions {
  readonly size: 2 | 3;
  readonly interactiveQueueCapacity: number;
  readonly backgroundQueueCapacity: number;
  readonly interactiveQueueTimeoutMs: number;
  readonly backgroundQueueTimeoutMs: number;
  readonly leaseWatchdogMs: number;
  readonly observe?: (event: SqliteReaderObservation) => void;
}

export interface SqliteReaderObservation {
  readonly phase: "queue" | "startup" | "checkpoint" | "execute" | "reply" | "lease";
  readonly priority: SqliteReadPriority;
  readonly slotId: number;
  readonly generation: number;
  readonly durationMs: number;
  readonly outcome: "ok" | "cancelled" | "timeout" | "unavailable" | "failed";
  readonly queueDepth: number;
  readonly estimated?: boolean;
  readonly uncertaintyMs?: number;
}
