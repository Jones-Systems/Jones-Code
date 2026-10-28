import type { SqliteReaderObservation, SqliteReaderPoolOptions } from "@t3tools/shared/jones/sqliteReaderContracts";
import { observeSafely } from "./observations.ts";

/** Optional at the persistence boundary: standalone CLI/auth callers need no server config. */
export const readerPoolOptions = (
  size: 2 | 3,
  observe?: (event: SqliteReaderObservation) => void,
): SqliteReaderPoolOptions => ({
  size,
  interactiveQueueCapacity: 64,
  backgroundQueueCapacity: 64,
  interactiveQueueTimeoutMs: 10_000,
  backgroundQueueTimeoutMs: 30_000,
  leaseWatchdogMs: 60_000,
  observe: observeSafely(observe),
});
