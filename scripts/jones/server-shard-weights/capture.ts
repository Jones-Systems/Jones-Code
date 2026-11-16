// @effect-diagnostics nodeBuiltinImport:off - The CI reporter appends runner-local timing records synchronously, outside any Effect runtime.
import * as NodeFS from "node:fs";

import type { WeightsInput } from "./envelope.ts";

/** One shard's timing capture: a header, module events, then a terminal record, one per line. */
export const CAPTURE_SCHEMA = "jones.server-shard-timings/v1";

/** Files whose bytes decide how the server suite is discovered, sharded and measured. */
export const CAPTURE_IDENTITY_FILES = [
  "package.json",
  "pnpm-lock.yaml",
  "pnpm-workspace.yaml",
  "vite.config.ts",
  "apps/server/package.json",
  "apps/server/vite.config.ts",
  "apps/server/src/testUtils/weightedShardSequencer.ts",
  "apps/server/src/testUtils/shardWeights.json",
  "scripts/jones/ci-test-progress-reporter.ts",
  "scripts/jones/server-shard-weights/capture.ts",
  "scripts/jones/server-shard-weights/envelope.ts",
] as const;

/**
 * Anything that narrows which files or tests run. The normal CI command sets
 * none of them; a capture that records any is not a full-suite measurement.
 */
export interface CaptureFilters {
  filenamePatterns: string[];
  testNamePattern: string | null;
  projects: string[];
  tagsFilter: string[];
  changed: string | null;
  related: string[];
  /** Discovered files that carry their own line, test ID, name or tag filter. */
  targetedFiles: number;
}

export interface CaptureHeader {
  type: "header";
  schema: typeof CAPTURE_SCHEMA;
  checkoutSha: string;
  githubSha: string;
  repository: string;
  workflowRef: string;
  event: string;
  ref: string;
  runId: string;
  runAttempt: number;
  shardIndex: number;
  shardCount: number;
  /** Informational: the hosted image can differ between the machines of one run. */
  runner: { image: string; imageVersion: string; release: string };
  tools: { node: string; vitest: string; vitePlus: string };
  identities: Record<string, string>;
  fileParallelism: false;
  filters: CaptureFilters;
  weightsInput: WeightsInput;
  /** Every file this run discovered, before sharding. Every shard records the same list. */
  manifest: string[];
}

export type ModuleEvent = "queued" | "collected" | "start" | "end";

export interface ModuleDiagnostic {
  duration: number;
  setupDuration: number;
  collectDuration: number;
  prepareDuration: number;
  environmentSetupDuration: number;
}

export interface CaptureModule {
  type: "module";
  event: ModuleEvent;
  path: string;
  /** Milliseconds since run start, as observed by the reporter in the main process. */
  elapsedMs: number;
  state: string;
  errorCount: number;
  errors: string[];
  /** Recorded on `end` only. */
  diagnostic?: ModuleDiagnostic;
}

export interface CaptureTerminal {
  type: "terminal";
  reason: "passed" | "failed" | "interrupted";
  unhandledErrorCount: number;
  completedFiles: string[];
  elapsedMs: number;
}

export type CaptureRecord = CaptureHeader | CaptureModule | CaptureTerminal;

/**
 * Appends records as they happen, so an interrupted shard still leaves every
 * record it completed. Exclusive creation never overwrites an earlier capture.
 */
export class CaptureWriter {
  private readonly path: string;

  constructor(path: string, header: CaptureHeader) {
    this.path = path;
    NodeFS.writeFileSync(path, `${JSON.stringify(header)}\n`, { flag: "wx" });
  }

  append(record: CaptureModule | CaptureTerminal): void {
    NodeFS.appendFileSync(this.path, `${JSON.stringify(record)}\n`);
  }
}
