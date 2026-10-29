import type { LogicalFinderHandle, FinderToken } from "./computeContract.ts";
import type { Bootstrap, Request } from "../serverCompute/protocol.ts";
import type { ComputeChild, SlotResult } from "../serverCompute/processSlot.ts";
import type { ThreadBudgetGeneration } from "./nativeThreadBudget.ts";
import type { NativePoolObservation } from "./resourceSamples.ts";

export const generationIdentity = (pid: number, boot: Bootstrap) =>
  JSON.stringify([
    pid,
    boot.version,
    boot.build,
    boot.authorityId,
    boot.slotId,
    boot.slotGeneration,
    boot.nonce,
  ]);
const keyIdentity = (handle: LogicalFinderHandle) =>
  JSON.stringify([handle.key.cwd, handle.key.variant]);

// Normal shutdown drains actual reads. The existing workload cancellation must
// still reach captured-child containment if an external OS read stops answering.
// A late result from an aborted read is consumed, never promoted to evidence.
function observeUntilAbort<T>(work: () => Promise<T>, signal?: AbortSignal): Promise<T> {
  if (signal?.aborted) return Promise.reject(new Error("Observation aborted"));
  return new Promise((resolve, reject) => {
    const dispose = () => signal?.removeEventListener("abort", abort);
    const abort = () => {
      dispose();
      reject(new Error("Observation aborted"));
    };
    signal?.addEventListener("abort", abort, { once: true });
    void Promise.resolve()
      .then(() => {
        if (signal?.aborted) throw new Error("Observation aborted");
        return work();
      })
      .then(
        (value) => {
          dispose();
          resolve(value);
        },
        (cause) => {
          dispose();
          reject(cause);
        },
      );
  });
}

interface CompletedGrep {
  readonly pid: number;
  readonly bootstrap: Bootstrap;
  readonly requestId: number;
  readonly token: FinderToken;
  readonly completedAt: number;
  readonly query: "needle";
  readonly match: { readonly path: string; readonly lineContent: string };
}
export type EvidenceGeneration = {
  -readonly [K in keyof ThreadBudgetGeneration]: ThreadBudgetGeneration[K];
} & {
  nativePools: NativePoolObservation[];
  faulted: boolean;
  retirementStartedAt?: number;
  grep?: CompletedGrep;
  basisGrepAttempted: boolean;
  basisGrepFailure?: "request-failed" | "empty-or-unbound";
  poolReads: {
    at: number;
    finishedAt: number;
    phase: string;
    pid: number;
    bootstrap: Bootstrap;
    grep: CompletedGrep;
    observation: NativePoolObservation;
  }[];
};

interface EvidencePorts {
  readonly captured: ReadonlyMap<number, ComputeChild>;
  readonly now: () => number;
  readonly pools: (pid: number) => Promise<NativePoolObservation>;
  readonly resources: () => Promise<void>;
  readonly method: NativePoolObservation["method"];
  readonly signal?: AbortSignal;
}

/** Harness checkpoints own no leases and never certify exit or refund charges. */
export class NativeGenerationEvidence {
  private readonly owners = new Map<string, { record: EvidenceGeneration; token: FinderToken }>();
  private readonly completedGreps = new Map<EvidenceGeneration, CompletedGrep>();
  private readonly attempted = new Map<EvidenceGeneration, Map<string, Promise<void>>>();
  private readonly pending = new Set<Promise<void>>();
  private accepting = true;
  private readonly ports: EvidencePorts;
  constructor(ports: EvidencePorts) {
    this.ports = ports;
  }

  private live(record: EvidenceGeneration) {
    const child = record.pid === undefined ? undefined : this.ports.captured.get(record.pid);
    return (
      !record.faulted &&
      record.retirementStartedAt === undefined &&
      record.exitCapturedAt === undefined &&
      child !== undefined &&
      generationIdentity(child.pid, child.bootstrap) ===
        generationIdentity(record.pid!, record.bootstrap)
    );
  }

  completed(record: EvidenceGeneration, request: Request, result: SlotResult, needle?: string) {
    if (
      !this.live(record) ||
      record.pid === undefined ||
      generationIdentity(record.pid, request) !==
        generationIdentity(record.pid, record.bootstrap) ||
      request.token.authorityId !== request.authorityId ||
      request.token.slotId !== request.slotId ||
      request.token.slotGeneration !== request.slotGeneration
    )
      return;
    if (
      request.operation === "open" &&
      typeof request.input === "object" &&
      request.input !== null &&
      "cwd" in request.input &&
      "variant" in request.input
    ) {
      this.owners.set(JSON.stringify([request.input.cwd, request.input.variant]), {
        record,
        token: Object.freeze({ ...request.token }),
      });
    }
    if (
      record.grep ||
      request.operation !== "searchContents" ||
      !needle ||
      typeof request.input !== "object" ||
      request.input === null ||
      !("query" in request.input) ||
      request.input.query !== "needle"
    )
      return;
    const match = result.items.find(
      (item): item is { path: string; lineContent: string } =>
        typeof item === "object" &&
        item !== null &&
        "path" in item &&
        "lineContent" in item &&
        typeof item.path === "string" &&
        /^file-[0-9]+\.txt$/.test(item.path) &&
        typeof item.lineContent === "string" &&
        item.lineContent.includes(needle),
    );
    if (!match) return;
    this.completedGreps.set(record, {
      pid: record.pid,
      bootstrap: record.bootstrap,
      requestId: request.requestId,
      token: Object.freeze({ ...request.token }),
      completedAt: this.ports.now(),
      query: "needle",
      match: { path: match.path, lineContent: match.lineContent },
    });
  }

  // Caller invokes this only after supervisor.run returned its useful positive
  // result. A late child reply rejected by the supervisor cannot supply proof.
  confirmPositive(handle: LogicalFinderHandle) {
    const owner = this.owners.get(keyIdentity(handle));
    if (owner?.record.grep && this.live(owner.record)) return;
    const grep = owner && this.completedGreps.get(owner.record);
    if (
      !owner ||
      !grep ||
      !this.live(owner.record) ||
      grep.token.keyId !== owner.token.keyId ||
      grep.token.incarnation !== owner.token.incarnation ||
      generationIdentity(grep.pid, grep.bootstrap) !==
        generationIdentity(owner.record.pid!, owner.record.bootstrap)
    )
      throw new Error("Positive grep result is empty, stale or unbound");
    owner.record.grep ??= grep;
  }

  async qualify(handle: LogicalFinderHandle, phase: string, grep: () => Promise<void>) {
    if (!this.accepting) throw new Error("Generation evidence admission stopped");
    const owner = this.owners.get(keyIdentity(handle));
    if (!owner || !this.live(owner.record)) throw new Error("Generation owner unavailable");
    const record = owner.record;
    if (!record.grep && !record.basisGrepAttempted) {
      record.basisGrepAttempted = true;
      try {
        await grep(); // Existing charged finder, through supervisor.run, at most once.
      } catch (cause) {
        record.basisGrepFailure = "request-failed";
        throw cause;
      }
    }
    if (!record.grep || !this.live(record) || this.owners.get(keyIdentity(handle)) !== owner) {
      record.basisGrepFailure ??= "empty-or-unbound";
      throw new Error("Generation lacks completed positive grep");
    }
    await this.checkpoint(record, phase);
  }

  checkpoint(record: EvidenceGeneration, phase: string): Promise<void> {
    if (!this.accepting || !record.grep) return Promise.resolve();
    let phases = this.attempted.get(record);
    if (!phases) this.attempted.set(record, (phases = new Map()));
    const prior = phases.get(phase);
    if (prior) return prior;
    const work = this.read(record, phase);
    phases.set(phase, work);
    this.pending.add(work);
    void work.then(
      () => this.pending.delete(work),
      () => this.pending.delete(work),
    );
    return work;
  }

  private async read(record: EvidenceGeneration, phase: string) {
    const pid = record.pid!,
      bootstrap = record.bootstrap,
      grep = record.grep!;
    const at = this.ports.now();
    const before = this.live(record);
    let observation: NativePoolObservation;
    try {
      observation = await observeUntilAbort(() => this.ports.pools(pid), this.ports.signal);
    } catch {
      observation = {
        pid,
        method: this.ports.method,
        status: "unavailable",
        threads: [],
        raw: "",
        error: this.ports.signal?.aborted
          ? "native-pool-observation-aborted"
          : "native-pool-observer-threw",
      };
    }
    if (
      !before ||
      !this.live(record) ||
      observation.pid !== pid ||
      generationIdentity(grep.pid, grep.bootstrap) !== generationIdentity(pid, bootstrap)
    )
      observation = {
        ...observation,
        status: "unavailable",
        error: "generation-changed-during-observation",
      };
    record.nativePools.push(observation); // Failed reads survive later successful phases.
    record.poolReads.push({
      at,
      finishedAt: this.ports.now(),
      phase,
      pid,
      bootstrap,
      grep,
      observation,
    });
    // Actual full row for short-lived generations; cost remains measured.
    await observeUntilAbort(this.ports.resources, this.ports.signal);
  }

  async stopAndDrain() {
    this.accepting = false;
    await Promise.all(this.pending);
  }
}
