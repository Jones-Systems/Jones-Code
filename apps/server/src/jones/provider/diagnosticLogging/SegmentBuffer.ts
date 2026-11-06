import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";

export interface PendingRecord {
  readonly stream: "native" | "canonical" | "orchestration";
  readonly threadSegment: string;
  readonly line: string;
  readonly bytes: number;
}

export interface LogSegment {
  readonly filePath: string;
  pending: Array<PendingRecord>;
  accepted: number;
  completed: number;
  requested: number;
  started: boolean;
  inFlight: boolean;
  retired: boolean;
  changed: Deferred.Deferred<void>;
}

export interface LogBatch {
  readonly segment: LogSegment;
  readonly records: ReadonlyArray<PendingRecord>;
  readonly bytes: number;
  readonly watermark: number;
  released: boolean;
}

type Admission =
  | { readonly kind: "closed" }
  | {
      readonly kind: "wait";
      readonly changed: Deferred.Deferred<void>;
      readonly workers: ReadonlyArray<LogSegment>;
    }
  | {
      readonly kind: "accepted";
      readonly segment: LogSegment;
      readonly watermark: number;
      readonly flush: boolean;
      readonly schedule: boolean;
      readonly workers: ReadonlyArray<LogSegment>;
    };

type DrainAction =
  | { readonly kind: "done" }
  | { readonly kind: "wait"; readonly changed: Deferred.Deferred<void> }
  | { readonly kind: "batch"; readonly batch: LogBatch };

function notify(changes: ReadonlyArray<Deferred.Deferred<void>>): void {
  // Completion resumes waiters inline; publish every state/token change before notifying.
  for (const changed of changes) Deferred.doneUnsafe(changed, Effect.void);
}

interface SegmentBufferOptions {
  readonly maxBufferedRecords: number;
  readonly maxBufferedBytes: number;
  readonly batchWindowMs: number;
}

/** All methods are synchronous; callers wrap transitions in Effect.sync, never disk I/O. */
export class SegmentBuffer {
  private readonly options: SegmentBufferOptions;
  private readonly segments = new Map<string, LogSegment>();
  private readonly removals = new Map<string, Deferred.Deferred<void>>();
  private capacityChanged = Deferred.makeUnsafe<void>();
  private records = 0;
  private bytes = 0;
  private closed = false;
  private flushScheduled = false;

  constructor(options: SegmentBufferOptions) {
    this.options = options;
  }

  get usage(): { readonly records: number; readonly bytes: number } {
    return { records: this.records, bytes: this.bytes };
  }

  get isClosed(): boolean {
    return this.closed;
  }

  private request(segments: Iterable<LogSegment>): ReadonlyArray<LogSegment> {
    const workers: Array<LogSegment> = [];
    const changes: Array<Deferred.Deferred<void>> = [];
    for (const segment of segments) {
      segment.requested = segment.accepted;
      changes.push(segment.changed);
      segment.changed = Deferred.makeUnsafe<void>();
      if (!segment.started) {
        segment.started = true;
        workers.push(segment);
      }
    }
    notify(changes);
    return workers;
  }

  private requestAll(): ReadonlyArray<LogSegment> {
    return this.request(this.segments.values());
  }

  admit(record: PendingRecord, filePath: string): Admission {
    if (this.closed) return { kind: "closed" };
    // An encoded, payload-bounded record larger than the byte cap runs alone.
    // Its entire size stays charged until completion; it never overlaps another record.
    const fitsBytes = this.bytes + record.bytes <= this.options.maxBufferedBytes;
    if (this.records >= this.options.maxBufferedRecords || (!fitsBytes && this.records > 0)) {
      return { kind: "wait", changed: this.capacityChanged, workers: this.requestAll() };
    }

    let segment = this.segments.get(filePath);
    if (!segment) {
      segment = {
        filePath,
        pending: [],
        accepted: 0,
        completed: 0,
        requested: 0,
        started: false,
        inFlight: false,
        retired: false,
        changed: Deferred.makeUnsafe<void>(),
      };
      this.segments.set(filePath, segment);
    }
    segment.pending.push(record);
    segment.accepted += 1;
    this.records += 1;
    this.bytes += record.bytes;
    const flush =
      this.options.batchWindowMs === 0 ||
      this.records >= this.options.maxBufferedRecords ||
      this.bytes >= this.options.maxBufferedBytes;
    const schedule = !flush && !this.flushScheduled;
    if (schedule) this.flushScheduled = true;
    return {
      kind: "accepted",
      segment,
      watermark: segment.accepted,
      flush,
      schedule,
      workers: flush ? this.request([segment]) : [],
    };
  }

  timerFired(): ReadonlyArray<LogSegment> {
    this.flushScheduled = false;
    return this.closed ? [] : this.requestAll();
  }

  next(segment: LogSegment): DrainAction {
    if (segment.retired) return { kind: "done" };
    if (segment.requested > segment.completed) {
      const removal = this.removals.get(segment.filePath);
      if (removal) return { kind: "wait", changed: removal };
      const records = segment.pending;
      segment.pending = [];
      segment.inFlight = true;
      return {
        kind: "batch",
        batch: {
          segment,
          records,
          bytes: records.reduce((sum, record) => sum + record.bytes, 0),
          watermark: segment.accepted,
          released: false,
        },
      };
    }
    if (this.closed && segment.completed === segment.accepted) return { kind: "done" };
    return { kind: "wait", changed: segment.changed };
  }

  complete(batch: LogBatch): void {
    if (batch.released) return;
    batch.released = true;
    this.records -= batch.records.length;
    this.bytes -= batch.bytes;
    batch.segment.inFlight = false;
    batch.segment.completed = batch.watermark;
    const changes = [batch.segment.changed, this.capacityChanged];
    batch.segment.changed = Deferred.makeUnsafe<void>();
    this.capacityChanged = Deferred.makeUnsafe<void>();
    notify(changes);
  }

  awaitWatermark(segment: LogSegment, watermark: number): Deferred.Deferred<void> | undefined {
    return segment.completed < watermark ? segment.changed : undefined;
  }

  closeAdmission(): {
    readonly workers: ReadonlyArray<LogSegment>;
    readonly watermarks: ReadonlyArray<{
      readonly segment: LogSegment;
      readonly watermark: number;
    }>;
  } {
    this.closed = true;
    this.flushScheduled = false;
    const capacityChanged = this.capacityChanged;
    this.capacityChanged = Deferred.makeUnsafe<void>();
    const watermarks = Array.from(this.segments.values(), (segment) => ({
      segment,
      watermark: segment.accepted,
    }));
    const workers = this.requestAll();
    notify([capacityChanged]);
    return { workers, watermarks };
  }

  retireIdle(): ReadonlyArray<LogSegment> {
    const retired: Array<LogSegment> = [];
    const changes: Array<Deferred.Deferred<void>> = [];
    for (const segment of this.segments.values()) {
      if (segment.inFlight || segment.completed !== segment.accepted) continue;
      this.segments.delete(segment.filePath);
      segment.retired = true;
      changes.push(segment.changed);
      segment.changed = Deferred.makeUnsafe<void>();
      retired.push(segment);
    }
    notify(changes);
    return retired;
  }

  reserveRemoval(filePath: string): (() => void) | undefined {
    const currentPath = /^(.*\.log)(?:\.\d+)?$/u.exec(filePath)?.[1] ?? filePath;
    if (this.segments.has(currentPath) || this.removals.has(currentPath)) return undefined;
    const claim = Deferred.makeUnsafe<void>();
    this.removals.set(currentPath, claim);
    return () => {
      if (this.removals.get(currentPath) !== claim) return;
      this.removals.delete(currentPath);
      Deferred.doneUnsafe(claim, Effect.void);
    };
  }
}
